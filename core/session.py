"""
Gerenciador de Sessão HTTP
==========================

Mantém uma sessão HTTP persistente com cookies e headers autenticados.
Faz refresh automático da sessão quando necessário.
"""

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

import httpx
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from core.config import SPX_BASE_URL
from core.auth import (
    is_session_material_valid,
    load_runtime_session_cache,
    save_runtime_session_cache,
)

console = Console()

BASE_DIR = Path(__file__).parent.parent
COOKIES_FILE = BASE_DIR / "cookies.json"
BASE_URL = SPX_BASE_URL


class SessionExpiredError(Exception):
    """Erro lançado quando a sessão expirou e não pode ser renovada automaticamente."""
    pass


class SPXSession:
    """Gerenciador de sessão para APIs do SPX Shopee."""
    
    def __init__(self):
        self.client: Optional[httpx.Client] = None
        self.session_data: Optional[dict] = None
        self._auto_login_attempted: bool = False  # Evita loop infinito de auto-login
        self._session_lock = threading.RLock()
        # Controles para evitar múltiplas renovações em curto espaço de tempo
        self._refresh_attempts: int = 0
        self._last_refresh_time: float = 0.0
        # Cooldown em segundos entre tentativas automáticas de refresh
        self._refresh_cooldown: int = int(os.getenv("SPX_REFRESH_COOLDOWN", "300"))
        self._load_session()
    
    def _load_session(self) -> None:
        """Carrega sessão priorizando cache efêmero do job; nuvem e arquivo local são fallback."""
        with self._session_lock:
            self.session_data = None

            # 1) Cache efêmero do job atual (GitHub Actions)
            runtime_session = load_runtime_session_cache()
            if runtime_session:
                self.session_data = runtime_session
                console.print("[green]✅ Sessão carregada do cache efêmero do CI.[/green]")

            # 2) Fonte principal sem cache: nuvem (CONFIG_CLOUD)
            if not self.session_data:
                try:
                    from core.config import SPREADSHEET_ID
                    from core.sheets import get_cloud_config
                    
                    if SPREADSHEET_ID:
                        cloud_config = get_cloud_config(SPREADSHEET_ID)
                        if cloud_config:
                            session_from_cloud = {}
                            if "cookies_json" in cloud_config:
                                cloud_session = json.loads(cloud_config["cookies_json"])
                                session_from_cloud.update(cloud_session)

                            session_from_cloud.update(cloud_config)
                            self.session_data = session_from_cloud
                            save_runtime_session_cache(self.session_data)
                            console.print("[green]✅ Sessão carregada da CONFIG_CLOUD.[/green]")
                except Exception as e:
                    console.print(f"[red]❌ Erro ao sincronizar com a nuvem: {e}[/red]")

            # 3) Fallback: local
            if not self.session_data and COOKIES_FILE.exists():
                try:
                    with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                        self.session_data = json.load(f)
                    console.print("[blue]ℹ️ Sessão carregada do arquivo local (fallback).[/blue]")
                except Exception:
                    self.session_data = None

            if self.session_data:
                self._create_client()
            else:
                console.print("[red]❌ Falha total ao carregar sessão. Use 'python main.py --auth'.[/red]")

    def _create_client(self) -> None:
        """Cria cliente HTTP com cookies e headers."""
        with self._session_lock:
            if not self.session_data:
                raise ValueError("Sessão não carregada.")

            cookies = httpx.Cookies()
            for name, value in self.session_data.get("cookies", {}).items():
                cookies.set(name, value, domain=".shopee.com.br")

            headers = self.session_data.get("headers", {})

            # Adiciona apenas headers globais para evitar sobrescrever x-sap
            # com variantes inbound/outbound. As variantes específicas são
            # enviadas pelos módulos via extra_headers quando necessário.
            sec_keys = {
                "x-sap-ri": "x-sap-ri",
                "x-sap-sec": "x-sap-sec",
                "device-id": "device-id",
                "app": "app",
            }

            for cloud_key, header_key in sec_keys.items():
                if cloud_key in self.session_data:
                    headers[header_key] = self.session_data[cloud_key]

            self.client = httpx.Client(
                base_url=BASE_URL,
                cookies=cookies,
                headers=headers,
                timeout=60.0,
                follow_redirects=True,
            )

            console.print(f"[green]✅ Sessão HTTP criada com {len(cookies)} cookies[/green]")

    def _has_required_material(self) -> bool:
        """Valida se a sessão tem artefatos mínimos para evitar 401 no CI."""
        if not self.session_data:
            return False

        cookies = self.session_data.get("cookies", {})
        if not isinstance(cookies, dict) or not cookies:
            return False

        # spx_cid é vital para chamadas internas da API no CI.
        if not cookies.get("spx_cid"):
            return False

        # x-sap global é usado por exception/workstation e fallback dos demais.
        has_ri = bool(self.session_data.get("x-sap-ri"))
        has_sec = bool(self.session_data.get("x-sap-sec"))
        return has_ri and has_sec

    def refresh(self) -> None:
        """Renova a sessão fazendo novo login (auto-login Google)."""
        from core.auth import refresh_session

        with self._session_lock:
            console.print("[yellow]🔄 Renovando sessão via auto-login Google...[/yellow]")
            old_session = self.session_data.copy() if self.session_data else {}
            self.session_data = refresh_session()
            if self.session_data:
                # Preserva x-sap headers se não foram capturados na renovação
                sap_keys = [
                    "x-sap-ri", "x-sap-sec",
                    "x-sap-ri-inbound", "x-sap-sec-inbound",
                    "x-sap-ri-outbound", "x-sap-sec-outbound",
                ]
                for key in sap_keys:
                    if not self.session_data.get(key) and old_session.get(key):
                        self.session_data[key] = old_session[key]
                        console.print(f"[dim]  ♻️ Preservado {key} da sessão anterior[/dim]")
                self._create_client()
                # Reset counters após renovação bem-sucedida
                self._refresh_attempts = 0
                # Marca tempo da renovação bem-sucedida para aplicar cooldown
                self._last_refresh_time = time.time()
                # Permite futuras renovações após o cooldown, mas evita loops imediatos
                self._auto_login_attempted = False

    def _has_required_material(self) -> bool:
        """Valida artefatos mínimos para evitar 401 em cascata."""
        valid, _ = is_session_material_valid(self.session_data)
        return valid
    
    def _handle_expired_session(self, url: str) -> None:
        """
        Trata sessão expirada.
        
        Fluxo:
            1. Tenta auto-login Google (uma única vez)
            2. Se falhar no CI → levanta SessionExpiredError
            3. Se falhar localmente → tenta refresh manual
        """
        is_github = os.getenv("GITHUB_ACTIONS") == "true"

        now = time.time()

        with self._session_lock:
            # Evita múltiplas renovações em curto espaço de tempo usando o timestamp
            if (now - self._last_refresh_time) < self._refresh_cooldown:
                console.print(f"[yellow]⚠️ Tentativa de refresh recente (há {now - self._last_refresh_time:.0f}s); pulando nova tentativa automática por {self._refresh_cooldown}s de cooldown.[/yellow]")
                if is_github:
                    raise SessionExpiredError(
                        "Sessão SPX expirada e auto-login recente falhou."
                    )
                return

            # Marca tentativa e tenta auto-login
            self._auto_login_attempted = True
            self._refresh_attempts += 1
            self._last_refresh_time = now
            console.print("[yellow]⚠️ Sessão expirada. Tentando auto-login Google...[/yellow]")

            try:
                self.refresh()
                if self._has_required_material():
                    console.print("[green]✅ Sessão renovada com sucesso via auto-login![/green]")
                    return
                console.print("[yellow]⚠️ Renovação retornou sessão incompleta (faltando spx_cid e/ou x-sap).[/yellow]")
            except Exception as e:
                console.print(f"[red]❌ Auto-login falhou: {e}[/red]")

        # Se chegou aqui, auto-login falhou
        if is_github:
            raise SessionExpiredError(
                "Sessão SPX expirada e auto-login falhou. "
                "Execute 'python main.py --setup-oauth' e 'python main.py --auth' localmente "
                "para sincronizar novamente a CONFIG_CLOUD."
            )
        else:
            console.print("[yellow]⚠️ Auto-login falhou e não será re-tentado automaticamente agora. Execute 'python main.py --auth' localmente para re-autenticar.[/yellow]")
            return
    
    def _check_retcode(self, data: dict, url: str) -> None:
        """Verifica retcode da API. Se 401, trata como sessão expirada."""
        if isinstance(data, dict):
            retcode = data.get("retcode", 0)
            message = data.get("message", "")
            if retcode == 401 or "expired" in message.lower():
                console.print(f"[red]❌ API retornou sessão expirada: {message}[/red]")
                self._handle_expired_session(url)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError))
    )
    def get(self, url: str, params: Optional[dict] = None, extra_headers: Optional[dict] = None) -> dict:
        """
        Faz requisição GET autenticada.
        
        Args:
            url: URL relativa ou absoluta
            params: Parâmetros de query string
            extra_headers: Headers adicionais para a requisição
        
        Returns:
            Resposta JSON
        """
        if not self.client:
            self._load_session()
        
        headers = {}
        if extra_headers:
            headers.update(extra_headers)
            
        response = self.client.get(url, params=params, headers=headers)
        
        # Verifica se sessão expirou via HTTP status
        if response.status_code in (401, 403):
            self._handle_expired_session(url)
            response = self.client.get(url, params=params, headers=headers)
            
            # Se após renovação ainda retorna 401/403, sessão é realmente inválida
            if response.status_code in (401, 403):
                raise SessionExpiredError(
                    f"Sessão inválida para {url} mesmo após renovação "
                    f"(HTTP {response.status_code})."
                )
        
        if response.status_code >= 400:
            console.print(f"[red]Erro HTTP {response.status_code} em {url}: {response.text[:500]}[/red]")
        
        response.raise_for_status()
        data = response.json()
        
        # Verifica se a API retornou retcode 401 no body
        self._check_retcode(data, url)
        
        return data
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError))
    )
    def post(self, url: str, json_data: Optional[dict] = None, extra_headers: Optional[dict] = None) -> dict:
        """
        Faz requisição POST autenticada.
        
        Args:
            url: URL relativa ou absoluta
            json_data: Dados JSON para enviar no body
            extra_headers: Headers adicionais
        
        Returns:
            Resposta JSON
        """
        if not self.client:
            self._load_session()
        
        headers = {}
        if extra_headers:
            headers.update(extra_headers)
        
        response = self.client.post(url, json=json_data, headers=headers)
        
        # Verifica se sessão expirou via HTTP status
        if response.status_code in (401, 403):
            self._handle_expired_session(url)
            response = self.client.post(url, json=json_data, headers=headers)
            
            # Se após renovação ainda retorna 401/403, sessão é realmente inválida
            if response.status_code in (401, 403):
                raise SessionExpiredError(
                    f"Sessão inválida para {url} mesmo após renovação "
                    f"(HTTP {response.status_code})."
                )
        
        if response.status_code >= 400:
            console.print(f"[red]Erro HTTP {response.status_code} em {url}: {response.text[:500]}[/red]")
        
        response.raise_for_status()
        data = response.json()
        
        # Verifica se a API retornou retcode 401 no body
        self._check_retcode(data, url)
        
        return data
    
    def close(self) -> None:
        """Fecha a sessão HTTP."""
        if self.client:
            self.client.close()
            self.client = None


# Singleton global
_session: Optional[SPXSession] = None


def get_session() -> SPXSession:
    """Retorna instância única da sessão."""
    global _session
    if _session is None:
        _session = SPXSession()
    return _session


if __name__ == "__main__":
    # Teste do módulo
    session = get_session()
    console.print("[green]Sessão pronta para uso![/green]")
