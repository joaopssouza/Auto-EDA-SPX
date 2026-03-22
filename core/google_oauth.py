"""
Google OAuth2 — Autenticação sem Senha
=======================================

Módulo centralizado para autenticação OAuth2 com Google.
Substitui o armazenamento de email/senha por um refresh_token
obtido via consentimento do usuário (uma única vez).

Fluxo:
    1. setup_oauth() → abre browser → usuário consente → salva refresh_token
    2. get_credentials() → carrega refresh_token → gera access_token automaticamente
    3. get_google_session_cookies() → após login no browser, extrai cookies Google
    4. inject_google_cookies() → injeta cookies no Chrome via Selenium CDP (para CI)

Pré-requisitos:
    - Criar credenciais OAuth 2.0 (Desktop App) no Google Cloud Console
    - Baixar o JSON como `oauth_client_secret.json` na raiz do projeto
"""

import json
import os
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from rich.console import Console

console = Console()

# Caminhos
BASE_DIR = Path(__file__).parent.parent

from core.config import (
    GOOGLE_OAUTH_CLIENT_SECRET_JSON,
    OAUTH_CLIENT_SECRET_FILE,
    OAUTH_TOKEN_FILE,
    GOOGLE_SESSION_COOKIES_FILE,
)

# Escopos necessários para OAuth2
# openid + email + profile = permite identificar o usuário
OAUTH_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

# Cookies de sessão do Google que devemos capturar
GOOGLE_SESSION_COOKIE_NAMES = {
    "SID", "HSID", "SSID", "APISID", "SAPISID",
    "NID", "1P_JAR", "__Secure-1PSID", "__Secure-3PSID",
    "__Secure-1PAPISID", "__Secure-3PAPISID",
    "__Secure-1PSIDTS", "__Secure-3PSIDTS",
    "__Secure-1PSIDCC", "__Secure-3PSIDCC",
    "SIDCC",
}


def setup_oauth() -> Credentials:
    """
    Fluxo de consentimento OAuth2 — executa uma única vez.

    Abre o browser para o usuário autorizar a aplicação.
    Salva o refresh_token no arquivo `oauth_token.json`.

    Returns:
        Credentials autenticadas do Google
    """
    client_secret_json = GOOGLE_OAUTH_CLIENT_SECRET_JSON.strip()

    if not client_secret_json and not os.path.exists(OAUTH_CLIENT_SECRET_FILE):
        console.print("[red]❌ Arquivo 'oauth_client_secret.json' não encontrado![/red]")
        console.print("\n[yellow]Para configurar o OAuth2:[/yellow]")
        console.print("[white]1. Acesse https://console.cloud.google.com/apis/credentials[/white]")
        console.print("[white]2. Crie credenciais → OAuth 2.0 Client ID → tipo Desktop App[/white]")
        console.print("[white]3. Salve o JSON como secret GOOGLE_OAUTH_CLIENT_SECRET ou como 'oauth_client_secret.json' na raiz do projeto[/white]")
        raise FileNotFoundError(
            f"Arquivo de credenciais OAuth2 não encontrado: {OAUTH_CLIENT_SECRET_FILE}"
        )

    console.print("[bold cyan]═══ Configuração OAuth2 Google ═══[/bold cyan]")
    console.print("[dim]Abrindo browser para autorização...[/dim]")
    console.print("[dim]Você só precisa fazer isso UMA vez.[/dim]\n")

    if client_secret_json:
        flow = InstalledAppFlow.from_client_config(
            json.loads(client_secret_json),
            scopes=OAUTH_SCOPES,
        )
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            OAUTH_CLIENT_SECRET_FILE,
            scopes=OAUTH_SCOPES,
        )

    # Abre browser para consentimento
    credentials = flow.run_local_server(
        port=8090,
        prompt="consent",
        access_type="offline",  # Garante que receberemos um refresh_token
    )

    # Salva o token (contém refresh_token)
    _save_token(credentials)

    console.print("\n[bold green]✅ OAuth2 configurado com sucesso![/bold green]")
    console.print(f"[green]📁 Token salvo em: {OAUTH_TOKEN_FILE}[/green]")
    console.print("[dim]O refresh_token nunca expira (a menos que você revogue).[/dim]")

    return credentials


def get_credentials() -> Optional[Credentials]:
    """
    Carrega credenciais OAuth2 salvas e renova automaticamente se expiradas.

    Tenta carregar de:
        1. Arquivo local `oauth_token.json`
        2. Variável de ambiente `GOOGLE_OAUTH_TOKEN` (compatibilidade)
        3. CONFIG_CLOUD (Google Sheets)

    Returns:
        Credentials autenticadas ou None se não configurado
    """
    credentials = None

    # 1. Tenta carregar do arquivo local
    if os.path.exists(OAUTH_TOKEN_FILE):
        try:
            credentials = Credentials.from_authorized_user_file(
                OAUTH_TOKEN_FILE, OAUTH_SCOPES
            )
        except Exception as e:
            console.print(f"[yellow]⚠️ Erro ao carregar token local: {e}[/yellow]")

    # 2. Tenta carregar da variável de ambiente (CI/GitHub Actions)
    if not credentials:
        token_json = os.getenv("GOOGLE_OAUTH_TOKEN")
        if token_json:
            try:
                token_data = json.loads(token_json)
                credentials = Credentials.from_authorized_user_info(
                    token_data, OAUTH_SCOPES
                )
                console.print("[green]✅ Credenciais OAuth2 carregadas do ambiente (CI).[/green]")
            except Exception as e:
                console.print(f"[red]❌ Erro ao carregar token do ambiente: {e}[/red]")

    # 3. Tenta carregar da CONFIG_CLOUD (fonte oficial no CI)
    if not credentials:
        try:
            from core.config import SPREADSHEET_ID
            from core.sheets import get_cloud_config

            if SPREADSHEET_ID:
                cloud_config = get_cloud_config(SPREADSHEET_ID)
                token_json = cloud_config.get("GOOGLE_OAUTH_TOKEN", "")
                if token_json:
                    token_data = json.loads(token_json)
                    credentials = Credentials.from_authorized_user_info(
                        token_data, OAUTH_SCOPES
                    )
                    console.print("[green]✅ Credenciais OAuth2 carregadas da CONFIG_CLOUD.[/green]")
        except Exception as e:
            console.print(f"[yellow]⚠️ Erro ao carregar OAuth2 da CONFIG_CLOUD: {e}[/yellow]")

    if not credentials:
        return None

    # Auto-renova se expirado
    if credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(Request())
            _save_token(credentials)
            console.print("[green]✅ Token OAuth2 renovado automaticamente.[/green]")
        except Exception as e:
            console.print(f"[red]❌ Erro ao renovar token OAuth2: {e}[/red]")
            return None

    return credentials


def _save_token(credentials: Credentials) -> None:
    """Salva credenciais OAuth2 em arquivo JSON."""
    token_data = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": list(credentials.scopes) if credentials.scopes else OAUTH_SCOPES,
    }

    with open(OAUTH_TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(token_data, f, indent=2)


def get_google_session_cookies(driver) -> list[dict]:
    """
    Extrai cookies de sessão do Google de um Selenium WebDriver.

    Após um login bem-sucedido no browser (via OAuth2 ou manual),
    captura os cookies do Google (SID, HSID, etc.) que mantêm a sessão.

    Args:
        driver: WebDriver do Selenium com sessão Google ativa

    Returns:
        Lista de dicionários com os cookies do Google
    """
    all_cookies = driver.get_cookies()

    google_cookies = []
    for cookie in all_cookies:
        domain = cookie.get("domain", "")
        name = cookie.get("name", "")

        # Captura cookies do Google (domínios .google.com, .google.com.br, etc.)
        if ".google.com" in domain or ".google.com.br" in domain:
            google_cookies.append({
                "name": name,
                "value": cookie["value"],
                "domain": domain,
                "path": cookie.get("path", "/"),
                "secure": cookie.get("secure", True),
                "httpOnly": cookie.get("httpOnly", False),
                "sameSite": cookie.get("sameSite", "None"),
            })

    if google_cookies:
        console.print(f"[green]✅ {len(google_cookies)} cookies Google capturados.[/green]")
        _save_google_cookies(google_cookies)
    else:
        console.print("[yellow]⚠️ Nenhum cookie Google encontrado no browser.[/yellow]")

    return google_cookies


def _save_google_cookies(cookies: list[dict]) -> None:
    """Salva cookies Google em arquivo JSON."""
    with open(GOOGLE_SESSION_COOKIES_FILE, "w", encoding="utf-8") as f:
        json.dump(cookies, f, indent=2)
    console.print(f"[green]📁 Cookies salvos em: {GOOGLE_SESSION_COOKIES_FILE}[/green]")


def load_google_cookies() -> list[dict]:
    """
    Carrega cookies Google salvos (arquivo local ou variável de ambiente).

    Returns:
        Lista de cookies ou lista vazia
    """
    # 1. Tenta arquivo local
    if os.path.exists(GOOGLE_SESSION_COOKIES_FILE):
        try:
            with open(GOOGLE_SESSION_COOKIES_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            if cookies:
                console.print(f"[blue]🍪 {len(cookies)} cookies Google carregados (local).[/blue]")
                return cookies
        except Exception:
            pass

    # 2. Tenta variável de ambiente (compatibilidade CI)
    cookies_json = os.getenv("GOOGLE_SESSION_COOKIES")
    if cookies_json:
        try:
            cookies = json.loads(cookies_json)
            console.print(f"[blue]🍪 {len(cookies)} cookies Google carregados (CI).[/blue]")
            return cookies
        except Exception:
            pass

    # 3. Tenta CONFIG_CLOUD (fonte oficial no CI)
    try:
        from core.config import SPREADSHEET_ID
        from core.sheets import get_cloud_config

        if SPREADSHEET_ID:
            cloud_config = get_cloud_config(SPREADSHEET_ID)
            cookies_json = cloud_config.get("GOOGLE_SESSION_COOKIES", "")
            if cookies_json:
                cookies = json.loads(cookies_json)
                console.print(f"[blue]🍪 {len(cookies)} cookies Google carregados (CONFIG_CLOUD).[/blue]")
                return cookies
    except Exception as e:
        console.print(f"[yellow]⚠️ Erro ao carregar cookies Google da CONFIG_CLOUD: {e}[/yellow]")

    return []


def inject_google_cookies(driver, cookies: Optional[list[dict]] = None) -> bool:
    """
    Injeta cookies do Google no Chrome via Selenium CDP.

    Isso permite que, ao navegar para o SPX, o Google reconheça
    a sessão como válida e faça auto-redirect sem pedir login.

    Args:
        driver: WebDriver do Selenium
        cookies: Lista de cookies (se None, carrega do arquivo)

    Returns:
        True se cookies foram injetados, False caso contrário
    """
    if cookies is None:
        cookies = load_google_cookies()

    if not cookies:
        console.print("[yellow]⚠️ Nenhum cookie Google disponível para injetar.[/yellow]")
        return False

    try:
        # Navega para o domínio do Google para poder setar cookies
        driver.get("https://accounts.google.com/")
        import time
        time.sleep(1)

        injected = 0
        for cookie in cookies:
            try:
                # Usa CDP para injetar cookies (mais robusto que driver.add_cookie)
                cookie_params = {
                    "name": cookie["name"],
                    "value": cookie["value"],
                    "domain": cookie.get("domain", ".google.com"),
                    "path": cookie.get("path", "/"),
                    "secure": cookie.get("secure", True),
                    "httpOnly": cookie.get("httpOnly", False),
                }

                # Tenta via CDP primeiro (mais confiável)
                try:
                    driver.execute_cdp_cmd("Network.setCookie", cookie_params)
                    injected += 1
                except Exception:
                    # Fallback: método padrão do Selenium
                    selenium_cookie = {
                        "name": cookie["name"],
                        "value": cookie["value"],
                        "domain": cookie.get("domain", ".google.com"),
                        "path": cookie.get("path", "/"),
                    }
                    driver.add_cookie(selenium_cookie)
                    injected += 1
            except Exception:
                continue

        console.print(f"[green]✅ {injected}/{len(cookies)} cookies Google injetados.[/green]")
        return injected > 0

    except Exception as e:
        console.print(f"[red]❌ Erro ao injetar cookies: {e}[/red]")
        return False
