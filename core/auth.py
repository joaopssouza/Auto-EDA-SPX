"""
Módulo de Autenticação — OAuth2 + Cookie Injection
====================================================

Autentica no SPX Shopee usando Google OAuth2 (sem armazenar senha).

Fluxo:
    1. Carrega cookies de sessão Google (capturados via OAuth2)
    2. Injeta cookies no Chrome via CDP
    3. Navega ao SPX → Google reconhece sessão → auto-redirect
    4. Extrai cookies SPX para uso nas APIs

Fallback:
    Se não houver cookies Google, tenta OAuth2 flow interativo.
    Se nenhum método funcionar, pede login manual (local).
"""

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from rich.console import Console
from dotenv import load_dotenv
from core.config import SPX_BASE_URL

console = Console()

# Caminhos
BASE_DIR = Path(__file__).parent.parent
COOKIES_FILE = BASE_DIR / "cookies.json"
AUTOMATION_PROFILE = BASE_DIR / "chrome_profile"


def _is_github_actions() -> bool:
    """Indica se o processo está rodando dentro do GitHub Actions."""
    return os.getenv("GITHUB_ACTIONS") == "true"


def get_runtime_session_cache_file() -> Optional[Path]:
    """Retorna o arquivo efêmero de cache de sessão usado apenas durante o job do CI."""
    if not _is_github_actions():
        return None

    runtime_root = os.getenv("RUNNER_TEMP")
    if runtime_root:
        return Path(runtime_root) / "spx_session_cache.json"

    return BASE_DIR / ".runtime" / "spx_session_cache.json"


def load_runtime_session_cache() -> Optional[dict]:
    """Lê a sessão efêmera do job atual, sem consultar a nuvem."""
    cache_file = get_runtime_session_cache_file()
    if not cache_file or not cache_file.exists():
        return None

    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_runtime_session_cache(session_data: dict) -> None:
    """Persiste a sessão em cache efêmero local para reuso entre módulos do mesmo job."""
    cache_file = get_runtime_session_cache_file()
    if not cache_file:
        return

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(session_data, f, indent=2, ensure_ascii=False)

# Configuração
load_dotenv(BASE_DIR / ".env")


def is_session_material_valid(session_data: Optional[dict]) -> tuple[bool, list[str]]:
    """Valida artefatos mínimos de sessão para execução segura no CI."""
    reasons = []

    if not isinstance(session_data, dict):
        return False, ["invalid_payload"]

    cookies = session_data.get("cookies", {})
    if not isinstance(cookies, dict) or not cookies:
        reasons.append("missing_cookies")
    elif not cookies.get("spx_cid"):
        reasons.append("missing_spx_cid")

    sap_ri = session_data.get("x-sap-ri") or session_data.get("headers", {}).get("x-sap-ri", "")
    sap_sec = session_data.get("x-sap-sec") or session_data.get("headers", {}).get("x-sap-sec", "")

    if not sap_ri:
        reasons.append("missing_x_sap_ri")
    if not sap_sec:
        reasons.append("missing_x_sap_sec")

    return len(reasons) == 0, reasons


def check_chrome_running() -> bool:
    """Verifica se o Chrome está rodando."""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq chrome.exe"],
            capture_output=True,
            text=True
        )
        return "chrome.exe" in result.stdout.lower()
    except Exception:
        return False


def create_driver(headless: bool = False) -> webdriver.Chrome:
    """Cria driver Selenium com perfil dedicado para automação."""
    AUTOMATION_PROFILE.mkdir(parents=True, exist_ok=True)

    options = Options()
    options.add_argument(f"--user-data-dir={AUTOMATION_PROFILE}")

    # Opções para evitar detecção
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # Configuração correta de logs de performance
    # Necessário para capturar headers x-sap
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

    options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    return driver


def extract_headers_from_logs(driver: webdriver.Chrome) -> dict:
    """
    Analisa os logs de performance do Chrome para encontrar headers de requisição.
    Busca especificamente por 'x-sap-ri', 'x-sap-sec', 'authorization' e 'pg-i'.
    """
    headers_found = {}
    
    try:
        logs = driver.get_log("performance")
        
        # Itera de trás para frente para pegar os mais recentes
        for entry in reversed(logs):
            try:
                message = json.loads(entry["message"])["message"]
                
                # Procura evento Network.requestWillBeSent
                if message["method"] == "Network.requestWillBeSent":
                    request = message["params"]["request"]
                    request_headers = request.get("headers", {})
                    
                    # Normaliza chaves para lowercase
                    request_headers_lower = {k.lower(): v for k, v in request_headers.items()}
                    
                    if "x-sap-ri" in request_headers_lower and "x-sap-ri" not in headers_found:
                        headers_found["x-sap-ri"] = request_headers_lower["x-sap-ri"]
                    
                    if "x-sap-sec" in request_headers_lower and "x-sap-sec" not in headers_found:
                        headers_found["x-sap-sec"] = request_headers_lower["x-sap-sec"]

                    pg_i_value = str(request_headers_lower.get("pg-i", "")).strip()
                    if pg_i_value and "pg-i" not in headers_found:
                        headers_found["pg-i"] = pg_i_value

                    auth_value = str(request_headers_lower.get("authorization", "")).strip()
                    if auth_value.lower().startswith("bearer ") and "authorization" not in headers_found:
                        headers_found["authorization"] = auth_value
                        
                    if (
                        "x-sap-ri" in headers_found
                        and "x-sap-sec" in headers_found
                        and "authorization" in headers_found
                        and "pg-i" in headers_found
                    ):
                        break
                        
            except (json.JSONDecodeError, KeyError):
                continue
                
    except Exception as e:
        console.print(f"[yellow]⚠️ Erro ao extrair headers dos logs: {e}[/yellow]")
        
    return headers_found


# =========================================================================
# Login via OAuth2 (cookies de sessão Google)
# =========================================================================


def _login_with_oauth_cookies(driver: webdriver.Chrome) -> bool:
    """
    Injeta cookies de sessão Google capturados via OAuth2.

    Se os cookies forem válidos, o Google reconhece a sessão
    e redireciona automaticamente ao SPX sem pedir login.

    Args:
        driver: WebDriver do Selenium

    Returns:
        True se login via cookies foi bem-sucedido
    """
    from core.google_oauth import inject_google_cookies

    console.print("[cyan]🔐 Tentando login via OAuth2 (cookies Google)...[/cyan]")

    # Injeta cookies do Google no browser
    if not inject_google_cookies(driver):
        console.print("[yellow]⚠️ Sem cookies Google para injetar.[/yellow]")
        return False

    # Navega ao SPX após injetar cookies
    console.print("[dim]  → Navegando ao SPX com cookies Google...[/dim]")
    driver.get(SPX_BASE_URL)
    time.sleep(5)

    current_url = driver.current_url

    # Se caiu na página de login do Google, cookies expiraram
    if "accounts.google.com" in current_url:
        console.print("[yellow]⚠️ Cookies Google expirados — login via OAuth2 falhou.[/yellow]")
        return False

    # Se chegou ao SPX, sucesso!
    if SPX_BASE_URL in current_url:
        console.print("[green]✅ Login via OAuth2 cookies bem-sucedido![/green]")
        return True

    # Aguarda um pouco mais para possível redirect
    try:
        WebDriverWait(driver, 15).until(
            lambda d: SPX_BASE_URL in d.current_url
        )
        console.print("[green]✅ Login via OAuth2 cookies bem-sucedido![/green]")
        return True
    except TimeoutException:
        console.print(f"[yellow]⚠️ URL inesperada: {driver.current_url}[/yellow]")
        return False


# =========================================================================
# Login via OAuth2 Flow Interativo (primeira vez ou cookies expirados)
# =========================================================================


def _login_with_oauth_flow(driver: webdriver.Chrome) -> bool:
    """
    Executa o fluxo OAuth2 interativo no browser.

    Navega ao Google OAuth2, o usuário já está logado no Chrome profile,
    e captura os cookies de sessão para uso futuro.

    Args:
        driver: WebDriver do Selenium

    Returns:
        True se login bem-sucedido
    """
    from core.google_oauth import get_credentials, get_google_session_cookies

    credentials = get_credentials()
    if not credentials:
        console.print("[yellow]⚠️ OAuth2 não configurado. Execute: python main.py --setup-oauth[/yellow]")
        return False

    console.print("[cyan]🔐 OAuth2 ativo — navegando ao SPX...[/cyan]")

    # Navega ao SPX
    driver.get(SPX_BASE_URL)
    time.sleep(3)

    current_url = driver.current_url

    # Se já está logado (cookies do profile), captura e sai
    if SPX_BASE_URL in current_url:
        console.print("[green]✅ Já logado no SPX (profile do Chrome)![/green]")
        get_google_session_cookies(driver)
        return True

    # Se está na página de login do Google, aguarda (user pode já ter sessão no profile)
    if "accounts.google.com" in current_url:
        console.print("[dim]  → Aguardando autenticação Google...[/dim]")

        # Tenta aguardar redirect automático (se profile tem sessão Google)
        try:
            WebDriverWait(driver, 10).until(
                lambda d: SPX_BASE_URL in d.current_url
                or "accounts.google.com" not in d.current_url
            )
        except TimeoutException:
            pass

        current_url = driver.current_url

        if SPX_BASE_URL in current_url:
            console.print("[green]✅ Auto-login via profile do Chrome![/green]")
            get_google_session_cookies(driver)
            return True

    return False


# =========================================================================
# Login Manual (último recurso — apenas local)
# =========================================================================


def _login_manual(driver: webdriver.Chrome) -> bool:
    """
    Último recurso: pede para o usuário fazer login manualmente.

    Só funciona em modo visual (não headless).
    """
    is_headless = "--headless" in " ".join(driver.options.arguments)

    if is_headless:
        return False

    console.print("\n[yellow]═══════════════════════════════════════════[/yellow]")
    console.print("[yellow]⚠️  Faça LOGIN manualmente na janela do Chrome[/yellow]")
    console.print("[yellow]═══════════════════════════════════════════[/yellow]")
    console.print("\n[bold white]1. Faça login com sua conta Google[/bold white]")
    console.print("[bold white]2. Complete a verificação 2FA se necessário[/bold white]")
    console.print("[bold white]3. Aguarde a página do SPX carregar[/bold white]")
    console.print("\n[bold cyan]>>> Pressione ENTER quando terminar o login <<<[/bold cyan]")
    input()

    time.sleep(2)

    if SPX_BASE_URL in driver.current_url:
        console.print("[green]✅ Login manual detectado![/green]")

        # Captura cookies Google para uso futuro
        try:
            from core.google_oauth import get_google_session_cookies
            get_google_session_cookies(driver)
        except Exception:
            pass

        return True

    console.print(f"[red]❌ Login não detectado. URL: {driver.current_url}[/red]")
    return False


# =========================================================================
# Fluxo Principal de Extração de Cookies
# =========================================================================


def extract_cookies_from_browser(headless: bool = False) -> dict:
    """
    Abre Chrome, autentica no SPX e extrai cookies.

    Estratégias (em ordem de prioridade):
        1. OAuth2 cookies injection (sem interação)
        2. OAuth2 flow interativo (profile do Chrome)
        3. Login manual (último recurso, apenas local)

    Args:
        headless: Se True, roda sem interface gráfica (CI)

    Returns:
        Dicionário com cookies e headers
    """
    console.print("[cyan]Iniciando Chrome para automação...[/cyan]")
    if not headless:
        console.print("[dim](Este é um perfil separado do seu Chrome pessoal)[/dim]")

    driver = create_driver(headless=headless)

    try:
        login_success = False

        # ==========================================
        # Estratégia 1: OAuth2 Cookies Injection
        # ==========================================
        login_success = _login_with_oauth_cookies(driver)

        # ==========================================
        # Estratégia 2: OAuth2 Flow (profile Chrome)
        # ==========================================
        if not login_success:
            login_success = _login_with_oauth_flow(driver)

        # ==========================================
        # Estratégia 3: Login manual (local only)
        # ==========================================
        if not login_success:
            if headless:
                raise RuntimeError(
                    "Todas as estratégias de auto-login falharam no CI. "
                    "Execute 'python main.py --setup-oauth' e 'python main.py --auth' localmente "
                    "para sincronizar OAuth/cookies na CONFIG_CLOUD."
                )
            login_success = _login_manual(driver)

        if not login_success:
            raise RuntimeError("Nenhuma estratégia de login funcionou.")

        # ==========================================
        # Extrai cookies SPX
        # ==========================================
        if SPX_BASE_URL not in driver.current_url:
            raise RuntimeError(f"Login não detectado. URL: {driver.current_url}")

        console.print("[green]✅ Login detectado no SPX![/green]")
        
        # Tenta forçar geração do cookie crítico spx_cid com poucas tentativas.
        spx_cid_found = False
        for attempt in range(1, 3):
            console.print(f"[cyan]🔄 Tentativa {attempt}/2: gerando cookies vitais na página interna...[/cyan]")
            driver.get(f"{SPX_BASE_URL}/#/dashboard/toProductivity?page_type=Inbound")
            time.sleep(5)

            console.print("[cyan]⏳ Aguardando cookie 'spx_cid' vital para a API...[/cyan]")
            try:
                WebDriverWait(driver, 45).until(
                    lambda d: any(c.get("name") == "spx_cid" for c in d.get_cookies())
                )
                spx_cid_found = True
                console.print("[green]✅ Cookie 'spx_cid' capturado![/green]")
                break
            except TimeoutException:
                console.print(
                    f"[yellow]⚠️ Tentativa {attempt}/2: 'spx_cid' não apareceu após 45s.[/yellow]"
                )

        if headless and not spx_cid_found:
            raise RuntimeError(
                "Cookie vital 'spx_cid' não foi gerado no CI. Sessão inválida para APIs internas. "
                "Rode 'python main.py --auth' localmente para regenerar e sincronizar a CONFIG_CLOUD."
            )

        time.sleep(3)

        cookies = driver.get_cookies()

        # Captura cookies Google para uso futuro (se ainda não foram capturados)
        try:
            from core.google_oauth import get_google_session_cookies
            get_google_session_cookies(driver)
        except Exception:
            pass

        # Procura CSRF token
        csrf_token = None
        for cookie in cookies:
            if cookie["name"] == "csrftoken":
                csrf_token = cookie["value"]
                break

        cookies_dict = {c["name"]: c["value"] for c in cookies}

        # Extrai headers de segurança dos logs de performance
        console.print("[cyan]🔍 Buscando headers 'x-sap' nos logs de rede...[/cyan]")
        extracted_headers = extract_headers_from_logs(driver)
        
        sap_ri = extracted_headers.get("x-sap-ri", "")
        sap_sec = extracted_headers.get("x-sap-sec", "")
        authorization = extracted_headers.get("authorization", "")
        pg_i = extracted_headers.get("pg-i", "")

        if sap_ri and sap_sec:
            console.print(f"[green]✅ Headers de segurança capturados via logs![/green]")
            console.print(f"[dim]  x-sap-ri: {sap_ri[:10]}...[/dim]")
            if authorization:
                console.print("[dim]  authorization: Bearer ...[/dim]")
            if pg_i:
                console.print(f"[dim]  pg-i: {pg_i[:10]}...[/dim]")
        else:
            console.print("[yellow]⚠️ Headers 'x-sap' não encontrados nos logs. Tentando preservar da sessão anterior...[/yellow]")
            # Preserva x-sap headers da sessão anterior se disponíveis
            old_session = load_session()
            if old_session:
                sap_ri = sap_ri or old_session.get("x-sap-ri", "")
                sap_sec = sap_sec or old_session.get("x-sap-sec", "")
                pg_i = (
                    pg_i
                    or old_session.get("pg-i", "")
                    or old_session.get("headers", {}).get("pg-i", "")
                )
                authorization = (
                    authorization
                    or old_session.get("Authorization", "")
                    or old_session.get("authorization", "")
                    or old_session.get("headers", {}).get("Authorization", "")
                    or old_session.get("headers", {}).get("authorization", "")
                )
                if sap_ri and sap_sec:
                    console.print(f"[green]✅ Headers 'x-sap' preservados da sessão anterior![/green]")
                else:
                    console.print("[yellow]⚠️ Sessão anterior também não tinha headers 'x-sap'. Usando .env ou vazio.[/yellow]")

        if headless and (not sap_ri or not sap_sec):
            raise RuntimeError(
                "Headers 'x-sap-ri/x-sap-sec' não foram obtidos no CI. "
                "Evitando sincronizar sessão incompleta para não propagar 401."
            )

        from core.config import SPX_DEVICE_ID

        headers = {
            "accept": "application/json, text/plain, */*",
            "app": "FMS Portal",
            "content-type": "application/json;charset=UTF-8",
            "device-id": SPX_DEVICE_ID,
            "x-csrftoken": csrf_token or "",
            "x-sap-ri": sap_ri,
            "x-sap-sec": sap_sec,
            "pg-i": pg_i,
            "Authorization": authorization,
        }

        # Remove chaves vazias
        headers = {k: v for k, v in headers.items() if v}

        session_data = {
            "cookies": cookies_dict,
            "cookies_list": cookies,
            "headers": headers,
            "device-id": headers.get("device-id", ""),
            "app": headers.get("app", ""),
            "csrf_token": csrf_token,
            "extracted_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "x-sap-ri": sap_ri,
            "x-sap-sec": sap_sec,
            "pg-i": pg_i,
            "Authorization": authorization,
        }

        valid_session, reasons = is_session_material_valid(session_data)
        if not valid_session:
            raise RuntimeError(
                "Sessão incompleta após autenticação: " + ",".join(reasons)
            )

        save_session(session_data)
        console.print(f"[green]✅ {len(cookies)} cookies SPX extraídos com sucesso![/green]")

        return session_data

    except WebDriverException as e:
        error_msg = str(e).lower()
        if "invalid session id" in error_msg or "has closed the connection" in error_msg or "target window already closed" in error_msg:
            console.print("\n[bold red]❌ Erro fatal: O navegador Chrome foi fechado inesperadamente.[/bold red]")
            console.print("[yellow]Isso ocorre se você fechar a janela manualmente antes do fim do processo ou se o Chrome falhar (crash). O script não pode continuar sem o navegador.[/yellow]")
        else:
            console.print(f"\n[red]❌ Erro de conexão com o navegador Chrome: {e}[/red]")
        raise RuntimeError("Conexão com o navegador foi perdida (janela fechada).")

    except Exception as e:
        console.print(f"[red]❌ Erro durante extração: {e}[/red]")
        try:
            driver.save_screenshot(str(BASE_DIR / "output" / "auth_error.png"))
        except Exception:
            pass
        raise

    finally:
        driver.quit()
        console.print("[dim]Chrome fechado.[/dim]")


def save_session(session_data: dict) -> None:
    """Salva dados da sessão em arquivo JSON e na nuvem (Google Sheets)."""
    valid_session, reasons = is_session_material_valid(session_data)
    if not valid_session:
        raise ValueError(
            "Sessão inválida. Persistência bloqueada para evitar propagar estado incompleto: "
            + ",".join(reasons)
        )

    save_runtime_session_cache(session_data)

    if _is_github_actions():
        cache_file = get_runtime_session_cache_file()
        if cache_file:
            console.print(f"[green]📁 Sessão salva no cache efêmero do CI: {cache_file}[/green]")
    else:
        COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)

        with open(COOKIES_FILE, "w", encoding="utf-8") as f:
            json.dump(session_data, f, indent=2, ensure_ascii=False)

        console.print(f"[green]📁 Sessão salva localmente: {COOKIES_FILE}[/green]")

    # Sincroniza com a nuvem (Google Sheets)
    try:
        from core.config import SPREADSHEET_ID
        from core.sheets import update_cloud_config

        if SPREADSHEET_ID:
            console.print("[blue]☁️ Sincronizando sessão com a nuvem (Google Sheets)...[/blue]")

            from core.config import (
                SAP_RI_INBOUND, SAP_SEC_INBOUND,
                SAP_RI_OUTBOUND, SAP_SEC_OUTBOUND,
                TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DISCORD_WEBHOOK_URL
            )

            cloud_data = {
                "cookies_json": json.dumps(session_data),
                "extracted_at": session_data.get("extracted_at", ""),
                "x-sap-ri": session_data.get("x-sap-ri", ""),
                "x-sap-sec": session_data.get("x-sap-sec", ""),
                "x-sap-ri-inbound": SAP_RI_INBOUND or session_data.get("x-sap-ri", ""),
                "x-sap-sec-inbound": SAP_SEC_INBOUND or session_data.get("x-sap-sec", ""),
                "x-sap-ri-outbound": SAP_RI_OUTBOUND or session_data.get("x-sap-ri", ""),
                "x-sap-sec-outbound": SAP_SEC_OUTBOUND or session_data.get("x-sap-sec", ""),
                "pg-i": (
                    session_data.get("pg-i", "")
                    or session_data.get("headers", {}).get("pg-i", "")
                ),
                "device-id": session_data.get("device-id", ""),
                "app": session_data.get("app", ""),
                "Authorization": session_data.get("Authorization", ""),
                "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
                "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
                "DISCORD_WEBHOOK_URL": DISCORD_WEBHOOK_URL
            }

            # Mantem CONFIG_CLOUD como fonte de verdade para OAuth/cookies Google no CI.
            try:
                from core.config import OAUTH_TOKEN_FILE, GOOGLE_SESSION_COOKIES_FILE

                if os.path.exists(OAUTH_TOKEN_FILE):
                    with open(OAUTH_TOKEN_FILE, "r", encoding="utf-8") as f:
                        cloud_data["GOOGLE_OAUTH_TOKEN"] = json.dumps(json.load(f))

                if os.path.exists(GOOGLE_SESSION_COOKIES_FILE):
                    with open(GOOGLE_SESSION_COOKIES_FILE, "r", encoding="utf-8") as f:
                        cloud_data["GOOGLE_SESSION_COOKIES"] = json.dumps(json.load(f))
            except Exception as e:
                console.print(f"[yellow]⚠️ Não foi possível sincronizar OAuth/cookies Google na nuvem: {e}[/yellow]")

            if update_cloud_config(SPREADSHEET_ID, cloud_data):
                console.print("[green]✅ Nuvem atualizada com sucesso.[/green]")
            else:
                console.print("[yellow]⚠️ Falha ao atualizar a nuvem.[/yellow]")
    except Exception as e:
        console.print(f"[red]❌ Erro ao sincronizar com a nuvem: {e}[/red]")


def load_session() -> Optional[dict]:
    """
    Carrega sessão salva.

    Ordem:
        1. Cache efêmero do job atual no GitHub Actions
        2. Arquivo local (ambiente local)
        3. Nuvem (Google Sheets)
    """
    session_data = None

    # 1. Tenta cache efêmero do job atual (somente CI)
    session_data = load_runtime_session_cache()
    if session_data:
        console.print("[blue]⚡ Sessão carregada do cache efêmero do CI.[/blue]")
        return session_data

    # 2. Tenta local
    if COOKIES_FILE.exists():
        try:
            with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                session_data = json.load(f)
        except Exception:
            pass

    # 3. Se não tem local, tenta Cloud
    if not session_data:
        try:
            from core.config import SPREADSHEET_ID
            from core.sheets import get_cloud_config

            if SPREADSHEET_ID:
                console.print("[blue]☁️ Tentando recuperar sessão da nuvem...[/blue]")
                cloud_config = get_cloud_config(SPREADSHEET_ID)

                if not cloud_config:
                    console.print("[yellow]⚠️ Planilha de configuração encontrada, mas está vazia.[/yellow]")
                elif "cookies_json" in cloud_config:
                    session_data = json.loads(cloud_config["cookies_json"])
                    session_data.update(cloud_config)
                    save_runtime_session_cache(session_data)
                    console.print("[green]✅ Sessão recuperada da nuvem![/green]")
                else:
                    found_keys = list(cloud_config.keys())
                    console.print(f"[yellow]⚠️ Sessão não encontrada na nuvem. Chaves: {found_keys}[/yellow]")
        except Exception as e:
            console.print(f"[red]❌ Falha ao buscar na nuvem: {e}[/red]")

    return session_data


def authenticate(headless: bool = False, force_refresh: bool = False) -> dict:
    """
    Autentica no SPX e captura cookies.

    Estratégias (em ordem):
        1. Reusar sessão existente (se não force_refresh)
        2. OAuth2 cookie injection (automático)
        3. OAuth2 flow / email-senha (fallback)
        4. Login manual (apenas local)

    Args:
        headless: Se True, tenta login sem interface (CI).
        force_refresh: Se True, força nova extração via navegador

    Returns:
        Dicionário com cookies e headers
    """
    if not force_refresh:
        existing = load_session()
        if existing:
            console.print("[blue]📂 Usando sessão salva...[/blue]")
            console.print(f"[dim]Extraída em: {existing.get('extracted_at', 'desconhecido')}[/dim]")
            return existing

    console.print("[bold cyan]═══ Autenticação SPX Shopee (OAuth2) ═══[/bold cyan]")

    is_github = os.getenv("GITHUB_ACTIONS") == "true"

    if is_github:
        console.print("[cyan]🤖 Auto-login no CI via OAuth2...[/cyan]")
        try:
            return extract_cookies_from_browser(headless=True)
        except Exception as e:
            console.print(f"[red]❌ Auto-login falhou no CI: {e}[/red]")
            return {}

    console.print("[yellow]🔄 Abrindo navegador para login...[/yellow]")
    return extract_cookies_from_browser(headless=headless)


def refresh_session() -> dict:
    """Força renovação da sessão."""
    console.print("[yellow]🔄 Renovando sessão...[/yellow]")
    is_github = os.getenv("GITHUB_ACTIONS") == "true"
    return authenticate(headless=is_github, force_refresh=True)


if __name__ == "__main__":
    console.print("[bold]Iniciando autenticação (OAuth2)...[/bold]")
    session = authenticate(force_refresh=True)
    if session:
        console.print(f"\n[bold green]✅ Autenticação concluída![/bold green]")
        console.print(f"[green]Cookies capturados: {len(session.get('cookies', {}))}[/green]")
    else:
        console.print(f"\n[bold red]❌ Autenticação falhou![/bold red]")
