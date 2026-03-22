"""
Configurações Centralizadas do Sistema
======================================

Este módulo contém todas as constantes e configurações do projeto.
"""

from datetime import timezone, timedelta
from pathlib import Path
import os

# Timezone Brasil (UTC-3)
BRT = timezone(timedelta(hours=-3))

# =============================================================================
# Configurações Gerais
# =============================================================================

SAVE_LOCAL_FILES = False  # Se True, salva JSON e CSV localmente. Se False, apenas na memória/Sheets.

#Paths
# =============================================================================

from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"
load_dotenv(BASE_DIR / ".env")

def _read_env(name: str, default: str = "") -> str:
    """Lê variáveis de ambiente removendo espaços acidentais."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


SPX_BASE_URL = _read_env("SPX_BASE_URL", "https://spx.shopee.com.br")

# Sec tokens
SAP_RI_INBOUND = os.getenv("SPX_SAP_RI_INBOUND", os.getenv("SPX_SAP_RI", ""))
SAP_SEC_INBOUND = os.getenv("SPX_SAP_SEC_INBOUND", os.getenv("SPX_SAP_SEC", ""))
SAP_RI_OUTBOUND = os.getenv("SPX_SAP_RI_OUTBOUND", os.getenv("SPX_SAP_RI", ""))
SAP_SEC_OUTBOUND = os.getenv("SPX_SAP_SEC_OUTBOUND", os.getenv("SPX_SAP_SEC", ""))
SPX_DEVICE_ID = _read_env("SPX_DEVICE_ID")

# Credentials for Auto-Login (OAuth2)
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(BASE_DIR / "service_account.json"))
OAUTH_CLIENT_SECRET_FILE = os.getenv("OAUTH_CLIENT_SECRET_FILE", str(BASE_DIR / "oauth_client_secret.json"))
OAUTH_TOKEN_FILE = os.getenv("OAUTH_TOKEN_FILE", str(BASE_DIR / "oauth_token.json"))
GOOGLE_SESSION_COOKIES_FILE = os.getenv("GOOGLE_SESSION_COOKIES_FILE", str(BASE_DIR / "google_session_cookies.json"))
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT", "")
GOOGLE_OAUTH_CLIENT_SECRET_JSON = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "")

# Notifications
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

# Recebimento SOC
RECEBIMENTO_SOC = {
    "station_id": "6042",  # SOC_MG_BETIM
    "order_status": "1,8,10,18,33,36,9,61,65,64,60,61,64,124,153,460,550,58,613,612,615,614,611,610,616,629,574,575,650,648,751,646,571,581,5,570,657,630",    "api_url": _read_env("SPX_API_TRACKING_LIST_SEARCH", "/api/fleet_order/order/tracking_list/search"),
    "days_ago_start": 30,
    "days_ago_end": 6,
    "page_size": 1665,
}

# Escalation Ticket
ESCALATION_TICKET = {
    "api_url": _read_env("SPX_API_ESCALATION_TICKET", "/ticketcenter-api/escalation_ticket/station/all_ticket/list"),
    "days_ago": 30,
}

# Exception Orders
EXCEPTION_ORDERS = {
    "api_url": _read_env("SPX_API_EXCEPTION_ORDER_HISTORY", "/exception-api/admin/soc/order/history"),
    "days_ago": 1,
}

# Liquidation (EO List - ER48)
LIQUIDATION = {
    "api_url": _read_env("SPX_API_EXCEPTION_HANDLING_EO_LIST", "/exception-api/admin/soc/exception_handling/eo_list"),
    "reason_id": "ER48",
    "days_ago": 60,
    "page_size": 24,
}

# Inbound
INBOUND = {
    "api_url": _read_env("SPX_API_WFM_DASHBOARD", "/api/wfm/admin/dashboard/list"),
    "scan_type": 1,  # Inbound
}

# Outbound
OUTBOUND = {
    "api_url": _read_env("SPX_API_WFM_DASHBOARD", "/api/wfm/admin/dashboard/list"),
    "scan_type": 2,  # Outbound
}

# SPX Duplicados
SPX_DUPLICADOS = {
    "api_url": _read_env("SPX_API_TRACKING_INFO", "/api/fleet_order/order/detail/tracking_info"),
    "delay_between_requests": 0.7,   # segundos entre requisições (rate limit)
    "max_retries": 3,                # tentativas por shipment_id
    "input_sheet_tab": "'SPX DUPLICADO'!A:A",
}

# Online SOC Tracking (Online_SOC-MG2 -> raw_tracking_info)
ONLINE_SOC_TRACKING = {
    "spreadsheet_id": _read_env("ONLINE_SOC_SPREADSHEET_ID"),
    "base_status_spreadsheet_id": _read_env("ONLINE_SOC_BASE_STATUS_SPREADSHEET_ID"),
    "api_url": _read_env("SPX_API_TRACKING_INFO", "/api/fleet_order/order/detail/tracking_info"),
    "max_workers": 8,
    "batch_size": 500,
    "delay_between_requests": 0,
    "max_retries": 3,
    "input_sheet_tab": "'Online_SOC-MG2'!A:A",
    "output_sheet_tab": "'raw_tracking_info'!A1",
    "base_status_tab": "'BASE STATUS'!A2:B",
}

# Status dos Duplicados
STATUS_DUPLICADOS = {
    "api_url": _read_env("SPX_API_TRACKING_LIST_SEARCH", "/api/fleet_order/order/tracking_list/search"),
    "page_size": 50,
    # número de IDs a consultar em cada lote quando usado por outros módulos
    # (por exemplo, spx_duplicados). A API admite até ~5000, mas valores
    # altos podem impactar a conexão; 1000 é um bom compromisso.
    "batch_size": 1000,
}

# =============================================================================
# Configurações de Paginação
# =============================================================================

DEFAULT_PAGE_SIZE = 1000
MAX_PAGES = 500

# =============================================================================
# Configurações de Logging
# =============================================================================

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

# =============================================================================
# Configurações do Google Sheets
# =============================================================================

_sheet_id = _read_env("GOOGLE_SHEETS_ID")
SPREADSHEET_ID = _sheet_id.split("=")[-1] if "=" in _sheet_id else _sheet_id

SHEETS_TABS = {
    "recebimento_soc": "'BASE SOC'!A1",
    "escalation_ticket": "'BASE Escalation Ticket'!A1",
    "exception_orders": "'BASE Exception Orders'!A1",
    "producao_eo": "'BASE Produção EO'!A1",
    "inbound": "'BASE Inbound'!A1",
    "outbound": "'BASE Outbound'!A1",
    "liquidation": "'BASE Liquidation'!A1",
    "spx_duplicados": "'SPX DUPLICADO'!A1",
    "status_duplicados": "'BASE STATUS DUP'!A1",
    "fora_estacao": "'FORA DE ESTAÇÃO'!A1",
    "cloud_config": "'CONFIG_CLOUD'!A1",
}
