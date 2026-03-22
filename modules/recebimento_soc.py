"""
Módulo de Extração: Recebimento SOC MG_BETIM
=============================================

Extrai dados de recebimento via POST.
Operação: POST de Recebimento SOC.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from rich.console import Console

from core.config import BRT, RECEBIMENTO_SOC, DEFAULT_PAGE_SIZE, MAX_PAGES, SPREADSHEET_ID
from core.save import save_data
from core.sheets import read_sheet
from core.session import get_session

console = Console()

MODULE_NAME = "recebimento_soc"

BAKED_COLUMNS = [
    "Order ID",
    "SLS Tracking Number",
    "3PL Tracking Number",
    "Shopee Order SN",
    "Sort Code Name",
    "Buyer Name",
    "Buyer Phone",
    "Buyer Address",
    "Location Type",
    "Postal Code",
    "Driver ID",
    "Driver Name",
    "Driver Phone",
    "Pick Up Time",
    "SOC Received time",
    "Current Station Received Time",
    "Delivered Time",
    "OnHold Time",
    "OnHoldReason",
    "Reschedule Time",
    "Status",
    "Reject remark",
    "Manifest Number",
    "Order Account",
    "Parcel weight(kg)",
    "Sls weight(kg)",
    "Length(cm)",
    "Width(cm)",
    "Height(cm)",
    "Original ASF",
    "Rounding ASF",
    "COD Fee",
    "Delivery Attempts",
    "Bulky Type",
    "SLA Target Date",
    "Time to SLA",
    "Payment Method",
    "Pickup Station",
    "Destination Station",
    "Next Station",
    "Current Station",
    "Return Destination",
    "Channel",
    "Previous 3PL",
    "Next 3PL",
    "Shop ID",
    "Shop Category",
    "Inbound 3PL",
    "Outbound 3PL",
    "Damaged Tag",
    "Chargeable Weight",
    "Liquid Tag",
    "Fragile Tag",
    "Magnetic Tag",
    "Hub RTO Number",
    "DC RTO Number",
    "Warehouse",
    "Weight(kg)",
    "Receive Time",
    "Handover Time",
    "Number of Return On-hold",
]

ORDER_ACCOUNT_MAP = {
    "51": "AMS Free Sample",
    "9": "Bulky Marketplace",
    "8": "Bulky Shopee Xpress",
    "38": "CB Five Day Collection",
    "62": "Economy Bulky",
    "63": "Economy Bulky Marketplace",
    "10": "Economy Delivery",
    "11": "Economy Marketplace Delivery",
    "50": "Express Collection",
    "52": "Fulfillment Pickup",
    "13": "Groceries Delivery",
    "26": "Groceries Delivery Collection",
    "56": "MP Crossdock(IDMY)",
    "45": "MP Direct Selling",
    "59": "MP Package Free",
    "2": "Marketplace",
    "41": "Marketplace 2DD",
    "32": "Marketplace 3PL Locker",
    "24": "Marketplace Air Freight",
    "16": "Marketplace Collection",
    "65": "Marketplace Inhouse Locker",
    "33": "Marketplace Next Day",
    "35": "Marketplace Next Day Collection",
    "22": "Marketplace Same Day",
    "53": "NS Economy",
    "37": "NS Marketplace Collection",
    "12": "NS Marketplace Standard",
    "44": "NS Reverse Logistics",
    "4": "Premium Express",
    "40": "SPX Domestic Economy",
    "25": "SPX Eco-Friendly Collection",
    "31": "SPX Eco-Friendly Marketplace Collection",
    "64": "SPX Liquidation",
    "7": "SPX Point-to-Point Delivery",
    "28": "SPX Reverse Logistics",
    "29": "SPX Reverse Logistics Collection",
    "39": "SPX Reverse Logistics Locker",
    "5": "SPX Standard",
    "48": "SPX Standard Collection",
    "54": "SPX Standard Locker",
    "6": "SPX Standard Marketplace",
    "49": "SPX Standard Marketplace Collection",
    "67": "Shopee Choice HD Next Day",
    "43": "Shopee Choice Same Day Collection",
    "55": "Shopee Choice Standard Collection",
    "1": "Shopee Xpress",
    "23": "Shopee Xpress Air Freight",
    "14": "Shopee Xpress Collection",
    "21": "Shopee Xpress Same Day",
    "17": "Standard Economy",
    "3": "Standard Express",
    "27": "Standard Express 3PL Locker",
    "18": "Standard Express Collection",
    "66": "Standard Express Inhouse Locker",
    "30": "Standard Seashipping",
    "46": "WHS Direct Selling",
    "47": "WHS Stock Transfer",
    "42": "Warehouse 2DD",
    "69": "Warehouse 3PL Locker",
    "68": "Warehouse Inhouse Locker",
    "34": "Warehouse Next Day",
    "36": "Warehouse Next Day Collection",
}

_BASE_STATUS_MAPPING_CACHE: dict[str, str] | None = None


def get_time_range(days_ago_start: int, days_ago_end: int) -> tuple[int, int]:
    """Calcula o range de timestamps."""
    now = datetime.now(BRT)
    start_date = (now - timedelta(days=days_ago_start)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = (now - timedelta(days=days_ago_end)).replace(hour=23, minute=59, second=59, microsecond=0)
    return int(start_date.timestamp()), int(end_date.timestamp())


def fetch_recebimento_soc(
    days_ago_start: int = None,
    days_ago_end: int = None,
    count_per_page: int = None
) -> list[dict]:
    """Busca todos os dados de Recebimento SOC."""
    
    # Usa configurações padrão se não informado
    days_ago_start = days_ago_start or RECEBIMENTO_SOC["days_ago_start"]
    days_ago_end = days_ago_end or RECEBIMENTO_SOC["days_ago_end"]
    count_per_page = count_per_page or RECEBIMENTO_SOC.get("page_size", DEFAULT_PAGE_SIZE)
    
    console.print(f"[cyan]Buscando Recebimento SOC (Station: {RECEBIMENTO_SOC['station_id']})...[/cyan]")
    
    session = get_session()
    start_ts, end_ts = get_time_range(days_ago_start, days_ago_end)
    
    start_date = datetime.fromtimestamp(start_ts, tz=BRT)
    end_date = datetime.fromtimestamp(end_ts, tz=BRT)
    console.print(f"  Período: {start_date.strftime('%d/%m/%Y %H:%M')} até {end_date.strftime('%d/%m/%Y %H:%M')}")
    
    all_data = []
    page = 1
    total_expected = None
    
    while page <= MAX_PAGES:
        body = {
            "current_station_received_time": f"{start_ts},{end_ts}",
            "current_station_ids": RECEBIMENTO_SOC["station_id"],
            "order_status": RECEBIMENTO_SOC["order_status"],
            "count": count_per_page,
            "page_no": page
        }
        
        try:
            response = session.post(RECEBIMENTO_SOC["api_url"], json_data=body)
            
            if isinstance(response, dict):
                retcode = response.get("retcode", -1)
                if retcode != 0:
                    console.print(f"[red]Erro API: {response.get('message', 'desconhecido')}[/red]")
                    break
                
                data_wrapper = response.get("data", response)
                
                if isinstance(data_wrapper, dict):
                    items = data_wrapper.get("list", data_wrapper.get("tracking_list", []))
                    total_expected = data_wrapper.get("total", data_wrapper.get("total_count", 0))
                else:
                    items = data_wrapper if isinstance(data_wrapper, list) else []
                    total_expected = len(items)
            else:
                console.print(f"[red]Resposta inesperada: {type(response)}[/red]")
                break
            
            if not items:
                break
            
            all_data.extend(items)
            console.print(f"  Página {page}: +{len(items)} ({len(all_data)}/{total_expected})")
            
            if len(all_data) >= total_expected:
                break
            
            page += 1
            
        except Exception as e:
            console.print(f"[red]❌ Erro na página {page}: {e}[/red]")
            break
    
    console.print(f"\n[bold green]✅ Total extraído: {len(all_data)} registros[/bold green]")
    return all_data


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).replace(",", ".")))
    except (ValueError, TypeError):
        return None


def _format_unix_datetime(value: Any) -> str:
    ts = _to_int(value)
    if ts is None or ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=BRT).strftime("%d-%m-%Y %H:%M")
    except (OverflowError, OSError, ValueError):
        return ""


def _format_decimal_keep_comma(value: Any) -> str:
    return _to_str(value)


def _map_status_code(value: Any, status_map: dict[str, str]) -> str:
    code = _to_str(value)
    if not code:
        return ""

    direct = status_map.get(code)
    if direct:
        return direct

    normalized_int = _to_int(code)
    if normalized_int is not None:
        mapped = status_map.get(str(normalized_int))
        if mapped:
            return mapped

    return code


def _map_status(value: Any, status_map: dict[str, str]) -> str:
    return _map_status_code(value, status_map)


def _map_on_hold_reason(value: Any, status_map: dict[str, str]) -> str:
    return _map_status_code(value, status_map)


def _map_tag(value: Any, positive_label: str, negative_label: str) -> str:
    tag = _to_str(value).lower()
    if tag == "":
        return ""
    if tag in {"1", "true", "yes"}:
        return positive_label
    if tag in {"0", "2", "false", "no"}:
        return negative_label
    return negative_label


def _format_payment_method(raw_row: dict[str, Any]) -> str:
    payment_method = _to_str(raw_row.get("payment_method"))
    if payment_method:
        return payment_method

    sub_payment_method = _to_str(raw_row.get("sub_payment_method"))
    if sub_payment_method:
        return sub_payment_method

    cod_status = _to_str(raw_row.get("cod_status")).lower()
    if cod_status in {"1", "true", "cod"}:
        return "COD"
    return "NON-COD"


def load_base_status_mapping() -> dict[str, str]:
    """Carrega mapping da aba BASE STATUS (A=nome, B=código) para {código: nome}."""
    global _BASE_STATUS_MAPPING_CACHE

    if _BASE_STATUS_MAPPING_CACHE is not None:
        return _BASE_STATUS_MAPPING_CACHE

    rows = read_sheet(SPREADSHEET_ID, "'BASE STATUS'!A2:B")
    status_map: dict[str, str] = {}

    for row in rows:
        if len(row) < 2:
            continue

        description = _to_str(row[0])
        code = _to_str(row[1])
        if not description or not code:
            continue

        status_map[code] = description

        normalized_int = _to_int(code)
        if normalized_int is not None:
            status_map[str(normalized_int)] = description

    _BASE_STATUS_MAPPING_CACHE = status_map
    console.print(f"[cyan]BASE STATUS carregada: {len(status_map)} códigos[/cyan]")
    return status_map


def transform_raw_to_baked_row(raw_row: dict[str, Any], status_map: dict[str, str]) -> dict[str, str]:
    """Transforma um registro RAW (API) no layout BAKED (export manual)."""
    reschedule_value = raw_row.get("reschedule_time_start") or raw_row.get("reschedule_time_end")
    pick_up_ts = raw_row.get("pickup_time") or raw_row.get("receive_time") or raw_row.get("current_station_received_time")

    row = {
        "Order ID": _to_str(raw_row.get("shipment_id")),
        "SLS Tracking Number": _to_str(raw_row.get("sls_tracking_number")),
        "3PL Tracking Number": _to_str(raw_row.get("third_party_tracking_num")),
        "Shopee Order SN": _to_str(raw_row.get("current_to_number")),
        "Sort Code Name": _to_str(raw_row.get("sort_code_name")),
        "Buyer Name": _to_str(raw_row.get("lowest_buyer_address_name")),
        "Buyer Phone": "",
        "Buyer Address": _to_str(raw_row.get("buyer_address")),
        "Location Type": _to_str(raw_row.get("location_type")),
        "Postal Code": _to_str(raw_row.get("buyer_postal_code")),
        "Driver ID": _to_str(raw_row.get("driver_id")),
        "Driver Name": _to_str(raw_row.get("driver_name")),
        "Driver Phone": "",
        "Pick Up Time": _format_unix_datetime(pick_up_ts),
        "SOC Received time": _format_unix_datetime(raw_row.get("receive_time")),
        "Current Station Received Time": _format_unix_datetime(raw_row.get("current_station_received_time")),
        "Delivered Time": _format_unix_datetime(raw_row.get("delivered_time")),
        "OnHold Time": _format_unix_datetime(raw_row.get("on_hold_time")),
        "OnHoldReason": "",
        "Reschedule Time": _format_unix_datetime(reschedule_value),
        "Status": _map_status(raw_row.get("order_status"), status_map),
        "Reject remark": _to_str(raw_row.get("reject_remark")),
        "Manifest Number": "",
        "Order Account": ORDER_ACCOUNT_MAP.get(_to_str(raw_row.get("order_account")), _to_str(raw_row.get("order_account"))),
        "Parcel weight(kg)": _format_decimal_keep_comma(raw_row.get("chargeable_weight")),
        "Sls weight(kg)": "",
        "Length(cm)": "",
        "Width(cm)": "",
        "Height(cm)": "",
        "Original ASF": "",
        "Rounding ASF": "",
        "COD Fee": "",
        "Delivery Attempts": _to_str(raw_row.get("return_attempts")),
        "Bulky Type": _to_str(raw_row.get("bulky_type__desc")) or _to_str(raw_row.get("new_bulky_type")),
        "SLA Target Date": _format_unix_datetime(raw_row.get("sla_target_time")),
        "Time to SLA": _to_str(raw_row.get("sla_target_time__desc")),
        "Payment Method": _format_payment_method(raw_row),
        "Pickup Station": _to_str(raw_row.get("pickup_station_name")),
        "Destination Station": _to_str(raw_row.get("station_name")),
        "Next Station": _to_str(raw_row.get("next_station_name")),
        "Current Station": _to_str(raw_row.get("current_station_name")),
        "Return Destination": _to_str(raw_row.get("return_dest_station_id")),
        "Channel": _to_str(raw_row.get("channel_name")),
        "Previous 3PL": "",
        "Next 3PL": "",
        "Shop ID": _to_str(raw_row.get("shop_id")),
        "Shop Category": _to_str(raw_row.get("shop_category_label")),
        "Inbound 3PL": _to_str(raw_row.get("first_channel_code")),
        "Outbound 3PL": _to_str(raw_row.get("last_channel_code")),
        "Damaged Tag": "Damaged" if _to_str(raw_row.get("damaged_tag")) in {"1", "true", "True"} else "",
        "Chargeable Weight": _format_decimal_keep_comma(raw_row.get("chargeable_weight")),
        "Liquid Tag": _map_tag(raw_row.get("liquid_tag"), "Liquid", "Non-Liquid"),
        "Fragile Tag": _map_tag(raw_row.get("fragile_tag"), "Fragile", "Non-Fragile"),
        "Magnetic Tag": _map_tag(raw_row.get("magnetic_tag"), "Magnetic", "Non-Magnetic"),
        "Hub RTO Number": "",
        "DC RTO Number": "",
        "Warehouse": _to_str(raw_row.get("whs_id")),
        "Weight(kg)": _format_decimal_keep_comma(raw_row.get("chargeable_weight")),
        "Receive Time": _format_unix_datetime(raw_row.get("receive_time")),
        "Handover Time": _to_str(raw_row.get("handover_time")),
        "Number of Return On-hold": _to_str(raw_row.get("on_hold_times")),
    }

    return {column: row.get(column, "") for column in BAKED_COLUMNS}


def transform_raw_to_baked(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Transforma lista RAW no formato BAKED."""
    if not rows:
        return []

    status_map = load_base_status_mapping()
    return [transform_raw_to_baked_row(raw_row, status_map) for raw_row in rows]


def run(days_ago_start: int = None, days_ago_end: int = None) -> tuple[Path, Path, int]:
    """Executa extração completa de Recebimento SOC."""
    console.print("[bold cyan]═══ Recebimento SOC MG_BETIM ═══[/bold cyan]")
    data = fetch_recebimento_soc(days_ago_start, days_ago_end)
    return save_data(data, MODULE_NAME)


def run_with_transform(days_ago_start: int = None, days_ago_end: int = None) -> tuple[Path, Path, int]:
    """Executa extração RAW e transforma para layout BAKED."""
    console.print("[bold cyan]═══ Recebimento SOC MG_BETIM (BAKED) ═══[/bold cyan]")
    raw_data = fetch_recebimento_soc(days_ago_start, days_ago_end)
    baked_data = transform_raw_to_baked(raw_data)

    console.print(f"[cyan]Transformação ETL concluída: {len(baked_data)} linhas BAKED[/cyan]")
    return save_data(
        baked_data,
        MODULE_NAME,
        save_json_file=False,
        save_csv_file=True,
        upload_sheets=True,
    )


if __name__ == "__main__":
    run()
