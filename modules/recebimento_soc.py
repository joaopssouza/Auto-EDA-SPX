"""
Módulo de Extração: Recebimento SOC MG_BETIM
=============================================

Extrai dados de recebimento via POST.
Operação: POST de Recebimento SOC.
"""

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from pathlib import Path
from typing import Any
import time

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


def _extract_items_and_total(response: Any) -> tuple[list[dict], int | None, str | None]:
    """Normaliza resposta da API em (itens, total, erro)."""
    if not isinstance(response, dict):
        return [], None, f"Resposta inesperada: {type(response)}"

    retcode = response.get("retcode", -1)
    if retcode != 0:
        return [], None, response.get("message", "desconhecido")

    data_wrapper = response.get("data", response)
    if isinstance(data_wrapper, dict):
        items = data_wrapper.get("list", data_wrapper.get("tracking_list", []))
        total_raw = data_wrapper.get("total", data_wrapper.get("total_count", None))
    elif isinstance(data_wrapper, list):
        items = data_wrapper
        total_raw = len(items)
    else:
        items = []
        total_raw = None

    total_int: int | None = None
    if isinstance(total_raw, int):
        total_int = total_raw
    elif isinstance(total_raw, str) and total_raw.isdigit():
        total_int = int(total_raw)

    return items if isinstance(items, list) else [], total_int, None


def _fetch_page_with_retry(
    session,
    api_url: str,
    base_body: dict[str, Any],
    page: int,
    max_retry_attempts: int,
    retry_backoff_base: float,
) -> tuple[int, list[dict], int | None, str | None]:
    """Busca uma página com retry local para retcode/erros temporários."""
    for attempt in range(1, max_retry_attempts + 1):
        body = dict(base_body)
        body["page_no"] = page

        try:
            response = session.post(api_url, json_data=body)
            items, total, error = _extract_items_and_total(response)
            if error is None:
                return page, items, total, None

            if attempt < max_retry_attempts:
                sleep_for = retry_backoff_base ** attempt
                time.sleep(sleep_for)
            else:
                return page, [], None, error
        except Exception as exc:
            if attempt < max_retry_attempts:
                sleep_for = retry_backoff_base ** attempt
                time.sleep(sleep_for)
            else:
                return page, [], None, str(exc)

    return page, [], None, "falha desconhecida"


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
    
    max_workers = max(1, int(RECEBIMENTO_SOC.get("max_workers", 5)))
    probe_threshold = max(2, int(RECEBIMENTO_SOC.get("probe_threshold", 5)))
    batch_size_pages = max(1, int(RECEBIMENTO_SOC.get("batch_size_pages", 20)))
    delay_between_batches = max(0.0, float(RECEBIMENTO_SOC.get("delay_between_batches", 1.0)))
    max_retry_attempts = max(1, int(RECEBIMENTO_SOC.get("max_retry_attempts", 3)))
    retry_backoff_base = max(1.1, float(RECEBIMENTO_SOC.get("retry_backoff_base", 1.3)))

    api_url = RECEBIMENTO_SOC["api_url"]
    base_body = {
        "current_station_received_time": f"{start_ts},{end_ts}",
        "current_station_ids": RECEBIMENTO_SOC["station_id"],
        "order_status": RECEBIMENTO_SOC["order_status"],
        "count": count_per_page,
    }

    # Sonda inicial para descobrir total e decidir estratégia de execução.
    page, items, total_expected, error = _fetch_page_with_retry(
        session,
        api_url,
        base_body,
        page=1,
        max_retry_attempts=max_retry_attempts,
        retry_backoff_base=retry_backoff_base,
    )
    if error:
        console.print(f"[red]❌ Erro na página {page}: {error}[/red]")
        return []

    if not items:
        console.print("\n[bold green]✅ Total extraído: 0 registros[/bold green]")
        return []

    all_data = list(items)
    console.print(f"  Página 1: +{len(items)} ({len(all_data)}/{total_expected or '?'})")

    if not total_expected or len(all_data) >= total_expected:
        console.print(f"\n[bold green]✅ Total extraído: {len(all_data)} registros[/bold green]")
        return all_data

    total_pages = min(MAX_PAGES, max(1, ceil(total_expected / count_per_page)))

    # Fallback sequencial para volume baixo ou quando paralelo estiver desativado.
    if max_workers == 1 or total_pages < probe_threshold:
        for current_page in range(2, total_pages + 1):
            _, page_items, _, page_error = _fetch_page_with_retry(
                session,
                api_url,
                base_body,
                page=current_page,
                max_retry_attempts=max_retry_attempts,
                retry_backoff_base=retry_backoff_base,
            )
            if page_error:
                console.print(f"[red]❌ Erro na página {current_page}: {page_error}[/red]")
                continue

            if not page_items:
                break

            all_data.extend(page_items)
            console.print(f"  Página {current_page}: +{len(page_items)} ({len(all_data)}/{total_expected})")

        console.print(f"\n[bold green]✅ Total extraído: {len(all_data)} registros[/bold green]")
        return all_data

    pages_to_fetch = list(range(2, total_pages + 1))
    console.print(
        f"[cyan]Paralelismo ativo: {max_workers} workers, {len(pages_to_fetch)} páginas restantes.[/cyan]"
    )

    failed_pages: list[int] = []

    for start_idx in range(0, len(pages_to_fetch), batch_size_pages):
        batch_pages = pages_to_fetch[start_idx:start_idx + batch_size_pages]
        batch_results: dict[int, list[dict]] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _fetch_page_with_retry,
                    session,
                    api_url,
                    base_body,
                    page_no,
                    max_retry_attempts,
                    retry_backoff_base,
                ): page_no
                for page_no in batch_pages
            }

            for future in as_completed(futures):
                page_no = futures[future]
                try:
                    _, page_items, _, page_error = future.result()
                    if page_error:
                        failed_pages.append(page_no)
                        console.print(f"[yellow]⚠️ Página {page_no} com erro: {page_error}[/yellow]")
                        continue
                    batch_results[page_no] = page_items
                except Exception as exc:
                    failed_pages.append(page_no)
                    console.print(f"[yellow]⚠️ Página {page_no} falhou no worker: {exc}[/yellow]")

        for page_no in sorted(batch_results.keys()):
            page_items = batch_results[page_no]
            if page_items:
                all_data.extend(page_items)

        console.print(
            f"  Lote de páginas {batch_pages[0]}-{batch_pages[-1]} concluído "
            f"({len(all_data)}/{total_expected})"
        )

        if delay_between_batches > 0 and start_idx + batch_size_pages < len(pages_to_fetch):
            time.sleep(delay_between_batches)

    if failed_pages:
        console.print(
            f"[yellow]⚠️ Reprocessando {len(failed_pages)} páginas com falha em modo sequencial...[/yellow]"
        )
        for page_no in sorted(set(failed_pages)):
            _, page_items, _, page_error = _fetch_page_with_retry(
                session,
                api_url,
                base_body,
                page=page_no,
                max_retry_attempts=max_retry_attempts,
                retry_backoff_base=retry_backoff_base,
            )
            if page_error:
                console.print(f"[red]❌ Página {page_no} permaneceu com erro: {page_error}[/red]")
                continue
            if page_items:
                all_data.extend(page_items)
    
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
