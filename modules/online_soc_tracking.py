"""
Módulo: Online SOC Tracking
===========================

Lê Order IDs da aba Online_SOC-MG2, consulta tracking_info por shipment_id
(equivale a tracking details), extrai o último children do último tracking_list,
expande SPXBR quando existir mais de um e grava snapshot em raw_tracking_info.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn

from core.config import BRT, ONLINE_SOC_TRACKING
from core.session import get_session
from core.sheets import append_sheet, read_sheet, update_sheet

console = Console()

MODULE_NAME = "online_soc_tracking"

OUTPUT_HEADERS = [
    "Order ID",
    "SOC Received time",
    "Last Status",
    "Last Status Timestamp",
    "SPXBR",
    "Status Code",
    "Tag",
    "Latest SOC Station",             
    "SOC Received (8/58) Timestamp",  # Atualizado
    "SOC Received (8/58) Status",     # Atualizado
    "SOC Received Tags",              
    "Pack / EHA (9/59/574) Timestamp",# Atualizado
    "Pack / EHA Status",              # Atualizado
]

# Etapa 2: Função para extrair status 8, 9, 574
def _extract_latest_soc_status(data: dict[str, Any]) -> dict[str, str]:
    tracking_list = data.get("tracking_list", [])
    if not isinstance(tracking_list, list):
        return {}

    all_events = []
    for tracking in tracking_list:
        if not isinstance(tracking, dict):
            continue
        all_events.append(tracking)
        children = tracking.get("children", [])
        if isinstance(children, list):
            for child in children:
                if isinstance(child, dict):
                    all_events.append(child)

    # Ordena cronologicamente
    all_events.sort(key=lambda x: _to_int(x.get("timestamp")) or 0)

    latest_8 = None
    latest_pack_eha = None

    # Busca reversa para pegar o último evento relevante
    for event in reversed(all_events):
        status = str(event.get("status"))
        # Procura por Packing (9), Return Packing (59) ou EHA (574)
        if not latest_8 and status in ("9", "59", "574"):
            if not latest_pack_eha:
                latest_pack_eha = event
        # Procura por SOC Received (8) ou Return SOC Received (58)
        elif status in ("8", "58"):
            latest_8 = event
            break

    result = {
        "station_name": "-",
        "status_8_ts": "-",
        "status_8_msg": "-",
        "tags": "-",
        "pack_eha_ts": "NÃO FOI PROCESSADO",
        "pack_eha_msg": "NÃO FOI PROCESSADO"
    }

    if latest_8:
        result["station_name"] = _to_str(latest_8.get("station_name"))
        result["status_8_ts"] = _format_ts(latest_8.get("timestamp"))
        result["status_8_msg"] = _to_str(latest_8.get("status"))
        result["tags"] = _extract_tag_value(latest_8)

    if latest_pack_eha:
        result["pack_eha_ts"] = _format_ts(latest_pack_eha.get("timestamp"))
        result["pack_eha_msg"] = _to_str(latest_pack_eha.get("status"))
        if not latest_8:
            result["station_name"] = _to_str(latest_pack_eha.get("station_name"))

    return result


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
    except (TypeError, ValueError):
        return None


def _format_ts(value: Any) -> str:
    ts = _to_int(value)
    if ts is None or ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=BRT).strftime("%d-%m-%Y %H:%M")
    except (ValueError, OSError, OverflowError):
        return ""


def _load_status_map() -> dict[str, str]:
    rows = read_sheet(
        ONLINE_SOC_TRACKING["base_status_spreadsheet_id"],
        ONLINE_SOC_TRACKING["base_status_tab"],
    )
    status_map: dict[str, str] = {}

    for row in rows:
        if len(row) < 2:
            continue

        name = _to_str(row[0])
        code = _to_str(row[1])
        if not name or not code:
            continue

        status_map[code] = name
        parsed = _to_int(code)
        if parsed is not None:
            status_map[str(parsed)] = name

    return status_map


def _read_order_ids() -> list[str]:
    rows = read_sheet(ONLINE_SOC_TRACKING["spreadsheet_id"], ONLINE_SOC_TRACKING["input_sheet_tab"])
    if not rows:
        return []

    order_ids: list[str] = []
    seen: set[str] = set()

    for idx, row in enumerate(rows):
        if not row:
            continue

        order_id = _to_str(row[0])
        if not order_id:
            continue

        if idx == 0 and order_id.lower() in {"order id", "order_id", "shipment_id"}:
            continue

        if order_id in seen:
            continue

        seen.add(order_id)
        order_ids.append(order_id)

    return order_ids


def _extract_target_event(data: dict[str, Any]) -> dict[str, Any]:
    tracking_list = data.get("tracking_list", [])
    if not isinstance(tracking_list, list) or not tracking_list:
        return {}

    last_tracking = tracking_list[-1] if isinstance(tracking_list[-1], dict) else {}
    children = last_tracking.get("children", []) if isinstance(last_tracking, dict) else []

    if isinstance(children, list) and children:
        last_child = children[-1]
        if isinstance(last_child, dict):
            return last_child

    return last_tracking if isinstance(last_tracking, dict) else {}


def _extract_spx_list(data: dict[str, Any]) -> list[str]:
    spx_values: list[str] = []

    for item in data.get("order_tag_info_list", []) or []:
        if not isinstance(item, dict):
            continue

        order_info = item.get("order_info", {})
        if not isinstance(order_info, dict):
            continue

        new_ids = order_info.get("new_shipment_id_list", [])
        if not isinstance(new_ids, list):
            continue

        for spx in new_ids:
            spx_str = _to_str(spx)
            if spx_str and spx_str.startswith("SPX"):
                spx_values.append(spx_str)

    if not spx_values:
        return [""]

    # Remove duplicados preservando ordem
    unique_spx: list[str] = []
    seen: set[str] = set()
    for spx in spx_values:
        if spx in seen:
            continue
        seen.add(spx)
        unique_spx.append(spx)

    return unique_spx


def _extract_tag_value(event: dict[str, Any]) -> str:
    tags = event.get("tags", []) if isinstance(event, dict) else []
    if not isinstance(tags, list) or not tags:
        return ""

    normalized = [_to_str(tag) for tag in tags if _to_str(tag)]
    if not normalized:
        return ""

    for preferred in ("Mass", "Single"):
        if preferred in normalized:
            return preferred

    return ", ".join(normalized)


def _extract_soc_received_tag(data: dict[str, Any], fallback_event: dict[str, Any]) -> str:
    """Busca tag no evento de SOC_Received (status 8/53 em SoC_MG_Betim)."""
    tracking_list = data.get("tracking_list", [])
    if isinstance(tracking_list, list):
        for tracking in reversed(tracking_list):
            if not isinstance(tracking, dict):
                continue

            children = tracking.get("children", [])
            if isinstance(children, list) and children:
                for child in reversed(children):
                    if not isinstance(child, dict):
                        continue
                    status = _to_str(child.get("status"))
                    station = _to_str(child.get("station_name"))
                    if status in {"8", "53"} and station == "SoC_MG_Betim":
                        found = _extract_tag_value(child)
                        if found:
                            return found

            status = _to_str(tracking.get("status"))
            station = _to_str(tracking.get("station_name"))
            if status in {"8", "53"} and station == "SoC_MG_Betim":
                found = _extract_tag_value(tracking)
                if found:
                    return found

    return _extract_tag_value(fallback_event)


def _fetch_tracking_info(session: Any, shipment_id: str) -> dict[str, Any] | None:
    max_retries = ONLINE_SOC_TRACKING["max_retries"]

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(ONLINE_SOC_TRACKING["api_url"], params={"shipment_id": shipment_id})
            if not isinstance(response, dict):
                raise ValueError(f"Resposta inesperada: {type(response)}")

            if response.get("retcode") != 0:
                return None

            data = response.get("data", {})
            return data if isinstance(data, dict) else None

        except Exception as exc:
            if attempt == max_retries:
                console.print(f"[red]❌ {shipment_id}: falha após {max_retries} tentativas ({exc})[/red]")
                return None
            time.sleep(1.2 * attempt)

    return None


def _build_rows_for_order(
    order_id: str,
    data: dict[str, Any],
    status_map: dict[str, str],
) -> list[list[str]]:
    event = _extract_target_event(data)

    status_code = _to_str(event.get("status"))
    status_desc = status_map.get(status_code, status_code)
    last_ts = _format_ts(event.get("timestamp"))

    soc_received_raw = data.get("current_station_received_time")
    soc_received = _format_ts(soc_received_raw)
    if not soc_received:
        soc_received = last_ts

    tag_value = _extract_soc_received_tag(data, event)
    spx_list = _extract_spx_list(data)

    soc_info = _extract_latest_soc_status(data)

    # Traduz status para descrição usando BASE STATUS
    status_8_desc = status_map.get(soc_info["status_8_msg"], soc_info["status_8_msg"])
    pack_eha_desc = status_map.get(soc_info["pack_eha_msg"], soc_info["pack_eha_msg"])

    rows: list[list[str]] = []
    for spx in spx_list:
        rows.append([
            order_id,
            soc_received,
            status_desc,
            last_ts,
            spx,
            status_code,
            tag_value,
            soc_info["station_name"],
            soc_info["status_8_ts"],
            status_8_desc,
            soc_info["tags"],
            soc_info["pack_eha_ts"],
            pack_eha_desc,
        ])

    return rows


def _process_single_order(
    index: int,
    order_id: str,
    session: Any,
    status_map: dict[str, str],
) -> tuple[int, list[list[str]]]:
    data = _fetch_tracking_info(session, order_id)
    if not data:
        return index, []

    return index, _build_rows_for_order(order_id, data, status_map)


def run() -> tuple[Path | None, Path | None, int]:
    """Executa coleta de tracking_info e salva snapshot em raw_tracking_info."""
    console.print("[bold cyan]═══ Online SOC Tracking ═══[/bold cyan]")

    order_ids = _read_order_ids()
    if not order_ids:
        console.print("[yellow]⚠️ Nenhum Order ID encontrado na aba Online_SOC-MG2.[/yellow]")
        return None, None, 0

    status_map = _load_status_map()
    session = get_session()

    max_workers = int(ONLINE_SOC_TRACKING.get("max_workers", 8))
    max_workers = max(1, max_workers)
    batch_size = int(ONLINE_SOC_TRACKING.get("batch_size", 500))
    batch_size = max(1, batch_size)
    total_rows_written = 0

    header_ok = update_sheet(
        ONLINE_SOC_TRACKING["spreadsheet_id"],
        ONLINE_SOC_TRACKING["output_sheet_tab"],
        [OUTPUT_HEADERS],
        clear_first=True,
    )
    if not header_ok:
        console.print("[red]❌ Falha ao preparar aba raw_tracking_info.[/red]")
        return None, None, 0

    indexed_orders = list(enumerate(order_ids))
    total_batches = (len(indexed_orders) + batch_size - 1) // batch_size

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Consultando tracking_info...", total=len(indexed_orders))

        for batch_idx in range(total_batches):
            start = batch_idx * batch_size
            end = min(start + batch_size, len(indexed_orders))
            batch_orders = indexed_orders[start:end]

            indexed_rows: list[tuple[int, list[list[str]]]] = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(_process_single_order, idx, order_id, session, status_map)
                    for idx, order_id in batch_orders
                ]

                for future in as_completed(futures):
                    try:
                        indexed_rows.append(future.result())
                    except Exception as exc:
                        console.print(f"[red]❌ Falha no processamento paralelo: {exc}[/red]")
                    finally:
                        progress.update(task, advance=1)

            indexed_rows.sort(key=lambda item: item[0])
            batch_rows: list[list[str]] = []
            for _, rows in indexed_rows:
                batch_rows.extend(rows)

            if batch_rows:
                ok_append = append_sheet(
                    ONLINE_SOC_TRACKING["spreadsheet_id"],
                    ONLINE_SOC_TRACKING["output_sheet_tab"],
                    batch_rows,
                )
                if not ok_append:
                    console.print(
                        f"[red]❌ Falha ao salvar lote {batch_idx + 1}/{total_batches}. Execução interrompida.[/red]"
                    )
                    return None, None, total_rows_written

                total_rows_written += len(batch_rows)
                console.print(
                    f"[cyan]📦 Lote {batch_idx + 1}/{total_batches} salvo: {len(batch_rows)} linhas (acumulado: {total_rows_written})[/cyan]"
                )
            else:
                console.print(f"[yellow]📦 Lote {batch_idx + 1}/{total_batches} sem linhas válidas.[/yellow]")

    console.print(f"[bold green]✅ Snapshot salvo em raw_tracking_info: {total_rows_written} linhas[/bold green]")
    return None, None, total_rows_written


if __name__ == "__main__":
    run()
