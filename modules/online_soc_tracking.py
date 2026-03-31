from __future__ import annotations

def _read_order_ids_from_query_base_soc() -> list[str]:
    """Lê Order IDs da coluna BJ2:BJ da aba 'query BASE SOC'."""
    # BJ = coluna 62 (A=1, ..., Z=26, AA=27, ..., BJ=62)
    # O método read_sheet pode retornar linhas de comprimentos variados;
    # acessamos a célula 61 (0-based) com segurança.
    rows = read_sheet(
        ONLINE_SOC_TRACKING["spreadsheet_id"],
        "query BASE SOC",
    )

    if not rows:
        return []

    order_ids: list[str] = []
    seen: set[str] = set()

    for idx, row in enumerate(rows):
        if not isinstance(row, list):
            continue

        # pega o valor em BJ (índice 61) de forma segura
        order_id = _to_str(row[61]) if len(row) > 61 else ""

        # pula cabeçalho caso exista
        if idx == 0 and order_id.lower() in {"order id", "order_id", "shipment_id"}:
            continue

        if not order_id:
            continue

        if order_id in seen:
            continue

        seen.add(order_id)
        order_ids.append(order_id)

    return order_ids

def run_query_base_soc() -> tuple[Path | None, Path | None, int]:
    """Executa coleta de tracking_info para Order IDs da query BASE SOC e salva snapshot em raw_tracking_info_RF."""
    console.print("[bold cyan]═══ Online SOC Tracking (query BASE SOC) ═══[/bold cyan]")

    order_ids = _read_order_ids_from_query_base_soc()
    console.print(f"[blue]🔎 {len(order_ids)} Order IDs lidos da coluna BJ da aba query BASE SOC.[/blue]")
    if order_ids:
        sample = ", ".join(order_ids[:5])
        console.print(f"[blue]🔎 Amostra (até 5): {sample}[/blue]")
    else:
        console.print("[yellow]⚠️ Nenhum Order ID encontrado na coluna BJ da aba query BASE SOC.[/yellow]")
        return None, None, 0

    status_map = _load_status_map()
    session = get_session()

    max_workers = int(ONLINE_SOC_TRACKING.get("max_workers", 8))
    max_workers = max(1, max_workers)
    batch_size = int(ONLINE_SOC_TRACKING.get("batch_size", 500))
    batch_size = max(1, batch_size)

# Nova função: processa coluna A2:A e salva na aba 'raw_tracking_info'
    total_rows_written = 0

    # Prepara a aba: limpa A2:M e reduz a grade para manter apenas o cabeçalho
    prepared = prepare_sheet_for_append(
        ONLINE_SOC_TRACKING["spreadsheet_id"],
        "raw_tracking_info_RF",
        start_col="A",
        end_col="M",
        header_rows=1,
    )
    if not prepared:
        console.print("[red]❌ Falha ao preparar aba raw_tracking_info_RF.[/red]")
        return None, None, 0

    # Garante header (escreve em A1 sem limpar a aba inteira)
    header_ok = update_sheet(
        ONLINE_SOC_TRACKING["spreadsheet_id"],
        "raw_tracking_info_RF!A1",
        [OUTPUT_HEADERS],
        clear_first=False,
    )
    if not header_ok:
        console.print("[red]❌ Falha ao escrever header em raw_tracking_info_RF.[/red]")
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
                for row in rows:
                    # Salva qualquer linha com Order ID não vazio (mesmo se os outros campos estiverem vazios)
                    if row and row[0]:
                        batch_rows.append(row)

            if batch_rows:
                # Usa append (A-M) — pad/trunc para 13 colunas
                ok_append = append_sheet(
                    ONLINE_SOC_TRACKING["spreadsheet_id"],
                    "raw_tracking_info_RF!A1",
                    batch_rows,
                    num_cols=len(OUTPUT_HEADERS),
                )
                if not ok_append:
                    console.print(
                        f"[red]❌ Falha ao salvar lote {batch_idx + 1}/{total_batches} na raw_tracking_info_RF. Execução interrompida.[/red]"
                    )
                    return None, None, total_rows_written

                total_rows_written += len(batch_rows)
                console.print(
                    f"[cyan]📦 Lote {batch_idx + 1}/{total_batches} salvo: {len(batch_rows)} linhas (acumulado: {total_rows_written})[/cyan]"
                )
            else:
                console.print(f"[yellow]📦 Lote {batch_idx + 1}/{total_batches} sem linhas válidas.[/yellow]")

    console.print(f"[bold green]✅ Snapshot salvo em raw_tracking_info_RF: {total_rows_written} linhas[/bold green]")
    return None, None, total_rows_written

"""
Módulo: Online SOC Tracking
===========================

Lê Order IDs da aba Painel Online_SOC-MG2, consulta tracking_info por shipment_id
(equivale a tracking details), extrai o último children do último tracking_list,
expande SPXBR quando existir mais de um e grava snapshot em raw_tracking_info.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn

from core.config import BRT, ONLINE_SOC_TRACKING
from core.session import get_session
from core.sheets import (
    append_sheet,
    read_sheet,
    update_sheet,
    col_to_letter,
    trim_sheet_rows,
    prepare_sheet_for_append,
    cleanup_orphan_rows,
)

console = Console()

MODULE_NAME = "online_soc_tracking"

OUTPUT_HEADERS = [
    "Order ID",
    "HUB/SOC Received time",
    "Last Status",
    "Last Status Timestamp",
    "SPXBR",
    "Status Code",
    "Tag",
    "Latest HUB/SOC Station",             
    "HUB/SOC Received (1/10/8/58) Timestamp",  # Atualizado
    "HUB/SOC Received (1/10/8/58) Status",     # Atualizado
    "HUB/SOC Received Tags",              
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

    latest_8_1_10 = None
    latest_pack_eha = None
    last_status_event = all_events[-1] if all_events else None

    # Busca reversa para pegar o último evento relevante
    for event in reversed(all_events):
        status = str(event.get("status"))
        # Procura por Packing (9), Return Packing (59) ou EHA (574)
        if not latest_8_1_10 and status in ("9", "59", "574"):
            if not latest_pack_eha:
                latest_pack_eha = event
        # Procura por SOC/HUB Received (1, 10, 8, 58)
        elif status in ("1", "10", "8", "58"):
            latest_8_1_10 = event
            break

    # Nova lógica: buscar o último station_name diferente de SoC_MG_Betim com status 1,10,8,58,9,59,574
    latest_station_name = None
    for event in reversed(all_events):
        status = str(event.get("status"))
        station = _to_str(event.get("station_name"))
        if status in ("1", "10", "8", "58", "9", "59", "574") and station and station != "SoC_MG_Betim":
            latest_station_name = station
            break

    result = {
        "station_name": "-",
        "status_8_1_10_ts": "-",
        "status_8_1_10_msg": "-",
        "tags": "-",
        "pack_eha_ts": "NÃO FOI PROCESSADO",
        "pack_eha_msg": "NÃO FOI PROCESSADO"
    }

    if latest_8_1_10:
        result["status_8_1_10_ts"] = _format_ts(latest_8_1_10.get("timestamp"))
        result["status_8_1_10_msg"] = _to_str(latest_8_1_10.get("status"))
        result["tags"] = _extract_tag_value(latest_8_1_10)

    if latest_pack_eha:
        result["pack_eha_ts"] = _format_ts(latest_pack_eha.get("timestamp"))
        result["pack_eha_msg"] = _to_str(latest_pack_eha.get("status"))

    # Definir o station_name conforme a nova regra
    # Se o último evento não vazio for SoC_MG_Betim, prioriza-o. Caso contrário, usa latest_station_name quando existir.
    last_event_station = _to_str(last_status_event.get("station_name")) if last_status_event else ""
    if last_event_station == "SoC_MG_Betim":
        result["station_name"] = last_event_station
    elif latest_station_name:
        result["station_name"] = latest_station_name
    else:
        # Busca o último station_name não vazio de all_events
        last_non_empty_station = None
        for event in reversed(all_events):
            station = _to_str(event.get("station_name"))
            if station:
                last_non_empty_station = station
                break
        if last_non_empty_station:
            result["station_name"] = last_non_empty_station
        elif last_status_event:
            result["station_name"] = _to_str(last_status_event.get("station_name"))

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
    status_8_1_10_desc = status_map.get(soc_info["status_8_1_10_msg"], soc_info["status_8_1_10_msg"])
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
            soc_info["status_8_1_10_ts"],
            status_8_1_10_desc,
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
    # Primeiro, processa os Order IDs da aba 'query BASE SOC' (coluna BJ -> raw_tracking_info_RF)
    rf_path, rf_csv, rf_count = run_query_base_soc()

    console.print("[bold cyan]═══ Online SOC Tracking ═══[/bold cyan]")

    order_ids = _read_order_ids()
    if not order_ids:
        console.print("[yellow]⚠️ Nenhum Order ID encontrado na aba Painel Online_SOC-MG2.[/yellow]")
        # Retorna pelo menos o que foi salvo em RF
        return None, None, rf_count

    status_map = _load_status_map()
    session = get_session()

    max_workers = int(ONLINE_SOC_TRACKING.get("max_workers", 8))
    max_workers = max(1, max_workers)
    batch_size = int(ONLINE_SOC_TRACKING.get("batch_size", 500))
    batch_size = max(1, batch_size)
    total_rows_written = 0

    # Prepara a aba: limpa A2:M e reduz a grade para manter apenas o cabeçalho
    out_tab = ONLINE_SOC_TRACKING["output_sheet_tab"]
    sheet_title = out_tab.split("!")[0] if "!" in out_tab else out_tab
    if sheet_title.startswith("'") and sheet_title.endswith("'"):
        sheet_title = sheet_title[1:-1]

    prepared = prepare_sheet_for_append(
        ONLINE_SOC_TRACKING["spreadsheet_id"],
        sheet_title,
        start_col="A",
        end_col="M",
        header_rows=1,
    )
    if not prepared:
        console.print("[red]❌ Falha ao preparar aba raw_tracking_info.[/red]")
        return None, None, 0

    # Garante header (escreve em A1 sem limpar a aba inteira)
    header_ok = update_sheet(
        ONLINE_SOC_TRACKING["spreadsheet_id"],
        f"'{sheet_title}'!A1",
        [OUTPUT_HEADERS],
        clear_first=False,
    )
    if not header_ok:
        console.print("[red]❌ Falha ao escrever header em raw_tracking_info.[/red]")
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
                for row in rows:
                    if row and row[0] and any(cell for cell in row[1:]):
                        batch_rows.append(row)

            if batch_rows:
                # Usa append (A-M) — pad/trunc para 13 colunas
                ok_append = append_sheet(
                    ONLINE_SOC_TRACKING["spreadsheet_id"],
                    f"'{sheet_title}'!A1",
                    batch_rows,
                    num_cols=len(OUTPUT_HEADERS),
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

    total = rf_count + total_rows_written
    console.print(f"[bold green]✅ Snapshot salvo em raw_tracking_info: {total_rows_written} linhas (RF: {rf_count}, total: {total})[/bold green]")
    # Pós-processamento: remover linhas órfãs/vazias em todas as abas
    try:
        cleanup_orphan_rows(ONLINE_SOC_TRACKING["spreadsheet_id"])
    except Exception:
        console.print("[yellow]⚠️ Aviso: falha ao executar cleanup_orphan_rows (continuando).[/yellow]")

    return None, None, total


if __name__ == "__main__":
    # Executa primeiro BJ2:BJ (raw_tracking_info_RF), depois A2:A (raw_tracking_info)
    run_query_base_soc()
    run()
