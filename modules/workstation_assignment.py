"""
Atualiza as colunas Workstation nas abas configuradas da planilha operacional.

Lê a aba BASE WORKSTATION para obter a lista de estações válidas,
busca o histórico de alocação do dia corrente (equivalente ao horário
de Brasília) e preenche em BASE HC/DW o nome da workstation mais
recente por Ops ID.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Tuple

from rich.console import Console

from core.config import BRT, SPREADSHEET_ID
from core.session import get_session
from core.sheets import read_sheet, update_sheet_batch

console = Console()

API_URL = "/api/wfm/admin/workstation/assignment/history/list"
PAGE_SIZE = 200
OPS_ID_COL_INDEX = 1  # Coluna B (0-based)
HEADER_ROW = 1
BASE_WORKSTATION_SHEET = "BASE WORKSTATION"
BASE_WORKSTATION_ID_COL_INDEX = 0  # Coluna B (quando range é B:C)
BASE_WORKSTATION_NAME_COL_INDEX = 1  # Coluna C (quando range é B:C)


@dataclass(frozen=True)
class SheetConfig:
    sheet_name: str
    target_column_letter: str
    ops_id_col_index: int = OPS_ID_COL_INDEX


TARGET_SHEETS: Tuple[SheetConfig, ...] = (
    SheetConfig(sheet_name="BASE HC", target_column_letter="F"),
    SheetConfig(sheet_name="BASE DW", target_column_letter="F"),
)


def _build_time_range(target_date: datetime | None = None) -> Tuple[int, int]:
    """Retorna timestamps Unix (UTC) para o dia inteiro informado."""
    ref = target_date.astimezone(BRT) if target_date else datetime.now(BRT)
    start = ref.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    start_ts = int(start.astimezone(timezone.utc).timestamp())
    end_ts = int(end.astimezone(timezone.utc).timestamp())
    return start_ts, end_ts


def _extract_items(payload: dict) -> List[dict]:
    """Garante que retornaremos uma lista de registros independente da chave usada."""
    candidate = payload.get("data", payload)
    if isinstance(candidate, dict):
        for key in (
            "list",
            "assignment_list",
            "assignment_history_list",
            "records",
        ):
            value = candidate.get(key)
            if isinstance(value, list):
                return value
    if isinstance(candidate, list):
        return candidate
    return []


def _get_total(payload: dict) -> int:
    candidate = payload.get("data", payload)
    if isinstance(candidate, dict):
        for key in ("total", "count", "total_count"):
            value = candidate.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
    return 0


def _get_assignment_timestamp(item: dict) -> int:
    for key in (
        "assignment_time",
        "assign_time",
        "start_time",
        "ctime",
        "timestamp",
        "update_time",
    ):
        value = item.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _column_letter_to_index(letter: str) -> int:
    result = 0
    for char in letter.strip().upper():
        if not char.isalpha():
            continue
        result = result * 26 + (ord(char) - ord("A") + 1)
    return max(result - 1, 0)


def _column_index_to_letter(index: int) -> str:
    index = max(index, 0)
    result = ""
    while index >= 0:
        index, remainder = divmod(index, 26)
        result = chr(ord("A") + remainder) + result
        index -= 1
    return result


def _fetch_assignments(session, start_ts: int, end_ts: int) -> List[dict]:
    records: List[dict] = []
    page = 1

    while True:
        params = {
            "start_time": start_ts,
            "end_time": end_ts,
            "pageno": page,
            "count": PAGE_SIZE,
        }
        response = session.get(API_URL, params=params)
        retcode = response.get("retcode", 0)
        if retcode != 0:
            console.print(
                f"[red]❌ API retornou retcode {retcode}: {response.get('message', 'erro desconhecido')}[/red]"
            )
            break

        page_items = _extract_items(response)
        if not page_items:
            break

        records.extend(page_items)
        total = _get_total(response)
        if total and len(records) >= total:
            break

        if len(page_items) < PAGE_SIZE:
            break

        page += 1

    console.print(f"[green]Dados coletados da API (histórico geral): {len(records)} registros[/green]")
    return records


def _normalize_workstation_name(name: str) -> str:
    return " ".join(str(name).strip().split()).lower()


def _load_base_workstations() -> Dict[str, str]:
    """Carrega workstations válidas da aba BASE WORKSTATION (chave normalizada -> nome original)."""
    # Lê apenas as colunas realmente usadas (B:C) para reduzir latência da API.
    rows = read_sheet(SPREADSHEET_ID, f"'{BASE_WORKSTATION_SHEET}'!B:C")
    allowed: Dict[str, str] = {}

    for idx, row in enumerate(rows, start=1):
        if idx == HEADER_ROW:
            continue

        workstation_name = (
            str(row[BASE_WORKSTATION_NAME_COL_INDEX]).strip()
            if len(row) > BASE_WORKSTATION_NAME_COL_INDEX
            else ""
        )
        workstation_id = (
            str(row[BASE_WORKSTATION_ID_COL_INDEX]).strip()
            if len(row) > BASE_WORKSTATION_ID_COL_INDEX
            else ""
        )
        if not workstation_name:
            continue

        normalized = _normalize_workstation_name(workstation_name)
        if not normalized:
            continue

        allowed.setdefault(normalized, workstation_name)

        # Mantém vínculo implícito com a linha de origem; útil para validação operacional.
        if not workstation_id:
            console.print(
                f"[yellow]⚠️ {BASE_WORKSTATION_SHEET} linha {idx}: Workstation Name '{workstation_name}' sem Workstation ID.[/yellow]"
            )

    return allowed


def _filter_target_assignments(records: List[dict], allowed_workstations: Dict[str, str]) -> Dict[str, Tuple[int, str]]:
    """Retorna a workstation válida mais recente por operador."""
    filtered: Dict[str, Tuple[int, str]] = {}
    allowed_keys: Set[str] = set(allowed_workstations.keys())

    for item in records:
        ops_id = str(item.get("ops_id", "")).strip()
        if not ops_id:
            continue

        workstation_name = str(item.get("workstation_name", "")).strip()
        workstation_key = _normalize_workstation_name(workstation_name)
        if workstation_key not in allowed_keys:
            continue

        timestamp = _get_assignment_timestamp(item)
        cached = filtered.get(ops_id)
        if cached is None or timestamp >= cached[0]:
            filtered[ops_id] = (timestamp, allowed_workstations[workstation_key])

    return filtered


def _load_sheet_ops(config: SheetConfig) -> Tuple[Dict[str, int], Dict[int, str], Dict[int, str]]:
    """Mapeia Ops IDs para índices de linha, linhas para Ops ID e valores atuais."""
    target_col_index = _column_letter_to_index(config.target_column_letter)
    column_indexes = [config.ops_id_col_index, target_col_index]
    max_index = max(column_indexes)
    range_end_letter = _column_index_to_letter(max_index)
    range_name = f"'{config.sheet_name}'!A:{range_end_letter}"
    rows = read_sheet(SPREADSHEET_ID, range_name)
    ops_map: Dict[str, int] = {}
    row_ops: Dict[int, str] = {}
    col_values: Dict[int, str] = {}

    for idx, row in enumerate(rows, start=1):
        if idx == HEADER_ROW:
            continue

        ops_val = row[config.ops_id_col_index] if len(row) > config.ops_id_col_index else ""
        ops_id = str(ops_val).strip()
        if not ops_id:
            continue

        ops_map.setdefault(ops_id, idx)
        row_ops[idx] = ops_id

        current_value = row[target_col_index] if len(row) > target_col_index else ""
        col_values[idx] = str(current_value)

    return ops_map, row_ops, col_values


def _apply_updates(sheet_config: SheetConfig, updates: List[Tuple[int, str]]) -> None:
    if not updates:
        return

    sorted_updates = sorted(updates, key=lambda item: item[0])
    block_start = None
    block_values: List[str] = []
    previous_row = None
    batch_ranges: List[Dict[str, List[List[str]]]] = []

    for row_idx, value in sorted_updates:
        if block_start is None:
            block_start = row_idx
            block_values = [value]
            previous_row = row_idx
            continue

        if previous_row is not None and row_idx == previous_row + 1:
            block_values.append(value)
        else:
            batch_ranges.append(_build_block_payload(sheet_config, block_start, block_values))
            block_start = row_idx
            block_values = [value]

        previous_row = row_idx

    if block_start is not None:
        batch_ranges.append(_build_block_payload(sheet_config, block_start, block_values))

    update_sheet_batch(SPREADSHEET_ID, batch_ranges)


def _build_block_payload(sheet_config: SheetConfig, start_row: int, values: List[str]) -> Dict[str, List[List[str]]]:
    end_row = start_row + len(values) - 1
    column_letter = sheet_config.target_column_letter
    if len(values) == 1:
        range_name = f"'{sheet_config.sheet_name}'!{column_letter}{start_row}"
    else:
        range_name = (
            f"'{sheet_config.sheet_name}'!{column_letter}{start_row}:{column_letter}{end_row}"
        )

    payload = [[val] for val in values]
    return {"range": range_name, "values": payload}


def run(target_date: datetime | None = None) -> Tuple[None, None, int]:
    console.print("[bold cyan]═══ Workstation (Planilhas) ═══[/bold cyan]")

    allowed_workstations = _load_base_workstations()
    if not allowed_workstations:
        console.print(
            f"[red]❌ Nenhuma workstation válida encontrada em '{BASE_WORKSTATION_SHEET}'.[/red]"
        )
        return None, None, 0

    console.print(
        f"[cyan]Workstations válidas carregadas de '{BASE_WORKSTATION_SHEET}': {len(allowed_workstations)}[/cyan]"
    )

    start_ts, end_ts = _build_time_range(target_date)
    session = get_session()
    records = _fetch_assignments(session, start_ts, end_ts)
    if not records:
        console.print(
            "[yellow]⚠️ Nenhum histórico retornado para o período informado. Validando marcações existentes...[/yellow]"
        )

    assignments = _filter_target_assignments(records, allowed_workstations) if records else {}
    if not assignments:
        console.print(
            "[yellow]⚠️ Nenhuma alocação recente em workstations válidas neste período. Validando e limpando valores inconsistentes, se houver.[/yellow]"
        )

    total_updates = 0

    for sheet in TARGET_SHEETS:
        console.print(
            f"[bold cyan]── Atualizando {sheet.sheet_name} (coluna {sheet.target_column_letter}) ──[/bold cyan]"
        )
        ops_map, row_ops, current_values = _load_sheet_ops(sheet)
        filled_cells = sum(1 for value in current_values.values() if str(value).strip())
        console.print(
            f"[dim]{sheet.sheet_name}: {filled_cells} linhas já preenchidas na coluna {sheet.target_column_letter}.[/dim]"
        )

        updates: Dict[int, str] = {}
        missing_ops: List[str] = []
        filled_count = 0
        replaced_count = 0
        cleared_count = 0

        assignment_ops = set(assignments.keys())
        for ops_id in assignment_ops:
            if ops_id not in ops_map:
                missing_ops.append(ops_id)

        for row_idx, ops_id in row_ops.items():
            current_value = str(current_values.get(row_idx, "")).strip()
            assignment = assignments.get(ops_id)
            expected_value = assignment[1] if assignment else ""

            if current_value == expected_value:
                continue

            updates[row_idx] = expected_value
            if expected_value and not current_value:
                filled_count += 1
            elif expected_value and current_value:
                replaced_count += 1
            elif not expected_value and current_value:
                cleared_count += 1

        if not updates:
            console.print(
                f"[green]{sheet.sheet_name}: nenhuma atualização necessária na coluna {sheet.target_column_letter}.[/green]"
            )
        else:
            update_list = list(updates.items())
            _apply_updates(sheet, update_list)
            parts: List[str] = []
            if filled_count:
                parts.append(f"{filled_count} preenchimentos")
            if replaced_count:
                parts.append(f"{replaced_count} substituições")
            if cleared_count:
                parts.append(f"{cleared_count} limpezas")
            detail = f" ({', '.join(parts)})" if parts else ""
            console.print(
                f"[green]✅ {sheet.sheet_name}: {len(update_list)} linhas atualizadas{detail}.[/green]"
            )
            total_updates += len(update_list)

        if missing_ops:
            console.print(
                f"[yellow]{sheet.sheet_name}: Ops IDs sem correspondência: {', '.join(sorted(set(missing_ops)))}[/yellow]"
            )

    if total_updates == 0:
        console.print("[green]Nenhuma atualização necessária nas abas configuradas.[/green]")
    else:
        console.print(f"[bold green]Total de atualizações realizadas: {total_updates}[/bold green]")

    return None, None, total_updates


if __name__ == "__main__":
    run()
