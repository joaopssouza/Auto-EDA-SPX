"""
Módulo de Extração: Status dos SPX Duplicados
===============================================

Consulta em batch (POST, até 50 IDs por vez)
para obter o order_status atual dos SPX que foram gerados como duplicados.

A lista de IDs vem da coluna F da aba SPX DUPLICADO (novo_spx).
Apenas IDs que NÃO existem na aba BASE STATUS DUP (coluna A) são processados.

Operação: POST de Status dos Duplicados.
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn, MofNCompleteColumn

from core.config import BRT, STATUS_DUPLICADOS, SPREADSHEET_ID
from core.session import get_session

console = Console()

MODULE_NAME = "status_duplicados"


def load_status_map() -> dict[int, str]:
    """
    Lê a aba BASE STATUS e retorna um dicionário {código: nome_do_status}.
    Coluna A = status (nome), Coluna B = codigo (int).
    """
    from core.sheets import read_sheet

    rows = read_sheet(SPREADSHEET_ID, "'BASE STATUS'!A:B")
    status_map: dict[int, str] = {}

    if rows:
        for i, row in enumerate(rows):
            if i == 0:
                continue  # header
            if len(row) >= 2 and row[1]:
                try:
                    code = int(str(row[1]).strip())
                    name = str(row[0]).strip()
                    status_map[code] = name
                except (ValueError, TypeError):
                    continue

    return status_map


def fetch_new_spx_ids() -> list[str]:
    """
    Lê os novo_spx da coluna F do SPX DUPLICADO, filtra 'Nenhum',
    e exclui os já existentes na coluna A de BASE STATUS DUP.

    Retorna lista de IDs novos para consultar.
    """
    from core.sheets import read_sheet

    # 1. Ler novo_spx do SPX DUPLICADO (coluna F)
    console.print("[cyan]Lendo novo_spx de: SPX DUPLICADO (col F)...[/cyan]")
    spx_rows = read_sheet(SPREADSHEET_ID, "'SPX DUPLICADO'!F:F")

    if not spx_rows:
        console.print("[yellow]⚠️ Aba SPX DUPLICADO vazia.[/yellow]")
        return []

    all_spx: list[str] = []
    for i, row in enumerate(spx_rows):
        if i == 0:
            continue  # header
        if row and row[0] and str(row[0]).strip():
            val = str(row[0]).strip()
            if val.startswith(("BR", "SPXBR")):
                all_spx.append(val)

    # Remover duplicados mantendo a ordem
    all_spx = list(dict.fromkeys(all_spx))
    console.print(f"[green]  → {len(all_spx)} SPXs únicos em SPX DUPLICADO[/green]")

    # 2. Ler já processados de BASE STATUS DUP (coluna A)
    console.print("[cyan]Lendo já processados de: BASE STATUS DUP...[/cyan]")
    status_rows = read_sheet(SPREADSHEET_ID, "'BASE STATUS DUP'!A:A")

    processed: set[str] = set()
    if status_rows:
        for i, row in enumerate(status_rows):
            if i == 0:
                continue
            if row and row[0] and str(row[0]).strip():
                processed.add(str(row[0]).strip())

    # 3. Filtrar novos
    new_spx = [s for s in all_spx if s not in processed]

    console.print(f"[green]  → {len(processed)} já processados, {len(new_spx)} novos[/green]")
    return new_spx


def fetch_status_batch(session, ids: list[str], status_map: dict[int, str]) -> list[list]:
    """
    Faz POST na API tracking_list/search com uma lista de IDs (máx 50).

    Retorna lista de [shipment_id, order_status, status_nome] para cada item.
    """
    api_url = STATUS_DUPLICADOS["api_url"]

    payload = {
        "count": STATUS_DUPLICADOS["page_size"],
        "search_id_list": ids,
        "page_no": 1,
    }

    response = session.post(api_url, json_data=payload)

    if not isinstance(response, dict):
        raise ValueError(f"Resposta inesperada: {type(response)}")

    retcode = response.get("retcode", -1)
    if retcode != 0:
        raise ValueError(f"API retornou retcode {retcode}: {response.get('message', '')}")

    data = response.get("data", {})
    items = data.get("list", [])

    resultados: list[list] = []
    for item in items:
        sid = item.get("shipment_id", "")
        status_code = item.get("order_status", "")
        status_nome = status_map.get(int(status_code), "Desconhecido") if status_code != "" else "-"
        resultados.append([sid, status_code, status_nome])

    return resultados


def run() -> tuple[Optional[Path], Optional[Path], int]:
    """
    Executa consulta de status dos SPX duplicados.
    """
    console.print("[bold cyan]═══ Status Duplicados ═══[/bold cyan]")

    # 1. Buscar SPX IDs novos
    spx_ids = fetch_new_spx_ids()

    if not spx_ids:
        console.print("[yellow]⚠️ Nenhum novo SPX para consultar status.[/yellow]")
        return None, None, 0

    # 2. Carregar mapa de status
    console.print("[cyan]Carregando mapa de status de: BASE STATUS...[/cyan]")
    status_map = load_status_map()
    console.print(f"[green]  → {len(status_map)} códigos de status carregados[/green]")

    # 3. Dividir em chunks de 50
    chunk_size = STATUS_DUPLICADOS["page_size"]
    chunks = [spx_ids[i:i + chunk_size] for i in range(0, len(spx_ids), chunk_size)]

    # 4. Processar cada chunk
    session = get_session()
    all_results: list[list] = []
    errors = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Consultando status...", total=len(chunks))

        for i, chunk in enumerate(chunks):
            try:
                resultados = fetch_status_batch(session, chunk, status_map)
                all_results.extend(resultados)
            except Exception as e:
                console.print(f"[red]❌ Erro no chunk {i + 1}: {e}[/red]")
                errors += 1

            progress.update(task, advance=1, description=f"Chunks: {i + 1}/{len(chunks)}")

            if i < len(chunks) - 1:
                time.sleep(0.5)

    # 6. Resumo
    console.print(f"\n[bold green]✅ TOTAL: {len(all_results)} registros ({errors} chunks com erro)[/bold green]")

    if not all_results:
        return None, None, 0

    # 7. Criar header se necessário e inserir no topo
    from core.sheets import read_sheet, insert_rows_at_top, update_sheet

    existing = read_sheet(SPREADSHEET_ID, "'BASE STATUS DUP'!A1:C1")
    if not existing:
        update_sheet(SPREADSHEET_ID, "'BASE STATUS DUP'!A1", [["shipment_id", "order_status", "status_nome"]], clear_first=False)

    console.print(f"[cyan]Inserindo {len(all_results)} linhas no topo de BASE STATUS DUP...[/cyan]")
    insert_rows_at_top(
        SPREADSHEET_ID,
        "BASE STATUS DUP",
        all_results,
        start_col="A",
        end_col="C"
    )

    return None, None, len(all_results)


def run_refresh() -> tuple[Optional[Path], Optional[Path], int]:
    """
    Re-verifica o status de TODOS os SPXs já existentes na aba BASE STATUS DUP.
    Atualiza as colunas B (order_status) e C (status_nome) in-place.
    """
    from core.sheets import read_sheet, batch_update_values

    console.print("[bold cyan]═══ Status Duplicados (Refresh) ═══[/bold cyan]")

    # 1. Ler todos os IDs existentes com suas linhas
    console.print("[cyan]Lendo SPXs existentes de: BASE STATUS DUP...[/cyan]")
    rows = read_sheet(SPREADSHEET_ID, "'BASE STATUS DUP'!A:A")

    if not rows or len(rows) <= 1:
        console.print("[yellow]⚠️ Aba BASE STATUS DUP vazia, nada para atualizar.[/yellow]")
        return None, None, 0

    # Mapear shipment_id → row_number (1-indexed, pula header)
    spx_row_map: dict[str, int] = {}
    for i, row in enumerate(rows):
        if i == 0:
            continue
        if row and row[0] and str(row[0]).strip():
            val = str(row[0]).strip()
            if val.startswith(("BR", "SPXBR")):
                spx_row_map[val] = i + 1  # linha no Sheets (1-indexed)

    spx_ids = list(spx_row_map.keys())
    console.print(f"[green]  → {len(spx_ids)} SPXs para re-verificar[/green]")

    if not spx_ids:
        return None, None, 0

    # 2. Carregar mapa de status
    console.print("[cyan]Carregando mapa de status de: BASE STATUS...[/cyan]")
    status_map = load_status_map()

    # 3. Consultar API em chunks
    chunk_size = STATUS_DUPLICADOS["page_size"]
    chunks = [spx_ids[i:i + chunk_size] for i in range(0, len(spx_ids), chunk_size)]

    session = get_session()
    updates: list[dict] = []
    errors = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Atualizando status...", total=len(chunks))

        for i, chunk in enumerate(chunks):
            try:
                resultados = fetch_status_batch(session, chunk, status_map)

                for sid, status_code, status_nome in resultados:
                    row_num = spx_row_map.get(sid)
                    if row_num:
                        updates.append({
                            'range': f"'BASE STATUS DUP'!B{row_num}:C{row_num}",
                            'values': [[status_code, status_nome]]
                        })
            except Exception as e:
                console.print(f"[red]❌ Erro no chunk {i + 1}: {e}[/red]")
                errors += 1

            progress.update(task, advance=1, description=f"Chunks: {i + 1}/{len(chunks)}")

            if i < len(chunks) - 1:
                time.sleep(0.5)

    # 4. Aplicar atualizações em batch
    if updates:
        console.print(f"[cyan]Atualizando {len(updates)} registros na BASE STATUS DUP...[/cyan]")
        batch_update_values(SPREADSHEET_ID, updates)

    console.print(f"\n[bold green]✅ REFRESH: {len(updates)} status atualizados ({errors} chunks com erro)[/bold green]")

    return None, None, len(updates)


if __name__ == "__main__":
    run()
