"""
Módulo de Extração: SPX Duplicados
===================================

Consulta dados de tracking para cada shipment_id e identifica
pedidos duplicados (tags "Determined Duplicate Order" / "New SPXTN").

Extrai: Input BR, Operador, Data, Hora, BR Original, Novo SPX.

A lista de shipment_ids é lida da aba BR_UNICO_conversao (coluna A).
Apenas BRs que NÃO existem na coluna A (input_br) do SPX DUPLICADO são processados.
Novos resultados são inseridos no topo (A2:F2) sem sobrescrever dados existentes.

Operação: GET de SPX Duplicados.
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn, MofNCompleteColumn

from core.config import BRT, SPX_DUPLICADOS, SPREADSHEET_ID
from core.session import get_session

console = Console()

MODULE_NAME = "spx_duplicados"

# Tags que indicam duplicação
DUPLICATE_TAGS = {"Determined Duplicate Order", "New SPXTN"}

# Status de log que indicam evento de duplicação
DUPLICATE_LOG_STATUSES = {650, 646}


def fetch_new_shipment_ids() -> list[str]:
    """
    Lê os BRs da aba BR_UNICO_conversao (coluna A) e compara com:
      - BRs já processados na aba SPX DUPLICADO (coluna E)
      - BRs já marcados na aba FORA DE ESTAÇÃO (coluna A)

    Retorna apenas os BRs novos que ainda não foram processados nem excluídos.
    """
    from core.sheets import read_sheet

    # 1. Ler todos os BRs de BR_UNICO_conversao
    console.print("[cyan]Lendo BRs de: BR_UNICO_conversao...[/cyan]")
    input_rows = read_sheet(SPREADSHEET_ID, "'BR_UNICO_conversao'!A:A")

    if not input_rows:
        console.print("[yellow]⚠️ Aba BR_UNICO_conversao vazia.[/yellow]")
        return []

    all_brs: list[str] = []
    for i, row in enumerate(input_rows):
        if i == 0:
            continue
        if row and row[0] and str(row[0]).strip():
            val = str(row[0]).strip()
            if val.startswith(("BR", "SPXBR")):
                all_brs.append(val)

    console.print(f"[green]  → {len(all_brs)} BRs em BR_UNICO_conversao[/green]")

    # 2. Ler BRs já processados do SPX DUPLICADO (coluna E = br_original)
    console.print("[cyan]Lendo br_original já processados de: SPX DUPLICADO...[/cyan]")
    output_rows = read_sheet(SPREADSHEET_ID, "'SPX DUPLICADO'!E:E")

    processed_brs: set[str] = set()
    if output_rows:
        for i, row in enumerate(output_rows):
            if i == 0:
                continue
            if row and row[0] and str(row[0]).strip():
                val = str(row[0]).strip()
                if val.startswith(("BR", "SPXBR")):
                    processed_brs.add(val)

    # 3. Ler BRs já marcados como FORA DE ESTAÇÃO
    console.print("[cyan]Lendo BRs excluídos de: FORA DE ESTAÇÃO...[/cyan]")
    fora_rows = read_sheet(SPREADSHEET_ID, "'FORA DE ESTAÇÃO'!A:A")

    fora_brs: set[str] = set()
    if fora_rows:
        for i, row in enumerate(fora_rows):
            if i == 0:
                continue
            if row and row[0] and str(row[0]).strip():
                val = str(row[0]).strip()
                if val.startswith(("BR", "SPXBR")):
                    fora_brs.add(val)

    # 4. Filtrar: apenas BRs novos (não processados e não excluídos)
    excluded = processed_brs | fora_brs
    new_brs = [br for br in all_brs if br not in excluded]

    console.print(f"[green]  → {len(processed_brs)} processados, {len(fora_brs)} fora de estação, {len(new_brs)} novos[/green]")
    return new_brs


def process_single_shipment(session, shipment_id: str) -> tuple[list[list], Optional[str]]:
    """
    Consulta a API tracking_info para um shipment_id e extrai dados de duplicação.

    Args:
        session: Instância de SPXSession autenticada.
        shipment_id: ID do shipment a consultar.

    Returns:
        Um par (resultados, error_type). Se sucesso, error_type é None.
    """
    api_url = SPX_DUPLICADOS["api_url"]
    max_retries = SPX_DUPLICADOS["max_retries"]

    tentativas = 0
    json_data = None

    while tentativas < max_retries:
        try:
            response = session.get(api_url, params={"shipment_id": shipment_id})

            if not isinstance(response, dict):
                raise ValueError(f"Resposta inesperada: {type(response)}")

            retcode = response.get("retcode", -1)

            if retcode == 0:
                json_data = response
                break

            # Retcode != 0 → pedido provavelmente não pertence à estação.
            return [], "FORA_ESTACAO"

        except Exception:
            tentativas += 1
            if tentativas >= max_retries:
                console.print(f"[dim]  ⏭️ {shipment_id}: ignorado após {max_retries} tentativas[/dim]")
                return [], "ERRO_CONEXAO"
            time.sleep(1.5 * tentativas)

    # --- Processamento dos dados ---
    data = json_data.get("data", {}) if json_data else {}
    original_id = data.get("shipment_id", shipment_id)

    # 1. Extrair novos IDs das tags de duplicação
    novos_ids: list[str] = []
    order_tag_list = data.get("order_tag_info_list", [])
    for tag in order_tag_list:
        tag_name = tag.get("tag_name", "")
        if tag_name in DUPLICATE_TAGS:
            order_info = tag.get("order_info", {})
            new_ids = order_info.get("new_shipment_id_list", [])
            novos_ids.extend(new_ids)

    # 2. Extrair logs "New SPXTN" (status 650) do nível de topo do tracking_list
    tracking_list = data.get("tracking_list", [])
    new_spxtn_logs = []

    if tracking_list:
        for log in tracking_list:
            if log.get("message", "") == "New SPXTN" and log.get("status", 0) == 650:
                new_spxtn_logs.append(log)

    # 3. Montar resultados (todas as colunas A-F)
    resultados: list[list] = []

    if not novos_ids:
        log_operator = "-"
        data_br = "-"
        hora_br = "-"
        if new_spxtn_logs:
            log_evento = new_spxtn_logs[-1]
            if log_evento.get("timestamp"):
                log_operator = log_evento.get("operator", "Sistema")
                dt_obj = datetime.fromtimestamp(log_evento["timestamp"], tz=BRT)
                data_br = dt_obj.strftime("%d/%m/%Y")
                hora_br = dt_obj.strftime("%H:%M:%S")
        resultados.append([shipment_id, log_operator, data_br, hora_br, original_id, "Nenhum"])
    else:
        for idx, novo_id in enumerate(novos_ids):
            log_operator = "-"
            data_br = "-"
            hora_br = "-"

            # Pareia o índice do novo_id com o log "New SPXTN" correspondente
            log_evento = None
            if idx < len(new_spxtn_logs):
                log_evento = new_spxtn_logs[idx]
            elif new_spxtn_logs:
                log_evento = new_spxtn_logs[-1]  # fallback se faltar log

            if log_evento and log_evento.get("timestamp"):
                log_operator = log_evento.get("operator", "Sistema")
                dt_obj = datetime.fromtimestamp(log_evento["timestamp"], tz=BRT)
                data_br = dt_obj.strftime("%d/%m/%Y")
                hora_br = dt_obj.strftime("%H:%M:%S")

            resultados.append([shipment_id, log_operator, data_br, hora_br, original_id, novo_id])

    return resultados, None


def run() -> tuple[Optional[Path], Optional[Path], int]:
    """
    Executa extração completa de SPX Duplicados.
    """
    console.print("[bold cyan]═══ SPX Duplicados ═══[/bold cyan]")

    # 1. Buscar BRs novos (já exclui processados e fora de estação)
    shipment_ids = fetch_new_shipment_ids()

    if not shipment_ids:
        console.print("[yellow]⚠️ Nenhum novo BR para processar.[/yellow]")
        return None, None, 0

    # 2. Processar cada shipment_id
    session = get_session()
    all_results: list[list] = []
    fora_estacao_brs: list[list] = []
    delay = SPX_DUPLICADOS["delay_between_requests"]
    skipped = 0
    now_str = datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Processando duplicados...", total=len(shipment_ids))

        for i, sid in enumerate(shipment_ids):
            try:
                resultados, error_type = process_single_shipment(session, sid)

                if error_type == "FORA_ESTACAO":
                    fora_estacao_brs.append([sid, now_str])
                    skipped += 1
                elif error_type == "ERRO_CONEXAO":
                    skipped += 1
                elif resultados:
                    all_results.extend(resultados)
                else:
                    skipped += 1

            except Exception as e:
                console.print(f"[red]❌ Erro inesperado em {sid}: {e}[/red]")
                skipped += 1

            progress.update(task, advance=1, description=f"Processados: {i + 1}/{len(shipment_ids)}")

            if i < len(shipment_ids) - 1:
                time.sleep(delay)

    # 3. Salvar BRs fora de estação na aba dedicada
    if fora_estacao_brs:
        from core.sheets import append_sheet, read_sheet as _read

        # Criar header se a aba estiver vazia
        existing = _read(SPREADSHEET_ID, "'FORA DE ESTAÇÃO'!A1:B1")
        if not existing:
            from core.sheets import update_sheet
            update_sheet(SPREADSHEET_ID, "'FORA DE ESTAÇÃO'!A1", [["BR", "Data Verificação"]], clear_first=False)

        console.print(f"[cyan]Salvando {len(fora_estacao_brs)} BRs na aba 'FORA DE ESTAÇÃO'...[/cyan]")
        append_sheet(SPREADSHEET_ID, "'FORA DE ESTAÇÃO'!A:B", fora_estacao_brs)

    # 4. Resumo final
    console.print(f"\n[bold green]✅ TOTAL: {len(all_results)} registros ({skipped} ignorados, {len(fora_estacao_brs)} fora de estação)[/bold green]")

    if not all_results:
        return None, None, 0

    # 5. Inserir no topo do SPX DUPLICADO (A2:F2)
    from core.sheets import insert_rows_at_top
    console.print(f"[cyan]Inserindo {len(all_results)} linhas no topo de SPX DUPLICADO...[/cyan]")
    insert_rows_at_top(
        SPREADSHEET_ID,
        "SPX DUPLICADO",
        all_results,
        start_col="A",
        end_col="F"
    )

    return None, None, len(all_results)


if __name__ == "__main__":
    run()
