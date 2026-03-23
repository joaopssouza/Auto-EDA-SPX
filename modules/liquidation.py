"""
Módulo de Extração: Liquidation (EO List - ER48)
=================================================

Extrai dados da API de Exception Order History com filtro exclusivo reason_id=ER48.
Salva: shipment_id, order_status, resolve_data, resolve_hora, related_order_id (sempre "-").

Filtragem em memória: apenas motivos ER48 com status=6 (Resolved).
Deduplicação por ID de log para garantir múltiplas liquidações do mesmo shipment.
"""

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from core.config import BRT, LIQUIDATION, MAX_PAGES, STATUS_DUPLICADOS
from core.save import save_data as core_save_data
from core.session import SessionExpiredError, get_session

console = Console()

MODULE_NAME = "liquidation"


def get_time_range(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> tuple[int, int]:
    """Calcula o range de timestamps Unix (BRT)."""
    if end_date is None:
        end_date = datetime.now(BRT)

    if start_date is None:
        start_date = end_date - timedelta(days=LIQUIDATION["days_ago"])
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    return int(start_date.timestamp()), int(end_date.timestamp())


def _load_status_map() -> dict[int, str]:
    """Carrega mapa {código: nome} da aba BASE STATUS."""
    from core.sheets import read_sheet
    from core.config import SPREADSHEET_ID

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


def _fetch_order_status_batch(session, shipment_ids: list[str]) -> dict[str, str]:
    """Busca order_status (código) para múltiplos shipment_ids de uma vez.

    Retorna dicionário {shipment_id: status_code (string)}.
    """
    from core.config import SPREADSHEET_ID

    api_url = STATUS_DUPLICADOS["api_url"]
    batch_size = 1000  # limite recomendado (até 5000)
    max_per_call = 5000

    result: dict[str, str] = {}
    # deduplicar
    unique_ids = list(dict.fromkeys(shipment_ids))

    for i in range(0, len(unique_ids), batch_size):
        chunk = unique_ids[i : i + batch_size]
        payload = {
            "count": len(chunk),
            "search_id_list": chunk,
            "page_no": 1,
        }
        try:
            resp = session.post(api_url, json_data=payload)
            if isinstance(resp, dict) and resp.get("retcode", -1) == 0:
                data = resp.get("data", {})
                items = data.get("list", [])
                for item in items:
                    sid = item.get("shipment_id", "")
                    status_code = item.get("order_status", "")
                    if sid:
                        result[sid] = str(status_code)
        except Exception:
            pass
        # pausa para evitar rate limit
        if i + batch_size < len(unique_ids):
            time.sleep(0.5)

    return result


def _ensure_liquidation_header() -> None:
    """Garante que a aba BASE Liquidation tenha o cabeçalho esperado."""
    from core.sheets import read_sheet, update_sheet
    from core.config import SPREADSHEET_ID

    header_row = read_sheet(SPREADSHEET_ID, "'BASE Liquidation'!A1:E1")
    if not header_row:
        update_sheet(
            SPREADSHEET_ID,
            "'BASE Liquidation'!A1",
            [["shipment_id", "order_status", "resolve_data", "resolve_hora", "related_order_id"]],
            clear_first=False,
        )


def _extract_fields(item: dict) -> Optional[dict]:
    """
    Extrai campos relevantes de um registro da API order/history.
    
    Filtragem: apenas ER48 + status=6 (Resolved).
    operator_time é o timestamp UNIX da resolução.
    
    Retorna None se não passar no filtro.
    """
    # Extrai motivo e status
    reason = item.get("reason", {})
    reason_id = str(reason.get("reason_id", ""))
    status = item.get("exception_order_status")
    
    # Filtro estrito
    if reason_id != LIQUIDATION["reason_id"] or status != 6:
        return None
    
    # Converte operator_time (UNIX timestamp) para data e hora BRT
    operator_time = item.get("operator_time", 0)
    if operator_time:
        dt_brt = datetime.fromtimestamp(operator_time, tz=BRT)
        resolve_data = dt_brt.strftime("%d/%m/%Y")
        resolve_hora = dt_brt.strftime("%H:%M:%S")
    else:
        resolve_data = ""
        resolve_hora = ""
    
    return {
        "shipment_id": item.get("shipment_id", ""),
        "order_status": "",  # Preenchido depois
        "resolve_data": resolve_data,
        "resolve_hora": resolve_hora,
        "related_order_id": "-",  # Sempre "-" conforme spec
    }


def fetch_liquidation_orders(
    session,
    start_date: datetime,
    end_date: datetime,
    count_per_page: int = None,
) -> list[dict]:
    """
    Busca dados de Exception Order History com filtro ER48 + status=6.
    
    Deduplicação: usa item.get("id") como chave para evitar duplicatas
    e permitir múltiplas liquidações do mesmo shipment_id.
    """
    count_per_page = count_per_page or LIQUIDATION["page_size"]
    start_ts, end_ts = get_time_range(start_date, end_date)

    console.print(f"[cyan]Buscando Liquidation (ER48)...[/cyan]")
    console.print(
        f"  Período: {datetime.fromtimestamp(start_ts, tz=BRT).strftime('%d/%m/%Y %H:%M')} "
        f"até {datetime.fromtimestamp(end_ts, tz=BRT).strftime('%d/%m/%Y %H:%M')}"
    )

    all_data: dict[str, dict] = {}  # Deduplicação por log ID
    page = 1
    search_after = None
    total_expected = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Extraindo...", total=None)

        while page <= MAX_PAGES:
            params = {
                "operator_time": f"{start_ts},{end_ts}",
                "pageno": page,
                "count": count_per_page,
            }

            if search_after:
                params["search_after"] = search_after

            try:
                data = session.get(LIQUIDATION["api_url"], params=params)

                if not isinstance(data, dict):
                    console.print(f"[red]Resposta inesperada: {type(data)}[/red]")
                    break

                retcode = data.get("retcode", -1)
                if retcode != 0:
                    console.print(f"[red]Erro API: {data.get('message', 'desconhecido')}[/red]")
                    break

                data_wrapper = data.get("data", {})
                items = data_wrapper.get("list", [])
                total_expected = data_wrapper.get("total", 0)
                search_after = data_wrapper.get("search_after")

                if not items:
                    break

                # Filtrar e desduplicar
                for item in items:
                    extracted = _extract_fields(item)
                    if extracted:  # Apenas ER48 + status 6
                        log_id = str(item.get("id", ""))
                        if log_id:
                            all_data[log_id] = extracted  # Dedup por ID

                progress.update(
                    task,
                    description=f"Página {page}: {len(all_data)} itens únicos",
                )

                if len(all_data) >= total_expected or not search_after:
                    break

                page += 1
                time.sleep(0.5)

            except SessionExpiredError:
                raise
            except Exception as e:
                console.print(f"[red]❌ Erro na página {page}: {e}[/red]")
                break

    result = list(all_data.values())
    console.print(f"[green]  → {len(result)} registros extraídos (desduplicados)[/green]")
    return result


def _find_max_resolve_datetime(data: list[dict]) -> Optional[datetime]:
    """Encontra o maior resolve_data/resolve_hora nos dados extraídos."""
    max_dt = None
    for item in data:
        data_str = item.get("resolve_data", "")
        hora_str = item.get("resolve_hora", "")
        if data_str and hora_str:
            try:
                dt = datetime.strptime(f"{data_str} {hora_str}", "%d/%m/%Y %H:%M:%S").replace(tzinfo=BRT)
                if not max_dt or dt > max_dt:
                    max_dt = dt
            except ValueError:
                pass
    return max_dt


def _refresh_status_for_codes(session, status_codes_to_refresh: list[int]) -> int:
    """Re-verifica o status de registros cujo order_status atual está em status_codes_to_refresh.

    Integra com BASE STATUS para traduzir códigos em nomes.
    Retorna número de registros atualizados.
    """
    from core.sheets import read_sheet, batch_update_values
    from core.config import SPREADSHEET_ID

    console.print(
        f"[cyan]Refrescando status para códigos: {status_codes_to_refresh}...[/cyan]"
    )

    # 1. Ler todos os registros da aba BASE Liquidation
    rows = read_sheet(SPREADSHEET_ID, "'BASE Liquidation'!A:E")
    if not rows or len(rows) <= 1:
        return 0

    # 2. Carregar mapa de status
    status_map = _load_status_map()

    # 3. Identificar shipment_ids que precisam refresh (coluna A)
    ids_to_refresh: dict[str, int] = {}  # {shipment_id: row_number}
    for i, row in enumerate(rows):
        if i == 0:  # header
            continue
        if len(row) >= 2 and row[0]:  # col A é shipment_id, col B é order_status
            sid = str(row[0]).strip()
            status_text = str(row[1]).strip() if row[1] else ""
            # procurar o código correspondente (valor numérico em status_map)
            current_code = None
            for code, nome in status_map.items():
                if nome == status_text:
                    current_code = code
                    break
            if current_code in status_codes_to_refresh:
                ids_to_refresh[sid] = i + 1  # 1-indexed row no Sheets

    if not ids_to_refresh:
        console.print("[yellow]⚠️ Não há registros para refrescar.[/yellow]")
        return 0

    # 4. Buscar status atual
    console.print(f"[cyan]  Buscando status de {len(ids_to_refresh)} registros...[/cyan]")
    new_status_codes = _fetch_order_status_batch(session, list(ids_to_refresh.keys()))

    # 5. Traduzir códigos em nomes
    updates: list[dict] = []
    for sid, row_num in ids_to_refresh.items():
        new_code = new_status_codes.get(sid, "")
        new_name = status_map.get(int(new_code), new_code) if new_code and new_code.isdigit() else new_code
        updates.append({
            "range": f"'BASE Liquidation'!B{row_num}",  # coluna B é order_status
            "values": [[new_name]],
        })

    # 6. Aplicar atualizações
    if updates:
        batch_update_values(SPREADSHEET_ID, updates)
        console.print(f"[green]✅ {len(updates)} status atualizados[/green]")

    return len(updates)


def _get_last_resolve_datetime_from_sheet() -> Optional[datetime]:
    """
    Lê a aba BASE Liquidation e retorna o maior resolve_data/resolve_hora
    encontrado nas linhas existentes.
    """
    from core.sheets import read_sheet
    from core.config import SPREADSHEET_ID, SHEETS_TABS

    # Usa range amplo para leitura (A:D cobre todas as colunas da aba)
    read_range = "'BASE Liquidation'!A:D"
    console.print(f"[cyan]Lendo última data/hora da aba BASE Liquidation...[/cyan]")

    rows = read_sheet(SPREADSHEET_ID, read_range)

    if not rows or len(rows) <= 1:
        console.print("[yellow]⚠️ Aba BASE Liquidation vazia ou sem dados.[/yellow]")
        return None

    max_dt = None
    for i, row in enumerate(rows):
        if i == 0:
            continue  # header
        if len(row) < 4:  # Precisa ter pelo menos até a coluna D (índice 3)
            continue

        # Nova estrutura: A=id, B=status, C=data, D=hora
        data_str = str(row[2]).strip() if row[2] else ""
        hora_str = str(row[3]).strip() if row[3] else ""

        if data_str and hora_str:
            try:
                dt = datetime.strptime(f"{data_str} {hora_str}", "%d/%m/%Y %H:%M:%S").replace(tzinfo=BRT)
                if not max_dt or dt > max_dt:
                    max_dt = dt
            except ValueError:
                pass

    if max_dt:
        console.print(f"[dim]Último registro na planilha: {max_dt.strftime('%d/%m/%Y %H:%M:%S')}[/dim]")

    return max_dt


def run(days_ago: int = None) -> tuple[Path, Path, int]:
    """
    Executa extração completa de Liquidation (ER48 apenas).
    Divide o período em chunks de 12 horas para lidar com grande volume.
    Sem dependência de Selenium - usa HTTP direto.
    """
    console.print("[bold cyan]═══ Liquidation (EO List ER48) ═══[/bold cyan]")
    session = None

    # Ler último registro da planilha
    last_dt = _get_last_resolve_datetime_from_sheet()

    days_ago = days_ago or LIQUIDATION["days_ago"]
    end_date = datetime.now(BRT)

    if last_dt:
        start_date = last_dt
        console.print(f"[cyan]Modo incremental: buscando a partir de {start_date.strftime('%d/%m/%Y %H:%M:%S')}[/cyan]")
    else:
        start_date = (end_date - timedelta(days=days_ago)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        console.print(f"[cyan]Primeira execução: buscando últimos {days_ago} dias[/cyan]")

    is_first_run = (last_dt is None)

    # Chunking por 12 horas (conforme spec)
    chunk_hours = 12
    all_data: list[dict] = []
    current_start = start_date
    chunk_num = 1

    console.print(
        f"[cyan]Período total: {start_date.strftime('%d/%m/%Y %H:%M')} "
        f"até {end_date.strftime('%d/%m/%Y %H:%M')}[/cyan]"
    )
    console.print(f"[dim]Dividindo em chunks de {chunk_hours} horas...[/dim]")

    try:
        session = get_session()

        while current_start < end_date:
            current_end = min(current_start + timedelta(hours=chunk_hours), end_date)

            console.print(
                f"\n[bold]Chunk {chunk_num}: "
                f"{current_start.strftime('%d/%m %H:%M')} → "
                f"{current_end.strftime('%d/%m %H:%M')}[/bold]"
            )

            chunk_data = fetch_liquidation_orders(session, current_start, current_end)

            if chunk_data:
                all_data.extend(chunk_data)
                console.print(f"[green]  → Total acumulado: {len(all_data)}[/green]")

            current_start = current_end
            chunk_num += 1

    except SessionExpiredError:
        console.print(f"[red]❌ Erro de Sessão na Extração[/red]")
        raise
    except Exception as e:
        console.print(f"[red]❌ Erro inesperado na Extração: {e}[/red]")
        raise

    console.print(f"\n[bold green]✅ TOTAL GERAL: {len(all_data)} registros![/bold green]")
    _ensure_liquidation_header()

    latest_dt = _find_max_resolve_datetime(all_data) if all_data else None
    has_new_data = bool(all_data)
    if has_new_data and last_dt and latest_dt:
        has_new_data = latest_dt > last_dt

    if has_new_data and last_dt and latest_dt:
        console.print(
            f"[dim]Último registro salvo: {last_dt.strftime('%d/%m/%Y %H:%M:%S')} | "
            f"Mais recente coletado: {latest_dt.strftime('%d/%m/%Y %H:%M:%S')}[/dim]"
        )

    if not has_new_data:
        if not all_data:
            console.print("[yellow]⚠️ API não retornou dados para o período solicitado.[/yellow]")
        elif last_dt:
            console.print(
                f"[yellow]⚠️ Nenhum novo ER48 desde {last_dt.strftime('%d/%m/%Y %H:%M:%S')}.[/yellow]"
            )
        else:
            console.print("[yellow]⚠️ Sem dados válidos para comparação de timestamps.[/yellow]")

    # Buscar order_status para todos os shipment_ids (somente quando há novidades)
    if has_new_data and all_data:
        console.print("[cyan]Buscando order_status dos shipment_ids...[/cyan]")
        if session is None:
            session = get_session()

        shipment_ids = [item["shipment_id"] for item in all_data if item.get("shipment_id")]
        status_codes = _fetch_order_status_batch(session, shipment_ids)
        status_map = _load_status_map()

        for item in all_data:
            sid = item.get("shipment_id", "")
            code = status_codes.get(sid, "")
            if code and code.isdigit():
                item["order_status"] = status_map.get(int(code), code)
            elif code:
                item["order_status"] = code
            else:
                item["order_status"] = ""

        console.print(
            f"[green]  → {sum(1 for item in all_data if item.get('order_status'))} com status obtido[/green]"
        )

    # Refrescar status de registros anteriores
    console.print("\n[cyan]Refrescando status anteriores...[/cyan]")
    if session is None:
        session = get_session()
    refresh_count = _refresh_status_for_codes(session, [58, 8, 203])
    if refresh_count > 0:
        console.print(f"[green]  ✅ {refresh_count} registros refrescados[/green]")

    if not has_new_data:
        return None, None, 0

    # Salvar: append se já havia dados, overwrite na primeira vez
    return core_save_data(all_data, MODULE_NAME, append=not is_first_run)


if __name__ == "__main__":
    run()
