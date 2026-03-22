"""
Módulo de Extração: Exception Orders
====================================

Extrai dados da API de Exception Orders com paginação automática.
Operação: GET de Exception Orders.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import json

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from core.config import BRT, EXCEPTION_ORDERS, MAX_PAGES, SAVE_LOCAL_FILES
from core.save import save_data as core_save_data
from core.session import SessionExpiredError, get_session

console = Console()

MODULE_NAME = "exception_orders"


def get_time_range(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> tuple[int, int]:
    """Calcula o range de timestamps."""
    if end_date is None:
        end_date = datetime.now(BRT)
    
    if start_date is None:
        yesterday = end_date - timedelta(days=EXCEPTION_ORDERS["days_ago"])
        start_date = yesterday.replace(hour=6, minute=30, second=0, microsecond=0)
    
    return int(start_date.timestamp()), int(end_date.timestamp())


def fetch_exception_orders(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    count_per_page: int = 1000
) -> list[dict]:
    """Busca dados de Exception Orders com paginação automática."""
    session = get_session()
    
    start_ts, end_ts = get_time_range(start_date, end_date)
    
    console.print(f"[cyan]Buscando Exception Orders...[/cyan]")
    console.print(f"  Período: {datetime.fromtimestamp(start_ts, tz=BRT).strftime('%d/%m/%Y %H:%M')} até {datetime.fromtimestamp(end_ts, tz=BRT).strftime('%d/%m/%Y %H:%M')}")
    
    all_data = []
    page = 1
    total_expected = None
    search_after = None
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Extraindo dados...", total=None)
        
        while page <= MAX_PAGES:
            params = {
                "operator_time": f"{start_ts},{end_ts}",
                "pageno": f"{page:02d}",
                "count": count_per_page
            }
            
            if search_after:
                params["search_after"] = search_after
            
            try:
                response = session.get(EXCEPTION_ORDERS["api_url"], params=params)
                
                # Verifica se é JSON válido
                try:
                    data = response
                except Exception as e:
                    console.print(f"[red]❌ Erro ao processar dados na página {page}: {e}[/red]")
                    break 

                if isinstance(data, dict):
                    retcode = data.get("retcode", -1)
                    if retcode != 0:
                        console.print(f"[red]Erro API: {data.get('message', 'desconhecido')}[/red]")
                        break
                    
                    data_wrapper = data.get("data", data)
                    
                    if isinstance(data_wrapper, dict):
                        items = data_wrapper.get("list", [])
                        total_expected = data_wrapper.get("total", 0)
                        search_after = data_wrapper.get("search_after")
                    else:
                        items = data_wrapper if isinstance(data_wrapper, list) else []
                        total_expected = len(items)
                else:
                    console.print(f"[red]Resposta inesperada: {type(data)}[/red]")
                    break
                
                if not items:
                    break
                
                all_data.extend(items)
                progress.update(task, description=f"Página {page}: {len(all_data)}/{total_expected}")
                
                if len(all_data) >= total_expected or len(all_data) >= 10000:
                    break
                
                page += 1
                
            except Exception as e:
                console.print(f"[red]❌ Erro na página {page}: {e}[/red]")
                if isinstance(e, SessionExpiredError):
                    raise
                break
    
    console.print(f"[green]  → {len(all_data)} registros extraídos[/green]")
    return all_data


def run(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> tuple[Path, Path, int]:
    """
    Executa extração completa de Exception Orders.
    Divide o período em chunks para contornar limite de 10k.
    """
    console.print("[bold cyan]═══ Exception Orders ═══[/bold cyan]")
    
    if end_date is None:
        end_date = datetime.now(BRT)
    
    if start_date is None:
        yesterday = end_date - timedelta(days=EXCEPTION_ORDERS["days_ago"])
        start_date = yesterday.replace(hour=6, minute=30, second=0, microsecond=0)
    
    # Divide em chunks de 4 horas para evitar limite de 10k
    chunk_hours = 4
    all_data = []
    current_start = start_date
    chunk_num = 1
    
    console.print(f"[cyan]Período total: {start_date.strftime('%d/%m/%Y %H:%M')} até {end_date.strftime('%d/%m/%Y %H:%M')}[/cyan]")
    console.print(f"[dim]Dividindo em chunks de {chunk_hours} horas...[/dim]")
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(hours=chunk_hours), end_date)
        
        console.print(f"\n[bold]Chunk {chunk_num}: {current_start.strftime('%d/%m %H:%M')} → {current_end.strftime('%d/%m %H:%M')}[/bold]")
        
        try:
            chunk_data = fetch_exception_orders(current_start, current_end)
        except SessionExpiredError:
            console.print("[red]🔒 Sessão expirada no chunk atual. Interrompendo execução dos próximos chunks.[/red]")
            raise
        
        if chunk_data:
            all_data.extend(chunk_data)
            console.print(f"[green]  → Total acumulado: {len(all_data)}[/green]")
        
        current_start = current_end
        chunk_num += 1
    
    console.print(f"\n[bold green]✅ TOTAL GERAL: {len(all_data)} registros![/bold green]")
    
    json_path, csv_path, count = core_save_data(all_data, MODULE_NAME, save_csv_file=True)
    
    if csv_path:
        # Executar transformação automática (ETL)
        try:
            from modules.exception_orders_transformer import ExceptionOrdersTransformer
            from core.config import SPREADSHEET_ID, SHEETS_TABS
            from core.sheets import update_sheet
            
            console.print(f"[cyan]Iniciando transformação ETL para 'Produção EO'...[/cyan]")
            transformer = ExceptionOrdersTransformer()
            
            # Define caminho de saída na mesma pasta do CSV original
            output_dir = csv_path.parent
            output_path = output_dir / "Cockpit RTS - BASE - Produção EO.csv"
            
            df_transformed = transformer.execute(str(csv_path), str(output_path))
            
            # Upload para Google Sheets
            if df_transformed is not None and not df_transformed.empty:
                range_name = SHEETS_TABS.get("producao_eo")
                if range_name and SPREADSHEET_ID:
                    console.print(f"[cyan]Enviando Produção EO para Google Sheets ({range_name})...[/cyan]")
                    
                    # Preenche NaN com vazio e converte para lista de listas
                    df_transformed = df_transformed.fillna("")
                    values = [df_transformed.columns.values.tolist()] + df_transformed.values.tolist()
                    
                    update_sheet(SPREADSHEET_ID, range_name, values)
                else:
                    console.print("[yellow]⚠️ Tab 'producao_eo' não configurada no SHEETS_TABS.[/yellow]")
            
            
        except Exception as e:
            console.print(f"[red]❌ Erro na transformação ETL: {e}[/red]")
            
        # Limpeza de arquivos se configurado para não salvar localmente
        if not SAVE_LOCAL_FILES:
            try:
                if csv_path and csv_path.exists():
                    csv_path.unlink()
                    console.print(f"[dim]🗑️ Arquivo removido: {csv_path.name}[/dim]")
                
                # O path de saída do ETL é construído dentro do try, recriando aqui para remoção
                output_dir = csv_path.parent
                output_path = output_dir / "Cockpit RTS - BASE - Produção EO.csv"
                if output_path.exists():
                    output_path.unlink()
                    console.print(f"[dim]🗑️ Arquivo removido: {output_path.name}[/dim]")
            except Exception as e:
                console.print(f"[yellow]⚠️ Erro ao limpar arquivos temporários: {e}[/yellow]")
            
    return json_path, csv_path, count




if __name__ == "__main__":
    run()
