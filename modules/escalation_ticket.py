"""
Módulo de Extração: Escalation Ticket
======================================

Extrai dados de tickets escalados via GET.
Operação: GET de Escalation Ticket.

Divide o período em chunks para contornar o limite de 10k do Elasticsearch.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from rich.console import Console

from core.config import BRT, ESCALATION_TICKET, DEFAULT_PAGE_SIZE, MAX_PAGES
from core.save import save_data
from core.session import get_session

console = Console()

MODULE_NAME = "escalation_ticket"


def fetch_escalation_tickets(
    start_date: datetime,
    end_date: datetime,
    count_per_page: int = 1000
) -> list[dict]:
    """Busca tickets escalados para um período específico."""
    
    session = get_session()
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())
    
    console.print(f"[cyan]Buscando Escalation Tickets...[/cyan]")
    console.print(f"  Período: {start_date.strftime('%d/%m/%Y %H:%M')} até {end_date.strftime('%d/%m/%Y %H:%M')}")
    
    all_data = []
    page = 1
    total_expected = None
    
    while page <= MAX_PAGES:
        params = {
            "min_creation_time": start_ts,
            "max_creation_time": end_ts,
            "count": count_per_page,
            "pageno": page
        }
        
        try:
            response = session.get(ESCALATION_TICKET["api_url"], params=params)
            
            if isinstance(response, dict):
                retcode = response.get("retcode", response.get("code", -1))
                if retcode != 0:
                    console.print(f"[red]Erro API: {response.get('message', 'desconhecido')}[/red]")
                    break
                
                data_wrapper = response.get("data", response)
                
                if isinstance(data_wrapper, dict):
                    items = data_wrapper.get("list", data_wrapper.get("tickets", []))
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
            
            if len(all_data) >= total_expected or len(all_data) >= 10000:
                break
            
            page += 1
            
        except Exception as e:
            console.print(f"[red]❌ Erro na página {page}: {e}[/red]")
            break
    
    console.print(f"[green]  → {len(all_data)} tickets extraídos[/green]")
    return all_data


def run(days_ago: int = None) -> tuple[Path, Path, int]:
    """
    Executa extração completa de Escalation Tickets.
    Divide o período em chunks de 2 dias para contornar limite de 10k do Elasticsearch.
    """
    console.print("[bold cyan]═══ Escalation Tickets ═══[/bold cyan]")
    
    days_ago = days_ago or ESCALATION_TICKET["days_ago"]
    
    end_date = datetime.now(BRT).replace(hour=23, minute=59, second=59, microsecond=0)
    start_date = (end_date - timedelta(days=days_ago)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Divide em chunks de 2 dias para evitar limite de 10k do Elasticsearch
    chunk_days = 2
    all_data = []
    current_start = start_date
    chunk_num = 1
    
    console.print(f"[cyan]Período total: {start_date.strftime('%d/%m/%Y %H:%M')} até {end_date.strftime('%d/%m/%Y %H:%M')}[/cyan]")
    console.print(f"[dim]Dividindo em chunks de {chunk_days} dias...[/dim]")
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=chunk_days), end_date)
        
        console.print(f"\n[bold]Chunk {chunk_num}: {current_start.strftime('%d/%m %H:%M')} → {current_end.strftime('%d/%m %H:%M')}[/bold]")
        
        chunk_data = fetch_escalation_tickets(current_start, current_end)
        
        if chunk_data:
            all_data.extend(chunk_data)
            console.print(f"[green]  → Total acumulado: {len(all_data)}[/green]")
        
        current_start = current_end
        chunk_num += 1
    
    console.print(f"\n[bold green]✅ TOTAL GERAL: {len(all_data)} tickets![/bold green]")
    
    return save_data(all_data, MODULE_NAME)


if __name__ == "__main__":
    run()
