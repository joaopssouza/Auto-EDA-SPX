"""
Módulo de Extração: Produtividade Inbound
=========================================

Extrai dados da API de Produtividade Inbound via POST.
Operação: POST do Inbound.
"""

from datetime import datetime
from pathlib import Path

from rich.console import Console

from core.config import INBOUND, DEFAULT_PAGE_SIZE, MAX_PAGES
from core.save import save_data as core_save_data
from core.session import get_session

console = Console()

MODULE_NAME = "inbound"


def fetch_inbound(count: int = DEFAULT_PAGE_SIZE) -> list[dict]:
    """Busca dados de Produtividade Inbound."""
    session = get_session()
    
    console.print("[cyan]Buscando Produtividade Inbound...[/cyan]")
    
    all_data = []
    page = 1
    
    while page <= MAX_PAGES:
        body = {
            "unit_type": 1,
            "process_type": INBOUND["scan_type"],
            "period_type": 1,
            "operator_email": "",
            "pageno": page,
            "count": count,
            "productivity": 1,
            "order_by_total": 100,
            "event_id_list": []
        }
        
        # Headers específicos - Tenta pegar da sessão (nuvem) ou do config
        from core.config import SAP_RI_INBOUND, SAP_SEC_INBOUND
        
        # Prioridade: 1. Nuvem/Sessão | 2. Config Local
        ri = session.session_data.get("x-sap-ri-inbound", session.session_data.get("x-sap-ri", SAP_RI_INBOUND))
        sec = session.session_data.get("x-sap-sec-inbound", session.session_data.get("x-sap-sec", SAP_SEC_INBOUND))

        extra_headers = {
            "x-sap-ri": ri,
            "x-sap-sec": sec
        }
        
        try:
            response = session.post(INBOUND["api_url"], json_data=body, extra_headers=extra_headers)
            
            if isinstance(response, dict):
                retcode = response.get("retcode", -1)
                if retcode != 0:
                    console.print(f"[red]Erro API: {response.get('message', 'desconhecido')}[/red]")
                    break
                
                data_wrapper = response.get("data", response)
                
                if isinstance(data_wrapper, dict):
                    items = data_wrapper.get("efficiency_list", data_wrapper.get("list", []))
                    total = data_wrapper.get("total", 0)
                else:
                    items = data_wrapper if isinstance(data_wrapper, list) else []
                    total = len(items)
            else:
                console.print(f"[red]Resposta inesperada: {type(response)}[/red]")
                break
            
            if not items:
                break
            
            all_data.extend(items)
            console.print(f"  Página {page}: +{len(items)} ({len(all_data)}/{total})")
            
            if len(all_data) >= total:
                break
            
            page += 1
            
        except Exception as e:
            console.print(f"[red]❌ Erro: {e}[/red]")
            break
    
    console.print(f"\n[bold green]✅ Total Inbound: {len(all_data)} registros[/bold green]")
    return all_data


def extract_user_total(data: list[dict]) -> list[dict]:
    """Extrai apenas Operator e Total dos dados."""
    result = []
    for item in data:
        operator = item.get("operator", "")
        total = item.get("efficiency_total", item.get("total", 0))
        
        if operator:
            result.append({"operator": operator, "total": total})
    
    return result


def run(extract_only_totals: bool = True) -> tuple[Path, Path, int]:
    """Executa extração de Produtividade Inbound."""
    console.print("[bold cyan]═══ Produtividade Inbound ═══[/bold cyan]")
    
    data = fetch_inbound()
    
    if extract_only_totals:
        data = extract_user_total(data)
    
    return core_save_data(data, MODULE_NAME)


if __name__ == "__main__":
    run()
