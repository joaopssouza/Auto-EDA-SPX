#!/usr/bin/env python3
"""
Teste simples e direto da extração Liquidation (ER48) via HTTP.
Não lê da planilha Google, apenas testa a coleta de dados.
"""

import time
import sys
from datetime import datetime, timedelta, timezone

# Configuração de path e imports
sys.path.insert(0, str(__file__).rsplit("\\", 1)[0])

from rich.console import Console
from core.session import get_session
from core.config import LIQUIDATION, BRT
from modules.liquidation import _fetch_page_http, _fetch_page_selenium, _create_browser_driver

console = Console()


def get_time_range(start_date: datetime, end_date: datetime):
    """Converte datetime para timestamp Unix."""
    start_ts = int(start_date.replace(tzinfo=BRT).timestamp())
    end_ts = int(end_date.replace(tzinfo=BRT).timestamp())
    return start_ts, end_ts


def test_http_only():
    """Testa apenas HTTP direto - não deve disparar auto-login."""
    console.print("\n[bold cyan]🧪 TESTE 1: HTTP Direto (sem auto-login)[/bold cyan]")
    
    session = get_session()
    console.print("[green]✅ Sessão carregada[/green]")
    
    # Período simples (últimas 24 horas)
    end_date = datetime.now(tz=BRT)
    start_date = end_date - timedelta(days=1)
    start_ts, end_ts = get_time_range(start_date, end_date)
    
    console.print(f"  Período: {start_date.strftime('%d/%m/%Y %H:%M')} → {end_date.strftime('%d/%m/%Y %H:%M')}")
    
    page = 1
    count = 0
    
    console.print(f"[cyan]Tentando página {page} via HTTP...[/cyan]")
    result = _fetch_page_http(session, start_ts, end_ts, page, count_per_page=50)
    
    if result is not None:
        eo_list, total = result
        count = len(eo_list)
        console.print(f"[green]✅ Sucesso! Obteve {count} itens de {total} esperados[/green]")
        if eo_list:
            console.print(f"[dim]  Exemplo: {eo_list[0]}[/dim]")
        return True
    else:
        console.print(f"[yellow]⚠️ HTTP retornou None (esperado - usar Selenium fallback)[/yellow]")
        return False


def test_selenium_only():
    """Testa apenas Selenium como fallback."""
    console.print("\n[bold cyan]🧪 TESTE 2: Selenium Fallback[/bold cyan]")
    
    # Criar browser
    console.print("[cyan]Abrindo browser...[/cyan]")
    driver = _create_browser_driver(headless=True)
    
    try:
        # Período simples (últimas 24 horas)
        end_date = datetime.now(tz=BRT)
        start_date = end_date - timedelta(days=1)
        start_ts, end_ts = get_time_range(start_date, end_date)
        
        console.print(f"  Período: {start_date.strftime('%d/%m/%Y %H:%M')} → {end_date.strftime('%d/%m/%Y %H:%M')}")
        
        page = 1
        console.print(f"[cyan]Tentando página {page} via Selenium...[/cyan]")
        result = _fetch_page_selenium(driver, start_ts, end_ts, page, count_per_page=50)
        
        if result is not None:
            eo_list, total = result
            count = len(eo_list)
            console.print(f"[green]✅ Sucesso! Obteve {count} itens de {total} esperados[/green]")
            if eo_list:
                console.print(f"[dim]  Exemplo: {eo_list[0]}[/dim]")
            return True
        else:
            console.print(f"[red]❌ Selenium falhou[/red]")
            return False
    finally:
        driver.quit()
        console.print("[dim]Browser fechado[/dim]")


def test_http_with_fallback():
    """Testa HTTP->Selenium fallback (como em produção)."""
    console.print("\n[bold cyan]🧪 TESTE 3: HTTP com Fallback para Selenium[/bold cyan]")
    
    session = get_session()
    console.print("[green]✅ Sessão carregada[/green]")
    
    driver = _create_browser_driver(headless=True)
    
    try:
        # Período simples (últimas 24 horas)
        end_date = datetime.now(tz=BRT)
        start_date = end_date - timedelta(days=1)
        start_ts, end_ts = get_time_range(start_date, end_date)
        
        console.print(f"  Período: {start_date.strftime('%d/%m/%Y %H:%M')} → {end_date.strftime('%d/%m/%Y %H:%M')}")
        
        page = 1
        total_items = 0
        use_selenium = False
        
        while page <= 3:  # Testa apenas 3 páginas
            try:
                if not use_selenium:
                    console.print(f"[cyan]Página {page}: tentando HTTP...[/cyan]")
                    result = _fetch_page_http(session, start_ts, end_ts, page, count_per_page=50)
                    
                    if result is not None:
                        eo_list, total = result
                        total_items += len(eo_list)
                        console.print(f"[green]✅ HTTP OK: {len(eo_list)} itens[/green]")
                    else:
                        console.print(f"[yellow]⚠️ HTTP falhou, ativando Selenium fallback...[/yellow]")
                        use_selenium = True
                
                if use_selenium:
                    console.print(f"[cyan]Página {page}: usando Selenium...[/cyan]")
                    result = _fetch_page_selenium(driver, start_ts, end_ts, page, count_per_page=50)
                    
                    if result is not None:
                        eo_list, total = result
                        total_items += len(eo_list)
                        console.print(f"[green]✅ Selenium OK: {len(eo_list)} itens[/green]")
                    else:
                        console.print(f"[red]❌ Selenium falhou[/red]")
                        break
                
                page += 1
                time.sleep(0.5)
            except Exception as e:
                console.print(f"[red]❌ Erro: {e}[/red]")
                break
        
        console.print(f"[green]✅ Total coletado: {total_items} itens[/green]")
        return total_items > 0
    finally:
        driver.quit()
        console.print("[dim]Browser fechado[/dim]")


def main():
    console.print("[bold magenta]╔═══════════════════════════════════════════════════╗[/bold magenta]")
    console.print("[bold magenta]║  Teste Simples de Extração Liquidation (ER48)      ║[/bold magenta]")
    console.print("[bold magenta]╚═══════════════════════════════════════════════════╝[/bold magenta]")
    
    choice = input("""
Escolha o teste:
  1) HTTP Direto (não deve disparar auto-login)
  2) Selenium Fallback
  3) HTTP → Selenium (fallback automático)
  4) Todos os testes
  
Digite (1-4): """).strip()
    
    results = {}
    
    try:
        if choice in ("1", "4"):
            results["HTTP Direto"] = test_http_only()
            time.sleep(2)
        
        if choice in ("2", "4"):
            results["Selenium Fallback"] = test_selenium_only()
            time.sleep(2)
        
        if choice in ("3", "4"):
            results["HTTP→Selenium"] = test_http_with_fallback()
            time.sleep(2)
        
        # Resumo
        console.print(f"\n[bold cyan]═══ RESUMO ═══[/bold cyan]")
        for test_name, success in results.items():
            status = "[green]✅ PASSOU[/green]" if success else "[yellow]⚠️ FALHOU[/yellow]"
            console.print(f"  {test_name}: {status}")
        
        all_passed = all(results.values()) if results else False
        console.print()
        if all_passed:
            console.print("[bold green]✅ TODOS OS TESTES PASSARAM![/bold green]")
        elif results:
            console.print("[bold yellow]⚠️ Alguns testes falharam (veja detalhes acima)[/bold yellow]")
    
    except KeyboardInterrupt:
        console.print("\n[red]❌ Teste interrompido pelo usuário[/red]")
    except Exception as e:
        console.print(f"[red]❌ Erro fatal: {e}[/red]")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
