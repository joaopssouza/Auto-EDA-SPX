"""
Orquestrador Principal - Sistema de Raspagem SPX Shopee
=======================================================

Este é o ponto de entrada principal do sistema.
Coordena a autenticação e execução dos módulos de extração.

Uso:
    # Executar tudo
    python main.py
    
    # Apenas autenticar (gerar cookies)
    python main.py --auth
    
    # Extrair específico
    python main.py --inbound
    python main.py --outbound
    python main.py --exception
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from core.config import BRT

def now_brt() -> datetime:
    """Retorna datetime atual em horário de Brasília."""
    return datetime.now(BRT)

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Adiciona diretório ao path
sys.path.insert(0, str(Path(__file__).parent))

from core.auth import authenticate, load_session, is_session_material_valid
from core.session import get_session
from modules import (
    exception_orders,
    inbound,
    outbound,
    online_soc_tracking,
    recebimento_soc,
    escalation_ticket,
    liquidation,
    workstation_assignment,
    spx_duplicados,
    status_duplicados,
)

console = Console()


def show_banner():
    """Exibe banner do sistema."""
    console.print(Panel.fit(
        "[bold cyan]Sistema de Raspagem SPX Shopee[/bold cyan]\n"
        "[dim]Extração automatizada via APIs internas[/dim]",
        border_style="cyan"
    ))
    console.print(f"[dim]Início: {now_brt().strftime('%d/%m/%Y %H:%M:%S')}[/dim]\n")


def check_session() -> bool:
    """Verifica sessão íntegra e tenta no máximo 1 renovação automática."""
    session_data = load_session()
    valid_session, reasons = is_session_material_valid(session_data)
    if valid_session:
        return True

    if session_data:
        console.print(
            "[yellow]⚠️ Sessão encontrada, mas incompleta. Motivos: "
            + ",".join(reasons)
            + "[/yellow]"
        )
        
    # Tenta auto-login (OAuth2 é prioridade)
    is_github = os.getenv("GITHUB_ACTIONS") == "true"
    
    # Verifica se OAuth2 está configurado
    from core.google_oauth import get_credentials

    has_oauth = get_credentials() is not None

    if is_github or has_oauth:
        console.print("[yellow]🔍 Sessão não encontrada. Tentando auto-login...[/yellow]")
        if has_oauth:
            console.print("[cyan]🔐 Usando OAuth2...[/cyan]")
        run_auth(headless=is_github)
        
        session_data = load_session()
        valid_session, reasons = is_session_material_valid(session_data)
        if valid_session:
            return True
        
        console.print(
            "[red]❌ Falha ao obter sessão íntegra automaticamente. Motivos: "
            + ",".join(reasons)
            + "[/red]"
        )
        return False

    console.print("[yellow]⚠️ Nenhuma sessão encontrada.[/yellow]")
    console.print("[yellow]Execute 'python main.py --setup-oauth' para configurar OAuth2.[/yellow]")
    
    from core.notifications import notify_auth_required
    notify_auth_required()
    
    return False


def run_auth(headless: bool = False):
    """Executa autenticação."""
    # Se estiver no GitHub Actions, força headless e tenta automático
    is_github = os.getenv("GITHUB_ACTIONS") == "true"
    headless = headless or is_github
    
    console.print(f"[bold]Iniciando {'automação de ' if is_github else ''}autenticação...[/bold]")
    authenticate(headless=headless, force_refresh=True)


from core.notifications import notify_status

def run_all():
    """Executa extração de todos os módulos."""
    if not check_session():
        return
    
    from core.session import SessionExpiredError
    
    start_time = time.time()
    results = {}
    session_expired = False
    
    modules_to_run = [
        ("Exception Orders", exception_orders, "run"),
        ("Inbound", inbound, "run"),
        ("Outbound", outbound, "run"),
        ("Recebimento SOC", recebimento_soc, "run_with_transform"),
        ("Escalation Ticket", escalation_ticket, "run"),
        ("Liquidation", liquidation, "run"),
        ("SPX Duplicados", spx_duplicados, "run"),
        ("Status Duplicados", status_duplicados, "run"),
    ]
    
    for name, module, run_func_name in modules_to_run:
        if session_expired:
            # Se sessão já expirou, nem tenta os próximos módulos
            results[name] = (0, "⏭️")
            continue
        try:
            run_func = getattr(module, run_func_name)
            json_path, csv_path, count = run_func()
            results[name] = (count, "✅")
        except SessionExpiredError:
            console.print(f"[red]🔒 {name}: Sessão expirada[/red]")
            results[name] = (0, "🔒")
            session_expired = True
        except Exception as e:
            console.print(f"[red]{name}: {e}[/red]")
            results[name] = (0, "❌")
    
    duration = time.time() - start_time
    duration_str = str(timedelta(seconds=int(duration)))

    # Resumo
    console.print("\n")
    table = Table(title=f"Resumo da Extração (Duração: {duration_str})")
    table.add_column("Módulo", style="cyan")
    table.add_column("Registros", justify="right")
    table.add_column("Status", justify="center")
    
    for name, (count, status) in results.items():
        table.add_row(name, str(count), status)
    
    console.print(table)
    
    # Notificação externa — UMA mensagem com tudo
    summary_text = "\n".join([f"{n}: {c} reg ({s})" for n, (c, s) in results.items()])
    notify_status(summary_text, duration_str, session_expired=session_expired)



def main():
    """Função principal."""
    parser = argparse.ArgumentParser(description="Sistema de Raspagem SPX Shopee")
    parser.add_argument("--setup-oauth", action="store_true", help="Configurar OAuth2 Google (uma vez)")
    parser.add_argument("--auth", action="store_true", help="Apenas autenticar")
    parser.add_argument("--headless", action="store_true", help="Autenticação sem interface")
    parser.add_argument("--inbound", action="store_true", help="Extrair apenas Inbound")
    parser.add_argument("--outbound", action="store_true", help="Extrair apenas Outbound")
    parser.add_argument("--exception", action="store_true", help="Extrair apenas Exception Orders")
    parser.add_argument("--recebimento", action="store_true", help="Extrair Recebimento SOC MG_BETIM (com ETL RAW -> BAKED)")
    parser.add_argument("--escalation", action="store_true", help="Extrair Escalation Tickets")
    parser.add_argument("--liquidation", action="store_true", help="Extrair Liquidation (EO List ER48)")
    parser.add_argument("--test-notify", action="store_true", help="Testar configuração de notificações")
    parser.add_argument("--spx-duplicados", action="store_true", help="Extrair SPX Duplicados")
    parser.add_argument("--online-soc-tracking", action="store_true", help="Consultar tracking_info dos Order IDs da aba Online_SOC-MG2 e gravar em raw_tracking_info")
    parser.add_argument("--status-dup", action="store_true", help="Consultar status dos SPX Duplicados")
    parser.add_argument("--status-dup-refresh", action="store_true", help="Atualizar status de todos os SPX Duplicados")
    parser.add_argument(
        "--workstation",
        action="store_true",
        help="Atualizar coluna Workstation de BASE HC/DW com base nas estações da aba BASE WORKSTATION",
    )
    parser.add_argument("--pipeline-spx", action="store_true", help="Pipeline completo: Liquidation → SPX Duplicados → Status Dup → Refresh")
    
    args = parser.parse_args()
    
    show_banner()
    
    # Validação de ambiente
    from core.utils import validate_config
    if not args.auth and not args.setup_oauth and not validate_config():
        sys.exit(1)
    
    try:
        if args.setup_oauth:
            from core.google_oauth import setup_oauth
            setup_oauth()
            console.print("\n[bold green]✅ OAuth2 configurado![/bold green]")
            console.print("[dim]Agora execute 'python main.py --auth' para gerar os cookies SPX.[/dim]")
            console.print("[dim]Para CI, adicione GOOGLE_OAUTH_TOKEN e GOOGLE_SESSION_COOKIES como Secrets.[/dim]")
            return

        if args.auth:
            run_auth(headless=args.headless)
            return

        if args.test_notify:
            console.print("[cyan]Enviando teste de notificação...[/cyan]")
            if notify_status("Teste de conexão do Monitoramento RTS! 🚀"):
                console.print("[green]✅ Teste concluído com sucesso![/green]")
            else:
                console.print("[red]❌ Falha ao enviar notificação. Verifique seu .env e tokens.[/red]")
            return

        # Execução modular com retorno padronizado
        # Execução modular dinâmica
        module_map = {
            "inbound": ("Inbound", inbound),
            "outbound": ("Outbound", outbound),
            "exception": ("Exception Orders", exception_orders),
            "recebimento": ("Recebimento SOC", recebimento_soc),
            "online_soc_tracking": ("Online SOC Tracking", online_soc_tracking),
            "escalation": ("Escalation Ticket", escalation_ticket),
            "liquidation": ("Liquidation", liquidation),
            "spx_duplicados": ("SPX Duplicados", spx_duplicados),
            "status_dup": ("Status Duplicados", status_duplicados),
            "status_dup_refresh": ("Status Dup Refresh", status_duplicados),
            "workstation": ("Workstation", workstation_assignment),
        }
        
        # Verifica se algum módulo individual foi chamado
        module_called = False
        for arg_name, (display_name, module) in module_map.items():
            if getattr(args, arg_name, False):
                module_called = True
                if check_session():
                    s_time = time.time()
                    # Chama 'run_refresh' apenas para Status Dup Refresh, caso contrário 'run'
                    if arg_name == "status_dup_refresh":
                        run_func_name = "run_refresh"
                    elif arg_name == "recebimento":
                        run_func_name = "run_with_transform"
                    else:
                        run_func_name = "run"

                    run_func = getattr(module, run_func_name)
                    _, _, count = run_func()
                    dur = str(timedelta(seconds=int(time.time() - s_time)))
                    notify_status(f"{display_name}: {count} reg (✅)", dur)
                else:
                    sys.exit(1)
        
        # Pipelines e execução completa
        if getattr(args, "pipeline_spx", False):
            if not check_session():
                sys.exit(1)
            from core.session import SessionExpiredError

            s_time = time.time()
            pipeline_steps = [
                ("Liquidation",        lambda: liquidation.run()),
                ("SPX Duplicados",     lambda: spx_duplicados.run()),
                ("Status Duplicados",  lambda: status_duplicados.run()),
                ("Status Dup Refresh", lambda: status_duplicados.run_refresh()),
            ]

            results = {}
            for idx, (name, step_fn) in enumerate(pipeline_steps):
                if idx > 0:
                    time.sleep(2)
                console.print(f"\n[bold magenta]▶ Etapa: {name}[/bold magenta]")
                try:
                    _, _, count = step_fn()
                    results[name] = (count, "✅")
                except SessionExpiredError:
                    console.print(f"[red]🔒 {name}: Sessão expirada[/red]")
                    results[name] = (0, "🔒")
                    break
                except Exception as e:
                    console.print(f"[red]❌ {name}: {e}[/red]")
                    results[name] = (0, "❌")

            dur = str(timedelta(seconds=int(time.time() - s_time)))

            # Resumo do pipeline
            console.print("\n")
            table = Table(title=f"Pipeline SPX (Duração: {dur})")
            table.add_column("Etapa", style="cyan")
            table.add_column("Registros", justify="right")
            table.add_column("Status", justify="center")
            for name, (count, status) in results.items():
                table.add_row(name, str(count), status)
            console.print(table)

            summary = "\n".join([f"{n}: {c} reg ({s})" for n, (c, s) in results.items()])
            notify_status(f"Pipeline SPX:\n{summary}", dur)
        elif not module_called:
            run_all()
        
        console.print(f"\n[dim]Concluído: {now_brt().strftime('%H:%M:%S')}[/dim]")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Operação cancelada pelo usuário.[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Erro fatal: {e}[/red]")
        raise



if __name__ == "__main__":
    main()
