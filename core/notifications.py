"""
Módulo de Notificações
======================

Envia alertas para Telegram ou Discord após a extração.
"""

import os
import httpx
from rich.console import Console

console = Console()

def send_telegram_message(message: str):
    """Envia mensagem via Bot do Telegram."""
    from core.session import get_session
    session = get_session()
    session_data = session.session_data or {}
    
    # Prioridade: 1. Nuvem/Sessão | 2. Env/Config
    token = session_data.get("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_BOT_TOKEN"))
    chat_id = session_data.get("TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID"))
    
    if not token or not chat_id:
        return False
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        # verify=False é usado para contornar problemas de certificado raiz no Windows (erro [SSL: CERTIFICATE_VERIFY_FAILED])
        with httpx.Client(verify=False) as client:
            client.post(url, json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown"
            })
        return True
    except Exception as e:
        console.print(f"[red]Erro ao enviar Telegram: {e}[/red]")
        return False

def send_discord_message(message: str):
    """Envia mensagem via Webhook do Discord."""
    from core.session import get_session
    session = get_session()
    session_data = session.session_data or {}
    
    # Prioridade: 1. Nuvem/Sessão | 2. Env/Config
    webhook_url = session_data.get("DISCORD_WEBHOOK_URL", os.getenv("DISCORD_WEBHOOK_URL"))
    
    if not webhook_url:
        return False
        
    try:
        # verify=False é usado para contornar problemas de certificado raiz no Windows
        with httpx.Client(verify=False) as client:
            client.post(webhook_url, json={
                "content": message
            })
        return True
    except Exception as e:
        console.print(f"[red]Erro ao enviar Discord: {e}[/red]")
        return False

def notify_status(summary_table_text: str, duration_str: str = "", session_expired: bool = False):
    """Envia o resumo para os canais configurados com informações detalhadas."""
    from datetime import datetime, timedelta, timezone
    import json
    from pathlib import Path
    from core.auth import load_runtime_session_cache
    
    # Timezone Brasil (UTC-3)
    BRT = timezone(timedelta(hours=-3))
    now = datetime.now(BRT)
    now_str = now.strftime("%d/%m/%Y às %H:%M:%S")
    
    # Status dos cookies
    if session_expired:
        cookie_status = "🔴 EXPIRADO - Renovação necessária!"
    else:
        cookies_file = Path(__file__).parent.parent / "cookies.json"
        cookie_status = "❌ Não encontrado"
        cookie_data = load_runtime_session_cache()
        if cookie_data:
            extracted_at = cookie_data.get("extracted_at", "")
            if extracted_at:
                cookie_date = datetime.strptime(extracted_at, "%Y-%m-%d %H:%M:%S")
                age = now.replace(tzinfo=None) - cookie_date
                hours_old = age.total_seconds() / 3600
                if hours_old < 24:
                    cookie_status = f"✅ Atualizado ({extracted_at})"
                elif hours_old < 72:
                    cookie_status = f"⚠️ {int(hours_old)}h atrás ({extracted_at})"
                else:
                    cookie_status = f"🔴 Expirado ({int(hours_old)}h - {extracted_at})"
            else:
                cookie_status = "⚠️ Sem data de extração"
        elif cookies_file.exists():
            try:
                with open(cookies_file, "r", encoding="utf-8") as f:
                    cookie_data = json.load(f)
                extracted_at = cookie_data.get("extracted_at", "")
                if extracted_at:
                    cookie_date = datetime.strptime(extracted_at, "%Y-%m-%d %H:%M:%S")
                    age = now.replace(tzinfo=None) - cookie_date
                    hours_old = age.total_seconds() / 3600
                    if hours_old < 24:
                        cookie_status = f"✅ Atualizado ({extracted_at})"
                    elif hours_old < 72:
                        cookie_status = f"⚠️ {int(hours_old)}h atrás ({extracted_at})"
                    else:
                        cookie_status = f"🔴 Expirado ({int(hours_old)}h - {extracted_at})"
                else:
                    cookie_status = "⚠️ Sem data de extração"
            except Exception:
                cookie_status = "⚠️ Erro ao ler cookies"
    
    # Formata linhas dos módulos
    lines = summary_table_text.split("\n")
    formatted_lines = []
    total_records = 0
    all_ok = True
    for line in lines:
        if ":" in line:
            name, rest = line.split(":", 1)
            formatted_lines.append(f"• *{name.strip()}*: {rest.strip()}")
            if "❌" in rest or "🔒" in rest or "⏭️" in rest:
                all_ok = False
            # Extrai contagem de registros
            try:
                count = int(rest.strip().split(" ")[0])
                total_records += count
            except (ValueError, IndexError):
                pass
        else:
            formatted_lines.append(line)
    
    body = "\n".join(formatted_lines)
    
    if session_expired:
        data_status = "🔴 Sessão expirada"
    elif all_ok:
        data_status = "✅ Todos os módulos OK"
    else:
        data_status = "⚠️ Alguns módulos falharam"
    
    msg = (
        f"📊 *Relatório de Extração SPX*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{body}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 *Status Geral*: {data_status}\n"
        f"📦 *Total de Registros*: {total_records}\n"
        f"🍪 *Cookies*: {cookie_status}\n"
    )
    if duration_str:
        msg += f"⏳ *Duração*: {duration_str}\n"
    msg += f"🕒 *Finalizado*: {now_str}\n"
    
    # Se expirado, adiciona instruções de renovação na mesma mensagem
    if session_expired:
        msg += (
            f"\n⚠️ *AÇÃO NECESSÁRIA*\n"
            f"Execute no seu computador:\n"
            f"`python main.py --auth`\n"
            f"Faça login e os cookies serão atualizados automaticamente na nuvem."
        )
    
    t_ok = send_telegram_message(msg)
    d_ok = send_discord_message(msg)
    


def notify_auth_required():
    """Alerta o usuário que a sessão expirou e é necessário rodar o script local."""
    msg = (
        "⚠️ *AÇÃO NECESSÁRIA: Sessão SPX Expirada*\n\n"
        "O robô no GitHub não conseguiu renovar o login automaticamente.\n\n"
        "🔑 *Como resolver:*\n"
        "1. Abra o VS Code no seu computador.\n"
        "2. Execute: `python main.py`\n"
        "3. Faça o login se solicitado.\n\n"
        "Isso atualizará a 'Chave Mestre' na nuvem e o robô voltará a funcionar sozinho! 🚀"
    )
    t_ok = send_telegram_message(msg)
    d_ok = send_discord_message(msg)
    return t_ok or d_ok
