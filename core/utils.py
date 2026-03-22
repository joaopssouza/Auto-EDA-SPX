"""
Utilitários Gerais
==================

Funções de suporte para validação e helper.
"""

from pathlib import Path
from rich.console import Console
import sys
import os

from core.config import BASE_DIR, SERVICE_ACCOUNT_FILE, SPREADSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON

console = Console()

def validate_config() -> bool:
    """
    Valida se todas as configurações e arquivos necessários estão presentes.
    """
    console.print("[yellow]🔍 Validando ambiente...[/yellow]")
    
    errors = []
    
    env_file = BASE_DIR / ".env"
    service_account_path = Path(SERVICE_ACCOUNT_FILE)

    if not env_file.exists() and os.getenv("GITHUB_ACTIONS") != "true":
        console.print("[dim]ℹ️ .env não encontrado. Seguindo com variáveis de ambiente já carregadas.[/dim]")

    if not GOOGLE_SERVICE_ACCOUNT_JSON and not service_account_path.exists():
        errors.append(
            "Credenciais do Google Sheets ausentes. Defina GOOGLE_SERVICE_ACCOUNT ou crie service_account.json localmente."
        )

    if not SPREADSHEET_ID:
        errors.append("GOOGLE_SHEETS_ID não configurado no ambiente.")

    if errors:
        console.print("\n[bold red]❌ Falha na Validação do Ambiente:[/bold red]")
        for err in errors:
            console.print(f"  - {err}")
        return False
        
    console.print("[green]✅ Ambiente validado com sucesso![/green]\n")
    return True
