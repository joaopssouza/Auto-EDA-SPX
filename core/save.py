"""
Helper de Salvamento de Dados
=============================

Funções centralizadas para salvar dados em JSON, CSV e Google Sheets.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from rich.console import Console

from core.config import OUTPUT_DIR, SHEETS_TABS, SPREADSHEET_ID, SAVE_LOCAL_FILES
from core.sheets import update_sheet, append_sheet

console = Console()


def get_output_paths(module_name: str) -> tuple[Path, Path]:
    """Retorna os paths de output para um módulo."""
    base_dir = OUTPUT_DIR / module_name
    json_dir = base_dir / "json"
    csv_dir = base_dir / "csv"
    
    json_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    
    return json_dir, csv_dir


def save_json(data: list[dict], module_name: str, filename: Optional[str] = None) -> Path:
    """Salva dados em arquivo JSON."""
    json_dir, _ = get_output_paths(module_name)
    
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{module_name}_{timestamp}.json"
    
    filepath = json_dir / filename
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    console.print(f"[green]📄 JSON: {filepath}[/green]")
    return filepath


def save_csv(data: list[dict], module_name: str, filename: Optional[str] = None) -> Path:
    """Salva dados em arquivo CSV."""
    _, csv_dir = get_output_paths(module_name)
    
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{module_name}_{timestamp}.csv"
    
    filepath = csv_dir / filename
    
    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    
    console.print(f"[green]📊 CSV: {filepath}[/green]")
    return filepath


def save_to_sheets(data: list[dict], module_name: str, append: bool = False) -> bool:
    """
    Salva dados no Google Sheets.
    
    Args:
        data: Lista de dicionários com os dados
        module_name: Nome do módulo
    
    Returns:
        True se sucesso, False caso contrário
    """
    range_name = SHEETS_TABS.get(module_name)
    
    if not range_name:
        console.print(f"[yellow]⚠️ Módulo '{module_name}' não configurado para Google Sheets (SHEETS_TABS).[/yellow]")
        return False
    
    if not SPREADSHEET_ID:
        console.print("[yellow]⚠️ SPREADSHEET_ID não configurado no config.py.[/yellow]")
        return False
        
    console.print(f"[cyan]Enviando para Google Sheets ({range_name})...[/cyan]")
    
    # Prepara os dados (cabeçalho + linhas)
    # Garante que os valores sejam strings para evitar problemas de formatação
    if not data:
        return False
        
    # Usa pandas para lidar com formatação e tipos de dados de forma robusta
    df = pd.DataFrame(data)
    
    # Preenche NaNs com string vazia
    df = df.fillna("")
    
    # Converte tudo para string para envio seguro
    # df = df.astype(str) # Opcional: converte tudo para string, mas Sheets aceita números
    
    # Converte para lista de listas
    if append:
        values = df.values.tolist()
        return append_sheet(SPREADSHEET_ID, range_name, values)
    else:
        values = [df.columns.values.tolist()] + df.values.tolist()
        return update_sheet(SPREADSHEET_ID, range_name, values)


def save_data(
    data: list[dict],
    module_name: str,
    save_json_file: bool = SAVE_LOCAL_FILES,
    save_csv_file: bool = SAVE_LOCAL_FILES,
    upload_sheets: bool = True,
    append: bool = False
) -> tuple[Optional[Path], Optional[Path], int]:
    """
    Salva dados em JSON, CSV e envia para Google Sheets.
    
    Args:
        data: Lista de dicionários com os dados
        module_name: Nome do módulo
        save_json_file: Se deve salvar JSON
        save_csv_file: Se deve salvar CSV
        upload_sheets: Se deve enviar para o Sheets
    
    Returns:
        Tuple com (json_path, csv_path, count)
    """
    if not data:
        console.print("[yellow]⚠️ Nenhum dado para salvar.[/yellow]")
        return None, None, 0
    
    json_path = None
    csv_path = None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if save_json_file:
        json_path = save_json(data, module_name, f"{module_name}_{timestamp}.json")
    
    if save_csv_file:
        csv_path = save_csv(data, module_name, f"{module_name}_{timestamp}.csv")
        
    if upload_sheets:
        save_to_sheets(data, module_name, append=append)
    
    return json_path, csv_path, len(data)
