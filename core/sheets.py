"""
Integração com Google Sheets
============================

Módulo para atualizar planilhas do Google Sheets.
"""

import json
import os.path
from pathlib import Path
import time
import random
from typing import List, Any, Callable, Dict

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from rich.console import Console

from core.config import GOOGLE_SERVICE_ACCOUNT_JSON, SERVICE_ACCOUNT_FILE

console = Console()

from google.oauth2 import service_account

# Escopos necessários
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Configuração de Retry
MAX_RETRIES = 5
BACKOFF_FACTOR = 1.5  # Fator de multiplicação do tempo de espera

# Reuso de cliente no processo para reduzir overhead de autenticação/discovery
_service_cache = None


def _load_service_account_credentials():
    """Carrega credenciais do Google Sheets via secret em memória ou arquivo local."""
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        try:
            credentials_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            return service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=SCOPES,
            )
        except json.JSONDecodeError as exc:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT não contém um JSON válido.") from exc

    service_account_path = Path(SERVICE_ACCOUNT_FILE)
    if service_account_path.exists():
        return service_account.Credentials.from_service_account_file(
            str(service_account_path),
            scopes=SCOPES,
        )

    raise FileNotFoundError(
        "Service Account não configurada. Defina GOOGLE_SERVICE_ACCOUNT ou crie service_account.json localmente."
    )


def get_service():
    """Autentica e retorna o serviço do Google Sheets via Service Account."""
    global _service_cache
    if _service_cache is not None:
        return _service_cache

    try:
        creds = _load_service_account_credentials()
        _service_cache = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return _service_cache
    except Exception as e:
        console.print(f"[red]Erro ao carregar Service Account: {e}[/red]")
        raise


def execute_with_retry(request: Any, retries: int = MAX_RETRIES) -> Any:
    """
    Executa uma requisição da API do Google com retry automático para erros temporários.
    
    Args:
        request: Objeto request da googleapiclient (ex: service.spreadsheets().get(...))
        retries: Número máximo de tentativas
        
    Returns:
        Resultado do request.execute()
    
    Raises:
        HttpError: Se falhar após todas as tentativas ou se for um erro não tratável
    """
    last_error = None
    
    for n in range(retries):
        try:
            return request.execute()
        except HttpError as err:
            last_error = err
            # Erros temporários: 429 (Too Many Requests), 500 (Internal Server Error), 
            # 502 (Bad Gateway), 503 (Service Unavailable), 504 (Gateway Timeout)
            if err.resp.status in [429, 500, 502, 503, 504]:
                # Exponential Backoff com Jitter
                sleep_time = (BACKOFF_FACTOR ** n) + random.uniform(0, 1)
                console.print(f"[yellow]⚠️ Erro {err.resp.status} na API. Tentativa {n+1}/{retries}. Aguardando {sleep_time:.2f}s...[/yellow]")
                time.sleep(sleep_time)
            else:
                # Se não for erro temporário, falha imediatamente
                raise
        except Exception as e:
            # Outros erros de conexão (socket timeout, reset, etc) também podem valer retry
            console.print(f"[red]❌ Erro de conexão/inesperado: {e}. Tentando novamente...[/red]")
            time.sleep(2)
            
    if last_error:
        raise last_error
    return None


def ensure_sheet_exists(service, spreadsheet_id: str, sheet_title: str):
    """Verifica se uma aba existe, se não, cria."""
    try:
        spreadsheet = execute_with_retry(service.spreadsheets().get(spreadsheetId=spreadsheet_id))
        sheets = spreadsheet.get("sheets", [])
        
        for sheet in sheets:
            if sheet["properties"]["title"] == sheet_title:
                return  # Já existe
        
        # Se não existe, cria
        console.print(f"[yellow]⚠️ Aba '{sheet_title}' não encontrada. Criando...[/yellow]")
        body = {
            "requests": [{
                "addSheet": {
                    "properties": {
                        "title": sheet_title
                    }
                }
            }]
        }
        execute_with_retry(service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ))
        console.print(f"[green]✅ Aba '{sheet_title}' criada com sucesso.[/green]")
        
    except HttpError as e:
        # Se o erro é "already exists", a aba existe — ignorar
        if "already exists" in str(e):
            return
        console.print(f"[red]Erro ao verificar/criar aba: {e}[/red]")
    except Exception as e:
        console.print(f"[red]Erro ao verificar/criar aba: {e}[/red]")


def _extract_sheet_title(range_name: str) -> str:
    """Extrai o título da aba a partir de um range A1."""
    sheet_title = range_name.split("!")[0] if "!" in range_name else range_name
    if sheet_title.startswith("'") and sheet_title.endswith("'"):
        sheet_title = sheet_title[1:-1]
    return sheet_title.strip()


def _extract_start_col(range_name: str) -> str:
    """Extrai a coluna inicial do range (fallback: A)."""
    if "!" in range_name:
        a1 = range_name.split("!", 1)[1]
    else:
        a1 = range_name

    start_ref = a1.split(":", 1)[0]
    letters = "".join(ch for ch in start_ref if ch.isalpha())
    return letters.upper() or "A"


def letter_to_col(col_letter: str) -> int:
    """Converte letra de coluna (A, AA...) para índice 1-based."""
    letters = str(col_letter or "").strip().upper()
    if not letters:
        return 1

    value = 0
    for ch in letters:
        if not ("A" <= ch <= "Z"):
            continue
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return max(1, value)


def update_sheet(
    spreadsheet_id: str,
    range_name: str,
    values: List[List[Any]],
    clear_first: bool = True
) -> bool:
    """
    Atualiza uma aba do Google Sheets.
    
    Args:
        spreadsheet_id: ID da planilha
        range_name: Nome da aba e intervalo (ex: 'Recebimento!A1')
        values: Lista de listas com os dados (linhas e colunas)
        clear_first: Se deve limpar o intervalo antes de escrever
    
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        service = get_service()
        
        # Extrai o nome da aba do range_name (ex: 'Status SPX'!A1 -> Status SPX)
        if "!" in range_name:
            sheet_title = range_name.split("!")[0]
            # Remove aspas simples se houver
            if sheet_title.startswith("'") and sheet_title.endswith("'"):
                sheet_title = sheet_title[1:-1]
            
            ensure_sheet_exists(service, spreadsheet_id, sheet_title)

        sheet = service.spreadsheets()
        
        if clear_first:
            # Limpa o conteúdo existente da aba inteira
            # Usa o nome da aba sem intervalo específico para limpar tudo
            if "!" in range_name:
                clear_range = range_name.split("!")[0]
            else:
                clear_range = range_name
            execute_with_retry(sheet.values().clear(
                spreadsheetId=spreadsheet_id,
                range=clear_range
            ))
        
        # Sanitiza e filtra linhas completamente vazias
        sanitized_values = _sanitize_and_filter_rows(values)
        if not sanitized_values:
            console.print(f"[yellow]⚠️ Nenhuma linha válida para append em {range_name}. Pulando.[/yellow]")
            return True

        body = {"values": sanitized_values}
        
        # Escreve os novos dados
        result = execute_with_retry(sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body=body
        ))
        
        updated_cells = result.get('updatedCells', 0)
        console.print(f"[green]✅ Planilha atualizada: {updated_cells} células alteradas.[/green]")
        return True
        
    except HttpError as err:
        console.print(f"[red]❌ Erro na API do Google Sheets: {err}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]❌ Erro inesperado ao atualizar planilha: {e}[/red]")
        return False


def append_sheet(
    spreadsheet_id: str,
    range_name: str,
    values: List[List[Any]],
    num_cols: int | None = None,
) -> bool:
    """
    Adiciona linhas ao final de uma aba do Google Sheets.
    
    Args:
        spreadsheet_id: ID da planilha
        range_name: Nome da aba (ex: 'SPX DUPLICADO')
        values: Lista de listas com os dados (linhas e colunas)
    
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        service = get_service()
        
        sheet_title = _extract_sheet_title(range_name)
        if sheet_title:
            ensure_sheet_exists(service, spreadsheet_id, sheet_title)

        sheet = service.spreadsheets()
        
        # Sanitiza e filtra linhas vazias
        sanitized_values = _sanitize_and_filter_rows(values)
        if not sanitized_values:
            console.print(f"[yellow]⚠️ Nenhuma linha válida para append em {range_name}. Pulando.[/yellow]")
            return True

        # Aplica pad/trunc para garantir número fixo de colunas quando solicitado
        if num_cols is not None:
            normalized: List[List[Any]] = []
            for row in sanitized_values:
                if len(row) >= num_cols:
                    normalized.append(row[:num_cols])
                else:
                    # preenche com strings vazias para manter consistência de colunas
                    normalized.append(row + [""] * (num_cols - len(row)))
            sanitized_values = normalized

        body = {"values": sanitized_values}
        
        # Escreve os novos dados usando append sem forçar inserção de linhas.
        # OVERWRITE evita crescimento desnecessário da grade e reduz risco de estourar 10M células.
        result = execute_with_retry(sheet.values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            insertDataOption="OVERWRITE",
            body=body
        ))
        
        updated_cells = result.get('updates', {}).get('updatedCells', 0)
        console.print(f"[green]✅ Planilha (append): {updated_cells} células adicionadas.[/green]")
        return True
        
    except HttpError as err:
        err_msg = str(err)
        # Fallback específico para limite de células do workbook.
        # Quando INSERT/APPEND não cabe mais, gravamos por update em faixa fixa.
        if "above the limit of 10000000 cells" in err_msg:
            try:
                service = get_service()
                sheet = service.spreadsheets()

                sheet_title = _extract_sheet_title(range_name)
                start_col = _extract_start_col(range_name)
                if not sheet_title:
                    raise ValueError(f"Range inválido para fallback: {range_name}")

                ensure_sheet_exists(service, spreadsheet_id, sheet_title)

                # Detecta próxima linha livre baseada na coluna inicial.
                scan_range = f"'{sheet_title}'!{start_col}:{start_col}"
                scan_result = execute_with_retry(sheet.values().get(
                    spreadsheetId=spreadsheet_id,
                    range=scan_range,
                ))
                used_rows = len(scan_result.get("values", []))
                next_row = max(1, used_rows + 1)

                sheet_obj = _get_sheet_metadata(spreadsheet_id, sheet_title)
                if not sheet_obj:
                    raise ValueError(f"Aba não encontrada no fallback: {sheet_title}")

                props = sheet_obj.get("properties", {})
                grid = props.get("gridProperties", {})
                current_rows = int(grid.get("rowCount", 0) or 0)
                column_count = int(grid.get("columnCount", 26) or 26)
                required_last_row = next_row + len(sanitized_values) - 1

                # Se necessário, tenta expandir somente até o máximo teórico permitido.
                if required_last_row > current_rows:
                    max_rows_allowed = max(1, 10_000_000 // max(1, column_count))
                    if required_last_row > max_rows_allowed:
                        available = max(0, max_rows_allowed - next_row + 1)
                        if available <= 0:
                            console.print(
                                "[red]❌ Sem espaço de células para escrever novas linhas nesta aba. "
                                "Reduza o tamanho da planilha.[/red]"
                            )
                            return False
                        console.print(
                            f"[yellow]⚠️ Limite de células atingido: gravando apenas {available} de {len(sanitized_values)} linhas.[/yellow]"
                        )
                        sanitized_values = sanitized_values[:available]
                        required_last_row = next_row + len(sanitized_values) - 1

                    if required_last_row > current_rows:
                        update_req = {
                            "requests": [{
                                "updateSheetProperties": {
                                    "properties": {
                                        "sheetId": props.get("sheetId"),
                                        "gridProperties": {"rowCount": required_last_row},
                                    },
                                    "fields": "gridProperties.rowCount",
                                }
                            }]
                        }
                        execute_with_retry(service.spreadsheets().batchUpdate(
                            spreadsheetId=spreadsheet_id,
                            body=update_req,
                        ))

                write_range = f"'{sheet_title}'!{start_col}{next_row}"
                result = execute_with_retry(sheet.values().update(
                    spreadsheetId=spreadsheet_id,
                    range=write_range,
                    valueInputOption="USER_ENTERED",
                    body={"values": sanitized_values},
                ))
                updated_cells = result.get("updatedCells", 0)
                console.print(
                    f"[green]✅ Planilha (fallback update): {updated_cells} células adicionadas sem INSERT_ROWS.[/green]"
                )
                return True
            except Exception as fallback_err:
                console.print(f"[red]❌ Fallback de append falhou: {fallback_err}[/red]")

        console.print(f"[red]❌ Erro na API do Google Sheets (append): {err}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]❌ Erro inesperado ao fazer append na planilha: {e}[/red]")
        return False


def update_sheet_batch(
    spreadsheet_id: str,
    data_ranges: List[Dict[str, Any]],
    value_input_option: str = "USER_ENTERED",
) -> bool:
    """
    Atualiza múltiplos intervalos em uma única chamada à API (values.batchUpdate).

    Args:
        spreadsheet_id: ID da planilha
        data_ranges: Lista de dicts no formato {"range": "'Aba'!A1:B2", "values": [[...], ...]}
        value_input_option: Opção de entrada da API do Sheets

    Returns:
        True se sucesso, False caso contrário
    """
    if not data_ranges:
        return True

    try:
        service = get_service()
        sheet = service.spreadsheets()

        # Garante existência das abas referenciadas nos ranges.
        sheet_titles = set()
        for item in data_ranges:
            range_name = str(item.get("range", ""))
            if "!" not in range_name:
                continue
            sheet_title = range_name.split("!")[0]
            if sheet_title.startswith("'") and sheet_title.endswith("'"):
                sheet_title = sheet_title[1:-1]
            if sheet_title:
                sheet_titles.add(sheet_title)

        for sheet_title in sheet_titles:
            ensure_sheet_exists(service, spreadsheet_id, sheet_title)

        sanitized_data = []
        for item in data_ranges:
            range_name = item.get("range")
            values = item.get("values", [])
            if not range_name:
                continue

            sanitized_values = []
            for row in values:
                new_row = []
                for cell in row:
                    if isinstance(cell, (str, int, float, bool)) or cell is None:
                        new_row.append(cell)
                    else:
                        new_row.append(str(cell))
                sanitized_values.append(new_row)

            sanitized_data.append({"range": range_name, "values": sanitized_values})

        if not sanitized_data:
            return True

        body = {
            "valueInputOption": value_input_option,
            "data": sanitized_data,
        }
        result = execute_with_retry(sheet.values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body,
        ))

        total_updated = result.get("totalUpdatedCells", 0)
        console.print(f"[green]✅ Batch update concluído: {total_updated} células alteradas.[/green]")
        return True

    except HttpError as err:
        console.print(f"[red]❌ Erro na API do Google Sheets (batch update): {err}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]❌ Erro inesperado no batch update da planilha: {e}[/red]")
        return False


def insert_rows_at_top(
    spreadsheet_id: str,
    sheet_title: str,
    values: List[List[Any]],
    start_col: str = "B",
    end_col: str = "F"
) -> bool:
    """
    Insere linhas logo após o header (linha 1) de uma aba,
    escrevendo dados em colunas específicas (ex: B2:F2).

    1. Insere N linhas vazias na posição 2 (empurra dados existentes para baixo).
    2. Escreve os valores nas colunas start_col:end_col das novas linhas.

    Args:
        spreadsheet_id: ID da planilha
        sheet_title: Nome da aba (sem aspas)
        values: Lista de listas com dados a inserir
        start_col: Coluna inicial (ex: 'B')
        end_col: Coluna final (ex: 'F')

    Returns:
        True se sucesso, False caso contrário
    """
    try:
        service = get_service()

        # 1. Obter o sheetId da aba
        spreadsheet = execute_with_retry(service.spreadsheets().get(spreadsheetId=spreadsheet_id))
        sheet_id = None
        for sheet in spreadsheet.get("sheets", []):
            if sheet["properties"]["title"] == sheet_title:
                sheet_id = sheet["properties"]["sheetId"]
                break

        if sheet_id is None:
            console.print(f"[red]❌ Aba '{sheet_title}' não encontrada.[/red]")
            return False

        num_rows = len(values)

        # 2. Inserir N linhas vazias na posição 2 (logo após o header)
        insert_request = {
            "requests": [{
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 1,  # Após o header (0-indexed)
                        "endIndex": 1 + num_rows
                    },
                    "inheritFromBefore": False
                }
            }]
        }
        execute_with_retry(service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=insert_request
        ))

        # 3. Sanitizar os valores
        sanitized_values = []
        for row in values:
            new_row = []
            for cell in row:
                if isinstance(cell, (str, int, float, bool)) or cell is None:
                    new_row.append(cell)
                else:
                    new_row.append(str(cell))
            sanitized_values.append(new_row)

        # 4. Escrever os dados nas colunas especificadas (B2:F<n+1>)
        write_range = f"'{sheet_title}'!{start_col}2:{end_col}{1 + num_rows}"
        body = {"values": sanitized_values}

        result = execute_with_retry(service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=write_range,
            valueInputOption="USER_ENTERED",
            body=body
        ))

        updated_cells = result.get('updatedCells', 0)
        console.print(f"[green]✅ {num_rows} linhas inseridas no topo ({updated_cells} células).[/green]")
        return True

    except HttpError as err:
        console.print(f"[red]❌ Erro na API do Google Sheets (insert_rows_at_top): {err}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]❌ Erro inesperado ao inserir linhas: {e}[/red]")
        return False


def read_sheet(spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    """Lê dados de uma aba do Google Sheets."""
    service = get_service()
    sheet = service.spreadsheets()
    try:
        result = execute_with_retry(sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ))
        return result.get('values', [])
    except HttpError as err:
        msg = str(err)
        console.print(f"[red]❌ Erro ao ler planilha: {err}[/red]")

        # Se a API reclamar do range (ex: Unable to parse range), tenta fallback seguro:
        if "Unable to parse range" in msg or "parse range" in msg:
            console.print(f"[yellow]⚠️ Range inválido: {range_name}. Tentando fallback seguro...[/yellow]")

            # Extrai nome da aba se possível
            sheet_title = None
            if "!" in range_name:
                sheet_title = range_name.split("!")[0]
                if sheet_title.startswith("'") and sheet_title.endswith("'"):
                    sheet_title = sheet_title[1:-1]
            else:
                sheet_title = range_name.strip().strip("'")

            if not sheet_title:
                return []

            # Verifica existência da aba
            sheet_obj = _get_sheet_metadata(spreadsheet_id, sheet_title)
            if not sheet_obj:
                console.print(f"[yellow]⚠️ Aba '{sheet_title}' não encontrada. Pulando leitura.[/yellow]")
                return []

            props = sheet_obj.get("properties", {})
            current_rows = props.get("gridProperties", {}).get("rowCount", 1000)
            col_count = props.get("gridProperties", {}).get("columnCount", 26)
            last_col = col_to_letter(col_count or 26)

            # Range explícito A1:<last_col><rows> como fallback
            fallback_range = f"'{sheet_title}'!A1:{last_col}{current_rows}"
            console.print(f"[dim]📋 Tentando ler com range de fallback: {fallback_range}[/dim]")
            try:
                result = execute_with_retry(sheet.values().get(
                    spreadsheetId=spreadsheet_id,
                    range=fallback_range
                ))
                return result.get('values', [])
            except Exception as e2:
                console.print(f"[red]❌ Falha no fallback de leitura: {e2}[/red]")
                return []

        return []
    except Exception as e:
        console.print(f"[red]❌ Erro inesperado ao ler planilha: {e}[/red]")
        return []


def col_to_letter(col_index: int) -> str:
    """Converte índice (1-based) de coluna para letra (ex: 1 -> A, 27 -> AA)."""
    letters = ""
    while col_index > 0:
        col_index -= 1
        letters = chr(ord('A') + (col_index % 26)) + letters
        col_index //= 26
    return letters


def _sanitize_and_filter_rows(values: List[List[Any]]) -> List[List[Any]]:
    """Sanitiza valores e remove linhas completamente vazias (todas células nulas/strings vazias).

    Retorna lista sanitizada pronta para enviar à API.
    """
    sanitized = []
    for row in values:
        if not isinstance(row, list):
            continue
        new_row: list[Any] = []
        for cell in row:
            if isinstance(cell, (str, int, float, bool)) or cell is None:
                new_row.append(cell)
            else:
                new_row.append(str(cell))

        # Considera vazia se todas as células são None ou string vazia
        all_empty = True
        for c in new_row:
            if c is not None and str(c).strip() != "":
                all_empty = False
                break

        if not all_empty:
            sanitized.append(new_row)

    return sanitized


def _get_sheet_metadata(spreadsheet_id: str, sheet_title: str) -> dict | None:
    """Retorna o objeto 'sheet' (propriedades) para uma aba pelo título, ou None se não existir."""
    service = get_service()
    spreadsheet = execute_with_retry(service.spreadsheets().get(spreadsheetId=spreadsheet_id))
    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_title:
            return sheet
    return None


def trim_sheet_rows(spreadsheet_id: str, sheet_title: str, keep_rows: int = 1) -> bool:
    """Reduz a grade de linhas para `keep_rows` quando possível.

    Se a redução da grade falhar por restrições da aba, faz fallback limpando conteúdo.
    """
    try:
        service = get_service()
        sheet_obj = _get_sheet_metadata(spreadsheet_id, sheet_title)
        if not sheet_obj:
            console.print(f"[yellow]⚠️ Aba '{sheet_title}' não encontrada para trimming.[/yellow]")
            return False

        props = sheet_obj.get("properties", {})
        sheet_id = props.get("sheetId")
        grid = props.get("gridProperties", {})
        current_rows = int(grid.get("rowCount", 0) or 0)
        frozen_rows = int(grid.get("frozenRowCount", 0) or 0)

        keep_rows = max(1, int(keep_rows))
        keep_rows = max(keep_rows, frozen_rows)

        # Nada a fazer se já for menor/igual
        if current_rows <= keep_rows:
            return True

        try:
            update_req = {
                "requests": [{
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {"rowCount": keep_rows},
                        },
                        "fields": "gridProperties.rowCount",
                    }
                }]
            }
            execute_with_retry(service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=update_req
            ))
            console.print(f"[green]✅ Grade reduzida em '{sheet_title}': {current_rows} -> {keep_rows} linhas.[/green]")
        except Exception:
            # Fallback seguro: limpa conteúdo excedente sem reduzir grid.
            last_col = col_to_letter(props.get("gridProperties", {}).get("columnCount", 26) or 26)
            start_row = keep_rows + 1
            clear_range = f"'{sheet_title}'!A{start_row}:{last_col}{current_rows}"
            execute_with_retry(service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=clear_range
            ))
            console.print(f"[yellow]⚠️ Não foi possível reduzir a grade de '{sheet_title}'. Conteúdo limpo em {start_row}-{current_rows}.[/yellow]")

        return True
    except HttpError as e:
        console.print(f"[red]❌ Erro ao reduzir linhas da aba '{sheet_title}': {e}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]❌ Erro inesperado ao reduzir linhas: {e}[/red]")
        return False


def prepare_sheet_for_append(
    spreadsheet_id: str,
    sheet_title: str,
    start_col: str = "A",
    end_col: str = "M",
    header_rows: int = 1,
) -> bool:
    """Prepara uma aba para append de dados em um range de colunas.

    Passos:
    1. Limpa o intervalo de dados abaixo do header (ex: A2:M).
    2. Reduz a grade para manter apenas as linhas de header (usa `trim_sheet_rows`).
    3. Ajusta número de colunas para o range de destino (ex: A:M -> 13 colunas).

    Retorna True se bem sucedido.
    """
    try:
        service = get_service()

        # Garante existência da aba
        ensure_sheet_exists(service, spreadsheet_id, sheet_title)

        # Garante que a aba tenha linhas suficientes antes de limpar o intervalo
        sheet_obj = _get_sheet_metadata(spreadsheet_id, sheet_title)
        if not sheet_obj:
            console.print(f"[red]❌ Aba '{sheet_title}' não encontrada após criação.[/red]")
            return False

        props = sheet_obj.get("properties", {})
        sheet_id = props.get("sheetId")
        current_rows = props.get("gridProperties", {}).get("rowCount", 0)
        required_row_index = header_rows + 1  # linha inicial do clear (1-based)

        if current_rows < required_row_index:
            # Expande a grade para conter pelo menos required_row_index linhas
            new_row_count = required_row_index
            update_req = {
                "requests": [{
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {"rowCount": new_row_count}
                        },
                        "fields": "gridProperties.rowCount"
                    }
                }]
            }
            execute_with_retry(service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=update_req
            ))
            # Recarrega metadata após alteração
            sheet_obj = _get_sheet_metadata(spreadsheet_id, sheet_title)
            props = sheet_obj.get("properties", {})
            current_rows = props.get("gridProperties", {}).get("rowCount", 0)

        # Limpa A{header_rows+1}:{end_col} (ex: A2:M)
        clear_range = f"'{sheet_title}'!{start_col}{header_rows + 1}:{end_col}"
        execute_with_retry(service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=clear_range
        ))

        # Reduz a grade para o header
        trim_sheet_rows(spreadsheet_id, sheet_title, keep_rows=header_rows)

        # Ajusta a largura da aba para o intervalo usado (A:M, por padrão)
        sheet_obj = _get_sheet_metadata(spreadsheet_id, sheet_title)
        if sheet_obj:
            props = sheet_obj.get("properties", {})
            sheet_id = props.get("sheetId")
            grid = props.get("gridProperties", {})
            current_cols = int(grid.get("columnCount", 26) or 26)
            frozen_cols = int(grid.get("frozenColumnCount", 0) or 0)
            target_cols = max(letter_to_col(end_col), frozen_cols, 1)

            if current_cols != target_cols:
                col_req = {
                    "requests": [{
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": sheet_id,
                                "gridProperties": {"columnCount": target_cols},
                            },
                            "fields": "gridProperties.columnCount",
                        }
                    }]
                }
                try:
                    execute_with_retry(service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=col_req,
                    ))
                    console.print(f"[green]✅ Grade de colunas ajustada em '{sheet_title}': {current_cols} -> {target_cols}.[/green]")
                except Exception as col_err:
                    console.print(f"[yellow]⚠️ Não foi possível ajustar colunas de '{sheet_title}': {col_err}[/yellow]")

        console.print(f"[green]✅ Aba '{sheet_title}' preparada para append ({start_col}{header_rows+1}:{end_col} limpa).[/green]")
        return True
    except Exception as e:
        console.print(f"[red]❌ Falha ao preparar aba '{sheet_title}' para append: {e}[/red]")
        return False


def cleanup_orphan_rows(spreadsheet_id: str, default_end_col: str = 'Z', per_sheet_col_ends: dict | None = None) -> bool:
    """Percorre todas as abas e deleta linhas vazias ao final (deleteDimension).

    - `per_sheet_col_ends`: dict opcional {sheet_title: 'M'|'B'...} para controlar colunas lidas.
    - Por segurança mantém ao menos 1 linha por aba.
    """
    try:
        service = get_service()
        spreadsheet = execute_with_retry(service.spreadsheets().get(spreadsheetId=spreadsheet_id))

        sheets = spreadsheet.get('sheets', [])
        for sheet in sheets:
            props = sheet.get('properties', {})
            title = props.get('title')
            sheet_id = props.get('sheetId')
            if not title:
                continue

            # Decide fim da coluna para leitura
            col_end = default_end_col
            if per_sheet_col_ends and title in per_sheet_col_ends:
                col_end = per_sheet_col_ends[title]
            else:
                low = title.lower()
                if 'raw_tracking' in low:
                    col_end = 'M'
                elif title == 'FORA DE ESTAÇÃO':
                    col_end = 'B'

            read_range = f"'{title}'!A1:{col_end}"
            rows = read_sheet(spreadsheet_id, read_range)
            last_non_empty = len(rows) if rows is not None else 0
            keep_rows = max(1, last_non_empty)

            current_rows = props.get('gridProperties', {}).get('rowCount', 0)
            if current_rows > keep_rows:
                # Em vez de deletar fisicamente as linhas (o que pode gerar erro quando
                # tenta remover todas as linhas não-frozen), apenas limpamos o conteúdo
                # das linhas excedentes.
                clear_range = f"'{title}'!A{keep_rows+1}:{col_end}{current_rows}"
                execute_with_retry(service.spreadsheets().values().clear(
                    spreadsheetId=spreadsheet_id,
                    range=clear_range
                ))
                console.print(f"[green]✅ Conteúdo limpo em '{title}' linhas {keep_rows+1}-{current_rows} (mantidas {keep_rows}).[/green]")

        return True
    except Exception as e:
        console.print(f"[red]❌ Falha no cleanup de abas: {e}[/red]")
        return False

_cloud_config_cache: dict | None = None

def get_cloud_config(spreadsheet_id: str) -> dict:
    """Retorna as configurações armazenadas na aba CONFIG_CLOUD."""
    global _cloud_config_cache
    if _cloud_config_cache is not None:
        return _cloud_config_cache.copy()

    # Range amplo para leitura (A:B cobre todas as linhas)
    read_range = "'CONFIG_CLOUD'!A:B"
    
    # Debug: mostra o ID e aba sendo usados
    console.print(f"[dim]📋 Lendo de: {spreadsheet_id} | Aba: CONFIG_CLOUD[/dim]")
    
    rows = read_sheet(spreadsheet_id, read_range)
    
    # Debug: mostra quantas linhas foram lidas
    console.print(f"[dim]📊 Linhas lidas da nuvem: {len(rows)}[/dim]")
    if rows:
        console.print(f"[dim]📝 Primeira linha: {rows[0] if rows else 'vazio'}[/dim]")
    
    config = {}
    for row in rows:
        if len(row) >= 2:
            config[row[0]] = row[1]
    _cloud_config_cache = config
    return config


def update_cloud_config(spreadsheet_id: str, config_dict: dict):
    """Atualiza as configurações na aba CONFIG_CLOUD."""
    global _cloud_config_cache
    _cloud_config_cache = None  # invalida cache ao salvar nova sessão

    from core.config import SHEETS_TABS
    tab = SHEETS_TABS["cloud_config"]
    
    values = [["Key", "Value"]] # Header
    for k, v in config_dict.items():
        values.append([k, v])
        
    return update_sheet(spreadsheet_id, tab, values, clear_first=True)


def batch_update_values(spreadsheet_id: str, data: List[dict]) -> bool:
    """
    Atualiza múltiplos intervalos de uma vez só.
    
    Args:
        spreadsheet_id: ID da planilha
        data: Lista de dicionários no formato {'range': 'A1', 'values': [['valor']]}
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        service = get_service()
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': data
        }
        execute_with_retry(service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ))
        console.print(f"[green]✅ Atualização em lote concluída: {len(data)} intervalos.[/green]")
        return True
    except Exception as e:
        console.print(f"[red]❌ Erro na atualização em lote: {e}[/red]")
        return False
