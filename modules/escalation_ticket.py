"""
Módulo de Extração: Escalation Ticket
======================================

Extrai dados de tickets escalados via GET.
Operação: GET de Escalation Ticket.

Divide o período em chunks para contornar o limite de 10k do Elasticsearch.
"""

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from rich.console import Console

from core.config import BRT, ESCALATION_TICKET, DEFAULT_PAGE_SIZE, MAX_PAGES
from core.save import save_data
from core.session import get_session

console = Console()

MODULE_NAME = "escalation_ticket"

# Mapeamento de status (1-4) para descrição legível
STATUS_MAP = {
    1: "Created",
    2: "Reviewing",
    3: "Resolved",
    4: "Cancelled",
}

DATETIME_FIELDS = ("end_time", "ctime")

ASSIGNEE_TYPE_LABELS = {
    "1": "Admin",
    "2": "Station Lead",
    "3": "Station",
}

_AGING_TIME_TOKEN_PATTERN = re.compile(
    r"(?P<value>\d+)\s*(?P<unit>d|dia|dias|h|hora|horas|m|min|mins|minuto|minutos)",
    re.IGNORECASE,
)


def _map_ticket_status_in_items(items: list[dict]) -> None:
    """Converte valores numéricos de `ticket_status` para as descrições.

    Modifica os dicionários in-place.
    """
    for it in items:
        if not isinstance(it, dict):
            continue

        if "ticket_status" not in it:
            continue

        val = it.get("ticket_status")
        try:
            if isinstance(val, str) and val.isdigit():
                key = int(val)
            elif isinstance(val, (int, float)):
                key = int(val)
            else:
                continue

            it["ticket_status"] = STATUS_MAP.get(key, it.get("ticket_status"))
        except Exception:
            continue


def _to_epoch_seconds(value) -> int | None:
    """Normaliza timestamp epoch para segundos (suporta ms)."""
    if value is None or isinstance(value, bool):
        return None

    try:
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            num = int(float(raw))
        elif isinstance(value, (int, float)):
            num = int(value)
        else:
            return None
    except (TypeError, ValueError):
        return None

    if abs(num) > 10_000_000_000:
        num = num // 1000

    return num


def _format_epoch_to_brt(value):
    """Converte epoch para string dd/mm/aaaa HH:MM:SS no fuso BRT."""
    epoch_seconds = _to_epoch_seconds(value)
    if epoch_seconds is None:
        return value

    try:
        return datetime.fromtimestamp(epoch_seconds, tz=BRT).strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return value


def _map_datetime_fields_in_items(items: list[dict]) -> None:
    """Converte campos epoch (`end_time`, `ctime`) para data/hora em BRT."""
    for it in items:
        if not isinstance(it, dict):
            continue

        for field in DATETIME_FIELDS:
            if field not in it:
                continue

            it[field] = _format_epoch_to_brt(it.get(field))


def _normalize_assignee_type(value: object) -> str:
    """Converte o código de assignee_type para uma descrição legível."""
    if value is None:
        return ""

    key = str(value).strip()
    if not key:
        return ""

    return ASSIGNEE_TYPE_LABELS.get(key, key)


def _normalize_response_sla(value: object) -> str:
    """Normaliza response_sla para um texto de data/hora consistente."""
    if value is None:
        return ""

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(value, tz=BRT).strftime("%Y-%m-%d %H:%M:%S")
        except (OverflowError, OSError, ValueError):
            pass

    text = str(value).strip()
    if not text:
        return ""

    candidate_texts = [text, text.replace("Z", "+00:00")]
    candidate_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
    ]

    for candidate in candidate_texts:
        for candidate_format in candidate_formats:
            try:
                return datetime.strptime(candidate, candidate_format).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

        try:
            return datetime.fromisoformat(candidate).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    return text


def _format_minutes_to_duration(total_minutes: int) -> str:
    """Formata minutos totais no padrão Xd Yh Zm."""
    if total_minutes <= 0:
        return "0m"

    days, remainder = divmod(total_minutes, 24 * 60)
    hours, minutes = divmod(remainder, 60)

    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes or not parts:
        parts.append(f"{minutes}m")

    return " ".join(parts)


def _normalize_aging_time(value: object) -> str:
    """Converte aging_time para duração legível (ex: 2d 11h 30m)."""
    if value is None:
        return ""

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Valores numéricos vindos da API normalmente chegam em segundos.
        total_minutes = int(float(value) // 60)
        return _format_minutes_to_duration(total_minutes)

    text = str(value).strip().lower()
    if not text:
        return ""

    if text.isdigit():
        total_minutes = int(text) // 60
        return _format_minutes_to_duration(total_minutes)

    total_minutes = 0
    found_any = False
    for match in _AGING_TIME_TOKEN_PATTERN.finditer(text):
        found_any = True
        token_value = int(match.group("value") or 0)
        unit = (match.group("unit") or "").lower()

        if unit in {"d", "dia", "dias"}:
            total_minutes += token_value * 24 * 60
        elif unit in {"h", "hora", "horas"}:
            total_minutes += token_value * 60
        else:
            total_minutes += token_value

    if not found_any:
        return text

    return _format_minutes_to_duration(total_minutes)


def _enrich_escalation_ticket(item: dict) -> dict:
    """Preserva o payload original e adiciona normalizações do escalation."""
    enriched_item = dict(item)
    enriched_item["ticket_status"] = enriched_item.get("ticket_status")
    assignee_type_label = _normalize_assignee_type(enriched_item.get("assignee_type"))
    enriched_item["assignee_type"] = assignee_type_label
    enriched_item["response_sla"] = _normalize_response_sla(enriched_item.get("response_sla"))
    enriched_item["aging_time"] = _normalize_aging_time(enriched_item.get("aging_time"))
    return enriched_item


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

            # Converte códigos numéricos de ticket_status para descrições legíveis
            try:
                _map_ticket_status_in_items(items)
            except Exception:
                # Não interrompe a extração se o mapeamento falhar
                pass

            # Converte campos de data (epoch) para data/hora local (UTC-3)
            try:
                _map_datetime_fields_in_items(items)
            except Exception:
                # Não interrompe a extração se a conversão falhar
                pass

            all_data.extend(
                _enrich_escalation_ticket(item)
                for item in items
                if isinstance(item, dict)
            )
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
