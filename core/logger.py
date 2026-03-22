"""
Logger Centralizado
===================

Configuração de logging para console e arquivo.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler

from core.config import (
    LOGS_DIR,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    LOG_FILE_MAX_BYTES,
    LOG_BACKUP_COUNT,
)

console = Console()


def setup_logger(name: str = "spx_scraper") -> logging.Logger:
    """
    Configura e retorna um logger com handlers para console e arquivo.
    
    Args:
        name: Nome do logger
    
    Returns:
        Logger configurado
    """
    # Cria diretório de logs se não existir
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger(name)
    
    # Evita duplicação de handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    # Handler para arquivo com rotação
    file_handler = RotatingFileHandler(
        LOGS_DIR / "app.log",
        maxBytes=LOG_FILE_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    
    # Handler para console com Rich
    console_handler = RichHandler(
        console=console,
        show_time=False,
        show_path=False,
        markup=True,
        rich_tracebacks=True
    )
    console_handler.setLevel(logging.INFO)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# Logger padrão do sistema
logger = setup_logger()
