"""
Pacote Modules - Módulos de Extração de Dados
"""

from modules.exception_orders import run as extract_exception_orders
from modules.inbound import run as extract_inbound
from modules.outbound import run as extract_outbound
from modules.recebimento_soc import run as extract_recebimento_soc
from modules.escalation_ticket import run as extract_escalation_ticket
from modules.liquidation import run as extract_liquidation
from modules.workstation_assignment import run as extract_workstation_assignment
from modules.spx_duplicados import run as extract_spx_duplicados

# Alias legado para compatibilidade com imports antigos.
extract_nurse_assignment = extract_workstation_assignment

__all__ = [
    "extract_exception_orders",
    "extract_inbound",
    "extract_outbound",
    "extract_recebimento_soc",
    "extract_escalation_ticket",
    "extract_liquidation",
    "extract_workstation_assignment",
    "extract_nurse_assignment",
    "extract_spx_duplicados",
]
