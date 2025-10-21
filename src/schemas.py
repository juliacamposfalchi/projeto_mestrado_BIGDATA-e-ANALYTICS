from __future__ import annotations
from dataclasses import dataclass
from typing import List

# Esquema unificado mínimo para remuneração mensal
# Uma linha por servidor por mês por TJ

@dataclass
class Columns:
    tj_code: str = "tj_code"                 # ex.: TJSP
    year_month: str = "year_month"           # YYYY-MM
    server_id: str = "server_id"             # identificador (hash ou matrícula)
    server_name: str = "server_name"
    role: str = "role"                        # função/cargo
    career: str = "career"                    # magistrado, servidor, etc.
    bond_type: str = "bond_type"              # estatutário, comissionado, etc. (se disponível)
    gross_pay: str = "gross_pay"              # remuneração bruta
    base_pay: str = "base_pay"                # vencimento básico
    benefits: str = "benefits"                # vantagens/indenizações (total)
    deductions: str = "deductions"            # descontos (total)
    net_pay: str = "net_pay"                  # remuneração líquida


UNIFIED_COLUMNS: List[str] = [
    Columns.tj_code,
    Columns.year_month,
    Columns.server_id,
    Columns.server_name,
    Columns.role,
    Columns.career,
    Columns.bond_type,
    Columns.gross_pay,
    Columns.base_pay,
    Columns.benefits,
    Columns.deductions,
    Columns.net_pay,
]
