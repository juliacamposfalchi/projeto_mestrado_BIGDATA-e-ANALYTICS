from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable
import pandas as pd

from src.schemas import UNIFIED_COLUMNS


class BaseExtractor(ABC):
    tj_code: str

    @abstractmethod
    def fetch_month(self, year_month: str) -> pd.DataFrame:
        """
        Baixa e normaliza a planilha de um mês para o TJ.
        Deve retornar DataFrame com colunas em UNIFIED_COLUMNS.
        """
        raise NotImplementedError

    def validate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in UNIFIED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas faltando no extrator {self.tj_code}: {missing}")
        return df[UNIFIED_COLUMNS].copy()

    def fetch_many(self, months: Iterable[str]) -> pd.DataFrame:
        frames = []
        for ym in months:
            mdf = self.fetch_month(ym)
            frames.append(self.validate_columns(mdf))
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
