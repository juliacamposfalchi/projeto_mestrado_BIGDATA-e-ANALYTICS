from __future__ import annotations
import pandas as pd
from bs4 import BeautifulSoup

from src.extractors.base import BaseExtractor
from src.schemas import Columns, UNIFIED_COLUMNS
from src.utils.ingest_local import load_month_data
from src.utils.parsing import make_server_id


class TJTOExtractor(BaseExtractor):
    tj_code = "TJTO"

    def __init__(self, user_agent: str = "Mozilla/5.0", timeout: int = 60):
        # Extrator baseado em arquivos locais colocados em data/raw/TJTO/<YYYY-MM>/
        pass

    def month_url(self, year_month: str) -> str:
        # TODO: substituir por endpoint real de remuneração mensal do TJTO
        return "https://www.tjto.jus.br/transparencia"

    def fetch_month(self, year_month: str) -> pd.DataFrame:
        df = load_month_data(self.tj_code, year_month, raw_root="data/raw")
        if df.empty:
            return pd.DataFrame(columns=UNIFIED_COLUMNS)
        if (df[Columns.server_id] == "").any() and Columns.server_name in df.columns:
            df[Columns.server_id] = df.apply(
                lambda r: make_server_id(self.tj_code, str(r.get(Columns.server_name, ""))), axis=1
            )
        return df
