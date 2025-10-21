from __future__ import annotations
import os
from typing import Iterable, Dict, Type
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime

from src.schemas import UNIFIED_COLUMNS
from src.extractors.tj_rs import TJRSExtractor
from src.extractors.tj_pi import TJPIExtractor
from src.extractors.tj_to import TJTOExtractor


EXTRACTOR_REGISTRY: Dict[str, Type] = {
    "TJRS": TJRSExtractor,
    "TJPI": TJPIExtractor,
    "TJTO": TJTOExtractor,
}


def month_range(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m")
    e = datetime.strptime(end, "%Y-%m")
    months = []
    cur = s
    while cur <= e:
        months.append(cur.strftime("%Y-%m"))
        cur = cur + relativedelta(months=1)
    return months


def run_pipeline(tj_codes: Iterable[str], start: str, end: str, user_agent: str = "Mozilla/5.0", timeout: int = 60) -> pd.DataFrame:
    months = month_range(start, end)
    frames = []
    for tj in tj_codes:
        extractor_cls = EXTRACTOR_REGISTRY.get(tj)
        if extractor_cls is None:
            print(f"[WARN] Sem extrator cadastrado para {tj}")
            continue
        extractor = extractor_cls(user_agent=user_agent, timeout=timeout)
        df = extractor.fetch_many(months)
        frames.append(df)
    if frames:
        unified = pd.concat(frames, ignore_index=True)
        # Tipagem básica
        for col in ["gross_pay", "base_pay", "benefits", "deductions", "net_pay"]:
            if col in unified.columns:
                unified[col] = pd.to_numeric(unified[col], errors="coerce").fillna(0.0)
        # Derivar líquido quando não informado
        if set(["gross_pay", "deductions", "net_pay"]).issubset(unified.columns):
            mask_missing_net = (unified["net_pay"] <= 0) & (unified["gross_pay"] > 0)
            unified.loc[mask_missing_net, "net_pay"] = (
                unified.loc[mask_missing_net, "gross_pay"] - unified.loc[mask_missing_net, "deductions"]
            ).clip(lower=0)
        return unified
    return pd.DataFrame(columns=UNIFIED_COLUMNS)
