from __future__ import annotations
import os
import yaml
from dataclasses import dataclass
from typing import Optional


@dataclass
class Settings:
    raw_dir: str
    processed_dir: str
    unified_parquet: str
    start: str
    end: str
    timeout: int
    user_agent: str
    retries: int
    backoff_factor: float


def load_settings(path: str = os.path.join("config", "settings.yaml")) -> Settings:
    with open(path, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)
    data = y["data"]
    period = y["period"]
    defaults = y.get("defaults", {})
    return Settings(
        raw_dir=data["raw_dir"],
        processed_dir=data["processed_dir"],
        unified_parquet=data["unified_parquet"],
        start=period["start"],
        end=period["end"],
        timeout=int(defaults.get("timeout", 60)),
        user_agent=str(defaults.get("user_agent", "Mozilla/5.0")),
        retries=int(defaults.get("retries", 3)),
        backoff_factor=float(defaults.get("backoff_factor", 0.5)),
    )
