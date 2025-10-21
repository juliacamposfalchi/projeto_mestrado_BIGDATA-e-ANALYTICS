from __future__ import annotations
import os
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd

from src.config import load_settings
from src.pipeline import run_pipeline, EXTRACTOR_REGISTRY

app = FastAPI(title="API Remuneração TJs", version="0.1.0")


class ExtractRequest(BaseModel):
    tjs: Optional[List[str]] = None  # ex.: ["TJRS", "TJPI", "TJTO"]
    start: Optional[str] = None      # YYYY-MM
    end: Optional[str] = None        # YYYY-MM


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/tjs")
def list_tjs():
    return sorted(list(EXTRACTOR_REGISTRY.keys()))


@app.post("/extract")
def extract(req: ExtractRequest):
    settings = load_settings()
    start = req.start or settings.start
    end = req.end or settings.end

    if req.tjs and len(req.tjs) > 0:
        tjs = [t.strip().upper() for t in req.tjs]
    else:
        tjs = sorted(list(EXTRACTOR_REGISTRY.keys()))

    df = run_pipeline(tjs, start, end, user_agent=settings.user_agent, timeout=settings.timeout)

    os.makedirs(os.path.dirname(settings.unified_parquet), exist_ok=True)
    df.to_parquet(settings.unified_parquet, index=False)

    return {
        "message": "dataset unificado gerado",
        "rows": int(df.shape[0]),
        "tjs": tjs,
        "period": {"start": start, "end": end},
        "output": settings.unified_parquet,
    }


@app.get("/unified")
def unified_info():
    settings = load_settings()
    path = settings.unified_parquet
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset unificado não encontrado. Execute /extract primeiro.")
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler parquet: {e}")

    # retorno resumido
    sample = df.head(20).to_dict(orient="records")
    return {
        "path": path,
        "rows": int(df.shape[0]),
        "cols": list(df.columns),
        "sample": sample,
    }


@app.get("/metrics")
def metrics():
    settings = load_settings()
    path = settings.unified_parquet
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset unificado não encontrado. Execute /extract primeiro.")
    df = pd.read_parquet(path)

    by_role = df.groupby(["year_month", "role"], dropna=False).agg(
        servidores=("server_id", "nunique"),
        media_bruta=("gross_pay", "mean"),
        mediana_bruta=("gross_pay", "median"),
    ).reset_index()

    by_month = df.groupby(["year_month"]).agg(
        servidores=("server_id", "nunique"),
        media_bruta=("gross_pay", "mean"),
        mediana_bruta=("gross_pay", "median"),
        p90_bruta=("gross_pay", lambda x: float(x.quantile(0.9))),
        p99_bruta=("gross_pay", lambda x: float(x.quantile(0.99))),
        max_bruta=("gross_pay", "max"),
    ).reset_index()

    # maior remuneração por mês
    idx = df.groupby("year_month")["gross_pay"].idxmax()
    top_by_month = df.loc[idx, ["year_month", "tj_code", "server_name", "role", "gross_pay"]]

    return {
        "by_role": by_role.to_dict(orient="records"),
        "by_month": by_month.to_dict(orient="records"),
        "top_by_month": top_by_month.to_dict(orient="records"),
    }
