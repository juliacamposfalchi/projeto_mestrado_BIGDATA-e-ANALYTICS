from __future__ import annotations
import argparse
import os
import pandas as pd

from src.config import load_settings
from src.pipeline import run_pipeline


def parse_args():
    ap = argparse.ArgumentParser(description="Pipeline de remuneração TJs")
    ap.add_argument("--tjs", type=str, default="", help="Lista de TJs separados por vírgula (ex.: TJRS,TJPI,TJTO). Vazio usa todos os TJs suportados neste projeto.")
    ap.add_argument("--start", type=str, default="", help="YYYY-MM início")
    ap.add_argument("--end", type=str, default="", help="YYYY-MM fim")
    return ap.parse_args()


def main():
    args = parse_args()
    settings = load_settings()

    start = args.start or settings.start
    end = args.end or settings.end

    # Por ora, usamos somente TJs registrados no código (RS, PI, TO)
    if args.tjs:
        tj_codes = [t.strip().upper() for t in args.tjs.split(",") if t.strip()]
    else:
        tj_codes = ["TJRS", "TJPI", "TJTO"]

    df = run_pipeline(tj_codes, start, end, user_agent=settings.user_agent, timeout=settings.timeout)

    os.makedirs(os.path.dirname(settings.unified_parquet), exist_ok=True)
    df.to_parquet(settings.unified_parquet, index=False)
    print(f"[OK] Dataset unificado salvo em: {settings.unified_parquet}")


if __name__ == "__main__":
    main()
