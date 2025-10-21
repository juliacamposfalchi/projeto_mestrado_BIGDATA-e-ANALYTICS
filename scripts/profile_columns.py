from __future__ import annotations
import os
import sys
import json
from typing import Dict, List, Optional, Tuple

# Garantir que o diretório raiz (que contém 'src/') esteja no sys.path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pandas as pd
from src.utils.ingest_local import _read_excel_robust

SUPPORTED_EXTS = {".csv", ".txt", ".xlsx", ".json", ".html", ".htm"}
TARGET_TJS = {"TJRS", "TJPI", "TJTO"}

PT_MONTHS = {
    "janeiro": "01",
    "fevereiro": "02",
    "marco": "03",
    "março": "03",
    "abril": "04",
    "maio": "05",
    "junho": "06",
    "julho": "07",
    "agosto": "08",
    "setembro": "09",
    "outubro": "10",
    "novembro": "11",
    "dezembro": "12",
}


def _safe_read_columns(path: str) -> List[str]:
    """Best effort to extract column names from a file without loading everything.
    Returns an empty list if not identifiable.
    """
    _, ext = os.path.splitext(path)
    ext = ext.lower()
    try:
        if ext in {".csv", ".txt"}:
            df = pd.read_csv(path, nrows=0)
            return list(df.columns)
        if ext == ".xlsx":
            # usar leitor robusto compartilhado com o pipeline
            try:
                df = _read_excel_robust(path)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    return list(df.columns)
            except Exception:
                pass
            # fallback simples
            try:
                df = pd.read_excel(path, nrows=5)
                return list(df.columns)
            except Exception:
                return []
        if ext == ".json":
            # Try pandas first
            try:
                df = pd.read_json(path, lines=True, nrows=5)
                return list(df.columns)
            except Exception:
                # Fallback: attempt to read as array of objects
                import json as _json
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    text = f.read(200000)  # read first ~200KB
                try:
                    data = _json.loads(text)
                    if isinstance(data, list) and data:
                        if isinstance(data[0], dict):
                            return list(data[0].keys())
                except Exception:
                    pass
                return []
        if ext in {".html", ".htm"}:
            try:
                tables = pd.read_html(path)
                if tables:
                    return list(tables[0].columns)
            except Exception:
                pass
            return []
    except Exception:
        return []
    return []


def _infer_year_month_from_name(name: str) -> Optional[str]:
    """Try to infer YYYY-MM from a filename containing Portuguese month names and a year.
    Examples: 'janeiro2025.csv' -> '2025-01', 'Maio2025_Piaui.csv' -> '2025-05'.
    Returns None if not inferable.
    """
    base = os.path.splitext(os.path.basename(name))[0]
    s = base.lower().replace("+", " ").replace("_", " ")
    # normalize cedilla/accents minimalistically
    s = s.replace("ç", "c").replace("ã", "a").replace("â", "a").replace("á", "a").replace("é", "e").replace("ê", "e").replace("í", "i").replace("ó", "o").replace("ô", "o").replace("ú", "u")
    # try to locate month token
    month_num = None
    for m_name, m_num in PT_MONTHS.items():
        if m_name in s:
            month_num = m_num
            break
    if month_num is None:
        return None
    # try to find a 4-digit year near the month token
    import re
    years = re.findall(r"(20\d{2})", s)
    if not years:
        return None
    # choose the last year occurrence
    year = years[-1]
    return f"{year}-{month_num}"


def profile_columns(raw_root: str = "data/raw") -> Dict:
    summary: Dict[str, Dict[str, Dict[str, int]]] = {}
    # structure: {TJ: {year_month: {column_name: frequency_across_files}}}

    if not os.path.isdir(raw_root):
        return {"error": f"raw_root not found: {raw_root}"}

    for tj in sorted(os.listdir(raw_root)):
        tj_path = os.path.join(raw_root, tj)
        if tj not in TARGET_TJS:
            continue
        if not os.path.isdir(tj_path):
            continue

        # Case A: layout com subpastas YYYY-MM
        subdirs = [d for d in sorted(os.listdir(tj_path)) if os.path.isdir(os.path.join(tj_path, d))]
        if subdirs:
            for ym in subdirs:
                ym_path = os.path.join(tj_path, ym)
                col_counts: Dict[str, int] = {}
                for fname in os.listdir(ym_path):
                    fpath = os.path.join(ym_path, fname)
                    if not os.path.isfile(fpath):
                        continue
                    _, ext = os.path.splitext(fpath)
                    if ext.lower() not in SUPPORTED_EXTS:
                        continue
                    cols = _safe_read_columns(fpath)
                    if not cols:
                        continue
                    for c in cols:
                        col_counts[c] = col_counts.get(c, 0) + 1
                if col_counts:
                    summary.setdefault(tj, {})[ym] = dict(sorted(col_counts.items(), key=lambda kv: (-kv[1], kv[0].lower())))
        else:
            # Case B: layout plano (arquivos diretamente dentro do TJ)
            # Agrupar por ano-mês inferido do nome do arquivo
            grouped: Dict[str, Dict[str, int]] = {}
            for fname in os.listdir(tj_path):
                fpath = os.path.join(tj_path, fname)
                if not os.path.isfile(fpath):
                    continue
                _, ext = os.path.splitext(fpath)
                if ext.lower() not in SUPPORTED_EXTS:
                    continue
                ym = _infer_year_month_from_name(fname)
                if ym is None:
                    # se não conseguir inferir, pule
                    continue
                cols = _safe_read_columns(fpath)
                if not cols:
                    continue
                bucket = grouped.setdefault(ym, {})
                for c in cols:
                    bucket[c] = bucket.get(c, 0) + 1
            for ym, col_counts in grouped.items():
                summary.setdefault(tj, {})[ym] = dict(sorted(col_counts.items(), key=lambda kv: (-kv[1], kv[0].lower())))

    return summary


def main():
    outdir = os.path.join("reports", "output")
    os.makedirs(outdir, exist_ok=True)
    prof = profile_columns(raw_root=os.path.join("data", "raw"))
    outpath = os.path.join(outdir, "columns_profile.json")
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(prof, f, ensure_ascii=False, indent=2)
    print(f"[OK] Perfil de colunas salvo em: {outpath}")


if __name__ == "__main__":
    main()
