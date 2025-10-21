from __future__ import annotations
import os
import pandas as pd
from typing import List, Dict

from src.schemas import Columns, UNIFIED_COLUMNS
from src.utils.parsing import to_float

# Mapeamento simples de possíveis nomes de colunas -> esquema unificado
COLUMN_CANDIDATES: Dict[str, List[str]] = {
    Columns.server_name: [
        "nome", "servidor", "nome_servidor", "nome do servidor", "servidor_nome",
    ],
    Columns.role: [
        "cargo", "funcao", "função", "posto", "emprego", "cargo/funcao",
    ],
    Columns.career: [
        "carreira", "grupo", "categoria", "vinculo_carreira",
    ],
    Columns.bond_type: [
        "vinculo", "tipo_vinculo", "regime", "tipo", "comissionado", "estatutario", "estatutário",
    ],
    Columns.gross_pay: [
        "remuneracao_bruta", "remuneração bruta", "bruta", "total_bruto", "valor_bruto",
        "total da remuneração", "total remuneracao", "remuneracao total",
        "rendimentos", "proventos",
        "total de creditos", "total de créditos",
    ],
    Columns.base_pay: [
        "vencimento_basico", "vencimento", "salario", "salário", "base",
    ],
    Columns.benefits: [
        "beneficios", "benefícios", "indenizacoes", "indenizações", "vantagens",
        "gratificacoes", "gratificações", "adiantamentos", "auxilios", "auxílios",
    ],
    Columns.deductions: [
        "descontos", "deducoes", "deduções", "impostos", "retencoes", "retenções",
        "total do desconto", "total de descontos", "total desconto",
    ],
    Columns.net_pay: [
        "liquido", "líquido", "remuneracao_liquida", "remuneração líquida", "total_liquido",
        "rendimento liquido", "rendimento líquido", "rendimento liquido (xi)", "rendimento líquido (xi)",
        "rendimento liquido xi", "rendimento líquido xi",
        "liquido do mes", "líquido do mês", "liquido do mês", "liquido mês",
    ],
}

READERS = {
    ".json": pd.read_json,
}


def _guess_column(df: pd.DataFrame, candidates: List[str]) -> str | None:
    cols_norm = {c: c.strip().lower() for c in df.columns}
    # tentativa por igualdade direta
    for c in df.columns:
        if cols_norm[c] in candidates:
            return c
    # tentativa por "contém"
    for c in df.columns:
        lc = cols_norm[c]
        if any(tok in lc for tok in candidates):
            return c
    return None


def _should_use_two_line_header(df: pd.DataFrame) -> bool:
    cols = [str(c).strip().lower() for c in df.columns]
    unnamed = sum(1 for c in cols if c.startswith("unnamed") or c == "")
    # Heurística: grupo de cabeçalhos (rendimentos/descontos) presentes e muitas colunas "unnamed"
    has_groups = any("rendimentos" in c for c in cols) or any("descontos" in c for c in cols)
    return has_groups and unnamed >= max(2, len(cols) // 5)


def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    # Achata MultiIndex e normaliza espaços/virgulas
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(p) for p in tup if str(p) != "nan"]).strip() for tup in df.columns.to_list()]
    df.columns = [str(c).strip().replace(",", "").replace("  ", " ") for c in df.columns]
    return df


def _read_excel_robust(path: str) -> pd.DataFrame:
    # Tenta leitura esperta varrendo abas e detectando linha de cabeçalho
    try:
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
    except Exception:
        sheet_names = [None]

    header_keywords = [
        "nome", "servidor", "cargo", "funcao", "função", "lotacao", "lotação",
        "total de creditos", "total de créditos", "liquido", "líquido", "descontos",
    ]

    def detect_header_row(df_preview: pd.DataFrame) -> int | None:
        # Procura até 200 linhas por uma que contenha cabeçalhos-alvo e muitas células não vazias
        max_rows = min(len(df_preview), 200)
        best_row = None
        best_score = -1
        for r in range(max_rows):
            vals = df_preview.iloc[r].to_list()
            row_vals = [str(v).strip().lower() for v in vals]
            keyword_score = sum(1 for v in row_vals if any(k in v for k in header_keywords))
            nonempty = sum(1 for v in row_vals if v not in ("", "nan", "none"))
            score = keyword_score * 10 + nonempty
            if score > best_score:
                best_score = score
                best_row = r
        # considera válido se ao menos alguma palavra-chave foi encontrada e há colunas suficientes
        if best_row is not None and best_score >= 15:
            return best_row
        return None

    def build_headers_from_rows(df_preview: pd.DataFrame, start_row: int, levels: int = 3) -> list[str]:
        import numpy as np
        hdr_block = df_preview.iloc[start_row:start_row+levels].fillna("")
        # Converte para strings normalizadas
        parts = hdr_block.applymap(lambda x: str(x).strip())
        # Forward-fill horizontalmente nomes vazios quando níveis superiores existem
        arr = parts.to_numpy(dtype=object)
        # Ffill vertical entre níveis para mesclas (se nível inferior vazio, herda do superior)
        for r in range(1, arr.shape[0]):
            for c in range(arr.shape[1]):
                if arr[r, c] == "" and arr[r-1, c] != "":
                    arr[r, c] = arr[r-1, c]
        # Construir nome final por coluna juntando níveis distintos
        headers = []
        for c in range(arr.shape[1]):
            parts_c = [str(arr[r, c]).strip() for r in range(arr.shape[0]) if str(arr[r, c]).strip() not in ("", "nan")]
            name = " ".join(dict.fromkeys(parts_c))  # remove repetições mantendo ordem
            name = name.replace(",", "").replace("  ", " ")
            headers.append(name if name != "" else f"col_{c}")
        return headers

    for sheet in sheet_names:
        try:
            # prévia sem cabeçalho para detectar linha de header
            preview = pd.read_excel(path, sheet_name=sheet, header=None)
            if not isinstance(preview, pd.DataFrame) or preview.empty:
                continue
            hdr = detect_header_row(preview)
            if hdr is not None:
                # Tenta construir cabeçalho com até 3 linhas
                headers = build_headers_from_rows(preview, hdr, levels=3)
                data = preview.iloc[hdr+1:].copy()
                data.columns = headers[:data.shape[1]]
                # Remove colunas completamente vazias
                data = data.dropna(axis=1, how="all")
                # Heurística: deve ter ao menos 3 colunas nomeadas significativas
                sig = sum(1 for h in data.columns if any(k in h.lower() for k in header_keywords))
                if sig >= 3 and data.shape[1] > 3:
                    return _normalize_headers(data)
                # fallback: tentar ler com header=hdr diretamente
                try:
                    df = pd.read_excel(path, sheet_name=sheet, header=hdr)
                    if isinstance(df, pd.DataFrame) and df.shape[1] > 1:
                        return _normalize_headers(df)
                except Exception:
                    pass
            # fallback: tentar header=[0,1]
            try:
                df2 = pd.read_excel(path, sheet_name=sheet, header=[0,1])
                if isinstance(df2, pd.DataFrame) and df2.shape[1] > 1:
                    return _normalize_headers(df2)
            except Exception:
                pass
            # fallback: tentar pular algumas linhas
            for skip in (1,2,3,4,5,6,7,8,9,10):
                try:
                    df3 = pd.read_excel(path, sheet_name=sheet, skiprows=skip)
                    if isinstance(df3, pd.DataFrame) and df3.shape[1] > 1:
                        return _normalize_headers(df3)
                except Exception:
                    continue
        except Exception:
            continue

    # tentativas mais simples sem sheet_name
    try:
        df = pd.read_excel(path)
        if isinstance(df, pd.DataFrame) and df.shape[1] > 1:
            return _normalize_headers(df)
    except Exception:
        pass
    return pd.DataFrame()


def _map_columns(df: pd.DataFrame, tj_code: str, year_month: str) -> pd.DataFrame:
    df = _normalize_headers(df)
    out = pd.DataFrame()
    out[Columns.tj_code] = [tj_code] * len(df)
    out[Columns.year_month] = [year_month] * len(df)

    # server_id será derivado posteriormente se necessário; aqui deixamos vazio
    out[Columns.server_id] = ""
    
    # tentativa de mapear campos
    def get_series(col_key: str):
        candidates = [s.lower() for s in COLUMN_CANDIDATES.get(col_key, [])]
        found = _guess_column(df, candidates)
        if found is not None:
            return df[found]
        return pd.Series([None] * len(df))

    out[Columns.server_name] = get_series(Columns.server_name)
    out[Columns.role] = get_series(Columns.role)
    out[Columns.career] = get_series(Columns.career)
    out[Columns.bond_type] = get_series(Columns.bond_type)

    # valores numéricos (tratando formatação PT-BR)
    for num_col in [Columns.gross_pay, Columns.base_pay, Columns.benefits, Columns.deductions, Columns.net_pay]:
        s = get_series(num_col)
        out[num_col] = s.apply(to_float)

    # garantir todas as colunas do esquema
    for c in UNIFIED_COLUMNS:
        if c not in out.columns:
            out[c] = None

    return out[UNIFIED_COLUMNS]


def _read_csv_robust(path: str) -> pd.DataFrame:
    # Tentativas: inferir separador, diferentes encodings e fallback explícito para ';'
    # 1) inferir separador (Sniffer) + utf-8
    try:
        df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8")
        if isinstance(df, pd.DataFrame) and df.shape[1] > 1:
            if _should_use_two_line_header(df):
                df2 = _read_csv_two_line_header(path)
                if isinstance(df2, pd.DataFrame) and df2.shape[1] > 1:
                    return df2
            return df
    except Exception:
        pass
    # 2) inferir separador + latin-1
    try:
        df = pd.read_csv(path, sep=None, engine="python", encoding="latin-1")
        if isinstance(df, pd.DataFrame) and df.shape[1] > 1:
            if _should_use_two_line_header(df):
                df2 = _read_csv_two_line_header(path)
                if isinstance(df2, pd.DataFrame) and df2.shape[1] > 1:
                    return df2
            return df
    except Exception:
        pass
    # 3) separador ';' + utf-8
    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8")
        if isinstance(df, pd.DataFrame) and df.shape[1] > 1:
            if _should_use_two_line_header(df):
                df2 = _read_csv_two_line_header(path)
                if isinstance(df2, pd.DataFrame) and df2.shape[1] > 1:
                    return df2
            return df
    except Exception:
        pass
    # 4) separador ';' + latin-1
    try:
        df = pd.read_csv(path, sep=";", encoding="latin-1")
        if isinstance(df, pd.DataFrame) and df.shape[1] > 1:
            if _should_use_two_line_header(df):
                df2 = _read_csv_two_line_header(path)
                if isinstance(df2, pd.DataFrame) and df2.shape[1] > 1:
                    return df2
            return df
    except Exception:
        pass
    # 5) fallback: tenta construir cabeçalho com as duas primeiras linhas (usado em alguns CSVs do TJRS)
    df2 = _read_csv_two_line_header(path)
    return df2 if isinstance(df2, pd.DataFrame) else pd.DataFrame()


def _read_csv_two_line_header(path: str) -> pd.DataFrame | None:
    import csv
    # Tenta ler primeiras 2 linhas manualmente para montar cabeçalho
    for enc in ("utf-8", "latin-1"):
        try:
            # Detecta delimitador simples por contagem de separadores prováveis
            with open(path, "r", encoding=enc, errors="ignore") as f:
                head = f.readline()
            if not head:
                continue
            semi = head.count(";")
            comma = head.count(",")
            delim = ";" if semi >= comma else ","

            with open(path, "r", encoding=enc, errors="ignore") as f:
                reader = csv.reader(f, delimiter=delim)
                row1 = next(reader, None)
                row2 = next(reader, None)
            if not row1 or not row2:
                continue
            headers = []
            max_len = max(len(row1), len(row2))
            for i in range(max_len):
                p1 = (row1[i] if i < len(row1) else "").strip()
                p2 = (row2[i] if i < len(row2) else "").strip()
                name = (p1 + " " + p2).strip()
                # normalizações simples
                name = name.replace(",", "").replace("  ", " ")
                headers.append(name if name != "" else f"col_{i}")
            # Lê o restante com esses nomes
            df = pd.read_csv(path, sep=delim, encoding=enc, header=None, skiprows=2, names=headers)
            if isinstance(df, pd.DataFrame) and df.shape[1] > 1:
                return df
        except Exception:
            continue
    return None


def load_month_data(tj_code: str, year_month: str, raw_root: str = "data/raw") -> pd.DataFrame:
    """
    Lê todos os arquivos dentro de data/raw/<TJ>/<YYYY-MM>/ e tenta mapear
    colunas comuns para o esquema unificado. Suporta CSV/TXT, XLSX, JSON.
    Arquivos HTML podem ser tratados em versão futura (pandas.read_html).
    """
    month_dir = os.path.join(raw_root, tj_code, year_month)
    if not os.path.isdir(month_dir):
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    frames: List[pd.DataFrame] = []
    for fname in os.listdir(month_dir):
        path = os.path.join(month_dir, fname)
        if not os.path.isfile(path):
            continue
        _, ext = os.path.splitext(path)
        ext = ext.lower()
        try:
            if ext in [".csv", ".txt"]:
                # leitura robusta para CSV/TXT
                df = _read_csv_robust(path)
            elif ext == ".xlsx":
                df = _read_excel_robust(path)
            elif ext in READERS:
                reader = READERS[ext]
                df = reader(path)
            elif ext in [".html", ".htm"]:
                # tentativa de ler primeira tabela
                tables = pd.read_html(path)
                df = tables[0] if tables else None
            else:
                df = None

            if not isinstance(df, pd.DataFrame):
                continue

            mapped = _map_columns(df, tj_code, year_month)
            frames.append(mapped)
        except Exception:
            # ignora arquivo problemático, poderia logar
            continue

    if frames:
        out = pd.concat(frames, ignore_index=True)
        # limpeza básica (mantém valores numéricos já tratados por to_float)
        numeric_cols = [Columns.gross_pay, Columns.base_pay, Columns.benefits, Columns.deductions, Columns.net_pay]
        for c in numeric_cols:
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
        return out

    return pd.DataFrame(columns=UNIFIED_COLUMNS)
