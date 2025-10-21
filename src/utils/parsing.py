from __future__ import annotations
import hashlib
import unicodedata
from bs4 import BeautifulSoup
from typing import Optional
import re


def normalize_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKC", s)
    return s


def to_float(x) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return 0.0
    # Normaliza espaços e remove símbolos de moeda/prefixos comuns (ex.: R$, BRL)
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u00a0", " ")
    s = s.replace("R$", "").replace("BRL", "").replace("brl", "")
    # Mantém apenas dígitos, vírgula, ponto, hífen e espaços intermediários
    s = re.sub(r"[^0-9,\.\- ]+", "", s)
    s = s.strip()
    # Remove espaços internos
    s = s.replace(" ", "")
    # Trata formatos brasileiros: 1.234,56 -> 1234.56
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def make_server_id(tj_code: str, name: str, maybe_mat: str | None = None) -> str:
    base = f"{tj_code}|{normalize_text(name)}|{normalize_text(maybe_mat or '')}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def parse_html_table(html: str):
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if table is None:
        return []
    rows = []
    for tr in table.find_all("tr"):
        cells = [normalize_text(td.get_text(" ")) for td in tr.find_all(["td", "th"]) ]
        if cells:
            rows.append(cells)
    return rows
