from __future__ import annotations
import argparse
import os
import pandas as pd
from jinja2 import Template


def parse_args():
    ap = argparse.ArgumentParser(description="Renderiza relatório HTML a partir de métricas e template MD")
    ap.add_argument("--metrics_dir", required=True, help="Diretório com parquet de métricas")
    ap.add_argument("--template", required=True, help="Template Markdown (.md)")
    ap.add_argument("--output", required=True, help="Arquivo HTML de saída")
    return ap.parse_args()


def main():
    args = parse_args()

    by_role = pd.read_parquet(os.path.join(args.metrics_dir, "by_role.parquet"))
    by_month = pd.read_parquet(os.path.join(args.metrics_dir, "by_month.parquet"))
    top_by_month = pd.read_parquet(os.path.join(args.metrics_dir, "top_by_month.parquet"))

    with open(args.template, "r", encoding="utf-8") as f:
        md_template = f.read()

    template = Template(md_template)
    html = template.render(
        by_role=by_role.to_dict(orient="records"),
        by_month=by_month.to_dict(orient="records"),
        top_by_month=top_by_month.to_dict(orient="records"),
    )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] Relatório gerado em: {args.output}")


if __name__ == "__main__":
    main()
