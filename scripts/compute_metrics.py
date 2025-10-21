from __future__ import annotations
import argparse
import os
import pandas as pd


def parse_args():
    ap = argparse.ArgumentParser(description="Computa métricas agregadas para relatório")
    ap.add_argument("--input", required=True, help="Arquivo Parquet unificado")
    ap.add_argument("--outdir", required=True, help="Diretório de saída para métricas")
    ap.add_argument("--teto", type=float, default=None, help="Valor do teto constitucional (opcional)")
    return ap.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    df = pd.read_parquet(args.input)
    # Filtra linhas informativas: mantém quando alguma rubrica financeira é > 0
    informative_mask = (
        (df.get("gross_pay", 0) > 0)
        | (df.get("net_pay", 0) > 0)
        | (df.get("benefits", 0) > 0)
        | (df.get("base_pay", 0) > 0)
    )
    df_f = df[informative_mask].copy()

    # Tamanho total e por função/carreira
    by_role = df_f.groupby(["year_month", "role"], dropna=False).agg(
        servidores=("server_id", "nunique"),
        media_bruta=("gross_pay", "mean"),
        mediana_bruta=("gross_pay", "median"),
    ).reset_index()
    by_role.to_parquet(os.path.join(args.outdir, "by_role.parquet"), index=False)

    # Por função e por TJ (comparativo entre estados por função)
    if "tj_code" in df_f.columns:
        by_role_tj = df_f.groupby(["year_month", "tj_code", "role"], dropna=False).agg(
            servidores=("server_id", "nunique"),
            media_bruta=("gross_pay", "mean"),
            mediana_bruta=("gross_pay", "median"),
        ).reset_index()
        by_role_tj.to_parquet(os.path.join(args.outdir, "by_role_tj.parquet"), index=False)

        # Versão líquida por função
        by_role_tj_net = df_f.groupby(["year_month", "tj_code", "role"], dropna=False).agg(
            servidores=("server_id", "nunique"),
            media_liquida=("net_pay", "mean"),
            mediana_liquida=("net_pay", "median"),
        ).reset_index()
        by_role_tj_net.to_parquet(os.path.join(args.outdir, "by_role_tj_net.parquet"), index=False)

    # Distribuição global por mês
    by_month = df_f.groupby(["year_month"]).agg(
        servidores=("server_id", "nunique"),
        media_bruta=("gross_pay", "mean"),
        mediana_bruta=("gross_pay", "median"),
        p90_bruta=("gross_pay", lambda x: x.quantile(0.9)),
        p99_bruta=("gross_pay", lambda x: x.quantile(0.99)),
        max_bruta=("gross_pay", "max"),
    ).reset_index()
    by_month.to_parquet(os.path.join(args.outdir, "by_month.parquet"), index=False)

    # Distribuição por mês e por TJ (comparativo entre estados)
    if "tj_code" in df_f.columns:
        by_month_tj = df_f.groupby(["year_month", "tj_code"]).agg(
            servidores=("server_id", "nunique"),
            media_bruta=("gross_pay", "mean"),
            mediana_bruta=("gross_pay", "median"),
            p90_bruta=("gross_pay", lambda x: x.quantile(0.9)),
            p99_bruta=("gross_pay", lambda x: x.quantile(0.99)),
            max_bruta=("gross_pay", "max"),
        ).reset_index()
        by_month_tj.to_parquet(os.path.join(args.outdir, "by_month_tj.parquet"), index=False)

        # Versão líquida (net_pay)
        by_month_tj_net = df_f.groupby(["year_month", "tj_code"]).agg(
            servidores=("server_id", "nunique"),
            media_liquida=("net_pay", "mean"),
            mediana_liquida=("net_pay", "median"),
            p90_liquida=("net_pay", lambda x: x.quantile(0.9)),
            p99_liquida=("net_pay", lambda x: x.quantile(0.99)),
            max_liquida=("net_pay", "max"),
        ).reset_index()
        by_month_tj_net.to_parquet(os.path.join(args.outdir, "by_month_tj_net.parquet"), index=False)

    # Maior remuneração por mês (quem é)
    base_for_top = df_f if not df_f.empty else df
    if not base_for_top.empty:
        idx = base_for_top.groupby("year_month")["gross_pay"].idxmax()
        top_by_month = base_for_top.loc[idx, ["year_month", "tj_code", "server_name", "role", "gross_pay"]]
    else:
        top_by_month = pd.DataFrame(columns=["year_month", "tj_code", "server_name", "role", "gross_pay"])
    top_by_month.to_parquet(os.path.join(args.outdir, "top_by_month.parquet"), index=False)

    # Top do ano (global)
    try:
      base_for_year = df_f if not df_f.empty else df
      top_idx = base_for_year["gross_pay"].idxmax()
      top_of_year = base_for_year.loc[[top_idx], ["year_month", "tj_code", "server_name", "role", "gross_pay"]]
      top_of_year.to_parquet(os.path.join(args.outdir, "top_of_year.parquet"), index=False)
    except Exception:
      pass

    # Contagem total de servidores e por função
    try:
      total_count = pd.DataFrame({"servidores_total": [int(df_f["server_id"].nunique())]})
      total_count.to_parquet(os.path.join(args.outdir, "counts_total.parquet"), index=False)

      by_role_count = df_f.groupby(["role"], dropna=False)["server_id"].nunique().reset_index(name="servidores")
      by_role_count.to_parquet(os.path.join(args.outdir, "counts_by_role.parquet"), index=False)
    except Exception:
      pass

    # Métricas de teto constitucional (opcional)
    if args.teto is not None:
        try:
            df_ex = df_f.copy() if not df_f.empty else df.copy()
            df_ex["excedente"] = (df_ex["gross_pay"] - float(args.teto)).clip(lower=0)
            exceeders = df_ex[df_ex["excedente"] > 0]

            exceeders_by_month = exceeders.groupby(["year_month"]).agg(
                servidores_acima=("server_id", "nunique"),
                excedente_total=("excedente", "sum"),
            ).reset_index()
            exceeders_by_month.to_parquet(os.path.join(args.outdir, "exceeders_by_month.parquet"), index=False)

            exceeders_by_career = exceeders.groupby(["career"], dropna=False).agg(
                servidores=("server_id", "nunique"),
                excedente_total=("excedente", "sum"),
            ).reset_index()
            if not exceeders_by_career.empty:
                exceeders_by_career["excedente_per_capita"] = (
                    exceeders_by_career["excedente_total"] / exceeders_by_career["servidores"].replace({0: float("nan")})
                )
            exceeders_by_career.to_parquet(os.path.join(args.outdir, "exceeders_by_career.parquet"), index=False)
        except Exception:
            # Mantém compatibilidade mesmo se não for possível calcular excedentes
            pass

    # Relatório de cobertura (por mês)
    try:
        coverage = (
            df.groupby("year_month")
              .apply(lambda g: pd.Series({
                  "gross_pay_nonzero_rate": float((g.get("gross_pay", 0) > 0).mean()),
                  "net_pay_nonzero_rate": float((g.get("net_pay", 0) > 0).mean()),
                  "benefits_nonzero_rate": float((g.get("benefits", 0) > 0).mean()),
                  "base_pay_nonzero_rate": float((g.get("base_pay", 0) > 0).mean()),
              }))
              .reset_index()
        )
        coverage.to_json(os.path.join(args.outdir, "coverage_by_month.json"), orient="records", force_ascii=False)
        if "tj_code" in df.columns:
            coverage_tj = (
                df.groupby(["year_month", "tj_code"]) 
                  .apply(lambda g: pd.Series({
                      "gross_pay_nonzero_rate": float((g.get("gross_pay", 0) > 0).mean()),
                      "net_pay_nonzero_rate": float((g.get("net_pay", 0) > 0).mean()),
                      "benefits_nonzero_rate": float((g.get("benefits", 0) > 0).mean()),
                      "base_pay_nonzero_rate": float((g.get("base_pay", 0) > 0).mean()),
                  }))
                  .reset_index()
            )
            coverage_tj.to_json(os.path.join(args.outdir, "coverage_by_month_tj.json"), orient="records", force_ascii=False)
    except Exception:
        pass

    print("[OK] Métricas geradas em:", args.outdir)


if __name__ == "__main__":
    main()
