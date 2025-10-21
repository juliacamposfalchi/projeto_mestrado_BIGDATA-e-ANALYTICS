from __future__ import annotations
import os
import json
import pandas as pd
import streamlit as st
import plotly.express as px

DATA_DIR = os.path.join("reports", "output")
BY_MONTH_TJ_PATH = os.path.join(DATA_DIR, "by_month_tj.parquet")
BY_ROLE_TJ_PATH = os.path.join(DATA_DIR, "by_role_tj.parquet")
COVERAGE_MONTH_PATH = os.path.join(DATA_DIR, "coverage_by_month.json")
COVERAGE_MONTH_TJ_PATH = os.path.join(DATA_DIR, "coverage_by_month_tj.json")

st.set_page_config(page_title="Dashboard Remuneração TJs", layout="wide")
st.title("Dashboard – Remuneração nos TJs Estaduais")
st.caption("Comparação entre TJs a partir do dataset e métricas geradas em reports/output/")

@st.cache_data(show_spinner=False)
def load_parquet(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_parquet(path)

@st.cache_data(show_spinner=False)
def load_json(path: str):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

by_month_tj = load_parquet(BY_MONTH_TJ_PATH)
by_role_tj = load_parquet(BY_ROLE_TJ_PATH)
coverage_month = load_json(COVERAGE_MONTH_PATH)
coverage_month_tj = load_json(COVERAGE_MONTH_TJ_PATH)

if by_month_tj.empty:
    st.warning(
        "Arquivos de métricas comparativas não encontrados ou vazios.\n\n"
        "Gere as métricas com:\n\n"
        "python scripts/compute_metrics.py --input data/processed/remuneracao_unificada.parquet "
        "--outdir reports/output --teto 44136"
    )
    st.stop()

# Sidebar filtros
st.sidebar.header("Filtros")
all_months = sorted(by_month_tj["year_month"].unique())
month_sel = st.sidebar.multiselect("Períodos (YYYY-MM)", all_months, default=all_months)

all_tjs = sorted(by_month_tj["tj_code"].unique())
tjs_sel = st.sidebar.multiselect("TJs", all_tjs, default=all_tjs)

all_roles = sorted([r for r in by_role_tj["role"].dropna().unique().tolist()]) if not by_role_tj.empty else []
roles_sel = st.sidebar.multiselect("Funções (opcional)", all_roles, default=[])

# Filtro base
bmf = by_month_tj[(by_month_tj["year_month"].isin(month_sel)) & (by_month_tj["tj_code"].isin(tjs_sel))].copy()
brf = pd.DataFrame()
if not by_role_tj.empty:
    brf = by_role_tj[(by_role_tj["year_month"].isin(month_sel)) & (by_role_tj["tj_code"].isin(tjs_sel))].copy()
    if roles_sel:
        brf = brf[brf["role"].isin(roles_sel)]

# KPIs rápidos
col1, col2, col3, col4 = st.columns(4)
if not bmf.empty:
    total_rows = int(bmf["servidores"].sum())
    col1.metric("Servidores (somatório dos meses)", f"{total_rows:,}".replace(",", "."))
    col2.metric("Média bruta (mediana dos meses)", f"R$ {bmf['media_bruta'].median():,.2f}".replace(",","X").replace(".",",").replace("X","."))
    col3.metric("P90 (mediana dos meses)", f"R$ {bmf['p90_bruta'].median():,.2f}".replace(",","X").replace(".",",").replace("X","."))
    col4.metric("Máximo (maior do período)", f"R$ {bmf['max_bruta'].max():,.2f}".replace(",","X").replace(".",",").replace("X","."))

st.markdown("### Evolução da remuneração bruta média por TJ")
if not bmf.empty:
    fig = px.line(
        bmf.sort_values(["year_month", "tj_code"]),
        x="year_month", y="media_bruta", color="tj_code",
        markers=True, line_group="tj_code", hover_data=["servidores", "mediana_bruta", "p90_bruta", "max_bruta"],
        labels={"year_month":"Mês", "media_bruta":"Média bruta"}
    )
    fig.update_layout(legend_title_text="TJ")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados após filtros.")

st.markdown("### Servidores por TJ e mês")
if not bmf.empty:
    fig = px.bar(
        bmf, x="year_month", y="servidores", color="tj_code", barmode="group",
        labels={"year_month":"Mês", "servidores":"Servidores"}
    )
    st.plotly_chart(fig, use_container_width=True)

if not brf.empty:
    st.markdown("### Remuneração bruta média por função (selecionadas)")
    fig = px.line(
        brf.sort_values(["year_month", "tj_code", "role"]),
        x="year_month", y="media_bruta", color="tj_code", line_group="role",
        markers=True, facet_row=None, hover_data=["role", "mediana_bruta", "servidores"],
        labels={"year_month":"Mês", "media_bruta":"Média bruta"}
    )
    fig.update_layout(legend_title_text="TJ")
    st.plotly_chart(fig, use_container_width=True)

# Cobertura
st.markdown("### Cobertura por mês e por TJ")
if coverage_month_tj:
    cov_df = pd.DataFrame(coverage_month_tj)
    covf = cov_df[(cov_df["year_month"].isin(month_sel)) & (cov_df["tj_code"].isin(tjs_sel))].copy()
    if not covf.empty:
        st.dataframe(
            covf.sort_values(["year_month", "tj_code"]).reset_index(drop=True),
            use_container_width=True,
        )
else:
    st.info("Arquivo coverage_by_month_tj.json não encontrado – gere novamente as métricas.")

# ========================= Seções adicionais =========================
# Carregar dataset unificado para análises detalhadas (se existir)
UNIFIED_PATH = os.path.join("data", "processed", "remuneracao_unificada.parquet")

@st.cache_data(show_spinner=False)
def load_unified(path: str) -> pd.DataFrame:
    try:
        if os.path.exists(path):
            return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame()

# Controles extras
st.sidebar.markdown("---")
st.sidebar.subheader("Parâmetros adicionais")
teto_val = st.sidebar.number_input("Teto constitucional (opcional)", min_value=0.0, value=0.0, step=1000.0, format="%.2f")
server_query = st.sidebar.text_input("Buscar servidor (nome contém)", value="")
role_for_traj = st.sidebar.selectbox("Trajetória por função (opcional)", options=[""] + (all_roles if all_roles else []))

df_u = load_unified(UNIFIED_PATH)
has_unified = isinstance(df_u, pd.DataFrame) and not df_u.empty
if has_unified:
    # Aplicar filtros globais do painel (meses e TJs)
    df_uf = df_u.copy()
    if "year_month" in df_uf.columns:
        df_uf = df_uf[df_uf["year_month"].isin(month_sel)]
    if "tj_code" in df_uf.columns:
        df_uf = df_uf[df_uf["tj_code"].isin(tjs_sel)]
    # Máscara informativa (coerente com compute_metrics)
    informative_mask = (
        (df_uf.get("gross_pay", 0) > 0)
        | (df_uf.get("net_pay", 0) > 0)
        | (df_uf.get("benefits", 0) > 0)
        | (df_uf.get("base_pay", 0) > 0)
    )
    df_inf = df_uf[informative_mask].copy()

    st.markdown("## Perguntas e respostas")

    # 1) Quantos servidores no total? E por função?
    st.markdown("### Quantidade de servidores")
    if not df_inf.empty:
        total_serv = int(df_inf.get("server_id", pd.Series(dtype=object)).nunique())
        st.metric("Servidores únicos (período filtrado)", f"{total_serv:,}".replace(",", "."))
        by_role_cnt = (
            df_inf.groupby(["role"], dropna=False)["server_id"].nunique().reset_index(name="servidores")
            if "role" in df_inf.columns else pd.DataFrame(columns=["role", "servidores"]))
        if not by_role_cnt.empty:
            st.dataframe(by_role_cnt.sort_values("servidores", ascending=False), use_container_width=True)
    else:
        st.info("Sem dados informativos após filtros.")

    # 2) Remuneração média mensal e distribuição (global e por função)
    st.markdown("### Remuneração – média e distribuição")
    if not df_inf.empty and {"year_month", "gross_pay"}.issubset(df_inf.columns):
        bym = df_inf.groupby(["year_month"]).agg(
            media_bruta=("gross_pay", "mean"), mediana_bruta=("gross_pay", "median")
        ).reset_index()
        fig = px.line(bym, x="year_month", y=["media_bruta", "mediana_bruta"], markers=True,
                      labels={"value":"R$", "variable":"Métrica"})
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("Distribuição da remuneração bruta (histograma)")
        figd = px.histogram(df_inf, x="gross_pay", nbins=60, color="tj_code" if "tj_code" in df_inf.columns else None)
        figd.update_xaxes(title_text="Remuneração bruta")
        st.plotly_chart(figd, use_container_width=True)

        if "role" in df_inf.columns:
            st.markdown("Distribuição por função (boxplot)")
            # limitar funções mais frequentes para clareza
            top_roles = df_inf["role"].value_counts().head(10).index.tolist()
            dfr = df_inf[df_inf["role"].isin(top_roles)]
            figb = px.box(dfr, x="role", y="gross_pay", color="tj_code" if "tj_code" in dfr.columns else None)
            st.plotly_chart(figb, use_container_width=True)

    # 3) Servidor com maior remuneração bruta no período
    st.markdown("### Maior remuneração bruta do período")
    if not df_inf.empty and {"year_month", "server_name", "gross_pay"}.issubset(df_inf.columns):
        idxmax = df_inf["gross_pay"].idxmax()
        rec = df_inf.loc[idxmax, ["year_month", "tj_code", "server_name", "role", "gross_pay"]]
        st.write({k: rec[k] for k in rec.index})

    # 4) Excedentes ao teto constitucional
    st.markdown("### Excedentes ao teto constitucional")
    if teto_val and teto_val > 0 and not df_inf.empty and "gross_pay" in df_inf.columns:
        df_ex = df_inf.copy()
        df_ex["excedente"] = (df_ex["gross_pay"] - float(teto_val)).clip(lower=0)
        exceeders = df_ex[df_ex["excedente"] > 0]
        st.metric("Servidores acima do teto (únicos)", f"{exceeders['server_id'].nunique():,}".replace(",", "."))
        st.metric("Excedente total (período)", f"R$ {exceeders['excedente'].sum():,.2f}".replace(",","X").replace(".",",").replace("X","."))
        if "career" in exceeders.columns:
            by_career = exceeders.groupby(["career"], dropna=False).agg(
                servidores=("server_id", "nunique"),
                excedente_total=("excedente", "sum"),
            ).reset_index()
            if not by_career.empty:
                by_career["excedente_per_capita"] = (
                    by_career["excedente_total"] / by_career["servidores"].replace({0: float("nan")})
                )
                st.dataframe(by_career.sort_values("excedente_total", ascending=False), use_container_width=True)

    # 5) Maior variação remuneratória ao longo do período (global e por função)
    st.markdown("### Maior variação remuneratória no período")
    if not df_inf.empty and {"server_id", "gross_pay", "year_month"}.issubset(df_inf.columns):
        # Medida: amplitude (max - min) por servidor
        var_by_srv = df_inf.groupby(["server_id", "server_name"], dropna=False).agg(
            var_amplitude=("gross_pay", lambda s: float(s.max() - s.min())),
            media=("gross_pay", "mean"),
            observacoes=("gross_pay", "count"),
        ).reset_index()
        top_var = var_by_srv.sort_values("var_amplitude", ascending=False).head(15)
        st.dataframe(top_var, use_container_width=True)

        if "role" in df_inf.columns:
            var_by_role = df_inf.groupby(["role"], dropna=False).agg(
                var_median=("gross_pay", lambda s: float(s.max() - s.min())),
                media=("gross_pay", "mean"),
                servidores=("server_id", "nunique"),
            ).reset_index()
            st.dataframe(var_by_role.sort_values("var_median", ascending=False), use_container_width=True)

    # 6) Trajetória remuneratória por servidor (busca por nome)
    st.markdown("### Trajetória por servidor (busca por nome)")
    if server_query and not df_inf.empty and {"server_name", "year_month"}.issubset(df_inf.columns):
        candidates = df_inf[df_inf["server_name"].astype(str).str.contains(server_query, case=False, na=False)]
        if not candidates.empty:
            # escolher o servidor com mais observações
            pick = (
                candidates.groupby(["server_id", "server_name"], dropna=False)["year_month"].count()
                .reset_index(name="obs").sort_values(["obs"], ascending=False).head(1)
            )
            sid = pick.iloc[0]["server_id"]
            name = pick.iloc[0]["server_name"]
            ts = df_inf[df_inf["server_id"] == sid].sort_values("year_month")
            figt = px.line(ts, x="year_month", y=["gross_pay", "net_pay"] if "net_pay" in ts.columns else ["gross_pay"],
                           markers=True, title=f"{name}")
            st.plotly_chart(figt, use_container_width=True)
        else:
            st.info("Nenhum servidor encontrado pelo termo informado.")

    # 7) Trajetória remuneratória média/mediana por função
    st.markdown("### Trajetória por função (média e mediana)")
    if role_for_traj and role_for_traj != "" and not df_inf.empty and {"role", "year_month"}.issubset(df_inf.columns):
        df_role = df_inf[df_inf["role"] == role_for_traj]
        if not df_role.empty:
            bym_role = df_role.groupby(["year_month"]).agg(
                media_bruta=("gross_pay", "mean"), mediana_bruta=("gross_pay", "median")
            ).reset_index()
            figr = px.line(bym_role, x="year_month", y=["media_bruta", "mediana_bruta"], markers=True,
                           labels={"value":"R$", "variable":"Métrica"})
            st.plotly_chart(figr, use_container_width=True)

    # 8) Relação com tipo de vínculo (comissionado/estatutário)
    st.markdown("### Remuneração por tipo de vínculo")
    if not df_inf.empty and "bond_type" in df_inf.columns:
        dft = df_inf.dropna(subset=["bond_type"]).copy()
        if not dft.empty:
            figv = px.box(dft, x="bond_type", y="gross_pay", color="tj_code" if "tj_code" in dft.columns else None)
            st.plotly_chart(figv, use_container_width=True)
else:
    st.info("Dataset unificado não encontrado. Para habilitar análises detalhadas, gere-o com o pipeline e certifique-se de que está em data/processed/remuneracao_unificada.parquet.")

st.markdown("---")
st.caption("Para atualizar os dados, reexecute o pipeline e as métricas, depois recarregue a página.\n"
           "Comandos: python -m src.main --tjs TJRS,TJPI,TJTO  •  scripts/compute_metrics.py  •  scripts/render_report.py")
