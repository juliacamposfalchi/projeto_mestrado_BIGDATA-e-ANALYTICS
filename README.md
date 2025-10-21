# Projeto: Remuneração de Servidores dos TJs Estaduais

Este projeto organiza a extração, normalização, integração e análise das remunerações de servidores dos seguintes Tribunais de Justiça estaduais:

- TJRS (Rio Grande do Sul)
- TJPI (Piauí)
- TJTO (Tocantins)

## Objetivos
- Extrair dados de remuneração por TJ e mês (com foco no último ano, até ago/2025).
- Normalizar esquemas heterogêneos em um modelo unificado.
- Integrar tudo em um dataset único (Parquet) para análises eficientes.
- Calcular métricas exigidas e gerar um relatório dinâmico (Markdown -> HTML) e um guia em PDF.

## Estrutura do projeto
```
projeto_big_data/
├─ config/
│  ├─ settings.yaml
│  └─ tj_catalog.csv
├─ data/
│  ├─ raw/
│  └─ processed/
├─ reports/
│  ├─ template_report.md
│  └─ output/ (gerado)
├─ scripts/
│  ├─ compute_metrics.py
│  └─ render_report.py
├─ src/
│  ├─ __init__.py
│  ├─ config.py
│  ├─ schemas.py
│  ├─ pipeline.py
│  ├─ main.py
│  ├─ api.py
│  ├─ utils/
│  │  ├─ __init__.py
│  │  ├─ http.py
│  │  └─ parsing.py
│  └─ extractors/
│     ├─ __init__.py
│     ├─ base.py
│     ├─ tj_rs.py
│     ├─ tj_pi.py
│     └─ tj_to.py
├─ requirements.txt
└─ README.md
```

## Instalação
1. Recomendado: criar um ambiente virtual (ex.: `venv`).
2. Instalar dependências:
```
pip install -r requirements.txt
```

## Configuração
- `config/tj_catalog.csv`: catálogo dos TJs (RS, PI, TO), com URLs de transparência, formato e observações.
- `config/settings.yaml`: parâmetros padrão (período, caminhos de dados, etc.).

## Execução do pipeline
- Para extrair e integrar dados dos TJs cadastrados:
```
python -m src.main --tjs TJRS,TJPI,TJTO --start 2024-09 --end 2025-08
```
Parâmetros:
- `--tjs`: lista de códigos de TJs (ex.: `TJRS,TJPI,TJTO`), ou omita para usar todos os suportados neste projeto.
- `--start` e `--end`: período YYYY-MM.

Saídas:
- Arquivos brutos em `data/raw/<TJ>/<YYYY-MM>/`.
- Dataset unificado em `data/processed/remuneracao_unificada.parquet`.

## Cálculo de métricas e relatório
1. Gerar métricas agregadas:
```
python scripts/compute_metrics.py --input data/processed/remuneracao_unificada.parquet \
  --outdir reports/output
```
2. Renderizar relatório (Markdown -> HTML):
```
python scripts/render_report.py --metrics_dir reports/output \
  --template reports/template_report.md \
  --output reports/output/relatorio.html
```

## API (opcional)
Suba um servidor local para acionar extrações e consultar resultados:
```
uvicorn src.api:app --reload --port 8000
```
Exemplos:
- `GET /tjs`
- `POST /extract` com body `{ "tjs": ["TJRS","TJPI","TJTO"], "start": "2025-01", "end": "2025-08" }`
- `GET /unified`, `GET /metrics`

## Extensões de extratores
- Crie/adapte extratores em `src/extractors/` para cada TJ seguindo `base.py`.
- Cada extrator deve padronizar as colunas conforme `src/schemas.py`.

## Observações e dificuldades
- Alguns TJs fornecem CSV/JSON; outros, HTML/PDF. PDFs podem exigir OCR (fora do escopo inicial). Comece pelos formatos tabulares.
- Heterogeneidade de nomenclaturas e benefícios exige mapeamento cuidadoso para o schema unificado.
- Controle de qualidade: usar validações e logs para identificar outliers e dados faltantes.

## Licença
Uso educacional. Verifique termos de uso dos dados de cada TJ.
