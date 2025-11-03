# TJTO – Coleta manual

Coloque aqui os arquivos de remuneração do TJTO organizados por mês:

Estrutura sugerida:
- data/raw/TJTO/2025-08/arquivo.csv
- data/raw/TJTO/2025-07/arquivo.xlsx

Formatos aceitos pelo projeto (para futura ingestão):
- CSV (.csv)
- Excel (.xlsx)
- JSON (.json)
- HTML (.html) – se for tabela

Boas práticas de nome:
- Use a pasta `YYYY-MM` para o mês/ano de referência.
- Nome do arquivo livre, preferencialmente descritivo.

Mínimo de colunas esperadas (serão mapeadas para o schema unificado):
- Nome do servidor
- Cargo/Função
- Remuneração bruta
- Vencimento/Salário base
- Benefícios/Vantagens
- Descontos

Observação: Caso existam várias planilhas por mês (magistrados, servidores, etc.), pode colocar mais de um arquivo na mesma pasta do mês.
