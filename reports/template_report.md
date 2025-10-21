<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>Relatório – Remuneração TJs</title>
<style>
body { font-family: Arial, sans-serif; margin: 24px; }
h1, h2, h3 { color: #1f3b4d; }
table { border-collapse: collapse; width: 100%; margin-bottom: 16px; }
th, td { border: 1px solid #ccc; padding: 6px 8px; }
th { background: #f3f6f9; }
.small { color: #666; font-size: 12px; }
.badge { display: inline-block; background:#eef3f8; padding:2px 6px; border-radius:6px; margin-left:6px; }
</style>
</head>
<body>
<h1>Relatório – Remuneração nos TJs Estaduais</h1>
<p class="small">Resultados preliminares a partir do dataset unificado gerado pelo pipeline.</p>

<h2>Resumo por mês</h2>
<table>
  <thead>
    <tr>
      <th>Mês</th>
      <th>Servidores</th>
      <th>Média bruta</th>
      <th>Mediana bruta</th>
      <th>P90</th>
      <th>P99</th>
      <th>Máximo</th>
    </tr>
  </thead>
  <tbody>
  {% for r in by_month %}
    <tr>
      <td>{{ r.year_month }}</td>
      <td>{{ r.servidores }}</td>
      <td>{{ 'R$ {:,.2f}'.format(r.media_bruta).replace(',', 'X').replace('.', ',').replace('X', '.') }}</td>
      <td>{{ 'R$ {:,.2f}'.format(r.mediana_bruta).replace(',', 'X').replace('.', ',').replace('X', '.') }}</td>
      <td>{{ 'R$ {:,.2f}'.format(r.p90_bruta).replace(',', 'X').replace('.', ',').replace('X', '.') }}</td>
      <td>{{ 'R$ {:,.2f}'.format(r.p99_bruta).replace(',', 'X').replace('.', ',').replace('X', '.') }}</td>
      <td>{{ 'R$ {:,.2f}'.format(r.max_bruta).replace(',', 'X').replace('.', ',').replace('X', '.') }}</td>
    </tr>
  {% endfor %}
  </tbody>
</table>

<h2>Resumo por função</h2>
<table>
  <thead>
    <tr>
      <th>Mês</th>
      <th>Função</th>
      <th>Servidores</th>
      <th>Média bruta</th>
      <th>Mediana bruta</th>
    </tr>
  </thead>
  <tbody>
  {% for r in by_role %}
    <tr>
      <td>{{ r.year_month }}</td>
      <td>{{ r.role }}</td>
      <td>{{ r.servidores }}</td>
      <td>{{ 'R$ {:,.2f}'.format(r.media_bruta).replace(',', 'X').replace('.', ',').replace('X', '.') }}</td>
      <td>{{ 'R$ {:,.2f}'.format(r.mediana_bruta).replace(',', 'X').replace('.', ',').replace('X', '.') }}</td>
    </tr>
  {% endfor %}
  </tbody>
</table>

<h2>Maior remuneração bruta por mês</h2>
<table>
  <thead>
    <tr>
      <th>Mês</th>
      <th>TJ</th>
      <th>Servidor</th>
      <th>Função</th>
      <th>Remuneração bruta</th>
    </tr>
  </thead>
  <tbody>
  {% for r in top_by_month %}
    <tr>
      <td>{{ r.year_month }}</td>
      <td>{{ r.tj_code }}</td>
      <td>{{ r.server_name }}</td>
      <td>{{ r.role }}</td>
      <td><span class="badge">{{ 'R$ {:,.2f}'.format(r.gross_pay).replace(',', 'X').replace('.', ',').replace('X', '.') }}</span></td>
    </tr>
  {% endfor %}
  </tbody>
</table>

<p class="small">Observação: valores e métricas dependem da cobertura dos extratores configurados.</p>
</body>
</html>
