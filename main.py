import json
import pandas
import requests
import pandasql as psql

dadoscodvi19 = requests.get('https://covid19-brazil-api.now.sh/api/report/v1')
dados_json = json.loads(dadoscodvi19.content)

df = pandas.DataFrame(dados_json['data'])

# Renomeando as colunas para Português
df.rename(columns={'state': 'Estados','cases': 'Casos', 'deaths': 'Mortes', 'suspects': 'Suspeitos', 'refuses': 'Recusados', 'datetime': 'Data'}, inplace=True)

#Ajuste dos tipos de Dados
df['uid'] = df['uid'].astype('string')
df['uf'] = df['uf'].astype('string')
df['Estados'] = df['Estados'].astype('string')
df['Data'] = pandas.to_datetime(df['Data']).dt.date

#Filtrando os 3 Estados mais atingidos pelo COVID usando SQL 

query = """
SELECT *
FROM df
ORDER BY casos DESC
LIMIT 3
"""
result = psql.sqldf(query, locals())

# Salvando o resultado da Query em formato parquet

result.to_parquet('Dado_processado/dataframe.parquet', engine='pyarrow', index=False)

print("DataFrame salvo em formato Parquet.")

