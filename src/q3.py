import pandas as pd
import json

# 1. Carregando o arquivo JSON bruto
with open('datasets/custos_importacao.json', 'r', encoding='utf-8') as file:
    dados_json = json.load(file)

# Transformando a estrutura aninhada em formato tabular
# record_path: o nome da lista que queremos expandir (as linhas filhas)
# meta: as colunas do "pai" que queremos repetir para cada linha filha
df_custos = pd.json_normalize(
    dados_json,
    record_path='historic_data',
    meta=['product_id', 'product_name', 'category']
)

# 2. Reordenando as colunas para ficar EXATAMENTE como a imagem pede na questão
ordem_colunas = ['product_id', 'product_name', 'category', 'start_date', 'usd_price']
df_custos = df_custos[ordem_colunas]

# 3. Validando o resultado 
print(f"Total de registros gerados após a tabularização: {len(df_custos)}")
print("\nAmostra do novo formato tabular:")
print(df_custos.head())

# 4. Imprimir em tela os tipos de cada coluna
print("\nTipos das colunas:")
print(df_custos.dtypes)

# 5. Conversão de tipos de dados (Casting Específico)
# Convertendo ID para inteiro e start_date para data real
df_custos['product_id'] = df_custos['product_id'].astype(int)
# apesar da saída ficar como object, a data está correta e ficará como date
df_custos['start_date'] = pd.to_datetime(df_custos['start_date'], dayfirst=True).dt.date

print("\nTipos das colunas após o ajuste (Item 6):")
print(df_custos.dtypes)

# 6. Salvando o CSV limpo 
df_custos.to_csv('datasets/custos_importacao.csv', index=False, encoding='utf-8')
print("\nArquivo 'custos_importacao.csv' gerado com sucesso!")

