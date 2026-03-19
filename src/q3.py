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

# 3. Reordenando as colunas para ficar EXATAMENTE como a imagem pede
ordem_colunas = ['product_id', 'product_name', 'category', 'start_date', 'usd_price']
df_custos = df_custos[ordem_colunas]

# 4. Validando o resultado em tela
print(f"Total de registros gerados após a tabularização: {len(df_custos)}")
print("\nAmostra do novo formato tabular:")
print(df_custos.head())

# 5. Salvando o CSV limpo (Sem o índice numérico lateral do Pandas)
df_custos.to_csv('datasets/custos_importacao_flat.csv', index=False, encoding='utf-8')
print("\nArquivo 'custos_importacao_flat.csv' gerado com sucesso!")