import pandas as pd
import requests
import matplotlib.pyplot as plt

# ==========================================
# 1. Carregar Dados e Padronizar Datas
# ==========================================
vendas = pd.read_csv('datasets/vendas_2023_2024.csv')
custos = pd.read_csv('datasets/custos_importacao_flat.csv')

# O .dt.normalize() remove as horas/minutos, deixando a data "pura" à meia-noite
vendas['sale_date'] = pd.to_datetime(vendas['sale_date'], format='mixed', dayfirst=True).dt.normalize()
vendas = vendas.dropna(subset=['sale_date']) # Limpa lixos

custos['start_date'] = pd.to_datetime(custos['start_date'], format='%d/%m/%Y').dt.normalize()

# ==========================================
# 2. Câmbio BCB (Extração e Limpeza)
# ==========================================
# Usando o endpoint Olinda. Removi o $top=100 para trazer todos os dias solicitados.
url_bcb = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='12-30-2022'&@dataFinalCotacao='12-31-2024'&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"

response = requests.get(url_bcb)
dados_json = response.json()['value'] # Acessa a chave "value" onde estão os dados
cambio = pd.DataFrame(dados_json)

# Padronizando a data do BCB para bater exatamente com vendas e custos
cambio['data_cambio'] = pd.to_datetime(cambio['dataHoraCotacao']).dt.normalize()
cambio = cambio[['data_cambio', 'cotacaoVenda']] # Só precisamos dessas duas

# ==========================================
# 3. Cruzamento Temporal (A Mágica)
# ==========================================
# Regra de ouro do merge_asof: Tudo DEVE estar ordenado pela data
vendas = vendas.sort_values('sale_date')
custos = custos.sort_values('start_date')
cambio = cambio.sort_values('data_cambio')

# A. Trazer o Custo em Dólar (o último válido antes ou na data da venda)
df_final = pd.merge_asof(
    vendas,
    custos[['product_id', 'start_date', 'usd_price']],
    left_on='sale_date',
    right_on='start_date',
    left_by='id_product',   # FK na tabela de vendas
    right_by='product_id',  # PK na tabela de custos
    direction='backward'
)

# B. Trazer o Câmbio (o último válido do BCB, resolvendo finais de semana)
df_final = pd.merge_asof(
    df_final,
    cambio,
    left_on='sale_date',
    right_on='data_cambio',
    direction='backward'
)

# ==========================================
# 4. Cálculos Financeiros
# ==========================================
# Custo na data da VENDA = Custo_USD_Histórico * Câmbio_do_Dia_da_Venda
df_final['custo_unit_brl'] = df_final['usd_price'] * df_final['cotacaoVenda']

# Custo Total = Unitário * Quantidade vendida (ajuste o nome de 'quantidade' se for outro no seu CSV)
df_final['custo_total_brl'] = df_final['custo_unit_brl'] * df_final['qtd']

# Prejuízo: o .clip(lower=0) garante que lucros fiquem zerados e apenas os prejuízos sejam guardados
df_final['prejuizo'] = (df_final['custo_total_brl'] - df_final['total']).clip(lower=0)

# ==========================================
# 5. Agregação e Exportação
# ==========================================
df_agg = df_final.groupby('id_product').agg(
    receita_total=('total', 'sum'),
    prejuizo_total=('prejuizo', 'sum')
).reset_index()

# Calculando percentual (somente para os que deram prejuízo)
df_agg['percentual_perda'] = (df_agg['prejuizo_total'] / df_agg['receita_total']) * 100

df_prejuizo = df_agg[df_agg['prejuizo_total'] > 0].sort_values('prejuizo_total', ascending=False)
print(df_prejuizo.head())