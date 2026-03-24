import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error

df = pd.read_csv('datasets/vendas_2023_2024.csv')

# Filtramos apenas o "Motor de Popa Yamaha Evo Dash 155HP" 
df_motor = df[df['id_product'] == 54].copy()

# Os dados originais possuem formatos de datas misturados (americano e brasileiro). Ja havia feito isso, mas mantive o dataset original.
# O parâmetro format='mixed' pede para o Pandas inferir e padronizar as datas (formato ISO).
df_motor['sale_date'] = pd.to_datetime(df_motor['sale_date'], format='mixed', dayfirst=True)

# Agrupamos as vendas por dia, pois o modelo pede a previsão em base diária.
df_diario = df_motor.groupby('sale_date')['qtd'].sum().reset_index()

# Definimos a data como o índice do DataFrame para facilitar a manipulação temporal.
df_diario.set_index('sale_date', inplace=True)

# Meu acrescimo para verificação se estava certo
print(df_diario.loc['2023-12-01':'2023-12-31'])
print('-'*20)


# Como o produto não é vendido todos os dias, a série temporal tem "buracos".
# Criamos um calendário contínuo cobrindo o período inteiro de Treino e Teste.
calendario_completo = pd.date_range(start='2023-01-01', end='2024-01-31', freq='D')

# Reindexamos o DataFrame para o calendário contínuo.
# Onde não havia vendas registradas, preenchemos com 0 (fill_value=0).
df_completo = df_diario.reindex(calendario_completo, fill_value=0)
df_completo.index.name = 'data'
df_completo.rename(columns={'qtd': 'vendas_reais'}, inplace=True)

# Criação do Modelo Baseline (Média Móvel 7 Dias) e Prevenção de Leakage
# Calculamos a média móvel dos últimos 7 dias. 
# O ".shift(1)" é CRUCIAL aqui: ele desloca os resultados em 1 dia.
# Isso garante que a previsão do dia "D" use apenas dados de "D-7" até "D-1".
df_completo['previsao_mm7'] = df_completo['vendas_reais'].rolling(window=7).mean().shift(1)

# Arredondamos a previsão para números inteiros (não se vende "meio" motor)
df_completo['previsao_mm7'] = np.round(df_completo['previsao_mm7'])


# Separamos apenas o mês de Janeiro de 2024 para avaliação (período de teste)
df_teste = df_completo.loc['2024-01-01':'2024-01-31'].copy()

# Calculamos o MAE (Erro Absoluto Médio) comparando as previsões com os dados reais
mae = mean_absolute_error(df_teste['vendas_reais'], df_teste['previsao_mm7'])
print(f"Métrica MAE em Janeiro/2024: {mae:.2f} unidades")

# Resposta para a Questão 7.2 (Soma da primeira semana)
soma_semana_1 = df_teste.loc['2024-01-01':'2024-01-07', 'previsao_mm7'].sum()
print(f"Previsão total (01/01 a 07/01): {int(soma_semana_1)} unidades")