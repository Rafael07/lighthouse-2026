import pandas as pd # pyright: ignore[reportMissingModuleSource]

df_vendas = pd.read_csv("datasets/vendas_2023_2024.csv")

#Quentidade total de linhas
total_linhas = len(df_vendas)
print(f"Quantidade total de linhas: {total_linhas}")

# Forçando a leitura de linhas vazias e realizando sua contagem
df_com_vazios = pd.read_csv('datasets/vendas_2023_2024.csv', skip_blank_lines=False)
linhas_vazias = df_com_vazios.isnull().all(axis=1).sum()
print(f"Linhas em branco detectadas: {linhas_vazias}")

#Quantidade total de colunas
total_colunas = len(df_vendas.columns)
print(f"Quantidade total de colunas: {total_colunas}")

#intervalo de datas analisado
data_minima = df_vendas['sale_date'].min()
data_maxima = df_vendas['sale_date'].max()
print(f"Intervalo de datas analisado: {data_minima} a {data_maxima}")


# 1. Separando os formatos de forma rigorosa
# Formato ISO: YYYY-MM-DD (O hífen está na posição 4)
vendas_iso = df_vendas[df_vendas['sale_date'].str[4] == '-']['sale_date']

# Formato BR: DD-MM-YYYY (O hífen está na posição 2)
vendas_br = df_vendas[df_vendas['sale_date'].str[2] == '-']['sale_date']

# 2. Obtendo os extremos de cada grupo (como strings puras, sem conversão)
print("--- ANÁLISE DE DATAS BRUTAS ---")
print(f"Formato ISO (YYYY-MM-DD):")
print(f"  Menor valor string: {vendas_iso.min()}")
print(f"  Maior valor string: {vendas_iso.max()}")

print(f"\nFormato BR (DD-MM-YYYY):")
print(f"  Menor valor string: {vendas_br.min()}")
print(f"  Maior valor string: {vendas_br.max()}")

# 3. Contagem para verificar se cobrimos todas as linhas
print(f"\nTotal de linhas ISO: {len(vendas_iso)}")
print(f"Total de linhas BR:  {len(vendas_br)}")
print(f"Soma total:         {len(vendas_iso) + len(vendas_br)} / {len(df_vendas)}")


# Cálculos para a coluna Total

total_vendas_min = df_vendas['total'].min()
total_vendas_max = df_vendas['total'].max()
total_vendas_media = df_vendas['total'].mean().round(2)

print(f"Total de vendas: {total_vendas_min} a {total_vendas_max}")
print(f"Média de vendas: {total_vendas_media}")

# Verifique o tipo de dado que o Pandas atribuiu à coluna
print(f"Tipo da coluna total: {df_vendas['total'].dtype}")

# Verifique as 5 linhas com os menores valores REAIS (ordenando como números)
print("\nTop 5 menores valores:")
print(df_vendas['total'].sort_values().head())

# Verifique as 5 linhas com os maiores valores REAIS (ordenando como números)
print("\nTop 5 maiores valores:")
print(df_vendas['total'].sort_values().tail())

# Verifique se há valores nulos que você pode ter ignorado ou que o Pandas preencheu
print(f"\nValores nulos em total: {df_vendas['total'].isna().sum()}")