# Tarefas para não esquecer:
# 1 — Padronize os nomes das categorias de produtos em: eletrônicos, propulsão e ancoragem.
# 2 — Converta os valores para o tipo numérico.
# 3 — Remova as duplicatas.

import unicodedata
import pandas as pd
import re

df_produtos = pd.read_csv("datasets/produtos_raw.csv")

# primeiro, vou contar a quantidade de linhas para saber quantos valores de 'actual_category' tenho no dataset
quantidade_linhas = len(df_produtos)
print(f"Quantidade de linhas: {quantidade_linhas}")

# O método .nunique() traz o número de categorias distintas
total_categorias_distintas = df_produtos['actual_category'].nunique()

print(f"Quantidade de categorias distintas: {total_categorias_distintas}")

# Mostra a categoria e a contagem de vezes que ela aparece
print(df_produtos['actual_category'].value_counts())

def clean_strings(texto):
    """
    Função genérica para sanitização de strings.
    """
    if pd.isna(texto): 
        return texto
    # converte tudo em minúsculo
    texto = texto.lower()
    # remove os acentos
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    # 3. Remover TUDO que não for letra 
    texto = re.sub(r'[^a-z]', '', texto)

    return texto

def category_mapping(clean_text):
    """
    Mapeia as categorias que passaram por uma primeira limpeza para as categorias finais.
    """
    if pd.isna(clean_text): return clean_text

    # como clean_strings() já sanitizou as categorias, agora vamos mapear tudo:
    if 'eletr' in clean_text or 'elt' in clean_text:
        return 'eletrônicos'
    elif 'prop' in clean_text:
        return 'propulsão'
    elif 'anc' in clean_text or 'enc' in clean_text:
        return 'ancoragem'
    else:
        return clean_text

df_produtos['actual_category'] = df_produtos['actual_category'].apply(clean_strings).apply(category_mapping)

total_clean_categories = df_produtos['actual_category'].nunique()
print(f"Quantidade de categorias distintas após limpeza inicial: {total_clean_categories}")

# Mostra a categoria e a contagem de vezes que ela aparece após a limpeza
print(df_produtos['actual_category'].value_counts())

# Aqui, vou constatar que price é do tipo object
tipo_price = df_produtos['price'].dtype
print(f"Tipo de dado da coluna 'price': {tipo_price}")

# A função irá converter strings para float
def text_to_float(price):
    """
    Função genérica para extrair float de strings da coluna price.
    Mantém apenas números e o ponto decimal.
    """
    if pd.isna(price): return price
    
    # por precaução, converte para string
    texto = str(price)
    # essa expressão regular elimina tudo que não seja número ou ponto
    texto_limpo = re.sub(r'[^0-9.]', '', texto)
    # faz um cast para float após a limpeza
    return float(texto_limpo)

df_produtos['price'] = df_produtos['price'].apply(text_to_float)
print(df_produtos[['name', 'price']].head())
print(f"\nNovo tipo da coluna: {df_produtos['price'].dtype}")

# Removendo as duplicatas
print(f"Quantidade de linhas antes de remover as duplicatas: {len(df_produtos)}")
df_produtos = df_produtos.drop_duplicates()
print(f"Quantidade de linhas depois de remover as duplicatas: {len(df_produtos)}")

# Salva o dataset tratado para uso na questão 5

df_produtos.to_csv('datasets/produtos_refined.csv', index=False, encoding='utf-8')
print(f"\n✓ Dataset limpo salvo em 'datasets/produtos_refined.csv'")
