import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# =============================================================================
# TAREFA 1: Criar matriz de interação Usuário × Produto
# Regras:
#   - Linhas: id_client
#   - Colunas: id_product  
#   - Valor: 1 se comprou, 0 se não comprou
#   - Ignorar quantidade (apenas presença/ausência)
# =============================================================================

# Carregar dados
df = pd.read_csv('datasets/vendas_2023_2024.csv')

# Criar a matriz com pivot_table
# aggfunc='count' conta ocorrências, fill_value=0 preenche vazios com zero
matriz_usuario_produto = pd.pivot_table(
    df,
    index='id_client',
    columns='id_product',
    values='id',  # qualquer coluna serve, só queremos contar
    aggfunc='count',
    fill_value=0
)

# Transformar em binário: comprou (1) ou não comprou (0)
matriz_usuario_produto = (matriz_usuario_produto > 0).astype(int)

print("TAREFA 1 - Matriz Usuário × Produto:")
print(f"Dimensões: {matriz_usuario_produto.shape[0]} clientes × {matriz_usuario_produto.shape[1]} produtos")
print(matriz_usuario_produto.head())

# =============================================================================
# TAREFA 2: Calcular Similaridade de Cosseno entre produtos
# Regras:
#   - Similaridade produto × produto
#   - Baseada nos clientes que compraram cada item
# =============================================================================

# O sklearn.cosine_similarity compara LINHAS entre si
# Nossa matriz tem clientes nas linhas e produtos nas colunas
# Precisamos transpor para ter produtos nas linhas
matriz_produto_cliente = matriz_usuario_produto.T

print("\nMatriz transposta (Produto × Cliente):")
print(f"Dimensões: {matriz_produto_cliente.shape[0]} produtos × {matriz_produto_cliente.shape[1]} clientes")

# Calcular similaridade de cosseno entre todos os produtos
matriz_similaridade = cosine_similarity(matriz_produto_cliente)

# Transformar em DataFrame para facilitar a leitura
# Linhas e colunas são os id_product
df_similaridade = pd.DataFrame(
    matriz_similaridade,
    index=matriz_produto_cliente.index,
    columns=matriz_produto_cliente.index
)

print("\nTAREFA 2 - Matriz de Similaridade (amostra):")
print(df_similaridade.head())

# =============================================================================
# TAREFA 3: Ranking dos 5 produtos mais similares ao GPS (id = 27)
# Regras:
#   - Produto referência: id_product = 27
#   - Desconsiderar o próprio GPS no ranking
# =============================================================================

id_gps = 27

# Pegar a coluna/linha do produto 27 (são iguais, matriz é simétrica)
similaridade_gps = df_similaridade[id_gps]

# Remover o próprio GPS e ordenar do maior para menor
ranking = (
    similaridade_gps
    .drop(id_gps)  # remove o próprio GPS
    .sort_values(ascending=False)  # ordena do mais similar ao menos
    .head(5)  # pega os 5 primeiros
)

print("\nTAREFA 3 - Top 5 produtos mais similares ao GPS (id=27):")
print("="*45)
for posicao, (id_produto, similaridade) in enumerate(ranking.items(), 1):
    print(f"{posicao}º lugar: Produto {id_produto} | Similaridade: {similaridade:.4f}")

# =============================================================================
# RESPOSTA FINAL
# =============================================================================
produto_mais_similar = ranking.index[0]
print("\n" + "="*45)
print(f"RESPOSTA: O produto com MAIOR similaridade é o id_product: {produto_mais_similar}")