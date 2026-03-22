"""
================================================================================
QUESTÃO 5 - ANÁLISE DE CLIENTES FIÉIS
================================================================================
Objetivo: Identificar os 10 clientes de elite com maior ticket médio,
          que compraram em 3 ou mais categorias distintas.

Critérios de "Cliente Fiel de Elite":
  - Alto gasto médio por transação (Ticket Médio)
  - Diversidade de compras (3+ categorias)

Autor: [Seu nome]
Data: [Data atual]
================================================================================
"""

import pandas as pd
import json

# Configuração para exibição mais limpa dos DataFrames
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


# ==============================================================================
# 1. CARREGAR DADOS
# ==============================================================================

print("=" * 80)
print("ETAPA 1: CARREGAMENTO DOS DADOS")
print("=" * 80)

# --- 1.1 Vendas ---
vendas = pd.read_csv('datasets/vendas_2023_2024.csv')
print(f"✓ Vendas carregadas: {len(vendas)} transações")

# --- 1.2 Produtos (já limpos pela Q2) ---
produtos = pd.read_csv('datasets/produtos_refined.csv')
print(f"✓ Produtos carregados: {len(produtos)} produtos")
print(f"  Categorias disponíveis: {produtos['actual_category'].unique().tolist()}")

# --- 1.3 Clientes (opcional, para enriquecer relatório) ---
with open('datasets/clientes_crm.json', 'r', encoding='utf-8') as f:
    clientes_json = json.load(f)
clientes = pd.DataFrame(clientes_json)
print(f"✓ Clientes carregados: {len(clientes)} clientes")


# ==============================================================================
# 2. MERGE: VENDAS + PRODUTOS
# ==============================================================================
# Objetivo: Trazer a categoria de cada produto para a tabela de vendas

print("\n" + "=" * 80)
print("ETAPA 2: MERGE VENDAS + PRODUTOS")
print("=" * 80)

# Merge pela chave: vendas.id_product = produtos.code
df = vendas.merge(
    produtos[['code', 'name', 'actual_category']],
    left_on='id_product',
    right_on='code',
    how='left'
)

# Renomear para facilitar
df = df.rename(columns={
    'name': 'product_name',
    'actual_category': 'categoria'
})

# Verificar se houve vendas sem produto correspondente
vendas_sem_produto = df['categoria'].isna().sum()
print(f"✓ Merge realizado")
print(f"  Vendas sem produto correspondente: {vendas_sem_produto}")

if vendas_sem_produto > 0:
    print("  ⚠️ Removendo vendas órfãs...")
    df = df.dropna(subset=['categoria'])

print(f"  Total de registros para análise: {len(df)}")


# ==============================================================================
# 3. AGREGAÇÃO POR CLIENTE
# ==============================================================================
# Métricas conforme enunciado:
#   - Faturamento Total: SUM(total)
#   - Frequência: COUNT(id) - cada linha é uma transação
#   - Ticket Médio: Faturamento / Frequência
#   - Diversidade: COUNT(DISTINCT categoria)

print("\n" + "=" * 80)
print("ETAPA 3: AGREGAÇÃO POR CLIENTE")
print("=" * 80)

df_clientes = df.groupby('id_client').agg(
    faturamento_total=('total', 'sum'),
    frequencia=('id', 'count'),
    diversidade_categorias=('categoria', 'nunique')
).reset_index()

# Calcular Ticket Médio
df_clientes['ticket_medio'] = df_clientes['faturamento_total'] / df_clientes['frequencia']

print(f"✓ Agregação concluída")
print(f"  Total de clientes únicos: {len(df_clientes)}")
print(f"  Clientes com 3+ categorias: {(df_clientes['diversidade_categorias'] >= 3).sum()}")


# ==============================================================================
# 4. FILTRO DE ELITE (3+ CATEGORIAS)
# ==============================================================================

print("\n" + "=" * 80)
print("ETAPA 4: FILTRO DE CLIENTES ELITE")
print("=" * 80)

# Filtrar apenas clientes que compraram em 3 ou mais categorias
df_elite = df_clientes[df_clientes['diversidade_categorias'] >= 3].copy()

print(f"✓ Filtro aplicado: diversidade >= 3 categorias")
print(f"  Clientes que atendem ao critério: {len(df_elite)}")


# ==============================================================================
# 5. RANKING TOP 10
# ==============================================================================
# Ordenação:
#   1º: Ticket Médio (DECRESCENTE) - maior primeiro
#   2º: id_client (CRESCENTE) - desempate por menor ID

print("\n" + "=" * 80)
print("ETAPA 5: RANKING TOP 10 CLIENTES ELITE")
print("=" * 80)

# Ordenar conforme regra de desempate
df_elite = df_elite.sort_values(
    by=['ticket_medio', 'id_client'],
    ascending=[False, True]
)

# Selecionar top 10
top_10 = df_elite.head(10).copy()
top_10['ranking'] = range(1, len(top_10) + 1)

# Enriquecer com nome do cliente (do CRM)
top_10 = top_10.merge(
    clientes[['code', 'full_name']],
    left_on='id_client',
    right_on='code',
    how='left'
)

print(f"✓ Top 10 clientes identificados")


# ==============================================================================
# 6. ANÁLISE: CATEGORIA PREDOMINANTE DOS TOP 10
# ==============================================================================
# Para os 10 clientes selecionados:
#   - Filtrar suas transações
#   - Agrupar por categoria
#   - Somar quantidade (qtd)
#   - Identificar categoria com maior volume

print("\n" + "=" * 80)
print("ETAPA 6: ANÁLISE DE CATEGORIA PREDOMINANTE")
print("=" * 80)

# IDs dos top 10 clientes
ids_top_10 = top_10['id_client'].tolist()

# Filtrar transações apenas dos top 10
df_top_10_transacoes = df[df['id_client'].isin(ids_top_10)]

# Agrupar por categoria e somar quantidade
categoria_predominante = df_top_10_transacoes.groupby('categoria').agg(
    total_itens=('qtd', 'sum'),
    total_transacoes=('id', 'count'),
    faturamento=('total', 'sum')
).reset_index()

# Ordenar por quantidade de itens
categoria_predominante = categoria_predominante.sort_values('total_itens', ascending=False)

print(f"✓ Análise de categorias concluída")


# ==============================================================================
# RESULTADOS
# ==============================================================================

print("\n")
print("█" * 80)
print("█" + " " * 30 + "RESULTADOS" + " " * 30 + "█")
print("█" * 80)

# --- Resultado 1: Top 10 Clientes Elite ---
print("\n" + "=" * 80)
print("TOP 10 CLIENTES FIÉIS DE ELITE")
print("=" * 80)
print("Critério: Maior Ticket Médio | Filtro: 3+ categorias | Desempate: ID crescente")
print("-" * 80)

# Preparar tabela de exibição
tabela_top_10 = top_10[[
    'ranking', 'id_client', 'full_name', 'faturamento_total', 
    'frequencia', 'ticket_medio', 'diversidade_categorias'
]].copy()

tabela_top_10.columns = [
    'Rank', 'ID', 'Nome', 'Faturamento Total (R$)', 
    'Transações', 'Ticket Médio (R$)', 'Categorias'
]

print(tabela_top_10.to_string(index=False))


# --- Resultado 2: Categoria Predominante ---
print("\n" + "=" * 80)
print("CATEGORIA PREDOMINANTE DOS TOP 10 CLIENTES")
print("=" * 80)
print("Análise: Qual categoria concentra maior quantidade de itens comprados?")
print("-" * 80)

print(categoria_predominante.to_string(index=False))

# Destacar a categoria campeã
cat_campeã = categoria_predominante.iloc[0]
print(f"\n🏆 CATEGORIA COM MAIOR VOLUME DE ITENS:")
print(f"   Categoria: {cat_campeã['categoria'].upper()}")
print(f"   Total de itens: {cat_campeã['total_itens']:,.0f} unidades")
print(f"   Total de transações: {cat_campeã['total_transacoes']:,.0f}")
print(f"   Faturamento: R$ {cat_campeã['faturamento']:,.2f}")


# --- Estatísticas Complementares ---
print("\n" + "=" * 80)
print("ESTATÍSTICAS COMPLEMENTARES")
print("=" * 80)

print(f"\n📊 RESUMO GERAL:")
print(f"   Total de clientes analisados: {len(df_clientes)}")
print(f"   Clientes com 3+ categorias: {len(df_elite)} ({len(df_elite)/len(df_clientes)*100:.1f}%)")
print(f"   Ticket médio geral: R$ {df_clientes['ticket_medio'].mean():,.2f}")
print(f"   Ticket médio dos Top 10: R$ {top_10['ticket_medio'].mean():,.2f}")

print(f"\n📊 DISTRIBUIÇÃO DE DIVERSIDADE:")
print(df_clientes['diversidade_categorias'].value_counts().sort_index().to_string())


# ==============================================================================
# EXPORTAÇÃO DOS RESULTADOS
# ==============================================================================

print("\n" + "=" * 80)
print("EXPORTAÇÃO DOS RESULTADOS")
print("=" * 80)

# Salvar Top 10 clientes
top_10.to_csv('reports/q5_top_10_clientes_elite.csv', index=False, encoding='utf-8')
print("✓ q5_top_10_clientes_elite.csv")

# Salvar análise de categorias
categoria_predominante.to_csv('reports/q5_categoria_predominante.csv', index=False, encoding='utf-8')
print("✓ q5_categoria_predominante.csv")

# Salvar todos os clientes com métricas (para análise adicional)
df_clientes.to_csv('reports/q5_clientes_metricas.csv', index=False, encoding='utf-8')
print("✓ q5_clientes_metricas.csv")

print("\n" + "=" * 80)
print("PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
print("=" * 80)

# ==============================================================================
# AUDITORIA MANUAL - TOP 5 CLIENTES ELITE
# ==============================================================================

print("\n")
print("█" * 80)
print("█" + " " * 25 + "AUDITORIA MANUAL" + " " * 25 + "█")
print("█" * 80)

# Top 5 clientes para validação
top_5_ids = top_10.head(5)['id_client'].tolist()

for cliente_id in top_5_ids:
    print(f"\n{'='*80}")
    print(f"CLIENTE #{cliente_id}")
    print(f"{'='*80}")
    
    # --- 1. Dados do CRM ---
    info_cliente = clientes[clientes['code'] == cliente_id]
    if not info_cliente.empty:
        print(f"\n👤 DADOS DO CLIENTE:")
        print(f"   Nome: {info_cliente.iloc[0]['full_name']}")
        print(f"   Email: {info_cliente.iloc[0]['email']}")
        print(f"   Localização: {info_cliente.iloc[0]['location']}")
    
    # --- 2. Transações do cliente ---
    transacoes_cliente = df[df['id_client'] == cliente_id]
    
    print(f"\n📦 AMOSTRA DE TRANSAÇÕES (5 primeiras):")
    colunas_exibir = ['id', 'sale_date', 'product_name', 'categoria', 'qtd', 'total']
    print(transacoes_cliente[colunas_exibir].head(5).to_string(index=False))
    
    # --- 3. Cálculos manuais ---
    faturamento_manual = transacoes_cliente['total'].sum()
    frequencia_manual = len(transacoes_cliente)
    ticket_medio_manual = faturamento_manual / frequencia_manual
    categorias_manual = transacoes_cliente['categoria'].unique().tolist()
    diversidade_manual = len(categorias_manual)
    
    print(f"\n📊 CÁLCULOS MANUAIS:")
    print(f"   Faturamento Total: R$ {faturamento_manual:,.2f}")
    print(f"   Frequência (transações): {frequencia_manual}")
    print(f"   Ticket Médio: R$ {ticket_medio_manual:,.2f}")
    print(f"   Categorias: {categorias_manual}")
    print(f"   Diversidade: {diversidade_manual} categorias")
    
    # --- 4. Comparação com valores do ranking ---
    dados_ranking = top_10[top_10['id_client'] == cliente_id].iloc[0]
    
    print(f"\n✅ COMPARAÇÃO COM RANKING:")
    print(f"   {'Métrica':<25} {'Ranking':<20} {'Manual':<20} {'Status'}")
    print(f"   {'-'*75}")
    
    # Faturamento
    fat_ok = abs(dados_ranking['faturamento_total'] - faturamento_manual) < 0.01
    print(f"   {'Faturamento Total':<25} R$ {dados_ranking['faturamento_total']:>14,.2f}   R$ {faturamento_manual:>14,.2f}   {'✓' if fat_ok else '✗'}")
    
    # Frequência
    freq_ok = dados_ranking['frequencia'] == frequencia_manual
    print(f"   {'Frequência':<25} {dados_ranking['frequencia']:>17}   {frequencia_manual:>17}   {'✓' if freq_ok else '✗'}")
    
    # Ticket Médio
    ticket_ok = abs(dados_ranking['ticket_medio'] - ticket_medio_manual) < 0.01
    print(f"   {'Ticket Médio':<25} R$ {dados_ranking['ticket_medio']:>14,.2f}   R$ {ticket_medio_manual:>14,.2f}   {'✓' if ticket_ok else '✗'}")
    
    # Diversidade
    div_ok = dados_ranking['diversidade_categorias'] == diversidade_manual
    print(f"   {'Diversidade':<25} {dados_ranking['diversidade_categorias']:>17}   {diversidade_manual:>17}   {'✓' if div_ok else '✗'}")
    
    # --- 5. Breakdown por categoria ---
    print(f"\n📈 BREAKDOWN POR CATEGORIA:")
    breakdown = transacoes_cliente.groupby('categoria').agg(
        qtd_itens=('qtd', 'sum'),
        transacoes=('id', 'count'),
        faturamento=('total', 'sum')
    ).reset_index()
    breakdown = breakdown.sort_values('faturamento', ascending=False)
    print(breakdown.to_string(index=False))

print("\n" + "=" * 80)
print("FIM DA AUDITORIA")
print("=" * 80)