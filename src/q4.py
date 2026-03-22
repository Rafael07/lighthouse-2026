"""
================================================================================
ANÁLISE DE PREJUÍZO EM VENDAS - QUESTÃO 4 (DADOS PÚBLICOS)
================================================================================
Objetivo: Identificar transações onde produtos foram vendidos abaixo do custo,
          considerando a variação cambial diária.

Autor: [Seu nome]
Data: [Data atual]
================================================================================
"""

import pandas as pd
import requests

# Configuração para exibição mais limpa dos DataFrames
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


# ==============================================================================
# 1. CARREGAR DADOS E PADRONIZAR DATAS
# ==============================================================================
# Premissa: Todas as datas serão convertidas para o formato ISO (YYYY-MM-DD)
# que é o padrão do pandas datetime64, permitindo operações e ordenações corretas.

print("=" * 80)
print("ETAPA 1: CARREGAMENTO E PADRONIZAÇÃO DOS DADOS")
print("=" * 80)

# --- 1.1 Carregar tabela de vendas ---
# Contém: id da venda, cliente, produto, quantidade, total (receita BRL), data
vendas = pd.read_csv('datasets/vendas_2023_2024.csv')

# Padronizar data: formato misto (alguns DD-MM-YYYY, outros YYYY-MM-DD)
# dayfirst=True interpreta corretamente quando o dia vem primeiro
# .dt.normalize() remove componente de hora, deixando apenas a data
vendas['sale_date'] = pd.to_datetime(
    vendas['sale_date'], 
    format='mixed', 
    dayfirst=True
).dt.normalize()

# Remove linhas com data inválida/nula (limpeza de dados)
vendas = vendas.dropna(subset=['sale_date'])

print(f"✓ Vendas carregadas: {len(vendas)} registros")
print(f"  Período: {vendas['sale_date'].min().date()} a {vendas['sale_date'].max().date()}")


# --- 1.2 Carregar tabela de custos de importação ---
# Contém: id do produto, nome, categoria, data da importação, custo unitário USD
# IMPORTANTE: Um mesmo produto pode ter múltiplas importações em datas diferentes
custos = pd.read_csv('datasets/custos_importacao_flat.csv')

# Padronizar data: formato DD/MM/YYYY
custos['start_date'] = pd.to_datetime(
    custos['start_date'], 
    format='%d/%m/%Y'
).dt.normalize()

print(f"✓ Custos carregados: {len(custos)} registros")
print(f"  Produtos únicos: {custos['product_id'].nunique()}")


# ==============================================================================
# 2. CÂMBIO BCB (EXTRAÇÃO VIA API)
# ==============================================================================
# Fonte: API Olinda do Banco Central do Brasil
# Dado utilizado: Cotação de VENDA do dólar (PTAX) - média diária
# Período: Cobrindo todas as vendas de 2023 e 2024

print("\n" + "=" * 80)
print("ETAPA 2: OBTENÇÃO DAS COTAÇÕES DE CÂMBIO (BCB)")
print("=" * 80)

# Endpoint da API - retorna cotação de venda do período solicitado
# Formato da data na URL: MM-DD-YYYY (padrão da API)
url_bcb = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    "?@dataInicial='12-30-2022'"
    "&@dataFinalCotacao='12-31-2024'"
    "&$format=json"
    "&$select=cotacaoVenda,dataHoraCotacao"
)

# Requisição à API
response = requests.get(url_bcb)

# Verifica se a requisição foi bem-sucedida
if response.status_code == 200:
    dados_json = response.json()['value']
    cambio = pd.DataFrame(dados_json)
    print(f"✓ Cotações obtidas da API: {len(cambio)} registros")
else:
    raise Exception(f"Erro na API do BCB: {response.status_code}")

# Padronizar data: extrair apenas a data (sem hora)
# A API já retorna a média do dia, então não precisamos agregar
cambio['data_cambio'] = pd.to_datetime(cambio['dataHoraCotacao']).dt.normalize()

# Manter apenas colunas necessárias
cambio = cambio[['data_cambio', 'cotacaoVenda']]

# Salvar CSV para referência/auditoria
cambio.to_csv('datasets/cambio.csv', index=False, encoding='utf-8')
print(f"✓ Arquivo 'cambio.csv' salvo para referência")
print(f"  Período: {cambio['data_cambio'].min().date()} a {cambio['data_cambio'].max().date()}")


# ==============================================================================
# 3. CRUZAMENTO TEMPORAL (MERGE_ASOF)
# ==============================================================================
# Técnica: merge_asof realiza junção aproximada por data
# direction='backward' → pega o registro mais recente ANTES ou IGUAL à data de referência
#
# REGRA OBRIGATÓRIA: DataFrames devem estar ordenados pela coluna de data

print("\n" + "=" * 80)
print("ETAPA 3: CRUZAMENTO TEMPORAL DOS DADOS")
print("=" * 80)

# --- 3.1 Ordenar todos os DataFrames por data ---
vendas = vendas.sort_values('sale_date')
custos = custos.sort_values('start_date')
cambio = cambio.sort_values('data_cambio')

print("✓ DataFrames ordenados por data")


# --- 3.2 Merge: Vendas + Custos ---
# Lógica: Para cada venda, buscar o custo USD da última importação do produto
#         que ocorreu ANTES ou NA data da venda.
# 
# Exemplo: Produto importado em Jun/2022, nova importação em Ago/2023
#          Venda em Jul/2023 → usa custo de Jun/2022
#          Venda em Set/2023 → usa custo de Ago/2023

df_final = pd.merge_asof(
    vendas,                                          # Tabela principal (esquerda)
    custos[['product_id', 'start_date', 'usd_price', 'product_name']],  # Colunas necessárias
    left_on='sale_date',                             # Data de referência (vendas)
    right_on='start_date',                           # Data de referência (custos)
    left_by='id_product',      # ← Coluna de match na tabela da ESQUERDA (vendas)
    right_by='product_id',     # ← Coluna de match na tabela da DIREITA (custos)
    direction='backward'                             # Pegar importação anterior ou igual
)

print(f"✓ Merge Vendas + Custos realizado")


# --- 3.3 Merge: Resultado anterior + Câmbio ---
# Lógica: Para cada venda, buscar a cotação do dólar no dia da venda.
#         Se não houver cotação (fim de semana/feriado), usa o último dia útil.

df_final = pd.merge_asof(
    df_final,
    cambio[['data_cambio', 'cotacaoVenda']],         # Apenas colunas necessárias
    left_on='sale_date',                             # Data da venda
    right_on='data_cambio',                          # Data da cotação
    direction='backward'                             # Última cotação disponível
)

print(f"✓ Merge com Câmbio realizado")


# --- 3.4 Validação de integridade ---
# Verificar se alguma venda ficou sem custo ou câmbio (NaN)
vendas_sem_custo = df_final['usd_price'].isna().sum()
vendas_sem_cambio = df_final['cotacaoVenda'].isna().sum()

print(f"\n[VALIDAÇÃO DE INTEGRIDADE]")
print(f"  Vendas sem custo encontrado: {vendas_sem_custo}")
print(f"  Vendas sem câmbio encontrado: {vendas_sem_cambio}")

if vendas_sem_custo > 0 or vendas_sem_cambio > 0:
    print("  ⚠️  ATENÇÃO: Existem vendas sem dados completos. Serão desconsideradas.")
    df_final = df_final.dropna(subset=['usd_price', 'cotacaoVenda'])


# ==============================================================================
# 4. CÁLCULOS FINANCEIROS
# ==============================================================================
# Fórmulas conforme definido no entendimento de negócio:
# - Custo unitário BRL = Custo USD × Câmbio do dia
# - Custo total BRL = Custo unitário BRL × Quantidade vendida
# - Resultado = Receita (total) - Custo total BRL
# - Prejuízo = |Resultado| apenas quando Resultado < 0

print("\n" + "=" * 80)
print("ETAPA 4: CÁLCULOS FINANCEIROS")
print("=" * 80)

# Custo unitário em BRL (conversão pelo câmbio do dia da venda)
df_final['custo_unit_brl'] = df_final['usd_price'] * df_final['cotacaoVenda']

# Custo TOTAL em BRL (considera a quantidade vendida)
df_final['custo_total_brl'] = df_final['custo_unit_brl'] * df_final['qtd']

# Resultado da transação (positivo = lucro, negativo = prejuízo)
df_final['resultado'] = df_final['total'] - df_final['custo_total_brl']

# Prejuízo: isola apenas os valores negativos (em módulo)
# .clip(lower=0) zera os positivos e mantém os negativos já invertidos
df_final['prejuizo'] = (-df_final['resultado']).clip(lower=0)

# Flag para identificar transações com prejuízo
df_final['teve_prejuizo'] = df_final['resultado'] < 0

print("✓ Cálculos realizados com sucesso")


# ==============================================================================
# 5. RESPOSTAS - PARTE 1: CÁLCULO E MODELAGEM
# ==============================================================================

print("\n")
print("█" * 80)
print("█" + " " * 30 + "RESULTADOS" + " " * 30 + "█")
print("█" * 80)

# --- 5.1 Custo total em BRL por transação ---
print("\n" + "=" * 80)
print("PARTE 1.1: CUSTO TOTAL EM BRL POR TRANSAÇÃO")
print("=" * 80)

# Seleciona colunas relevantes para visualização
colunas_transacao = [
    'id', 'id_product', 'product_name', 'sale_date', 'qtd', 
    'total', 'usd_price', 'cotacaoVenda', 'custo_total_brl', 'resultado'
]

print("\nAmostra das transações com custos calculados (10 primeiras):")
print(df_final[colunas_transacao].head(10).to_string(index=False))


# --- 5.2 Transações com prejuízo ---
print("\n" + "=" * 80)
print("PARTE 1.2: TRANSAÇÕES COM PREJUÍZO IDENTIFICADAS")
print("=" * 80)

# Filtra apenas transações deficitárias
df_prejuizo_transacoes = df_final[df_final['teve_prejuizo'] == True].copy()

total_transacoes = len(df_final)
transacoes_prejuizo = len(df_prejuizo_transacoes)
percentual_transacoes = (transacoes_prejuizo / total_transacoes) * 100

print(f"\n📊 RESUMO:")
print(f"   Total de transações analisadas: {total_transacoes}")
print(f"   Transações com prejuízo: {transacoes_prejuizo}")
print(f"   Percentual de transações deficitárias: {percentual_transacoes:.2f}%")

print(f"\n   Prejuízo total (todas transações): R$ {df_prejuizo_transacoes['prejuizo'].sum():,.2f}")

print("\nTransações com prejuízo (ordenadas por prejuízo decrescente):")
colunas_prejuizo = [
    'id', 'id_product', 'product_name', 'sale_date', 
    'total', 'custo_total_brl', 'prejuizo'
]
print(df_prejuizo_transacoes[colunas_prejuizo].sort_values('prejuizo', ascending=False).head(15).to_string(index=False))


# --- 5.3 Agregação por id_produto ---
print("\n" + "=" * 80)
print("PARTE 1.3: AGREGAÇÃO POR PRODUTO")
print("=" * 80)

# Agrupa por produto: soma receita de TODAS vendas, soma prejuízo das deficitárias
df_agregado = df_final.groupby('id_product').agg(
    receita_total=('total', 'sum'),              # Soma de todas as vendas
    prejuizo_total=('prejuizo', 'sum'),          # Soma dos prejuízos (lucros são 0)
    qtd_transacoes=('id', 'count'),              # Quantidade de transações
    qtd_com_prejuizo=('teve_prejuizo', 'sum')    # Quantas tiveram prejuízo
).reset_index()

# Calcula percentual de perda
df_agregado['percentual_perda'] = (
    df_agregado['prejuizo_total'] / df_agregado['receita_total']
) * 100

# Enriquece com nome do produto (pega o primeiro nome encontrado)
nomes_produtos = df_final.groupby('id_product')['product_name'].first().reset_index()
df_agregado = df_agregado.merge(nomes_produtos, on='id_product', how='left')

# Reorganiza colunas para melhor visualização
df_agregado = df_agregado[[
    'id_product', 'product_name', 'qtd_transacoes', 'qtd_com_prejuizo',
    'receita_total', 'prejuizo_total', 'percentual_perda'
]]

print("\nTODOS OS PRODUTOS (ordenados por prejuízo total):")
print(df_agregado.sort_values('prejuizo_total', ascending=False).to_string(index=False))


# --- 5.4 Produtos com prejuízo (para gráfico) ---
print("\n" + "-" * 80)
print("PRODUTOS QUE TIVERAM PREJUÍZO (filtro para gráfico):")
print("-" * 80)

df_produtos_prejuizo = df_agregado[df_agregado['prejuizo_total'] > 0].copy()
df_produtos_prejuizo = df_produtos_prejuizo.sort_values('prejuizo_total', ascending=False)

print(f"\nTotal de produtos com prejuízo: {len(df_produtos_prejuizo)} de {len(df_agregado)}")
print(df_produtos_prejuizo.to_string(index=False))


# ==============================================================================
# 6. RESPOSTAS - PARTE 3: EXPLICAÇÕES SOBRE O DESENVOLVIMENTO
# ==============================================================================

print("\n")
print("█" * 80)
print("█" + " " * 20 + "PARTE 3: EXPLICAÇÕES TÉCNICAS" + " " * 20 + "█")
print("█" * 80)

print("""
┌──────────────────────────────────────────────────────────────────────────────┐
│ QUAL DATA DE CÂMBIO VOCÊ UTILIZOU?                                           │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ Utilizei a cotação de VENDA do dólar (PTAX) do dia da venda, conforme        │
│ disponibilizada pela API Olinda do Banco Central do Brasil.                  │
│                                                                              │
│ Para dias sem cotação (finais de semana e feriados), foi utilizada a         │
│ última cotação disponível anterior à data da venda, através da técnica       │
│ de merge_asof com direction='backward'.                                      │
│                                                                              │
│ Fonte: https://olinda.bcb.gov.br/olinda/servico/PTAX                        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ COMO DEFINIU O PREJUÍZO?                                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ O prejuízo foi definido como a diferença negativa entre a RECEITA da         │
│ venda e o CUSTO TOTAL convertido em BRL.                                     │
│                                                                              │
│ Fórmulas utilizadas:                                                         │
│                                                                              │
│   Custo Total BRL = Custo Unitário USD × Quantidade × Câmbio do Dia         │
│                                                                              │
│   Resultado = Receita (total da venda) - Custo Total BRL                     │
│                                                                              │
│   Prejuízo = |Resultado| quando Resultado < 0                               │
│            = 0 quando Resultado >= 0                                         │
│                                                                              │
│ Uma transação é considerada deficitária quando o custo total em BRL          │
│ (calculado com o câmbio do dia) supera a receita obtida na venda.           │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ ALGUMA SUPOSIÇÃO RELEVANTE?                                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ 1. CUSTO VIGENTE POR PERÍODO:                                               │
│    O custo unitário em USD utilizado é o da última importação realizada      │
│    ANTES ou NA data da venda. Isso simula o conceito de estoque onde o      │
│    produto vendido hoje foi adquirido na última reposição.                  │
│                                                                              │
│ 2. IMPOSTOS E FRETE IGNORADOS:                                              │
│    Conforme premissa do problema, não foram considerados impostos,          │
│    frete ou outras taxas no cálculo do custo.                               │
│                                                                              │
│ 3. RECEITA TOTAL:                                                           │
│    O campo 'total' em vendas já representa a receita líquida da             │
│    transação (valor unitário × quantidade), não sendo necessário            │
│    recalculá-lo.                                                            │
│                                                                              │
│ 4. CÂMBIO EM DIAS NÃO ÚTEIS:                                                │
│    Para vendas em finais de semana ou feriados, utilizou-se a cotação       │
│    do último dia útil anterior, assumindo que operações financeiras         │
│    seriam liquidadas com base nessa referência.                             │
│                                                                              │
│ 5. INTEGRIDADE DOS DADOS:                                                   │
│    Assumiu-se que todo produto vendido possui ao menos uma importação       │
│    anterior registrada no sistema.                                          │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
""")


# ==============================================================================
# 7. EXPORTAÇÃO DOS RESULTADOS
# ==============================================================================

print("\n" + "=" * 80)
print("EXPORTAÇÃO DOS RESULTADOS")
print("=" * 80)

# Salva DataFrame detalhado (todas as transações com cálculos)
df_final.to_csv('datasets/resultado_transacoes.csv', index=False, encoding='utf-8')
print("✓ resultado_transacoes.csv - Todas as transações com cálculos")

# Salva DataFrame agregado por produto
df_agregado.to_csv('datasets/resultado_agregado_produtos.csv', index=False, encoding='utf-8')
print("✓ resultado_agregado_produtos.csv - Agregação por produto")

# Salva apenas produtos com prejuízo (para gráfico)
df_produtos_prejuizo.to_csv('datasets/produtos_com_prejuizo.csv', index=False, encoding='utf-8')
print("✓ produtos_com_prejuizo.csv - Apenas produtos deficitários")

print("\n" + "=" * 80)
print("PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
print("=" * 80)

# Validação manual - transação id=42, produto 6
teste = df_final[df_final['id'] == 42]
print("\n[VALIDAÇÃO MANUAL - ID 42]")
print(teste[['id_product', 'qtd', 'total', 'usd_price', 'cotacaoVenda', 'custo_total_brl', 'resultado', 'prejuizo']].to_string(index=False))

# ==============================================================================
# AUDITORIA MANUAL - TOP 5 PRODUTOS PROBLEMÁTICOS
# ==============================================================================

print("\n")
print("█" * 80)
print("█" + " " * 25 + "AUDITORIA MANUAL" + " " * 25 + "█")
print("█" * 80)

# Lista dos produtos mais problemáticos
produtos_auditoria = [72, 83, 74, 71, 55]

for prod_id in produtos_auditoria:
    print(f"\n{'='*80}")
    print(f"PRODUTO #{prod_id}")
    print(f"{'='*80}")
    
    # --- 1. Histórico de importações (custos) ---
    print(f"\n📦 HISTÓRICO DE IMPORTAÇÕES (custos_importacao):")
    custos_prod = custos[custos['product_id'] == prod_id].sort_values('start_date')
    print(custos_prod[['product_id', 'product_name', 'start_date', 'usd_price']].to_string(index=False))
    
    # --- 2. Amostra de vendas deste produto ---
    print(f"\n💰 AMOSTRA DE VENDAS (5 transações):")
    vendas_prod = df_final[df_final['id_product'] == prod_id].sort_values('sale_date')
    colunas_auditoria = [
        'id', 'sale_date', 'qtd', 'total', 
        'start_date', 'usd_price', 'cotacaoVenda', 
        'custo_total_brl', 'resultado', 'prejuizo'
    ]
    print(vendas_prod[colunas_auditoria].head(5).to_string(index=False))
    
    # --- 3. Verificação: Custo unitário vs Preço unitário ---
    print(f"\n📊 ANÁLISE DE MARGEM:")
    amostra = vendas_prod.head(1).iloc[0]
    preco_unit_venda = amostra['total'] / amostra['qtd']
    custo_unit_brl = amostra['custo_unit_brl']
    
    print(f"   Preço unitário de venda: R$ {preco_unit_venda:,.2f}")
    print(f"   Custo unitário em BRL:   R$ {custo_unit_brl:,.2f}")
    print(f"   Custo USD:               $ {amostra['usd_price']:,.2f}")
    print(f"   Câmbio utilizado:        {amostra['cotacaoVenda']:.4f}")
    print(f"   Margem unitária:         R$ {preco_unit_venda - custo_unit_brl:,.2f}")
    print(f"   Margem %:                {((preco_unit_venda - custo_unit_brl) / custo_unit_brl) * 100:.2f}%")

print("\n" + "="*80)
print("FIM DA AUDITORIA")
print("="*80)