"""
================================================================================
QUESTÃO 6 - CARGA DE DADOS PARA POSTGRESQL
================================================================================
Objetivo: Carregar vendas_2023_2024.csv para tabela no PostgreSQL

Pré-requisitos:
    - PostgreSQL instalado e rodando
    - Banco de dados criado
    - pip install psycopg2-binary pandas sqlalchemy

Uso:
    python q6_carga.py
================================================================================
"""

import pandas as pd
from sqlalchemy import create_engine, text

# ==============================================================================
# CONFIGURAÇÃO DE CONEXÃO
# ==============================================================================
# Ajuste conforme seu ambiente

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'lighthouse',  # Nome do banco (crie antes de rodar)
    'user': 'indicium',        # Ajuste para seu usuário
    'password': 'indicium'     # Ajuste para sua senha
}

# String de conexão SQLAlchemy
CONNECTION_STRING = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)


# ==============================================================================
# CARGA DOS DADOS
# ==============================================================================

def main():
    print("=" * 60)
    print("QUESTÃO 6 - CARGA DE DADOS PARA POSTGRESQL")
    print("=" * 60)
    
    # --- 1. Carregar CSV ---
    print("\n[1/4] Carregando CSV...")
    vendas_2023_2024 = pd.read_csv('datasets/vendas_2023_2024.csv')
    
    # Padronizar data (mesmo tratamento da Q4)
    vendas_2023_2024['sale_date'] = pd.to_datetime(
        vendas_2023_2024['sale_date'], 
        format='mixed', 
        dayfirst=True
    ).dt.date  # Apenas date, sem hora
    
    print(f"      ✓ {len(vendas_2023_2024)} registros carregados")
    print(f"      ✓ Período: {vendas_2023_2024['sale_date'].min()} a {vendas_2023_2024['sale_date'].max()}")
    
    # --- 2. Conectar ao PostgreSQL ---
    print("\n[2/4] Conectando ao PostgreSQL...")
    try:
        engine = create_engine(CONNECTION_STRING)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"      ✓ Conectado em {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    except Exception as e:
        print(f"      ✗ Erro de conexão: {e}")
        print("\n      Verifique:")
        print("        - PostgreSQL está rodando?")
        print("        - Banco 'lighthouse' existe?")
        print("        - Credenciais estão corretas?")
        return
    
    # --- 3. Criar/Recriar tabela de vendas_2023_2024 ---
    print("\n[3/4] Criando tabela 'vendas_2023_2024'...")
    
    # Drop se existir (para permitir re-execução)
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS vendas_2023_2024 CASCADE"))
        conn.commit()
    
    # Inserir dados
    vendas_2023_2024.to_sql(
        name='vendas_2023_2024',
        con=engine,
        if_exists='replace',
        index=False
    )
    print(f"      ✓ Tabela 'vendas_2023_2024' criada com {len(vendas_2023_2024)} registros")
    
    # --- 4. Verificação ---
    print("\n[4/4] Verificando carga...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_registros,
                MIN(sale_date) as data_inicio,
                MAX(sale_date) as data_fim,
                COUNT(DISTINCT sale_date) as dias_com_venda
            FROM vendas_2023_2024
        """))
        row = result.fetchone()
        print(f"      ✓ Total de registros: {row[0]}")
        print(f"      ✓ Período: {row[1]} a {row[2]}")
        print(f"      ✓ Dias com venda: {row[3]}")
    
    print("\n" + "=" * 60)
    print("CARGA CONCLUÍDA COM SUCESSO!")
    print("=" * 60)
    print("\nPróximo passo: Execute as queries em q6_queries.sql")


if __name__ == "__main__":
    main()