import pandas as pd
from sqlalchemy import create_engine
import os

# Configurações de conexão baseadas no seu docker-compose
DB_USER = 'indicium'
DB_PASS = 'indicium'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'lighthouse'

# Cria a string de conexão (Database URL)
engine_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(engine_url)

def carregar_arquivo_para_postgres(nome_arquivo, nome_tabela):
    try:
        print(f"Lendo o arquivo {nome_arquivo}...")
        
        # Identifica a extensão do arquivo para escolher a função de leitura correta
        _, extensao = os.path.splitext(nome_arquivo)
        
        if extensao.lower() == '.csv':
            # Lê arquivos CSV
            df = pd.read_csv(nome_arquivo)
        elif extensao.lower() == '.json':
            # Lê arquivos JSON
            df = pd.read_json(nome_arquivo)
        else:
            print(f"Erro: O formato '{extensao}' do arquivo {nome_arquivo} não é suportado por este script.\n")
            return
        
        print(f"Carregando dados na tabela '{nome_tabela}'...")
        
        # Envia os dados para o PostgreSQL
        # if_exists='replace' recria a tabela se ela já existir.
        df.to_sql(nome_tabela, engine, if_exists='replace', index=False)
        
        print(f"Sucesso! Tabela '{nome_tabela}' carregada.\n")
        
    except Exception as e:
        print(f"Erro ao processar {nome_arquivo}: {e}\n")

if __name__ == "__main__":
    # Carrega o arquivo JSON (criará a tabela 'clientes_crm')
    carregar_arquivo_para_postgres('datasets/clientes_crm.json', 'clientes_crm')
    
    # Carrega o arquivo CSV (criará a tabela 'produtos_refined')
    carregar_arquivo_para_postgres('datasets/produtos_refined.csv', 'produtos_refined')
    
    # Carrega o primeiro CSV
    #carregar_csv_para_postgres('datasets/cambio.csv', 'cambio')
    
    # Carrega o segundo CSV
    #carregar_csv_para_postgres('datasets/custos_importacao.csv', 'custos_importacao')
    
    print("Processo finalizado!")