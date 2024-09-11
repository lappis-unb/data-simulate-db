import psutil
import time
import pandas as pd
from db import load_config
from sqlalchemy import create_engine


def get_sqlalchemy_engine():
    """Cria uma engine SQLAlchemy usando as configurações do banco de dados"""
    config = load_config()
    db_config = config["db_config"]

    # Constrói a URI do banco de dados para SQLAlchemy
    db_uri = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    engine = create_engine(db_uri)
    print(f"Conectado ao banco de dados: {db_config['dbname']}")
    return engine


def measure_performance(table_name):
    """Mede o tempo de execução e memória utilizada ao abrir e fechar a tabela"""
    config = load_config()
    query_limit = config["analysis_config"]["query_limit"]

    start_time = time.time()
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024**2  # Memória em MB

    # Usar SQLAlchemy para conectar ao banco de dados
    engine = get_sqlalchemy_engine()

    try:
        query = f"SELECT * FROM {table_name} LIMIT {query_limit}"
        # Usando a engine do SQLAlchemy no pandas
        df = pd.read_sql(query, engine)
        print(f"Carregados {len(df)} registros")
    except Exception as e:
        print(f"Erro ao carregar dados: {e}")

    mem_after = process.memory_info().rss / 1024**2
    end_time = time.time()

    print(f"Tempo de execução: {end_time - start_time} segundos")
    print(f"Memória utilizada: {mem_after - mem_before} MB")
