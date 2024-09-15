import psutil
import time
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import asyncpg
from db import load_config
from pyspark.sql import SparkSession

def get_sqlalchemy_engine():
    config = load_config()
    db_config = config["db_config"]
    db_uri = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    engine = create_engine(db_uri)
    return engine

def get_psycopg2_connection():
    config = load_config()
    db_config = config["db_config"]
    conn = psycopg2.connect(
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port']
    )
    return conn

async def get_asyncpg_connection():
    config = load_config()
    db_config = config["db_config"]
    conn = await asyncpg.connect(
        user=db_config['user'], password=db_config['password'],
        database=db_config['dbname'], host=db_config['host']
    )
    return conn

def measure_performance_pandas(table_name):
    config = load_config()
    query_limit = config["analysis_config"]["query_limit"]

    start_time = time.time()
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024**2

    engine = get_sqlalchemy_engine()
    try:
        query = f"SELECT * FROM {table_name} LIMIT {query_limit}"
        df = pd.read_sql(query, engine)
        print(f"Carregados {len(df)} registros")
    except Exception as e:
        print(f"Erro ao carregar dados: {e}")

    mem_after = process.memory_info().rss / 1024**2
    end_time = time.time()
    print(f"Tempo de execução: {end_time - start_time} segundos")
    print(f"Memória utilizada: {mem_after - mem_before} MB")

def measure_performance_psycopg2(table_name):
    config = load_config()
    query_limit = config["analysis_config"]["query_limit"]

    start_time = time.time()
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024**2

    conn = get_psycopg2_connection()
    try:
        query = f"SELECT * FROM {table_name} LIMIT {query_limit}"
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            print(f"Carregados {len(rows)} registros")
    except Exception as e:
        print(f"Erro ao carregar dados: {e}")
    finally:
        conn.close()

    mem_after = process.memory_info().rss / 1024**2
    end_time = time.time()
    print(f"Tempo de execução: {end_time - start_time} segundos")
    print(f"Memória utilizada: {mem_after - mem_before} MB")

async def measure_performance_asyncpg(table_name):
    config = load_config()
    query_limit = config["analysis_config"]["query_limit"]

    start_time = time.time()
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024**2

    conn = await get_asyncpg_connection()
    try:
        query = f"SELECT * FROM {table_name} LIMIT {query_limit}"
        rows = await conn.fetch(query)
        print(f"Carregados {len(rows)} registros")
    except Exception as e:
        print(f"Erro ao carregar dados: {e}")
    finally:
        await conn.close()

    mem_after = process.memory_info().rss / 1024**2
    end_time = time.time()
    print(f"Tempo de execução: {end_time - start_time} segundos")
    print(f"Memória utilizada: {mem_after - mem_before} MB")

def measure_performance_pyspark(table_name):
    config = load_config()
    query_limit = config["analysis_config"]["query_limit"]

    start_time = time.time()
    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024**2

    # Inicia a sessão Spark
    spark = SparkSession.builder \
        .appName("Postgres Performance Test") \
        .config("spark.jars", "postgresql-42.7.4.jar") \
        .getOrCreate()

    db_config = config["db_config"]
    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

    try:
        # Executa a query para carregar dados da tabela
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"(SELECT * FROM {table_name} LIMIT {query_limit}) as subquery") \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        print(f"Carregados {df.count()} registros")
    except Exception as e:
        print(f"Erro ao carregar dados: {e}")
    finally:
        spark.stop()

    mem_after = process.memory_info().rss / 1024**2
    end_time = time.time()
    print(f"Tempo de execução: {end_time - start_time} segundos")
    print(f"Memória utilizada: {mem_after - mem_before} MB")