import psycopg2
import json
import random
from concurrent.futures import ThreadPoolExecutor
from faker import Faker


def load_config():
    """Carrega as configurações do arquivo config.json"""
    with open("config.json") as config_file:
        config = json.load(config_file)
    return config


def load_data():
    """Carrega os possíveis valores do arquivo data.json"""
    with open("data.json") as f:
        data = json.load(f)
    return data


def get_db_connection():
    """Estabelece uma conexão com o banco de dados e retorna o objeto de conexão"""
    config = load_config()
    db_config = config["db_config"]

    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


def create_table(table_name):
    """Cria uma tabela no banco de dados com colunas baseadas no JSON"""
    possible_values = load_data()

    # Monta a definição das colunas
    columns_definitions = []
    for column_name, column_info in possible_values.items():
        column_type = column_info["type"]
        columns_definitions.append(f"{column_name} {column_type}")

    # Adiciona a coluna 'id' como chave primária
    columns_sql = ",\n    ".join(["id SERIAL PRIMARY KEY"] + columns_definitions)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            conn.commit()
            cursor.close()
            print(f"Tabela '{table_name}' criada com sucesso.")
        except Exception as e:
            print(f"Erro ao criar a tabela '{table_name}': {e}")
        finally:
            conn.close()
    else:
        print(
            f"Falha ao obter conexão com o banco de dados para criar a tabela '{table_name}'."
        )


def delete_table(table_name):
    """Remove a tabela especificada do banco de dados"""
    delete_table_query = f"DROP TABLE IF EXISTS {table_name};"

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(delete_table_query)
            conn.commit()
            cursor.close()
            print(f"Tabela '{table_name}' removida com sucesso.")
        except Exception as e:
            print(f"Erro ao remover a tabela '{table_name}': {e}")
        finally:
            conn.close()
    else:
        print(
            f"Falha ao conectar ao banco de dados para remover a tabela '{table_name}'."
        )


def clear_table(table_name):
    """Remove todos os dados da tabela especificada sem apagar a estrutura"""
    clear_table_query = f"DELETE FROM {table_name};"

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(clear_table_query)
            conn.commit()
            cursor.close()
            print(f"Todos os dados da tabela '{table_name}' foram apagados.")
        except Exception as e:
            print(f"Erro ao apagar dados da tabela '{table_name}': {e}")
        finally:
            conn.close()
    else:
        print(
            f"Falha ao conectar ao banco de dados para apagar os dados da tabela '{table_name}'."
        )


def list_tables():
    """Lista todas as tabelas no esquema 'public' do banco de dados"""
    list_tables_query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(list_tables_query)
            tables = cursor.fetchall()
            print("Tabelas disponíveis no banco de dados:")
            for table in tables:
                print(f"- {table[0]}")
            cursor.close()
        except Exception as e:
            print(f"Erro ao listar as tabelas: {e}")
        finally:
            conn.close()
    else:
        print("Falha ao conectar ao banco de dados para listar as tabelas.")


def count_rows_in_table(table_name):
    """Conta o número de registros em uma tabela específica"""
    count_rows_query = f"SELECT COUNT(*) FROM {table_name};"

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(count_rows_query)
            count = cursor.fetchone()[0]
            print(f"Tabela '{table_name}' contém {count} registros.")
            cursor.close()
        except Exception as e:
            print(f"Erro ao contar os dados na tabela '{table_name}': {e}")
        finally:
            conn.close()
    else:
        print(
            f"Falha ao conectar ao banco de dados para contar dados na tabela '{table_name}'."
        )


def show_table_data(table_name, limit=10):
    """Exibe os dados da tabela especificada"""
    select_query = f"SELECT * FROM {table_name} LIMIT {limit};"

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(select_query)
            rows = cursor.fetchall()
            print(f"Exibindo os primeiros {limit} registros da tabela '{table_name}':")
            for row in rows:
                print(row)
            cursor.close()
        except Exception as e:
            print(f"Erro ao exibir os dados da tabela '{table_name}': {e}")
        finally:
            conn.close()
    else:
        print(
            f"Falha ao conectar ao banco de dados para exibir os dados da tabela '{table_name}'."
        )


def insert_batch(batch_num, batch_size, table_name, possible_values):
    """Insere um lote de dados na tabela usando uma conexão separada por thread"""
    conn = get_db_connection()
    if not conn:
        print("Falha ao conectar ao banco de dados.")
        return
    try:
        cursor = conn.cursor()
        columns = list(possible_values.keys())
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = (
            f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        )

        data = []
        for _ in range(batch_size):
            row = []
            for col in columns:
                value = random.choice(possible_values[col]["values"])
                row.append(value)
            data.append(tuple(row))

        cursor.executemany(insert_query, data)
        conn.commit()
        cursor.close()
        print(f"Lote {batch_num} inserido.")
    except Exception as e:
        print(f"Erro ao inserir dados no lote {batch_num}: {e}")
    finally:
        conn.close()


def insert_simulated_data(batch_size, total_batches, table_name):
    """Popula a tabela com dados simulados usando threads, executando 8 batches por vez"""
    possible_values = load_data()
    max_workers = 6  # Número máximo de threads simultâneas

    batches = list(range(1, total_batches + 1))
    # Divide os batches em grupos de 8
    batch_groups = [
        batches[i : i + max_workers] for i in range(0, len(batches), max_workers)
    ]

    for group_num, batch_group in enumerate(batch_groups, 1):
        print(f"Iniciando grupo {group_num} com batches: {batch_group}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for batch_num in batch_group:
                future = executor.submit(
                    insert_batch,
                    batch_num,
                    batch_size,
                    table_name,
                    possible_values,
                )
                futures.append(future)

            # Espera todos os futures do grupo serem concluídos
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Erro no lote {batch_num}: {e}")
        print(f"Grupo {group_num} concluído.")
    print("Inserção de dados simulados concluída.")
