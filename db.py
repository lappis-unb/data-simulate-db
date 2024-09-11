import psycopg2
import json
import random
from faker import Faker


def load_config():
    """Carrega as configurações do arquivo config.json"""
    with open("config.json") as config_file:
        config = json.load(config_file)
    return config


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
    """Cria uma tabela no banco de dados com o nome fornecido"""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        sender_id BIGINT NOT NULL,
        event_type VARCHAR(50),
        timestamp BIGINT,
        action_name VARCHAR(255)
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


def insert_simulated_data(batch_size, total_batches, table_name):
    """Popula a tabela com dados simulados"""
    fake = Faker()
    conn = get_db_connection()
    if not conn:
        print("Falha ao conectar ao banco de dados.")
        return

    try:
        cursor = conn.cursor()

        # Atualizamos a consulta para incluir o nome da tabela dinamicamente
        insert_query = f"""
            INSERT INTO {table_name} (sender_id, event_type, timestamp, action_name)
            VALUES (%s, %s, %s, %s)
        """

        for batch in range(total_batches):
            data = []
            for _ in range(batch_size):
                sender_id = random.randint(1000000000, 9999999999)
                event_type = "action"
                timestamp = fake.unix_time()
                action_name = "action_session_start"
                data.append((sender_id, event_type, timestamp, action_name))

            # Execute o batch inserindo as linhas de uma vez só
            cursor.executemany(insert_query, data)
            conn.commit()
            print(f"Lote {batch + 1} de {total_batches} inserido.")

        cursor.close()
        print("Inserção de dados simulados concluída.")
    except Exception as e:
        print(f"Erro ao inserir dados simulados na tabela '{table_name}': {e}")
    finally:
        conn.close()
