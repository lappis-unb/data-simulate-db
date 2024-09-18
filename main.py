import sys
import asyncio
from performance import (
    measure_performance_pandas,
    measure_performance_psycopg2,
    measure_performance_asyncpg,
    measure_performance_pyspark,
)
from db import (
    create_table,
    insert_simulated_data,
    clear_table,
    delete_table,
    list_tables,
    count_rows_in_table,
    show_table_data,
)
from db import load_config


def print_menu():
    """Imprime o menu de opções"""
    print("\nMenu de Opções:")
    print("1. Criar uma tabela")
    print("2. Inserir dados simulados em uma tabela")
    print("3. Apagar todos os dados de uma tabela")
    print("4. Remover uma tabela completamente")
    print("5. Medir a performance de uma tabela")
    print("6. Listar todas as tabelas disponíveis")
    print("7. Contar registros em uma tabela específica")
    print("8. Mostrar os primeiros registros de uma tabela")
    print("9. Sair")


def select_tool():
    """Mostra o submenu de seleção de ferramenta"""
    print("\nEscolha a ferramenta para medir performance:")
    print("1. pandas (SQLAlchemy)")
    print("2. psycopg2")
    print("3. asyncpg")
    print("4. PySpark")
    tool_choice = input("Escolha uma ferramenta: ")
    return tool_choice


def handle_option(option):
    """Executa a ação com base na escolha do usuário"""
    if option == 1:
        table_name = input("Informe o nome da tabela que deseja criar: ")
        create_table(table_name)

    elif option == 2:
        table_name = input(
            "Informe o nome da tabela onde deseja inserir os dados simulados: "
        )
        config = load_config()
        batch_size = config["simulation_config"]["batch_size"]
        total_batches = config["simulation_config"]["total_batches"]
        insert_simulated_data(batch_size, total_batches, table_name)

    elif option == 3:
        table_name = input("Informe o nome da tabela que deseja limpar: ")
        confirm = input(
            f"Tem certeza que deseja apagar todos os dados da tabela '{table_name}'? (s/n): "
        ).lower()
        if confirm == "s":
            clear_table(table_name)

    elif option == 4:
        table_name = input("Informe o nome da tabela que deseja remover: ")
        confirm = input(
            f"Tem certeza que deseja remover a tabela '{table_name}' completamente? (s/n): "
        ).lower()
        if confirm == "s":
            delete_table(table_name)

    elif option == 5:
        table_name = input("Informe o nome da tabela que deseja medir a performance: ")
        tool_choice = select_tool()

        if tool_choice == "1":
            measure_performance_pandas(table_name)
        elif tool_choice == "2":
            measure_performance_psycopg2(table_name)
        elif tool_choice == "3":
            asyncio.run(measure_performance_asyncpg(table_name))
        elif tool_choice == "4":
            measure_performance_pyspark(table_name)
        else:
            print("Ferramenta inválida!")

    elif option == 6:
        list_tables()

    elif option == 7:
        table_name = input("Informe o nome da tabela para contar os registros: ")
        count_rows_in_table(table_name)

    elif option == 8:
        table_name = input("Informe o nome da tabela para exibir os registros: ")
        limit = int(input("Informe o número de registros que deseja exibir: "))
        show_table_data(table_name, limit)

    elif option == 9:
        print("Saindo...")
        sys.exit(0)

    else:
        print("Opção inválida. Por favor, tente novamente.")


def main():
    """Executa o menu interativo"""
    while True:
        print_menu()
        try:
            option = int(input("Escolha uma opção: "))
            handle_option(option)
        except ValueError:
            print("Entrada inválida. Por favor, insira um número.")


if __name__ == "__main__":
    main()
