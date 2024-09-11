import sys
from db import (
    create_table,
    insert_simulated_data,
    clear_table,
    delete_table,
    list_tables,
    count_rows_in_table,
)
from performance import measure_performance
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
    print("8. Sair")


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
        measure_performance(table_name)

    elif option == 6:
        list_tables()

    elif option == 7:
        table_name = input("Informe o nome da tabela para contar os registros: ")
        count_rows_in_table(table_name)

    elif option == 8:
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
