# Projeto de Simulação de Inserção de Dados e Medição de Performance

Este projeto permite a inserção de dados simulados em um banco de dados PostgreSQL, além de fornecer funcionalidades para medir a performance de leitura de dados e realizar outras operações, como limpar, remover e contar registros em tabelas específicas.

## Requisitos

Antes de começar, você precisa ter as seguintes ferramentas instaladas:

- **Python 3.7+**
- **Docker-compose**

  Você precisa iniciar o banco de dados:
  ```
  docker-compose up
  ```
- As dependências do projeto listadas no arquivo `requirements.txt`:

  **AVISO**: Recomendado criar um ambiente virtual antes de instalar as dependencias:
  ```
  python3 -m venv .venv
  source .venv/bin/activate
  ```
  ```bash
  pip install -r requirements.txt
  ```
  

## Configuração

### 1. Configuração do arquivo `config.json`

Você deve configurar o arquivo `config.json` para definir os parâmetros de conexão ao banco de dados e as configurações da simulação.

Aqui está um exemplo do arquivo `config.json`:

```json
{
  "db_config": {
    "dbname": "rasa",
    "user": "rasa",
    "password": "rasa",
    "host": "172.26.0.2",
    "port": "5432"
  },
  "simulation_config": {
    "batch_size": 1000,
    "total_batches": 10
  },
  "analysis_config": {
    "query_limit": 100000
  }
}
```

#### Parâmetros:

- **`db_config`**: Configurações do banco de dados PostgreSQL.
  - `dbname`: Nome do banco de dados.
  - `user`: Nome do usuário do banco de dados.
  - `password`: Senha do banco de dados.
  - `host`: Endereço do servidor onde o banco de dados está rodando.
  
    **AVISO**: caso esteja usando docker, para descobrir seu host, basta executar o comando:
    ```
    docker inspect <container id> | grep "IPAddress"
    ```

    a saida deve ser algo como:

    ```
    > docker inspect d57f6ed76c76 | grep "IPAddress"
                "SecondaryIPAddresses": null,
                "IPAddress": "",
                        "IPAddress": "172.26.0.2",
    ```

    nesse caso o host é `172.26.0.2`
  - `port`: Porta do PostgreSQL (geralmente `5432`).

- **`simulation_config`**: Configurações para simulação de inserção de dados.
  - `batch_size`: Número de linhas que serão inseridas em cada transação (lote).
  - `total_batches`: Número de vezes que os lotes serão inseridos no banco.

- **`analysis_config`**: Configurações para medir a performance de consultas.
  - `query_limit`: Limite de linhas a serem lidas ao medir a performance com o Pandas.

### 2. Configuração do Banco de Dados

Certifique-se de que o PostgreSQL esteja rodando e de que o banco de dados esteja acessível com as credenciais fornecidas no arquivo `config.json`.

Você também deve criar um banco de dados com o nome especificado em `dbname`. No terminal, execute:

```bash
psql -U <seu_usuario> -c "CREATE DATABASE rasa;"
```

Substitua `<seu_usuario>` pelo nome do seu usuário PostgreSQL.

## Como Executar

### 1. Executando o Menu Interativo

Para iniciar o menu interativo, basta rodar o seguinte comando:

```bash
python main.py
```

O menu interativo permitirá que você escolha entre várias operações:

```plaintext
Menu de Opções:
1. Criar uma tabela
2. Inserir dados simulados em uma tabela
3. Apagar todos os dados de uma tabela
4. Remover uma tabela completamente
5. Medir a performance de uma tabela
6. Listar todas as tabelas disponíveis
7. Contar registros em uma tabela específica
8. Sair
```

### 2. Explicação das Opções

#### 1. Criar uma tabela
Cria uma nova tabela com o nome especificado. Você será solicitado a informar o nome da tabela.

#### 2. Inserir dados simulados em uma tabela
Insere dados simulados na tabela especificada com base nas configurações de `batch_size` e `total_batches` definidas no `config.json`.

#### 3. Apagar todos os dados de uma tabela
Remove todos os registros de uma tabela específica, mas mantém a estrutura da tabela.

#### 4. Remover uma tabela completamente
Apaga uma tabela do banco de dados, removendo tanto os registros quanto a estrutura da tabela.

#### 5. Medir a performance de uma tabela
Realiza uma leitura dos dados da tabela especificada e mede o tempo e a memória utilizados. O número de linhas lidas é controlado pelo parâmetro `query_limit` em `config.json`.

#### 6. Listar todas as tabelas disponíveis
Mostra uma lista de todas as tabelas existentes no banco de dados.

#### 7. Contar registros em uma tabela específica
Conta e exibe o número de registros em uma tabela específica.

#### 8. Sair
Encerra o programa.

## Exemplo de Uso

1. **Criar uma tabela**:
   Escolha a opção 1 e forneça o nome da tabela que deseja criar:
   ```plaintext
   Informe o nome da tabela que deseja criar: my_new_table
   ```

2. **Inserir dados simulados**:
   Escolha a opção 2 e forneça o nome da tabela onde os dados simulados serão inseridos:
   ```plaintext
   Informe o nome da tabela onde deseja inserir os dados simulados: my_new_table
   ```

3. **Medição de Performance**:
   Escolha a opção 5 e forneça o nome da tabela para medir a performance:
   ```plaintext
   Informe o nome da tabela que deseja medir a performance: my_new_table
   ```

## Licença

Este projeto é distribuído sob a licença [MIT](https://opensource.org/licenses/MIT).