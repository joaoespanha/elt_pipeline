
# Pipeline de ELT com Airflow, PostgreSQL e GCP BigQuery

Este projeto configura um ambiente Apache Airflow, juntamente com dois bancos de dados PostgreSQL, usando Docker Compose. O objetivo principal deste projeto é extrair dados de pedidos de arquivos Parquet não processados, carregá-los como dados brutos em um banco de dados PostgreSQL, transformá-los e, finalmente, carregá-los no Google BigQuery.

## Estrutura do Projeto

A estrutura do projeto é organizada da seguinte forma:

```
ELT_PIPELINE/
│
├── airflow/
│
├── elt/
│   ├── data/
│   ├── model/
│   │   ├── google_connections/
│   │   │   ├── bigquery.py
│   │   │   └── storage_client.py
│   │   └── pg_connections/
│   ├── services/
│   │   ├── transformations/
│   │   │   └── main.py
│   │   ├── get_order_proof_data.py
│   │   └── utils.py
│   └── tests/
│       └── services/
│           └── transformations/
│               └── test_get_order_proof_data.py
│
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
└── README.md
```

### Descrição dos Diretórios Principais

- **airflow/**: Contém os DAGs do Airflow e arquivos relacionados.
- **elt/**: Diretório principal para o código de ETL/ELT.
  - **data/**: Diretório para armazenar dados locais ou intermediários.
  - **model/**: Contém os módulos de conexão e clientes para Google BigQuery e PostgreSQL.
    - **google_connections/**: Scripts para conexão com o Google BigQuery e Google Cloud Storage.
    - **pg_connections/**: Scripts para conexão com o PostgreSQL.
  - **services/**: Scripts principais que realizam transformações e operações de ETL.
    - **transformations/**: Contém scripts que transformam os dados brutos antes de carregá-los no BigQuery.
  - **tests/**: Testes unitários e de integração para garantir a qualidade do código.

## Serviços Docker Compose

### `destination_postgres`
- **Image**: `postgres:latest`
- **Ports**: 
  - Mapeia a porta `5434` no host para `5432` dentro do contêiner.
- **Networks**: 
  - Conectado à rede `elt_network`.
- **Variáveis de Ambiente**:
  - `POSTGRES_DB`: Nome do banco de dados (`destination_db`).
  - `POSTGRES_USER`: Usuário do banco de dados (`postgres`).
  - `POSTGRES_PASSWORD`: Senha do banco de dados (`secret`).
- **Volumes**:
  - Persistência dos dados no volume `destination_db_data` mapeado para `/var/lib/postgresql/data` no contêiner.

### `postgres`
- **Image**: `postgres:latest`
- **Networks**: 
  - Conectado à rede `elt_network`.
- **Variáveis de Ambiente**:
  - `POSTGRES_USER`: Usuário do banco de dados (`airflow`).
  - `POSTGRES_PASSWORD`: Senha do banco de dados (`airflow`).
  - `POSTGRES_DB`: Nome do banco de dados (`airflow`).

### `init-airflow`
- **Image**: `apache/airflow:latest`
- **Depends on**: `postgres`
- **Networks**: 
  - Conectado à rede `elt_network`.
- **Variáveis de Ambiente**:
  - `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: String de conexão do banco de dados para o Airflow.
- **Comando**:
  - Inicializa o banco de dados do Airflow e cria um usuário administrador com as seguintes credenciais:
    - Usuário: `airflow`
    - Senha: `password`
    - Email: `admin@example.com`

### `webserver`
- **Build**:
  - Usa o `Dockerfile` no contexto atual.
- **User**: `root`
- **Depends on**: `postgres`
- **Networks**: 
  - Conectado à rede `elt_network`.
- **Extra Hosts**:
  - Adiciona `host.docker.internal` como `host-gateway` para resolver nomes de host locais dentro do contêiner.
- **Volumes**:
  - Monta os seguintes volumes:
    - `./airflow/dags` para `/opt/airflow/dags`.
    - `./elt` para `/opt/airflow/elt`.
    - Socket do Docker para cenários Docker-in-Docker.
    - Arquivo de credenciais do serviço Google Cloud.
    - Arquivo de requisitos Python.
- **Variáveis de Ambiente**:
  - Configurações diversas de conexão do Airflow e PostgreSQL.
  - Credenciais do Google Cloud.
- **Ports**:
  - Exposição da porta `8080` para a interface web do Airflow.
- **Comando**:
  - Inicia o servidor web do Airflow.

### `scheduler`
- **Build**:
  - Usa o `Dockerfile` no contexto atual.
- **User**: `root`
- **Depends on**: `postgres`
- **Networks**: 
  - Conectado à rede `elt_network`.
- **Extra Hosts**:
  - Adiciona `host.docker.internal` como `host-gateway` para resolver nomes de host locais dentro do contêiner.
- **Volumes**:
  - Monta os seguintes volumes:
    - `./airflow/dags` para `/opt/airflow/dags`.
    - `./elt` para `/opt/airflow/elt`.
    - Socket do Docker para cenários Docker-in-Docker.
    - Arquivo de credenciais do serviço Google Cloud.
    - Arquivo de requisitos Python.
- **Variáveis de Ambiente**:
  - Configurações diversas de conexão do Airflow e PostgreSQL.
  - Credenciais do Google Cloud.
- **Comando**:
  - Inicia o agendador do Airflow.

## Configuração do Google Cloud Service Account

Para permitir que o Airflow se conecte ao Google BigQuery e Google Cloud Storage, você precisará criar uma conta de serviço no Google Cloud e obter um arquivo de credenciais JSON. Siga os passos abaixo:

1. Acesse o [Console do Google Cloud](https://console.cloud.google.com/).
2. Navegue até **IAM & Admin > Service Accounts**.
3. Crie uma nova conta de serviço com as permissões necessárias para acessar o BigQuery e o Google Cloud Storage.
4. Gere e faça o download do arquivo de credenciais JSON.
5. Coloque o arquivo de credenciais no caminho definido no Docker Compose (ex: `~/bigquery_gcp_key.json`).

## Configuração do Ambiente

O projeto usa arquivos `.env` para armazenar credenciais sensíveis e variáveis de ambiente. Siga os passos abaixo para configurar:

1. Renomeie o arquivo `.env.example` para `.env`.
2. Edite o arquivo `.env` e ajuste as variáveis de ambiente conforme necessário, como as credenciais do PostgreSQL e Google Cloud.
3. Certifique-se de que as variáveis de ambiente no `docker-compose.yml` estejam adequadamente configuradas para corresponder ao seu ambiente.

## Iniciando o Projeto

1. Clone o repositório.
2. Certifique-se de que você tenha Docker e Docker Compose instalados.
3. Execute o seguinte comando para iniciar o processo de inicialização do Airflow:
   ```bash
   docker-compose up init-airflow --build
   ```
4. Em seguida, inicie todos os serviços com o comando:
   ```bash
   docker-compose up --build
   ```
5. Acesse a interface web do Airflow em `http://localhost:8080` com as seguintes credenciais:
   - Usuário: `airflow`
   - Senha: `password`

## Notas

- Lembre-se de ajustar as configurações de acordo com o seu ambiente específico.
- O banco de dados do Airflow será inicializado automaticamente, e o usuário admin será criado com as credenciais fornecidas.

