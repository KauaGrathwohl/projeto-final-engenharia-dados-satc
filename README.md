# Guia para Criar uma Pipeline de Dados com PostgreSQL, Apache Airflow, ADLS Gen2 e Databricks

Criar uma pipeline de dados utilizando **PostgreSQL**, **Apache Airflow**, **Azure Data Lake Storage Gen2 (ADLS Gen2)** e **Databricks** envolve uma série de etapas desde a preparação do ambiente até a orquestração dos processos de ingestão, transformação e armazenamento dos dados. Abaixo, fornecerei um guia passo a passo para criar uma pipeline de dados com essas ferramentas.

## Etapas para Criar a Pipeline de Dados

### 1. Configuração do Ambiente

Antes de começar, é necessário ter um ambiente de trabalho com os seguintes componentes:

- **PostgreSQL**: Base de dados relacional onde os dados de entrada ou saída podem ser armazenados.
- **Apache Airflow**: Ferramenta para orquestração de workflows.
- **Azure Data Lake Storage Gen2 (ADLS Gen2)**: Serviço de armazenamento de dados em nuvem para armazenar dados brutos ou processados.
- **Databricks**: Plataforma baseada no Apache Spark para processamento e análise de dados.

Certifique-se de que você tem acesso aos seguintes serviços:

- Conta do Azure com acesso ao ADLS Gen2.
- Acesso ao Databricks e ao seu workspace.
- Instalação do Apache Airflow (localmente ou em uma infraestrutura como o Cloud Composer do Google Cloud).
- Configuração de um banco de dados PostgreSQL (pode ser em um servidor local ou na nuvem).

### 2. Criação da Conexão no Airflow

1. **Acessar a interface do Airflow**: Acesse o Airflow via sua interface web, geralmente em `http://localhost:8080/` (ou no seu servidor se estiver rodando em um ambiente diferente).

2. **Configurar as Conexões no Airflow**:
  - **Conexão PostgreSQL**: No Airflow, vá para `Admin > Connections` e crie uma conexão com as credenciais do seu banco de dados PostgreSQL.
  - **Conexão Azure ADLS Gen2**: Crie uma conexão para o Azure Data Lake Storage. Será necessário configurar a autenticação com as credenciais apropriadas (ex.: chave de conta ou autenticação via Azure Active Directory).
  - **Conexão Databricks**: Para a conexão com o Databricks, adicione as credenciais da API REST ou o token de acesso do Databricks.

### 3. Criar um DAG no Airflow

No Airflow, o **DAG (Directed Acyclic Graph)** define a sequência de operações que você deseja executar na pipeline. O DAG pode ser criado utilizando Python e será responsável por orquestrar as tarefas.

1. **Criar um arquivo Python para o DAG**:
  - No Airflow, crie um arquivo Python em `~/airflow/dags/` com a lógica de orquestração.
  - Exemplo de DAG que conecta e movimenta dados entre PostgreSQL, Databricks e ADLS Gen2:

```python
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.microsoft.azure.transfers.postgres_to_azure import PostgresToAzureDataLakeOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

# Definir o DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 22),
    'retries': 1
}

dag = DAG(
    'data_pipeline_postgres_databricks_adls',
    default_args=default_args,
    description='Uma pipeline de dados usando Airflow, PostgreSQL, ADLS e Databricks',
    schedule_interval='@daily',  # Rodar diariamente
)

# Tarefa 1: Extração de dados do PostgreSQL
extract_data = PostgresOperator(
    task_id='extract_data_from_postgres',
    postgres_conn_id='your_postgres_connection',  # Conexão configurada no Airflow
    sql='SELECT * FROM your_table;',
    autocommit=True,
    database='your_database',
    dag=dag
)

# Tarefa 2: Armazenar dados extraídos no ADLS Gen2
store_data_adls = PostgresToAzureDataLakeOperator(
    task_id='store_data_in_adls',
    postgres_conn_id='your_postgres_connection',
    azure_data_lake_conn_id='your_adls_connection',  # Conexão ADLS configurada no Airflow
    sql='SELECT * FROM your_table;',
    azure_data_lake_path='/path/to/your/data',
    replace=True,
    dag=dag
)

# Tarefa 3: Processar dados no Databricks
databricks_task = DatabricksRunNowOperator(
    task_id='run_databricks_job',
    databricks_conn_id='your_databricks_connection',  # Conexão Databricks configurada no Airflow
    job_id='your_databricks_job_id',  # O ID do job no Databricks
    notebook_params={'input': '/path/to/your/data'},
    dag=dag
)

# Definindo a ordem de execução
extract_data >> store_data_adls >> databricks_task
```

## 4. Configurando e Executando o Job no Databricks

### Criar um Notebook ou Job no Databricks

1. Acesse o **Databricks** e crie um notebook ou job para processamento de dados.
2. O notebook pode usar **Spark** para transformar ou processar os dados recebidos do **ADLS Gen2** ou diretamente do **PostgreSQL**.

Exemplo de código Spark no Databricks (PySpark):

```python
# Leitura de dados do ADLS Gen2
df = spark.read.csv("abfss://<container>@<account_name>.dfs.core.windows.net/<path>/data.csv", header=True, inferSchema=True)

# Processamento de dados (exemplo simples)
df_transformed = df.filter(df['column_name'] > 100)

# Armazenando os dados processados de volta no ADLS Gen2
df_transformed.write.parquet("abfss://<container>@<account_name>.dfs.core.windows.net/<path>/transformed_data/")
```

## 5. Testando e Monitorando a Pipeline

### Testar o DAG

Execute o **DAG** manualmente no **Airflow** para garantir que todas as tarefas estão sendo executadas corretamente. Verifique os logs de cada tarefa para identificar erros e garantir que os dados estão sendo processados corretamente.

### Monitorar a Pipeline

O **Airflow** oferece uma interface de monitoramento, onde você pode ver o status das execuções do **DAG**, reexecutar tarefas e verificar logs detalhados para depuração.

---

## 6. Agendamento e Automação

Após testar, o **DAG** pode ser agendado para rodar automaticamente em intervalos regulares, como uma vez por dia, semanalmente, ou conforme a sua necessidade.

No exemplo do DAG acima, o agendamento é definido para rodar diariamente (`schedule_interval='@daily'`).

