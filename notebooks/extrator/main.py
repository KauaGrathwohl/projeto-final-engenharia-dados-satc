import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from urllib.parse import quote_plus
from sqlalchemy import create_engine

import os

# ADLS_ACCOUNT_NAME
account_name = "datalaked39ba291b2180f36"

# ADLS_FILE_SYSTEM_NAME
file_system_name = "landing-zone"

# ADLS_DIRECTORY_NAME
directory_name = "a"

# ADLS_SAS_TOKEN
sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-11-27T11:21:11Z&st=2024-11-27T03:21:11Z&spr=https&sig=1Eax4jC0KxtBGNdxnmLvUp5h4ii1z8xQn9Vpwt1%2BHH0%3D"

# SQL_SERVER
server = "sql-proud-lizard.database.windows.net"

# SQL_DATABASE
database = "SampleDB"

# SQL_SCHEMA
schema = "dbo"

# SQL_USERNAME
username = "azureadmin"

# SQL_PASSWORD
Password = "Satc@1234"

def export_adls():
    password = quote_plus(Password)
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server" 
    engine = create_engine(conn_str)
    query = f"SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema}'"

    file_system_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net", 
        credential=sas_token,
        api_version="2020-02-10"
    )

    # Tentar criar o diretório, se não existir
    try:
        directory_client = file_system_client.get_file_system_client(file_system_name).get_directory_client(directory_name)
        directory_client.create_directory()
    except ResourceExistsError:
        print(f"O diretório '{directory_name}' já existe.")

    # Executar a consulta para obter todas as tabelas do esquema
    tables_df = pd.read_sql(query, engine)

    # Para cada tabela encontrada, ler os dados e carregar para o Azure Data Lake Storage
    for index, row in tables_df.iterrows():
        table_name = row["table_name"]
        query = f"SELECT * FROM {schema}.{table_name}"
        df = pd.read_sql(query, conn_str)
        
        # Carregar os dados para o Azure Data Lake Storage
        file_client = directory_client.get_file_client(f"{table_name}.csv")
        data = df.to_csv(index=False).encode()
        file_client.upload_data(data, overwrite=True)
        print(f"Dados da tabela '{table_name}' carregados com sucesso.")

if __name__ == '__main__':
    export_adls()