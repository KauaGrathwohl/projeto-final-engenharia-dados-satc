from sqlalchemy import create_engine
import pandas as pd
from minio import Minio
import io
from env import *

# Precisa da lib psycopg2
def postgres_all():
    db_connection_str = f'postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}/{PG_DB}'
    db_connection = create_engine(db_connection_str)
    df = pd.read_sql("SELECT * FROM pg_catalog.pg_tables where tableowner = 'eng';", con=db_connection)
    for x in df["tablename"]:
        df = pd.read_sql(f"SELECT * FROM {x}", con=db_connection)
        data = df.to_csv(index=False).encode()
        dataIo = io.BytesIO(data)

        min = Minio(
            endpoint = MIN_END, 
            access_key = MIN_ACCESS, 
            secret_key = MIN_SECRET, 
            secure=False
        )
        min.put_object(bucket_name="landing", object_name=f"{x}.csv", data=dataIo, length=len(data))

def postgres():
    db_connection_str = f'postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}/{PG_DB}'
    db_connection = create_engine(db_connection_str)
    
    df = pd.read_sql('SELECT * FROM clientes', con=db_connection)
    data = df.to_csv(index=False).encode()
    dataIo = io.BytesIO(data)

    min = Minio(
        endpoint = MIN_END, 
        access_key = MIN_ACCESS, 
        secret_key = MIN_SECRET, 
        secure=False
    )
    min.put_object(bucket_name="landing", object_name="teste.csv", data=dataIo, length=len(data))

# precisa da lib mysqlconnector
def mysql():
    db_connection_str = 'mysql+mysqlconnector://eng:eng@localhost/sakila'
    db_connection = create_engine(db_connection_str)
    
    df = pd.read_sql('SELECT * FROM teste', con=db_connection)
    data = df.to_csv(index=False).encode()
    dataIo = io.BytesIO(data)

    min = Minio(
        endpoint="localhost:9000", 
        access_key="", 
        secret_key="", 
        secure=False
    )

    min.put_object(bucket_name="teste", object_name="teste.csv", data=dataIo, length=len(data))
    print(df)
    print(f"Total Buckets: ", len(min.list_buckets()))