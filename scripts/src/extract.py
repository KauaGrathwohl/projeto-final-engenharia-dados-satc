from sqlalchemy import create_engine
import pandas as pd
from minio import Minio
import io

db_connection_str = 'mysql+mysqlconnector://eng:eng@localhost/sakila'
db_connection = create_engine(db_connection_str)

df = pd.read_sql('SELECT * FROM teste', con=db_connection)
data = df.to_csv(index=False).encode()
dataIo = io.BytesIO(data)

min = Minio(
    endpoint="localhost:9000", 
    access_key="ck2aQGzAUXljukN91six", 
    secret_key="LSU8ElP7KNLwDMnQBdV7PYFF8fP3RoTMQEYWP7u4", 
    secure=False
)

# feather_output = BytesIO()
# df.to_feather(feather_output)
# nb_bytes = feather_output.tell()
# feather_output.seek(0)
# client.put_object(
#     bucket_name="datasets",
#     object_name="demo.feather", 
#     length=nb_bytes,
#     data=feather_output
# )


min.put_object(bucket_name="teste", object_name="teste.csv", data=dataIo, length=len(data))


# Create a BytesIO instance that will behave like a file opended in binary mode 

# Write feather file

# Get numver of bytes

# Go back to the start of the opened file


# Put the object into minio



print(df)
print(f"Total Buckets: ", len(min.list_buckets()))

# import os
# import mysql.connector as connection
# import pandas as pd

# try:
#     mydb = connection.connect(host="localhost", database = 'sys', user="eng", passwd="eng", use_pure=True)
#     query = "SHOW DATABASES"
#     result_dataFrame = pd.read_sql(query, mydb)
#     mydb.close() #close the connectionexcept Exception as e:
#     mydb.close()
#     print(str(e))
# except Exception as e:
#     mydb.close()
#     print(str(e))