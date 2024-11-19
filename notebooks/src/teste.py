from sqlalchemy import create_engine
import pandas as pd

db_connection_str = 'mysql+mysqlconnector://eng:eng@localhost/sys'
db_connection = create_engine(db_connection_str)

df = pd.read_sql('SHOW DATABASES', con=db_connection)
print(df)

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