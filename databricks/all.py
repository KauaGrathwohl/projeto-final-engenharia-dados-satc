# Variables
storageAccountName = ""
sasToken = ""

def mount_adls(blobContainerName):
    try:
        dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
            mount_point = f"/mnt/{storageAccountName}/{blobContainerName}",
            #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
            extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
        )
        print("OK!")
    except Exception as e:
        print("Falha", e)


# BRONZE
from datetime import datetime
from pyspark.sql.functions import lit

def bronze():
    try:
        # Recuperar dados do landing de csv
        df_clientes     = spark.read.option("infeschema", "true").option("header", "true").csv(f"/mnt/{storageAccountName}/landing/clientes.csv")
        df_categorias   = spark.read.option("infeschema", "true").option("header", "true").csv(f"/mnt/{storageAccountName}/landing/categorias.csv")
        df_produtos     = spark.read.option("infeschema", "true").option("header", "true").csv(f"/mnt/{storageAccountName}/landing/produtos.csv")
        df_pedidos      = spark.read.option("infeschema", "true").option("header", "true").csv(f"/mnt/{storageAccountName}/landing/pedidos.csv")
        df_itens_pedido = spark.read.option("infeschema", "true").option("header", "true").csv(f"/mnt/{storageAccountName}/landing/itens_pedido.csv")
        df_enderecos    = spark.read.option("infeschema", "true").option("header", "true").csv(f"/mnt/{storageAccountName}/landing/enderecos.csv")
        df_pagamentos   = spark.read.option("infeschema", "true").option("header", "true").csv(f"/mnt/{storageAccountName}/landing/pagamentos.csv")
        df_estoque      = spark.read.option("infeschema", "true").option("header", "true").csv(f"/mnt/{storageAccountName}/landing/estoque.csv")
    except Exception as e:
        print("Erro ao recuperar dados da landing")
        raise e
        return

    try: 
        # Adicionando metadados de data e hora de processamento e nome do arquivo de origem
        df_clientes     = df_clientes     .withColumn("DATA_HORA_BRONZE", lit(datetime.now().timestamp())).withColumn("NOME_ARQUIVO_BRONZE", lit("clientes.csv"))
        df_categorias   = df_categorias   .withColumn("DATA_HORA_BRONZE", lit(datetime.now().timestamp())).withColumn("NOME_ARQUIVO_BRONZE", lit("categorias.csv"))
        df_produtos     = df_produtos     .withColumn("DATA_HORA_BRONZE", lit(datetime.now().timestamp())).withColumn("NOME_ARQUIVO_BRONZE", lit("produtos.csv"))
        df_pedidos      = df_pedidos      .withColumn("DATA_HORA_BRONZE", lit(datetime.now().timestamp())).withColumn("NOME_ARQUIVO_BRONZE", lit("pedidos.csv"))
        df_itens_pedido = df_itens_pedido .withColumn("DATA_HORA_BRONZE", lit(datetime.now().timestamp())).withColumn("NOME_ARQUIVO_BRONZE", lit("itens_pedido.csv"))
        df_enderecos    = df_enderecos    .withColumn("DATA_HORA_BRONZE", lit(datetime.now().timestamp())).withColumn("NOME_ARQUIVO_BRONZE", lit("enderecos.csv"))
        df_pagamentos   = df_pagamentos   .withColumn("DATA_HORA_BRONZE", lit(datetime.now().timestamp())).withColumn("NOME_ARQUIVO_BRONZE", lit("pagamentos.csv"))
        df_estoque      = df_estoque      .withColumn("DATA_HORA_BRONZE", lit(datetime.now().timestamp())).withColumn("NOME_ARQUIVO_BRONZE", lit("estoque.csv"))
    except Exception as e:
        print("Erro ao adicionar data e nome")
        raise e
        return

    try:
        # Salvando os dataframes em delta lake (formato de arquivo) no data lake (repositorio cloud)
        df_clientes     .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/bronze/clientes")
        df_categorias   .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/bronze/categorias")
        df_produtos     .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/bronze/produtos")
        df_pedidos      .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/bronze/pedidos")
        df_itens_pedido .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/bronze/itens_pedido")
        df_enderecos    .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/bronze/enderecos")
        df_pagamentos   .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/bronze/pagamentos")
        df_estoque      .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/bronze/estoque")
    except Exception as e:
        print("Erro ao salvar arquivos no bronze")
        raise e
        return

# SILVER
from datetime import datetime
from pyspark.sql.functions import lit

def silver():
    # Recuperar dados da bronze
    try:
        df_clientes     = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/clientes")
        df_categorias   = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/categorias")
        df_produtos     = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/produtos")
        df_pedidos      = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/pedidos")
        df_itens_pedido = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/itens_pedido")
        df_enderecos    = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/enderecos")
        df_pagamentos   = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/pagamentos")
        df_estoque      = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/estoque")
    except Exception as e:
        print("Erro ao recuperar arquivos do bronze")
        raise e
        return
    
    # Metadodos
    try:
        df_clientes     = df_clientes    .withColumn("DL_DATA_HORA_SILVER", lit(datetime.now().timestamp())).withColumn("DL_NOME_ARQUIVO_SILVER", lit("clientes"))
        df_categorias   = df_categorias  .withColumn("DL_DATA_HORA_SILVER", lit(datetime.now().timestamp())).withColumn("DL_NOME_ARQUIVO_SILVER", lit("categorias"))
        df_produtos     = df_produtos    .withColumn("DL_DATA_HORA_SILVER", lit(datetime.now().timestamp())).withColumn("DL_NOME_ARQUIVO_SILVER", lit("produtos"))
        df_pedidos      = df_pedidos     .withColumn("DL_DATA_HORA_SILVER", lit(datetime.now().timestamp())).withColumn("DL_NOME_ARQUIVO_SILVER", lit("pedidos"))
        df_itens_pedido = df_itens_pedido.withColumn("DL_DATA_HORA_SILVER", lit(datetime.now().timestamp())).withColumn("DL_NOME_ARQUIVO_SILVER", lit("itens_pedido"))
        df_enderecos    = df_enderecos   .withColumn("DL_DATA_HORA_SILVER", lit(datetime.now().timestamp())).withColumn("DL_NOME_ARQUIVO_SILVER", lit("enderecos"))
        df_pagamentos   = df_pagamentos  .withColumn("DL_DATA_HORA_SILVER", lit(datetime.now().timestamp())).withColumn("DL_NOME_ARQUIVO_SILVER", lit("pagamentos"))
        df_estoque      = df_estoque     .withColumn("DL_DATA_HORA_SILVER", lit(datetime.now().timestamp())).withColumn("DL_NOME_ARQUIVO_SILVER", lit("estoque"))
    except Exception as e:
        print("Erro ao adicionar metadados silver")
        raise e
        return
    
    # Alterar nome das colunas
    try:
        df_clientes = df_clientes \
            .withColumnRenamed("cliente_id",    "ID") \
            .withColumnRenamed("nome",          "NOME") \
            .withColumnRenamed("email",         "EMAIL") \
            .withColumnRenamed("telefone",      "TELEFONE") \
            .withColumnRenamed("data_cadastro", "DATA_CADASTRO") \
            .withColumnRenamed("DATA_HORA_BRONZE",    "DL_DATA_HORA_BRONZE") \
            .withColumnRenamed("NOME_ARQUIVO_BRONZE", "DL_NOME_ARQUIVO_BRONZE")
        
        df_categorias = df_categorias \
            .withColumnRenamed("categoria_id",   "ID") \
            .withColumnRenamed("nome_categoria", "NOME") \
            .withColumnRenamed("DATA_HORA_BRONZE",    "DL_DATA_HORA_BRONZE") \
            .withColumnRenamed("NOME_ARQUIVO_BRONZE", "DL_NOME_ARQUIVO_BRONZE")
            
        df_produtos = df_produtos \
            .withColumnRenamed("produto_id",     "ID") \
            .withColumnRenamed("categoria_id",   "ID_CATEGORIA") \
            .withColumnRenamed("nome_produto",   "NOME") \
            .withColumnRenamed("preco_unitario", "PRECO_UNITARIO") \
            .withColumnRenamed("DATA_HORA_BRONZE",    "DL_DATA_HORA_BRONZE") \
            .withColumnRenamed("NOME_ARQUIVO_BRONZE", "DL_NOME_ARQUIVO_BRONZE")
            

        df_pedidos = df_pedidos \
            .withColumnRenamed("pedido_id",   "ID") \
            .withColumnRenamed("cliente_id",  "ID_CLIENTE") \
            .withColumnRenamed("data_pedido", "DATA_PEDIDO") \
            .withColumnRenamed("valor_total", "VALOR_TOTAL") \
            .withColumnRenamed("DATA_HORA_BRONZE",    "DL_DATA_HORA_BRONZE") \
            .withColumnRenamed("NOME_ARQUIVO_BRONZE", "DL_NOME_ARQUIVO_BRONZE")
            
        df_itens_pedido = df_itens_pedido \
            .withColumnRenamed("item_id",        "ID") \
            .withColumnRenamed("pedido_id",      "ID_PEDIDO") \
            .withColumnRenamed("produto_id",     "ID_PRODUTO") \
            .withColumnRenamed("quantidade",     "QUANTIDADE") \
            .withColumnRenamed("preco_unitario", "PRECO_UNITARIO") \
            .withColumnRenamed("DATA_HORA_BRONZE",    "DL_DATA_HORA_BRONZE") \
            .withColumnRenamed("NOME_ARQUIVO_BRONZE", "DL_NOME_ARQUIVO_BRONZE")
        
        df_enderecos = df_enderecos \
            .withColumnRenamed("endereco_id", "ID") \
            .withColumnRenamed("cliente_id",  "ID_CLIENTE") \
            .withColumnRenamed("rua",         "RUA") \
            .withColumnRenamed("cidade",      "CIDADE") \
            .withColumnRenamed("estado",      "ESTADO") \
            .withColumnRenamed("cep",         "CEP") \
            .withColumnRenamed("DATA_HORA_BRONZE",   "DL_DATA_HORA_BRONZE") \
            .withColumnRenamed("NOME_ARQUIVO_BRONZE", "DL_NOME_ARQUIVO_BRONZE")

        df_pagamentos = df_pagamentos \
            .withColumnRenamed("pagamento_id",     "ID") \
            .withColumnRenamed("pedido_id",        "ID_PEDIDO") \
            .withColumnRenamed("data_pagamento",   "DATA_PAGAMENTO") \
            .withColumnRenamed("metodo_pagamento", "METODO_PAGAMENTO") \
            .withColumnRenamed("valor_pago",       "VALOR_PAGO") \
            .withColumnRenamed("DATA_HORA_BRONZE",    "DL_DATA_HORA_BRONZE") \
            .withColumnRenamed("NOME_ARQUIVO_BRONZE", "DL_NOME_ARQUIVO_BRONZE")
            
        df_estoque = df_estoque \
            .withColumnRenamed("produto_id",            "ID") \
            .withColumnRenamed("quantidade_em_estoque", "QUANTIDADE_EM_ESTOQUE") \
            .withColumnRenamed("DATA_HORA_BRONZE",      "DL_DATA_HORA_BRONZE") \
            .withColumnRenamed("NOME_ARQUIVO_BRONZE",   "DL_NOME_ARQUIVO_BRONZE")
    except Exception as e:
        print("Erro ao alterar nomes das colunas")
        raise e
        return
    
    try:
        df_clientes    .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/silver/clientes")
        df_categorias  .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/silver/categorias")
        df_produtos    .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/silver/produtos")
        df_pedidos     .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/silver/pedidos")
        df_itens_pedido.write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/silver/itens_pedido")
        df_enderecos   .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/silver/enderecos")
        df_pagamentos  .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/silver/pagamentos")
        df_estoque     .write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/silver/estoque")
    except Exception as e:
        print("Erro ao salvar dados na silver")
        raise e
        return
    

# GOLD
from datetime import datetime
from pyspark.sql.functions import lit

def gold():
    def gold():
    try:
        # Recuperar dados da camada Silver
        df_clientes     = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/clientes")
        df_pedidos      = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/pedidos")
        df_itens_pedido = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/itens_pedido")
        df_produtos     = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/produtos")
        df_categorias   = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/categorias")
        df_pagamentos   = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/pagamentos")
        df_enderecos    = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/enderecos")
    except Exception as e:
        print("Erro ao carregar dados da camada Silver")
        raise e
        return

    # Criar Dimensão Clientes
    try:
        df_dim_clientes = df_clientes.join(
            df_enderecos,
            df_clientes["ID"] == df_enderecos["ID_CLIENTE"],
            "left"
        ).select(
            df_clientes["ID"].alias("CLIENTE_ID"),
            df_clientes["NOME"],
            df_clientes["EMAIL"],
            df_clientes["TELEFONE"],
            df_clientes["DATA_CADASTRO"],
            df_enderecos["RUA"],
            df_enderecos["CIDADE"],
            df_enderecos["ESTADO"],
            df_enderecos["CEP"]
        )
        
        df_dim_clientes.write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/gold/dim_clientes")
    except Exception as e:
        print("Erro ao criar Dimensão Clientes")
        raise e
        return

    # Criar Fato Vendas
    try:
        # Relacionar tabelas de pedidos, itens de pedido, produtos e categorias
        df_fato_vendas = df_pedidos.join(
            df_itens_pedido,
            df_pedidos["ID"] == df_itens_pedido["ID_PEDIDO"],
            "inner"
        ).join(
            df_produtos,
            df_itens_pedido["ID_PRODUTO"] == df_produtos["ID"],
            "inner"
        ).join(
            df_categorias,
            df_produtos["ID_CATEGORIA"] == df_categorias["ID"],
            "left"
        ).join(
            df_pagamentos,
            df_pedidos["ID"] == df_pagamentos["ID_PEDIDO"],
            "left"
        ).select(
            df_pedidos["ID"].alias("PEDIDO_ID"),
            df_pedidos["DATA_PEDIDO"],
            df_pedidos["ID_CLIENTE"],
            df_itens_pedido["ID_PRODUTO"],
            df_produtos["NOME"].alias("PRODUTO_NOME"),
            df_categorias["NOME"].alias("CATEGORIA"),
            df_itens_pedido["QUANTIDADE"],
            df_itens_pedido["PRECO_UNITARIO"],
            (df_itens_pedido["QUANTIDADE"] * df_itens_pedido["PRECO_UNITARIO"]).alias("VALOR_TOTAL_ITEM"),
            df_pagamentos["METODO_PAGAMENTO"],
            df_pagamentos["VALOR_PAGO"]
        )

        df_fato_vendas.write.format('delta').mode("overwrite").save(f"/mnt/{storageAccountName}/gold/fato_vendas")
    except Exception as e:
        print("Erro ao criar Fato Vendas")
        raise e
        return

    print("Camada Gold criada com sucesso!")

