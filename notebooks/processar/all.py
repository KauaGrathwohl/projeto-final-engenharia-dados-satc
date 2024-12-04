# Variables
storageAccountName = "datalakec4056b1df75aa671"
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
    except(e):
        print("Erro ao recuperar dados da landing")
        return

    try: 
        # Adicionando metadados de data e hora de processamento e nome do arquivo de origem
        df_clientes     = df_clientes     .withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("clientes.csv"))
        df_categorias   = df_categorias   .withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("categorias.csv"))
        df_produtos     = df_produtos     .withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("produtos.csv"))
        df_pedidos      = df_pedidos      .withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("pedidos.csv"))
        df_itens_pedido = df_itens_pedido .withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("itens_pedido.csv"))
        df_enderecos    = df_enderecos    .withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("enderecos.csv"))
        df_pagamentos   = df_pagamentos   .withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("pagamentos.csv"))
        df_estoque      = df_estoque      .withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("estoque.csv"))
    except(e):
        print("Erro ao adicionar data e nome")

    try:
        # Salvando os dataframes em delta lake (formato de arquivo) no data lake (repositorio cloud)
        df_clientes     .write.format('delta').save(f"/mnt/{storageAccountName}/bronze/clientes")
        df_categorias   .write.format('delta').save(f"/mnt/{storageAccountName}/bronze/categorias")
        df_produtos     .write.format('delta').save(f"/mnt/{storageAccountName}/bronze/produtos")
        df_pedidos      .write.format('delta').save(f"/mnt/{storageAccountName}/bronze/pedidos")
        df_itens_pedido .write.format('delta').save(f"/mnt/{storageAccountName}/bronze/itens_pedido")
        df_enderecos    .write.format('delta').save(f"/mnt/{storageAccountName}/bronze/enderecos")
        df_pagamentos   .write.format('delta').save(f"/mnt/{storageAccountName}/bronze/pagamentos")
        df_estoque      .write.format('delta').save(f"/mnt/{storageAccountName}/bronze/estoque")
    except(e):
        print("Erro ao salvar arquivos no bronze")

def prata():
    pass

def outro():
    pass
