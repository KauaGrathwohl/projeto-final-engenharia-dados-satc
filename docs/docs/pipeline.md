## <span style="color: #48c;">**Pipeline de Dados**</span>

### **Introdução**

Uma pipeline de dados é um conjunto de processos que extrai, transforma e carrega dados de várias fontes para um destino final. Este documento descreve a criação de uma pipeline de dados robusta, incluindo o tratamento das camadas landing, bronze, silver e gold, e a organização do workflow via Databricks. <br><br>



### <span style="color: #48c;">**Criação da Pipeline de Dados**</span>

#### **1. Extração de Dados**

A extração de dados é o primeiro passo na pipeline de dados. Os dados podem ser extraídos de várias fontes. Em nosso projeto ele é extraído de um banco sequencial (SQL Server), e logo após, cada tabela é transformada em arquivos CSV.


#### **2. Transformação de Dados**

A transformação de dados envolve aplicar tratamentos e transformações aos dados originais, e salvá-los em formatos de dados Delta Tables<br><br>



### <span style="color: #48c;">**Tratamento das Camadas**</span>

#### **1. Camada Landing**

A camada landing é onde os dados brutos são inicialmente armazenados após a extração. Esta camada serve como um ponto de entrada para os dados na pipeline.

Para essa camada são acessados os dados do banco de dados sequencial (SQL Server) hospenada na Azure e extrai os dados de cada tabela em csv para um container blob storage de nome landing no Azure ADLS2 seguindo as etapas:

**Conectar no SQL server**
```
    jdbc_url = f"jdbc:sqlserver://{DB_SERVER}:1433;database={DB_DATABASE}"
    connection_properties = {
    "user" : DB_USER,
    "password" : DB_PASS,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
```

**Realizar um query para saber quantas tabelas existem no database**
```
query = f"(SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{DB_SCHEMA}') AS query"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties).toPandas()
```

**Para cada tabela recuperar os seus conteudos e salvar em csv na camada landing**
```
for index, row in df.iterrows():
    table_name = row["table_name"]
    query2 = f"(SELECT * FROM {DB_SCHEMA}.{table_name}) as query"
    df_table = spark.read.jdbc(url=jdbc_url, table=query2, properties=connection_properties) 
    df_table.write \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"/mnt/{ACCOUNT_NAME}/landing/{table_name}.csv")
    print(f"Dados da tabela '{table_name}' carregados com sucesso.")
```



#### **2. Camada Bronze**

A camada bronze é onde os dados brutos são armazenados após uma limpeza inicial. Esta camada é usada para armazenar dados que ainda precisam de processamento adicional.

Nela Recuperamos os dados salvos na camada landing em csv adicionamos metadados de processamento como data e nome de arquivo original caso tenha necessidade posteriormente e salvamos os dados na camada Bronze usando o formato delta table:

**Recuperar os dados da landing**   
**Adicionar dados extras e metadados**  
**Salvar na camada Bronze em formato delta table**  

#### **3. Camada Silver**

A camada silver é onde os dados são transformados e enriquecidos. Esta camada é usada para armazenar dados que estão prontos para análise.

Nela ajustamos os dados pra qualquer trabalho futuro, ajustando incoerencias e definindo a melhor estrutura para todo os dados

**Recuperar dados da bronze**   
**Adicionar dados extras e metadados**  
**Ajustar estrutura, campos, nomes, da tebala e colunas**   
**Salvar os dados alterados na camada Silver no formato delta** 


#### **4. Camada Gold**

A camada gold é onde os dados finais e prontos para consumo são armazenados. Esta camada é usada para armazenar dados que estão prontos para serem usados em relatórios e dashboards. 

Nela os dados são tratados e convertidos para um uso expecifico, no nosso caso passar para um modelo dimensional para analise em um dashboard do power BI

**Recuperar dados do silver**   
**Filtrar e converter para o modelo desejado**  
**Salvar em na camada gold em formato delta**   

<br><br>



### <span style="color: #48c;">**Organização do Workflow via Databricks**</span>

Databricks é uma plataforma de análise de dados que facilita a criação e a gestão de pipelines de dados. Abaixo está um exemplo de como organizar o workflow usando Databricks.

#### **1. Criação de um Notebook**

Crie um notebook no Databricks para organizar o seu workflow. Um notebook permite que você escreva e execute código em blocos.


#### **2. Configuração do Cluster**

Configure um cluster no Databricks para executar o seu notebook. Um cluster é um conjunto de máquinas virtuais que executam o seu código.


#### **3. Organização do Workflow**

Organize o seu workflow em etapas lógicas dentro do notebook. Cada etapa deve corresponder a uma parte da pipeline de dados.


#### **4. Agendamento do Workflow**

Use o Databricks Job Scheduler para agendar a execução do seu notebook em intervalos regulares. Isso garante que a sua pipeline de dados seja executada automaticamente. <br><br>



### <span style="color: #48c;">**Conclusão**</span>

Este documento forneceu uma visão geral da criação de uma pipeline de dados, incluindo o tratamento das camadas landing, bronze, silver e gold, e a organização do workflow via Databricks. Seguindo estas etapas, você pode criar uma pipeline de dados robusta e eficiente.