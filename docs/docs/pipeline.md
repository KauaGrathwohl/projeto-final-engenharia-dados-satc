## <span style="color: #48c;">**Pipeline de Dados**</span>

### **Introdução**

Uma pipeline de dados é um conjunto de processos que extrai, transforma e carrega dados de várias fontes para um destino final. Este documento descreve a criação de uma pipeline de dados robusta, incluindo o tratamento das camadas landing, bronze, silver e gold, e a organização do workflow via Databricks.

### <span style="color: #48c;">**Criação da Pipeline de Dados**</span>

#### **1. Extração de Dados**

A extração de dados é o primeiro passo na pipeline de dados. Os dados podem ser extraídos de várias fontes. Em nosso projeto ele é extraído de um banco sequencial (SQL Server), e logo após, cada tabela é transformada em arquivos CSV.


#### **2. Transformação de Dados**

A transformação de dados envolve aplicar tratamentos e transformações aos dados originais, e salvá-los em formatos de dados Delta Tables.


### <span style="color: #48c;">**Tratamento das Camadas**</span>

#### **1. Camada Landing**

A camada landing é onde os dados brutos são inicialmente armazenados após a extração. Esta camada serve como um ponto de entrada para os dados na pipeline.

Para essa camada são acessados os dados do banco de dados sequencial (SQL Server) hospenada na Azure e extrai os dados de cada tabela em csv para um container blob storage de nome landing no Azure ADLS2 seguindo as etapas:

1. **Conectar no SQL server**
2. **Realizar um query para saber quantas tabelas existem no database**
3. **Para cada tabela recuperar os seus conteudos e salvar em csv na camada landing**
4. **[Código para referência](https://github.com/KauaGrathwohl/projeto-final-engenharia-dados-satc/blob/master/notebooks/camadas/1-landing.ipynb)**

#### **2. Camada Bronze**

A camada bronze é onde os dados brutos são armazenados após uma limpeza inicial. Esta camada é usada para armazenar dados que ainda precisam de processamento adicional.

Nela Recuperamos os dados salvos na camada landing em csv adicionamos metadados de processamento como data e nome de arquivo original caso tenha necessidade posteriormente e salvamos os dados na camada Bronze usando o formato delta table:

1. **Recuperar os dados da landing**
2. **Adicionar dados extras e metadados**
3. **Salvar na camada Bronze em formato delta table**
4. **[Código para referência](https://github.com/KauaGrathwohl/projeto-final-engenharia-dados-satc/blob/master/notebooks/camadas/2-bronze.ipynb)**

#### **3. Camada Silver**

A camada silver é onde os dados são transformados e enriquecidos. Esta camada é usada para armazenar dados que estão prontos para análise.

Nela ajustamos os dados pra qualquer trabalho futuro, ajustando incoerencias e definindo a melhor estrutura para todo os dados

1. **Recuperar dados da bronze**
2. **Adicionar dados extras e metadados**
3. **Ajustar estrutura, campos, nomes, da tebala e colunas**
4. **Salvar os dados alterados na camada Silver no formato delta**
5. **[Código para referência](https://github.com/KauaGrathwohl/projeto-final-engenharia-dados-satc/blob/master/notebooks/camadas/3-silver.ipynb)**

#### **4. Camada Gold**

A camada gold é onde os dados finais e prontos para consumo são armazenados. Esta camada é usada para armazenar dados que estão prontos para serem usados em relatórios e dashboards.

Nela os dados são tratados e convertidos para um uso expecifico, no nosso caso passar para um modelo dimensional para analise em um dashboard do power BI

1. **Recuperar dados do silver**
2. **Filtrar e converter para o modelo desejado**
3. **Salvar em na camada gold em formato delta**
4. **[Código para referência](https://github.com/KauaGrathwohl/projeto-final-engenharia-dados-satc/blob/master/notebooks/camadas/4-gold.ipynb)**


### <span style="color: #48c;">**Organização do Workflow via Databricks**</span>

Databricks é uma plataforma de análise de dados que facilita a criação e a gestão de pipelines de dados. Abaixo está como foi organizado o workflow usando Databricks.

1. **Definir um cluster para rodar o workflow**
2. **Adicionar em ordem as camadas landing, bronze, silver e gold**
3. **Definir um schedule de execução (nenhum definido para o projeto)**
4. **Finalizar a criação do workflow**
5. **[Código para referência](https://github.com/KauaGrathwohl/projeto-final-engenharia-dados-satc/blob/master/notebooks/workflow/etl-pipeline.yml)**

Depois desse processo bastar executar ou esperar ser executado caso tenha um schedule


### <span style="color: #48c;">**Conclusão**</span>

Este documento forneceu uma visão geral da criação de uma pipeline de dados, incluindo o tratamento das camadas landing, bronze, silver e gold, e a organização do workflow via Databricks. Seguindo estas etapas, você pode criar uma pipeline de dados robusta e eficiente.