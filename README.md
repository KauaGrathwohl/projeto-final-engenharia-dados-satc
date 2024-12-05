# Projeto final de Engenharia de Dados - SATC

# Introdução
Projeto com o objetivo da construção de uma simulação de uma pipeline de dados utilizando o modelo de arquitetura Medalhão (Medallion Architecture). Visando garantir a eficiência no processamento de grandes volumes de dados, proporcionando uma estrutura escalável, flexível e otimizada para consumo analítico e tomada de decisão.

[Documentação](https://kauagrathwohl.github.io/projeto-final-engenharia-dados-satc)


# Desenho de Arquitetura

![Desenho Arquitetura](./desenho_arquitetura.png)

# Pré-requisitos
* [Python 3.12.6](https://www.python.org/downloads/release/python-3126/)

* [Criar uma conta na Azure](https://azure.microsoft.com/en-us/)

* [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)

* [Docker](https://docs.docker.com/engine/install/)  

* [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

* [Power BI Desktop](https://www.microsoft.com/pt-br/power-platform/products/power-bi/desktop)

# Instalação e Setup
## Python Para o gerador de dados
Em [notebooks/gerador](./notebooks/gerador/)
```
pip install -r requirements.txt
``` 

## Python para o MkDocs
```
pip install mkdocs 
pip install mkdocs-ivory
```

## Azure
### Usando o CLI Logar da Azure Logar
```
az login
``` 
### Listar dados da conta e do resource group, os dados aqui seram usados posteriormente
```
az account show -o table
az group list -o table
```

## SGDB SQL Server (MMSQL) na Azure   
### Navegar até [iac/sqlserver](./iac/sqlserver)
### Alterar os valores das variaveis no arquivo [variables.tf](./iac/sqlserver/variables.tf)
```
variable "resource_group_name" {
    ...
    default = "Inserir Aqui o nom do seu resource group"
}
```

### Rodar os comandos do terraform para subir uma instacia do SQL server no seu azure
```
terrafrom init 
terraform apply
```
### Assim o seu sql sever vai estar configurado, caso precise deletar ele rodar:
```
terrafrom destroy
```

## Azure ADLS2
### Navegar até [./iac/adls](./iac/adls)
### Alterar os valores das variaveis no arquivo [variables.tf](./iac/adls/variables.tf)
```
variable "subscription_id" {
  ...
  default = "alterar pelo seu subscription id"
}
```
### Rodar os comandos do terraform para subir uma instacia do ADLS2 no seu azure
```
terrafrom init 
terraform apply
```
### Assim o seu ADLS2 vai estar configurado, caso precise deletar ele rodar:
```
terrafrom destroy
```

## Databricks na Azure
### Navegar até [./iac/databricks](./iac/databricks)
### Alterar os valores das variaveis no arquivo [variables.tf](./iac/databricks/variables.tf)
```
variable "azure_client_id" {
  ...
  default = "alterar pelo seu azure client id"
}

variable "azure_client_secret" {
  ...
  default = "alterar pelo seu azure client secret"
}

variable "azure_tenant_id" {
  ...
  default = "alterar pelo seu azure tenant id"
}

variable "subscription_id" {
  ...
  default = "alterar pelo seu subscription id"
}
```

### Rodar os comandos do terraform para subir uma instacia do databrick no seu azure
```
terrafrom init 
terraform apply
```

### Assim o seu databricks vai estar configurado, caso precise deletar ele rodar:
```
terrafrom destroy
```

# Implantação
## SQL Server

## ADLS

## Databricks

## Dashboard


# Ferramentas utilizadas
* Databricks - https://www.databricks.com/br - Plataforma utilizada para a criação do Workflow do projeto
* MkDocs - https://www.mkdocs.org/ - Ferramenta utilizada para a criação da documentação
* Azure - https://azure.microsoft.com/pt-br/get-started/azure-portal/ - Ferramenta utilizada para a criação do ambiente de nuvem
* Python - https://www.python.org/doc/ - Linguagem de programação utilizada para o desenvolvimento dos scripts
* PySpark - https://spark.apache.org/docs/latest/api/python/index.html - Biblioteca de processamento de dados distribuídos
* SQL Server - https://www.microsoft.com/pt-br/sql-server/sql-server-downloads - Banco de dados relacional utilizado para armazenamento dos dados
* Azure ADLS Gen 2 - https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction -  Conjunto de funcionalidades dedicadas à análise de Big Data
* Project Tree Generator - https://woochanleee.github.io/project-tree-generator - gerador de tree para github
# Estrutura do Projeto
```
├─ .gitignore
├─ LICENSE.md
├─ README.md
├─ css
│  └─ extra.css
├─ data
│  └─ raw
│     ├─ dump.sql
│     ├─ sakila-data.sql
│     ├─ sakila-schema.sql
│     └─ schema.sql
├─ desenho_arquitetura.excalidraw
├─ desenho_arquitetura.png
├─ docs
│  ├─ docs
│  │  ├─ index.md
│  │  ├─ pipeline.md
│  │  └─ projeto.md
│  └─ mkdocs.yml
├─ iac
│  ├─ .gitignore
│  ├─ adls
│  │  ├─ .terraform.lock.hcl
│  │  ├─ main.tf
│  │  ├─ output.tf
│  │  ├─ provider.tf
│  │  └─ variables.tf
│  ├─ databricks
│  │  ├─ .terraform.lock.hcl
│  │  ├─ main.tf
│  │  ├─ output.tf
│  │  ├─ provider.tf
│  │  └─ variables.tf
│  └─ sqlserver
│     ├─ .terraform.lock.hcl
│     ├─ main.tf
│     ├─ outputs.tf
│     ├─ providers.tf
│     └─ variables.tf
├─ notebooks
│  ├─ camadas
│  │  ├─ 1-landing.ipynb
│  │  ├─ 2-bronze.ipynb
│  │  ├─ 3-silver.ipynb
│  │  ├─ 4-gold.ipynb
│  │  ├─ all.py
│  │  └─ env.ipynb
│  └─ gerador
│     ├─ .python-version
│     ├─ gerador.py
│     └─ requirements.txt
└─ requirements.txt
```
©generated by [Project Tree Generator](https://woochanleee.github.io/project-tree-generator)


# Versão
`Versão 1.0`


# Autores
* Kauã Machado Grathwohl - [linkParaPerfil](https://github.com/KauaGrathwohl)
* Thiago Larangeira De Souza - [linkParaPerfil](https://github.com/thiagolarangeiras)
* Filipe Milaneze de Aguiar - [linkParaPerfil](https://github.com/phillCD)

# Licença
Este projeto está sob a licença MIT - veja o arquivo [`LICENSE`](./LICENSE.md) para detalhes.

# Referências
* [How to Create Databricks Workflows. Bryan Cafferky](https://www.youtube.com/watch?v=TAg2u5jKvrk)
* [Exemplos de terraform. Jorge Silva](https://github.com/jlsilva01/engenharia-dados-azure-databricks)
* [Exemplo de terraform. Jorge Silva](https://github.com/jlsilva01/adls-azure)
* [Referencias e exemplos estruturais. Jorge Silva](https://github.com/jlsilva01/projeto-ed-satc)
* [Documentação do Python](https://www.python.org/doc/)
* [Documentação do Poetry](https://python-poetry.org/docs/)
* [Documentação do Pyenv](https://github.com/pyenv/pyenv)
* [Documentação do Azure](https://learn.microsoft.com/en-us/azure/)
* [Documentação do Databricks](https://docs.databricks.com/en/index.html)
* [Documentação do Mkdocs](https://www.mkdocs.org/)