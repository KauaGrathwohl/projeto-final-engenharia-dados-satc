# <span style="color: #48c;">Configurações</span>
## <span style="color: #48c;">Conta Azure</span>
Todo o projeto depende da Azure então voce deve ter acesso a uma conta seja a gratuita ou paga.     
Para Criar uma conta se dirija ao [Site Oficial da Azure](https://azure.microsoft.com/pt-br/) e clique em criar uma conta ou comece a usar.

Você tambem ira precisar baixar o [CLI do azure](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
### Logando no Azure CLI
**Rodar esse comando para logar**
```
az login
``` 
**Listar dados da conta e do resource group, os dados aqui seram usados posteriormente**
```
az account show -o table
az group list -o table
```

## <span style="color: #48c;">Inciando os serviços Azure</span>
### 1. SGDB SQL Server (MMSQL) na Azure
**Navegar até [iac/sqlserver](./iac/sqlserver)**     
**Alterar os valores das variaveis no arquivo [variables.tf](./iac/sqlserver/variables.tf)**    
```
variable "resource_group_name" {
    ...
    default = "Inserir Aqui o nom do seu resource group"
}
```

**Rodar os comandos do terraform para subir uma instacia do SQL server no seu azure**
```sh
terrafrom init 
terraform apply
```
**Assim o seu sql sever vai estar configurado, caso precise deletar ele rodar:**
```sh
terrafrom destroy
```

### 2. Azure ADLS2
**Navegar até [./iac/adls](./iac/adls)**     
**Alterar os valores das variaveis no arquivo [variables.tf](./iac/adls/variables.tf)**
```
variable "subscription_id" {
  ...
  default = "alterar pelo seu subscription id"
}
```
**Rodar os comandos do terraform para subir uma instacia do ADLS2 no seu azure**
```
terrafrom init 
terraform apply
```
**Assim o seu ADLS2 vai estar configurado, caso precise deletar ele rodar:**
```
terrafrom destroy
```

### 3. Databricks na Azure
**Navegar até [./iac/databricks](./iac/databricks)**    
**Alterar os valores das variaveis no arquivo [variables.tf](./iac/databricks/variables.tf)**
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

**Rodar os comandos do terraform para subir uma instacia do databrick no seu azure**
```
terrafrom init 
terraform apply
```

**Assim o seu databricks vai estar configurado, caso precise deletar ele rodar:**
```
terrafrom destroy
```

## <span style="color: #48c;">Configurações do MKDOCS</span>
**1. Instalar as libs do mkdocs**
```
pip install mkdocs 
pip install mkdocs-ivory
```
**2. Entrar na pasta `/docs`**

**3. Rodar localmente**
```sh
mkdocs serve
```

**4. Build em uma pasta**
```sh
mkdocs build
```

**5. Deploy para o github**
Ele é criado na branch `gh-pages`
```sh
mkdocs gh-deploy
```

## <span style="color: #48c;">Configurações do Gerador de dados</span>

**Na pasta `notebooks/gerador` rodar os comandos**
```
pip install -r requirements.txt
``` 

**Executar o script**
```
python gerador.py
``` 