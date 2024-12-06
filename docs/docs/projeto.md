## <span style="color: #48c;">**Projeto Final Engenharia de Dados - SATC**</span>



#### **Tecnologias Utilizadas**

<div style="border: 1px solid #ccc; padding: 10px; margin: 10px 0;">
- Python: Linguagem de programação utilizada para desenvolvimento dos scripts. <br><br>
- Azure ADLS2: O Azure Data Lake Storage é um conjunto de funcionalidades dedicadas à análise de Big Data, Armazenamento de Blobs do Azure. <br><br> 
- Databricks: Ferramenta para organização do workflow. <br><br>
- SQL Server: Banco de dados relacional utilizado para armazenamento dos dados. <br><br>
- PySpark: Biblioteca de processamento de dados distribuídos.
</div>



#### **Estrutura do Projeto**

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