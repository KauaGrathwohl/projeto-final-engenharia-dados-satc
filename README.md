# projeto-final-engenharia-dados-satc

Projeto final da matéria de Engenharia de Dados, com o objetivo de criar uma nova pipeline de Dados.


# Util
## Instalando o docker
https://medium.com/@selvamraju007/how-to-install-docker-desktop-on-ubuntu-22-04-1ebe4b2f8a14

## Instalando o python 
adicionar as dependecias    
`sudo apt-get install`
```
make 
build-essential 
libssl-dev 
zlib1g-dev
libbz2-dev 
libreadline-dev 
libsqlite3-dev 
wget
curl
llvm
libncurses5-dev
libncursesw5-dev
xz-utils
tk-dev
libffi-dev
liblzma-dev
```

usar esses tutoriais    
https://realpython.com/intro-to-pyenv/#installing-pyenv     
https://github.com/pyenv/pyenv-installer        


```
pyenv install 3.13.0
```

```
sudo apt update
sudo apt install pipx
pipx ensurepath
```

```
pipx install poetry
```


# 1. Extração de dados
## 1.1. Criando um ambiente simulado de banco de dados relacional
### 1.1.1 Inciando com um banco relacional
Com o docker Instalado rode os comandos 
```
docker pull mysql:latest
```

```
docker run  -p 3306:3306 --name mysql1 -e MYSQL_ROOT_PASSWORD=1234 -d mysql:latest
```

```
docker exec -it mysql1 bash
```

```
mysql -u root -p
```

```
CREATE USER 'eng'@'%' IDENTIFIED BY 'eng';

GRANT ALL PRIVILEGES ON *.* TO 'eng'@'%' WITH GRANT OPTION;

FLUSH PRIVILEGES;
```
### 1.1.2 Inserindo dados ao banco relacional     
* Baixar a versão free/comunity do dbeaver em: https://dbeaver.io/download/

* Conectar como mysql usando os dados:  

        Porta: localhost:3306  
        Usuario: eng    
        Senha: eng  
        Database: sys       
    
    Trocar a propriedade do driver: alowPublicKeyRetrival para TRUE, caso de algum erro de conexão

* Usando o executador de scripts do dbeaver que pode ser acessado clicando com o botão direito sobre uma database rodar os seguintes arquivos nessa ordem:
    ```
    ./data/sakila-chemas.sql

    ./dados/sakila-data.sql
    ```
` Com isso voce dever ter um sistema relacional funcional em mysql ` 

## 1.2. Extraindo os dados do sistema 

docker run -d -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"

