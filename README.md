# projeto-final-engenharia-dados-satc

Projeto final da matéria de Engenharia de Dados, com o objetivo de criar uma nova pipeline de Dados.

# 1. Extação de dados
## 1.1. Baixar o docker e a imagem do mysql
Baixar o docker 
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

## 1.2. Conectar com dbeaver
Baixar a versão free/comunity do dbeaver em: https://dbeaver.io/download/

Conectar na porta localhost:3306 
Usuario: eng
Senha: eng
database: sys
trocar a propriedade do driver: alowPublicKeyRetrival para TRUE

conexão deve funcionar

## 1.3. importando os dados da origem 
* importar os dados de "./data/sakila-chemas.sql" e "./dados/sakila-data.sql" nessa ordem usando o executar de script do dbeaver 

