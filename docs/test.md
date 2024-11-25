# Configurando os Sistema para teste local

# SQLSERVER
* Instalar docker 
* rodar

docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=1234@Senha" \
   -p 1433:1433 --name sql1 --hostname sql1 \
   -d \
   mcr.microsoft.com/mssql/server:2022-latest


* conectar com algo tipo dbeaver depois