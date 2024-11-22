from datetime import date, datetime 
import psycopg2
from psycopg2.extensions import cursor, connection
from faker import Faker
from env import *

class Clientes:
    cliente_id: int
    nome: str
    email: str
    telefone: str
    data_cadastro: date

class produtos: 
    produto_id: int 
    nome_produto: str 
    categoria: str 
    preco_unitario: float 

class categorias:
    categoria_id: int
    nome_categoria: str

class pedidos:
    pedido_id: int
    cliente_id: int #FK clientes(cliente_id)
    data_pedido: date
    valor_total: float 

class itens_pedido:
    item_id: int 
    pedido_id: int #FK pedidos(pedido_id)
    produto_id: int #FK produtos(produto_id)
    quantidade: int
    preco_unitario: float

class enderecos:
    endereco_id: int
    cliente_id: int #FK clientes(cliente_id)
    rua: str
    cidade: str
    estado: str
    cep: str

class pagamentos:
    pagamento_id: int
    pedido_id: int #FK pedidos(pedido_id)
    data_pagamento: date
    metodo_pagamento: str
    valor_pago: float

class estoque:
    produto_id: int #FK produtos(produto_id)
    quantidade_em_estoque: int

faker = Faker("pt_BR")

def connect()-> (cursor, connection):
    conn = psycopg2.connect(
        database = PG_DB,
        host = PG_HOST,
        user = PG_USER,
        password = PG_PASS,
        port="5432"
    )
    cur: cursor = conn.cursor()
    return cur, conn

def gerar_inserir_clientes():    
    for y in range(100):
        cur, conn = connect() 
        clientes = []
        try:
            for x in range(100):
                cliente = []
                cliente.append(faker.name())
                cliente.append(faker.email())
                cliente.append(faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15])
                cliente.append(faker.date())
                clientes.append(cliente)
            cur.executemany("""INSERT INTO clientes (nome, email, telefone, data_cadastro) VALUES (%s, %s, %s, %s)""", clientes)
            conn.commit()
            print("Dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            cur.close()
            conn.close()

def gerar_inserir_produtos(categorias):
    nome_produto: str 
    categoria: str 
    preco_unitario: float 
    for y in range(100):
        cur, conn = connect() 
        ps = []
        try:
            for x in range(100):
                p = []
                p.append(faker.bs()[:100])
                p.append(categorias[x])
                p.append(faker.random_float(min=1, max=10000, ndigits=2))
                ps.append(p)
            cur.executemany("""INSERT INTO produtos (nome, email, telefone, data_cadastro) VALUES (%s, %s, %s, %s)""", clientes)
            conn.commit()
            print("Dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            cur.close()
            conn.close()

def gerar_inserir_categorias():
    pass

def gerar_inserir_pedidos():
    pass

def gerar_inserir_itens_pedido():
    pass

def gerar_inserir_enderecos():
    pass

def gerar_inserir_pagamentos():
    pass

def gerar_inserir_estoque():
    pass

def gerar_sql_clientes():
    with open('clientes.sql', 'w') as f:
        x = 1
        while x < 1000:
            texto = f"INSERT INTO clientes (cliente_id, nome, email, telefone, data_cadastro) VALUES\n"
            for y in range(500):
                tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
                texto = texto + f"({x}, '{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}'),\n" 
                x = x + 1
            tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
            texto = texto + f"({x}, '{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}');\n"
            x = x + 1
            f.write(texto)
    print("Dados salvos no arquivo")

def gerar_produtos():
    with open('produtos.sql', 'w') as f:
        x = 1
        while x < 10000:
            texto = f"INSERT INTO produtos (produto_id, categoria_id, nome_produto, preco_unitario) VALUES\n"
            for y in range(1,600):
                preco = faker.random_number(digits=5) + faker.random_int(min=0, max=99) / 100
                texto = texto + f"({x}, {y}, '{faker.bs()}', {preco}),\n" 
                x = x + 1
            preco = faker.random_number(digits=5) + faker.random_int(min=0, max=99) / 100
            texto = texto + f"({x}, {y}, '{faker.bs()}', {preco});\n" 
            x = x + 1
            f.write(texto)
    print("Dados salvos no arquivo")

def gerar_categorias():
    with open('categorias.sql', 'w') as f:
        x = 1
        while x < 1000:
            texto = f"INSERT INTO categorias (categoria_id, nome_categoria) VALUES\n"
            for _ in range(10):
                texto = texto + f"({x}, '{faker.bs()[:50]}'),\n" 
                x = x + 1
            texto = texto + f"({x}, '{faker.bs()[:50]}');\n" 
            x = x + 1
            f.write(texto)
    print("Dados salvos no arquivo")

def gerar_pedidos(): 
    with open('pedidos.sql', 'w') as f:
        x = 1
        while x < 10000:
            texto = f"INSERT INTO pedidos (pedido_id, cliente_id, data_pedido, valor_total) VALUES\n"
            for _ in range(1000):
                preco = faker.random_number(digits=5) + faker.random_int(min=0, max=99) / 100
                texto = texto + f"({x}, {faker.random_int(min=1, max=700)}, '{faker.date()}', {preco}),\n" 
                x = x + 1
            preco = faker.random_number(digits=5) + faker.random_int(min=0, max=99) / 100
            texto = texto + f"({x}, {faker.random_int(min=1, max=700)}, '{faker.date()}', {preco});\n" 
            x = x + 1
            f.write(texto)
    print("Acabou")

def gerar_itens_pedido():
    with open('clientes.sql', 'w') as f:
        for _ in range(1000):
            texto = f"INSERT INTO clientes (nome, email, telefone, data_cadastro) VALUES\n"
            for x in range(10):
                tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
                texto = texto + f"('{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}'),\n" 
            tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
            texto = texto + f"('{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}');\n"
            f.write(texto)
    print("Dados salvos no arquivo 'dados_faker.txt'")

def gerar_enderecos():
    with open('clientes.sql', 'w') as f:
        for _ in range(1000):
            texto = f"INSERT INTO clientes (nome, email, telefone, data_cadastro) VALUES\n"
            for x in range(10):
                tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
                texto = texto + f"('{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}'),\n" 
            tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
            texto = texto + f"('{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}');\n"
            f.write(texto)
    print("Dados salvos no arquivo 'dados_faker.txt'")

def gerar_pagamentos():
    with open('clientes.sql', 'w') as f:
        for _ in range(1000):
            texto = f"INSERT INTO clientes (nome, email, telefone, data_cadastro) VALUES\n"
            for x in range(10):
                tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
                texto = texto + f"('{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}'),\n" 
            tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
            texto = texto + f"('{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}');\n"
            f.write(texto)
    print("Dados salvos no arquivo 'dados_faker.txt'")

def gerar_estoque():
    with open('clientes.sql', 'w') as f:
        for _ in range(1000):
            texto = f"INSERT INTO clientes (nome, email, telefone, data_cadastro) VALUES\n"
            for x in range(10):
                tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
                texto = texto + f"('{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}'),\n" 
            tele = faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15]
            texto = texto + f"('{faker.name()}', '{faker.email()}', '{tele}','{faker.date()}');\n"
            f.write(texto)
    print("Dados salvos no arquivo 'dados_faker.txt'")


