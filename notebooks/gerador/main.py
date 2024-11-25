from faker import Faker
import random

faker = Faker("pt_BR")

LINE_COUNT = 10000
LINE_SEP = 1000

def clientes():
    with open("dump.sql", "a") as f:
        f.write(f"\n\n\n\n-------------CLIENTES-------------\n\n")
        f.write("SET IDENTITY_INSERT clientes ON;")
        x = 1
        while x < LINE_COUNT:
            texto = "INSERT INTO clientes (cliente_id, nome, email, telefone, data_cadastro) VALUES\n"
            for y in range(LINE_SEP):
                if y != 0:
                    texto += ",\n"
                texto += "({}, '{}', '{}', '{}','{}')".format(
                    x, 
                    faker.name(), 
                    faker.email(), 
                    faker.cellphone_number().replace("+", "").replace(" ", "").replace("-", "")[:15], 
                    faker.date()
                ) 
                x += 1
            texto += ";\n"
            f.write(texto)
        f.write("SET IDENTITY_INSERT clientes OFF;")
    print("OK")

def categorias():
    with open("dump.sql", "a") as f:
        f.write(f"\n\n\n\n-------------CATAGORIAS-------------\n\n")
        f.write("SET IDENTITY_INSERT categorias ON;")
        x = 1
        while x < LINE_COUNT:
            texto = "INSERT INTO categorias (categoria_id, nome_categoria) VALUES\n"
            for y in range(LINE_SEP):
                if y != 0:
                    texto += ",\n"
                texto += "({}, '{}')".format(
                    x, faker.bs()[:50]
                ) 
                x += 1 
            texto += ";\n"
            f.write(texto)
        f.write("SET IDENTITY_INSERT categorias OFF;")
    print("OK")

def produtos():
    with open("dump.sql", "a") as f:
        f.write(f"\n\n\n\n-------------PRODUTOS-------------\n\n")
        f.write("SET IDENTITY_INSERT produtos ON;")
        x = 1
        while x < LINE_COUNT:
            texto = "INSERT INTO produtos (produto_id, categoria_id, nome_produto, preco_unitario) VALUES\n"
            for y in range(LINE_SEP):
                if y != 0:
                    texto += ",\n"
                texto += "({}, {}, '{}', {})".format(
                    x, 
                    faker.random_int(min=1, max=LINE_COUNT), 
                    faker.bs(), 
                    faker.random_number(digits=5) + faker.random_int(min=0, max=99) / 100
                ) 
                x += 1
            texto +=  ";\n"
            f.write(texto)
        f.write("SET IDENTITY_INSERT produtos OFF;")
    print("OK")

def pedidos(): 
    with open("dump.sql", "a") as f:
        f.write(f"\n\n\n\n-------------PEDIDOS-------------\n\n")
        f.write("SET IDENTITY_INSERT pedidos ON;")
        x = 1
        while x < LINE_COUNT:
            texto = "INSERT INTO pedidos (pedido_id, cliente_id, data_pedido, valor_total) VALUES\n"
            for y in range(LINE_SEP):
                if y != 0:
                    texto += ",\n"
                texto += "({}, {}, '{}', {})".format(
                    x, 
                    faker.random_int(min=1, max=LINE_COUNT),
                    faker.date(),
                    faker.random_number(digits=5) + faker.random_int(min=0, max=99) / 100
                ) 
                x += 1
            ";\n" 
            f.write(texto)
        f.write("SET IDENTITY_INSERT pedidos OFF;")
    print("OK")

def itens_pedido():
    with open("dump.sql", "a") as f:
        f.write(f"\n\n\n\n-------------ITENS-PEDIDOS-------------\n\n")
        f.write("SET IDENTITY_INSERT itens_pedido ON;")
        x = 1
        while x < LINE_COUNT:
            texto = "INSERT INTO itens_pedido (item_id, pedido_id, produto_id, quantidade, preco_unitario) VALUES\n"
            for y in range(LINE_SEP):
                if y != 0:
                    texto += ",\n"
                texto += "({}, {}, {}, {}, {})".format(
                    x, 
                    faker.random_int(min=1, max=LINE_COUNT),
                    faker.random_int(min=1, max=LINE_COUNT),
                    faker.random_int(min=-LINE_COUNT, max=LINE_COUNT),
                    faker.random_number(digits=5) + faker.random_int(min=0, max=99) / 100
                )
                x += 1
            texto += ";\n"
            f.write(texto)
        f.write("SET IDENTITY_INSERT itens_pedido OFF;")
    print("OK")

def enderecos():
    with open("dump.sql", "a") as f:
        f.write(f"\n\n\n\n-------------ENDERECOS-------------\n\n")
        f.write("SET IDENTITY_INSERT enderecos ON;")
        x = 1
        while x < LINE_COUNT:
            texto = "INSERT INTO enderecos (endereco_id, cliente_id, rua, cidade, estado, cep) VALUES\n"
            for y in range(LINE_SEP):
                if y != 0:
                    texto += ",\n"
                texto += "({}, {}, '{}', '{}', '{}', '{}')".format(
                    x, 
                    faker.random_int(min=1, max=LINE_COUNT),
                    faker.street_address()[:255],
                    faker.city()[:255],
                    faker.state()[:255],
                    faker.postcode()[:10]
                )
                x += 1 
            texto += ";\n"
            f.write(texto)
        f.write("SET IDENTITY_INSERT enderecos OFF;")
    print("OK")


metodos_de_pagamentos = [
    "Dinheiro em espécie",
    "Cartão de crédito e débito",
    "Boleto bancário",
    "Transferência bancária",
    "Pagamento por aproximação",
    "Pagamentos pelo celular",
    "Link de pagamento",
    "Carteira digital",
    "Pix",
    "QR Code",
]

def pagamentos():
    with open("dump.sql", "a") as f:
        f.write("\n\n\n\n-------------PAGAMENTOS-------------\n\n")
        f.write("SET IDENTITY_INSERT pagamentos ON;")
        x = 1
        while x < LINE_COUNT:
            texto = "INSERT INTO pagamentos (pagamento_id, pedido_id, data_pagamento, metodo_pagamento, valor_pago) VALUES\n"
            for y in range(LINE_SEP):
                if y != 0:
                    texto += ",\n"
                texto += "({}, {}, '{}', '{}', {})".format(
                    x, 
                    faker.random_int(min=1, max=LINE_COUNT),
                    faker.date(),
                    random.choice(metodos_de_pagamentos),
                    faker.random_number(digits=5) + faker.random_int(min=0, max=99) / 100,
                )
                x += 1
            texto += ";\n"
            f.write(texto)
        f.write("SET IDENTITY_INSERT pagamentos OFF;")
    print("OK")

def estoque():
    with open("dump.sql", "a") as f:
        f.write(f"\n\n\n\n-------------ESTOQUE-------------\n\n")
        x = 1
        while x < LINE_COUNT:
            texto = "INSERT INTO estoque (produto_id, quantidade_em_estoque) VALUES\n"
            for y in range(LINE_SEP):
                if y != 0:
                    texto += ",\n"
                texto += "({}, {})".format(
                    x, 
                    faker.random_int(min=1, max=LINE_COUNT)
                )
                x += 1
            texto += ";\n"
            f.write(texto)
    print("OK")

clientes()
categorias()
produtos()
pedidos()
itens_pedido()
enderecos()
pagamentos()
estoque()