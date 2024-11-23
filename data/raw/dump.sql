CREATE DATABASE sistema;

USE sistema;

CREATE TABLE clientes (
    cliente_id INT IDENTITY(1, 1) PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    telefone VARCHAR(15),
    data_cadastro DATE NOT NULL
);

CREATE TABLE categorias (
    categoria_id INT IDENTITY(1, 1) PRIMARY KEY,
    nome_categoria VARCHAR(50) NOT NULL
);

CREATE TABLE produtos (
    produto_id INT IDENTITY(1, 1) PRIMARY KEY,
    categoria_id INT,
    nome_produto VARCHAR(100) NOT NULL,
    preco_unitario DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (categoria_id) REFERENCES categorias(categoria_id)
);

CREATE TABLE pedidos (
    pedido_id INT IDENTITY(1, 1) PRIMARY KEY,
    cliente_id INT,
    data_pedido DATE NOT NULL,
    valor_total DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id)
);

CREATE TABLE itens_pedido (
    item_id INT IDENTITY(1, 1) PRIMARY KEY,
    pedido_id INT,
    produto_id INT,
    quantidade INT NOT NULL,
    preco_unitario DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (pedido_id) REFERENCES pedidos(pedido_id),
    FOREIGN KEY (produto_id) REFERENCES produtos(produto_id)
);

CREATE TABLE enderecos (
    endereco_id INT IDENTITY(1, 1) PRIMARY KEY,
    cliente_id INT,
    rua VARCHAR(255) NOT NULL,
    cidade VARCHAR(100) NOT NULL,
    estado VARCHAR(50) NOT NULL,
    cep VARCHAR(10) NOT NULL,
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id)
);

CREATE TABLE pagamentos (
    pagamento_id INT IDENTITY(1, 1) PRIMARY KEY,
    pedido_id INT,
    data_pagamento DATE NOT NULL,
    metodo_pagamento VARCHAR(50) NOT NULL,
    valor_pago DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (pedido_id) REFERENCES pedidos(pedido_id)
);

CREATE TABLE estoque (
    produto_id INT PRIMARY KEY,
    quantidade_em_estoque INT NOT NULL,
    FOREIGN KEY (produto_id) REFERENCES produtos(produto_id)
);
