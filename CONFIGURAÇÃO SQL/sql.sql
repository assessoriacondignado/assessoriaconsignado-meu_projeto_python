-- 1. Criação do Schema 'cliente'
CREATE SCHEMA IF NOT EXISTS cliente;

-- 2. Tabela: Lista de Carteiras dos Clientes
CREATE TABLE IF NOT EXISTS cliente.cliente_carteira_lista (
    id SERIAL PRIMARY KEY,
    cpf_cliente VARCHAR(20) NOT NULL,
    nome_cliente VARCHAR(255),
    nome_carteira VARCHAR(100),
    custo_carteira NUMERIC(10, 2), -- Valor monetário (ex: 150.00)
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Tabela: Modelo de Transações da Carteira
CREATE TABLE IF NOT EXISTS cliente.cliente_carteira_transacoes_modelo (
    id SERIAL PRIMARY KEY,
    cpf_cliente VARCHAR(20),
    nome_cliente VARCHAR(255),
    motivo VARCHAR(255),           -- Ex: Recarga, Consumo, Estorno
    origem_lancamento VARCHAR(100), -- Ex: Sistema, Pix, Cartão
    data_transacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tipo_lancamento VARCHAR(50),    -- Ex: CREDITO ou DEBITO
    valor NUMERIC(10, 2),
    saldo_anterior NUMERIC(10, 2),
    saldo_novo NUMERIC(10, 2)
);

-- 4. Tabela: Relação entre Produto e Carteira
CREATE TABLE IF NOT EXISTS cliente.cliente_carteira_relacao_pedido_carteira (
    id SERIAL PRIMARY KEY,
    produto VARCHAR(255),
    nome_carteira VARCHAR(100),
    data_vinculo TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices recomendados para performance (Opcional, mas recomendado)
CREATE INDEX IF NOT EXISTS idx_carteira_cpf ON cliente.cliente_carteira_lista(cpf_cliente);
CREATE INDEX IF NOT EXISTS idx_transacoes_cpf ON cliente.cliente_carteira_transacoes_modelo(cpf_cliente);