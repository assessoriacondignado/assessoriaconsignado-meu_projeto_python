-- 1. Tabela de Pedidos
CREATE TABLE IF NOT EXISTS pedidos (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE NOT NULL,
    
    -- Dados do Cliente (Cópia para histórico)
    id_cliente INTEGER,
    nome_cliente VARCHAR(255),
    cpf_cliente VARCHAR(20),
    telefone_cliente VARCHAR(20),
    
    -- Dados do Produto
    id_produto INTEGER,
    nome_produto VARCHAR(255),
    categoria_produto VARCHAR(100),
    
    -- Detalhes do Pedido
    quantidade INTEGER DEFAULT 1,
    valor_unitario DECIMAL(10, 2),
    valor_total DECIMAL(10, 2),
    
    status VARCHAR(50) DEFAULT 'Solicitado', -- Solicitado, Pago, Registrar, Pendente, Cancelado
    
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    observacao TEXT
);