CREATE TABLE IF NOT EXISTS produtos_servicos (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) NOT NULL,
    nome VARCHAR(255) NOT NULL,
    tipo VARCHAR(50) NOT NULL,
    resumo TEXT,
    preco DECIMAL(10, 2),
    caminho_pasta TEXT,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP,
    obs_atualizacao TEXT,
    ativo BOOLEAN DEFAULT TRUE
);