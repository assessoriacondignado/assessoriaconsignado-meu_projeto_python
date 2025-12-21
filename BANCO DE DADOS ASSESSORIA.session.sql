-- Cria a tabela completa de Clientes/Usuários
CREATE TABLE IF NOT EXISTS clientes_usuarios (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(200) NOT NULL,
    cpf VARCHAR(20) UNIQUE NOT NULL,
    telefone VARCHAR(20) NOT NULL,
    is_whatsapp BOOLEAN DEFAULT FALSE,
    email VARCHAR(200),
    dados_bancarios TEXT,
    grupo_whats VARCHAR(100),
    observacao TEXT,
    pasta_caminho VARCHAR(300),
    ativo BOOLEAN DEFAULT TRUE,
    data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);