-- 1. Cria o Schema (Pasta do Banco)
CREATE SCHEMA IF NOT EXISTS conexoes;

-- 2. Cria a Tabela (Planilha: Conexoes_relação)
CREATE TABLE IF NOT EXISTS conexoes.relacao (
    id SERIAL PRIMARY KEY,
    nome_conexao VARCHAR(255) NOT NULL,
    tipo_conexao VARCHAR(50), -- Ex: SAIDA, ENTRADA, API, BANCO
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    descricao TEXT,
    usuario_conexao VARCHAR(255),
    senha_conexao VARCHAR(255),
    key_conexao TEXT,
    status VARCHAR(50) DEFAULT 'ATIVO' -- ATIVO, INATIVO
);