-- 1. Garante que as colunas suportem os novos dados (Texto longo para listas)
ALTER TABLE permissão.permissão_usuario_regras_nível 
ALTER COLUMN chave TYPE TEXT,
ALTER COLUMN nivel TYPE TEXT;

-- 2. Criação das tabelas de apoio (se não existirem)
CREATE TABLE IF NOT EXISTS permissão.permissão_usuario_cheve (
    id SERIAL PRIMARY KEY,
    chave VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS permissão.permissão_usuario_categoria (
    id SERIAL PRIMARY KEY,
    categoria VARCHAR(255)
);