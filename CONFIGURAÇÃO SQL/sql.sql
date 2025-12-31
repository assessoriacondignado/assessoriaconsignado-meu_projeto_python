-- 1. Atualiza a Tabela de Clientes
ALTER TABLE admin.clientes
ADD COLUMN IF NOT EXISTS nome_empresa VARCHAR(255),
ADD COLUMN IF NOT EXISTS cnpj_empresa VARCHAR(20),
ADD COLUMN IF NOT EXISTS ids_agrupamento_empresa TEXT, -- Armazena IDs como texto "1;2;3"
ADD COLUMN IF NOT EXISTS ids_agrupamento_cliente TEXT, -- Armazena IDs como texto "1;2;3"
ADD COLUMN IF NOT EXISTS telefone2 VARCHAR(20);

-- 2. Tabela de Agrupamento de Clientes
CREATE TABLE IF NOT EXISTS admin.agrupamento_clientes (
    id SERIAL PRIMARY KEY,
    nome_agrupamento VARCHAR(100) NOT NULL UNIQUE
);

-- 3. Tabela de Agrupamento de Empresas
CREATE TABLE IF NOT EXISTS admin.agrupamento_empresas (
    id SERIAL PRIMARY KEY,
    nome_agrupamento VARCHAR(100) NOT NULL UNIQUE
);