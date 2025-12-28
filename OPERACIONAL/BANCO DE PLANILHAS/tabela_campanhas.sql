-- 1. Cria a tabela de Campanhas conforme especificado
CREATE TABLE IF NOT EXISTS pf_campanhas (
    id SERIAL PRIMARY KEY,
    nome_campanha VARCHAR(100) NOT NULL,
    data_criacao DATE DEFAULT CURRENT_DATE,
    filtros_config TEXT,      -- JSON técnico para o sistema ler
    filtros_aplicaveis TEXT,  -- Texto legível para exibição (Ex: Idade > 30...)
    objetivo TEXT,
    status VARCHAR(20) DEFAULT 'ATIVO' -- ATIVO/INATIVO
);

-- 2. Garante que a tabela de dados dos clientes tenha a coluna id_campanha
ALTER TABLE pf_dados 
ADD COLUMN IF NOT EXISTS id_campanha VARCHAR(50);