-- Adiciona a coluna id_campanha na tabela de dados pessoais
ALTER TABLE pf_dados 
ADD COLUMN IF NOT EXISTS id_campanha VARCHAR(50);