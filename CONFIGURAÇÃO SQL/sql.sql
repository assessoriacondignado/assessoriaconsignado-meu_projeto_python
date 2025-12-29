-- 1. Remove a restrição que impede a mudança (Chave Estrangeira)
ALTER TABLE banco_pf.pf_dados 
DROP CONSTRAINT IF EXISTS pf_dados_importacao_id_fkey;

-- 2. Agora altera a coluna para TEXTO para aceitar listas (ex: "10, 12, 15")
ALTER TABLE banco_pf.pf_dados 
ALTER COLUMN importacao_id TYPE VARCHAR(255);