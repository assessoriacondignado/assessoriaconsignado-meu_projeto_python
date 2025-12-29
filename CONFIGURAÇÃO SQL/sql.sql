-- Adiciona a coluna TAG na tabela de contratos CLT
ALTER TABLE banco_pf.pf_contratos_clt 
ADD COLUMN IF NOT EXISTS tag TEXT;