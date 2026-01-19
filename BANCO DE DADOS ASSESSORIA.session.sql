-- 1. Adicionar CPF na tabela de Staging (Temporária)
ALTER TABLE sistema_consulta.importacao_staging_convenio_clt 
ADD COLUMN IF NOT EXISTS cpf VARCHAR(20);

-- 2. Adicionar CPF na tabela Final (Dados CTT)
ALTER TABLE sistema_consulta.sistema_consulta_dados_ctt 
ADD COLUMN IF NOT EXISTS cpf VARCHAR(20);