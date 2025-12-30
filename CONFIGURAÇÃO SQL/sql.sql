-- 1. Excluir as colunas indesejadas
ALTER TABLE banco_pf.pf_modelos_exportacao 
DROP COLUMN IF EXISTS tipo_processamento,
DROP COLUMN IF EXISTS colunas_visiveis;

-- 2. Incluir a nova coluna 'codigo_de_consulta' 
-- O tipo TEXT é o ideal para textos longos com parágrafos
ALTER TABLE banco_pf.pf_modelos_exportacao 
ADD COLUMN IF NOT EXISTS codigo_de_consulta TEXT;

-- 3. Comentário opcional para organização no banco de dados
COMMENT ON COLUMN banco_pf.pf_modelos_exportacao.codigo_de_consulta IS 'Armazena os códigos de consulta formatados com parágrafos';