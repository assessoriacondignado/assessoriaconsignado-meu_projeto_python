-- 1. Renomeia a tabela física
ALTER TABLE banco_pf.pf_contratos_clt 
RENAME TO pf_matricula_dados_clt;

-- 2. Atualiza o mapeamento para que o sistema saiba onde buscar os dados do convênio 'CLT'
UPDATE banco_pf.convenio_por_planilha 
SET nome_planilha_sql = 'banco_pf.pf_matricula_dados_clt' 
WHERE convenio = 'CLT';