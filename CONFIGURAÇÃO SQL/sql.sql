-- 1. Renomear cpf_ref para cpf nas tabelas satélites
ALTER TABLE banco_pf.pf_telefones RENAME COLUMN cpf_ref TO cpf;
ALTER TABLE banco_pf.pf_emails RENAME COLUMN cpf_ref TO cpf;
ALTER TABLE banco_pf.pf_enderecos RENAME COLUMN cpf_ref TO cpf;

-- 2. Excluir a tabela pf_modelo_exportacao (Se existir)
DROP TABLE IF EXISTS banco_pf.pf_modelo_exportacao;