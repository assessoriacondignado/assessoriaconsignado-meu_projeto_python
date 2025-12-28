-- 1. Cria a nova "pasta" (Schema)
CREATE SCHEMA IF NOT EXISTS banco_pf;

-- 2. Move as tabelas do schema 'admin' para o novo schema 'banco_pf'
ALTER TABLE admin.pf_contratos_clt SET SCHEMA banco_pf;

-- (Opcional) Se a tabela de contratos CLT também for movida:
-- ALTER TABLE admin.pf_contratos_clt SET SCHEMA banco_pf;