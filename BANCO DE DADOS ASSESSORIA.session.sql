-- 1. Apagar os telefones duplicados (mantendo apenas o mais recente ou o primeiro)
DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone a
USING sistema_consulta.sistema_consulta_dados_cadastrais_telefone b
WHERE a.id < b.id
  AND a.cpf = b.cpf
  AND a.telefone = b.telefone;

-- 2. Criar a "Trava" de Unicidade
-- Isso impede que o banco aceite CPF + Telefone repetidos
ALTER TABLE sistema_consulta.sistema_consulta_dados_cadastrais_telefone
ADD CONSTRAINT unique_cpf_telefone UNIQUE (cpf, telefone);