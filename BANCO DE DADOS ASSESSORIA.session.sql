-- RODAR NO SEU BANCO DE DADOS
ALTER TABLE sistema_consulta.sistema_consulta_importacao 
ADD COLUMN IF NOT EXISTS id_usuario TEXT;

ALTER TABLE sistema_consulta.sistema_consulta_importacao 
ADD COLUMN IF NOT EXISTS nome_usuario TEXT;