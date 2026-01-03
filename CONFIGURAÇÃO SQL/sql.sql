-- 1. Adicionar novas colunas na tabela de Origem Fator
ALTER TABLE conexoes.fatorconferi_origem_consulta_fator
ADD COLUMN IF NOT EXISTS produto VARCHAR(255),
ADD COLUMN IF NOT EXISTS carteira_vinculada VARCHAR(255);

-- 2. Excluir a tabela antiga (se existir)
DROP TABLE IF EXISTS conexoes.fatorconferi_origem_consulta;