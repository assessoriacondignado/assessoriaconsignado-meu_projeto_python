-- =================================================================
-- AJUSTE DE PERFORMANCE: TABELA DE LOGS (SCHEMA CONEXOES)
-- =================================================================

-- 1. Criar coluna numérica para o CPF (Log mais leve e rápido)
ALTER TABLE conexoes.fatorconferi_registo_consulta
ADD COLUMN cpf_consultado_num BIGINT;

-- 2. Migrar os dados antigos (Limpa pontos e traços e converte)
-- (Isso pode demorar alguns segundos dependendo do tamanho do log atual)
UPDATE conexoes.fatorconferi_registo_consulta
SET cpf_consultado_num = CAST(NULLIF(regexp_replace(cpf_consultado, '[^0-9]', '', 'g'), '') AS BIGINT);

-- 3. Criar Índice (Essencial para relatórios: "Quem consultou esse CPF?")
CREATE INDEX idx_log_cpf_num ON conexoes.fatorconferi_registo_consulta(cpf_consultado_num);

-- 4. Criar Índice de Data (Essencial para relatórios: "Consultas de ontem")
CREATE INDEX idx_log_data ON conexoes.fatorconferi_registo_consulta(data_hora);