-- 1. Cria a tabela de Tipos de Importação
CREATE TABLE IF NOT EXISTS sistema_consulta.sistema_importacao_tipo (
    id SERIAL PRIMARY KEY,
    convenio TEXT,
    nome_planilha TEXT, -- Nome da tabela de staging (ex: sistema_consulta.importacao_staging)
    colunas_filtro TEXT -- JSON com a lista de colunas
);

-- 2. Atualiza a tabela de Staging existente para suportar novos campos
ALTER TABLE sistema_consulta.importacao_staging ADD COLUMN IF NOT EXISTS nome_pai TEXT;
ALTER TABLE sistema_consulta.importacao_staging ADD COLUMN IF NOT EXISTS campanhas TEXT;
ALTER TABLE sistema_consulta.importacao_staging ADD COLUMN IF NOT EXISTS bairro TEXT;