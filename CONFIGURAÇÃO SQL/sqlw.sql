CREATE TABLE IF NOT EXISTS banco_pf.convenio_por_planilha (
    id SERIAL PRIMARY KEY,
    convenio VARCHAR(100) NOT NULL,       -- Nome do Convênio (Ex: INSS, SIAPE)
    nome_planilha_sql VARCHAR(100) NOT NULL, -- Nome da tabela no banco (Ex: pf_contratos_inss)
    
    -- Opcional: Garante que não tenha dois cadastros iguais
    UNIQUE(convenio, nome_planilha_sql)
);