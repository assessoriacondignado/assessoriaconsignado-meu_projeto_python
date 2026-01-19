CREATE TABLE IF NOT EXISTS sistema_consulta.importacao_staging_convenio_clt (
    sessao_id UUID,                 -- Controle do sistema (obrigatório para staging)
    matricula VARCHAR(100),
    convenio VARCHAR(150),
    cnpj_nome VARCHAR(255),
    cnpj_numero VARCHAR(30),
    qtd_funcionarios VARCHAR(255),
    data_abertura_empresa DATE,     -- Se a planilha tiver data formatada, pode precisar ser TEXT temporariamente
    cnae_nome VARCHAR(255),
    cnae_codigo VARCHAR(50),
    data_admissao DATE,
    cbo_codigo VARCHAR(50),
    cbo_nome VARCHAR(255),
    data_inicio_emprego DATE,
    importacao_id VARCHAR(255)      -- Para vincular ao histórico
);