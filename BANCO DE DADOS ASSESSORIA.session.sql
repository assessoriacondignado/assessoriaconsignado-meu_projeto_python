-- Criação da Tabela de Palco (Staging)
CREATE TABLE IF NOT EXISTS sistema_consulta.importacao_staging (
    sessao_id UUID,              -- Identificador único da importação (para não misturar usuários)
    cpf VARCHAR(20),
    nome TEXT,
    identidade TEXT,
    data_nascimento DATE,
    sexo TEXT,
    nome_mae TEXT,
    cnh TEXT,
    titulo_eleitoral TEXT,
    convenio TEXT,
    cep TEXT,
    rua TEXT,
    cidade TEXT,
    uf TEXT,
    -- Campos para os 10 telefones
    telefone_1 TEXT, telefone_2 TEXT, telefone_3 TEXT, telefone_4 TEXT, telefone_5 TEXT,
    telefone_6 TEXT, telefone_7 TEXT, telefone_8 TEXT, telefone_9 TEXT, telefone_10 TEXT,
    -- Campos para os 3 emails
    email_1 TEXT, email_2 TEXT, email_3 TEXT
);

-- Índice para acelerar o processamento
CREATE INDEX IF NOT EXISTS idx_staging_sessao ON sistema_consulta.importacao_staging(sessao_id);