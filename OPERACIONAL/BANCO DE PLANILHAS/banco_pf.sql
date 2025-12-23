-- Tabela Principal: Dados Cadastrais
CREATE TABLE IF NOT EXISTS pf_dados (
    id SERIAL PRIMARY KEY,
    cpf VARCHAR(20) UNIQUE NOT NULL,
    nome VARCHAR(255) NOT NULL,
    data_nascimento DATE,
    rg VARCHAR(30),
    uf_rg VARCHAR(5),
    data_exp_rg DATE,
    cnh VARCHAR(50),
    pis VARCHAR(50),
    ctps_serie VARCHAR(50),
    nome_mae VARCHAR(255),
    nome_pai VARCHAR(255),
    nome_procurador VARCHAR(255),
    cpf_procurador VARCHAR(20),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela: Telefones (Vinculado por CPF)
CREATE TABLE IF NOT EXISTS pf_telefones (
    id SERIAL PRIMARY KEY,
    cpf_ref VARCHAR(20) REFERENCES pf_dados(cpf) ON DELETE CASCADE,
    numero VARCHAR(20),
    data_atualizacao DATE,
    tag_whats VARCHAR(50),
    tag_qualificacao VARCHAR(50)
);

-- Tabela: E-mails (Vinculado por CPF)
CREATE TABLE IF NOT EXISTS pf_emails (
    id SERIAL PRIMARY KEY,
    cpf_ref VARCHAR(20) REFERENCES pf_dados(cpf) ON DELETE CASCADE,
    email VARCHAR(150)
);

-- Tabela: Endereços (Vinculado por CPF)
CREATE TABLE IF NOT EXISTS pf_enderecos (
    id SERIAL PRIMARY KEY,
    cpf_ref VARCHAR(20) REFERENCES pf_dados(cpf) ON DELETE CASCADE,
    rua VARCHAR(255),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf VARCHAR(5),
    cep VARCHAR(20)
);