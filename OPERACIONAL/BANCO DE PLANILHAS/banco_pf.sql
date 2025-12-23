-- 1. Tabela Principal: Dados Cadastrais
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

-- 2. Tabelas de Contato e Endereço
CREATE TABLE IF NOT EXISTS pf_telefones (
    id SERIAL PRIMARY KEY,
    cpf_ref VARCHAR(20) REFERENCES pf_dados(cpf) ON DELETE CASCADE,
    numero VARCHAR(20),
    data_atualizacao DATE,
    tag_whats VARCHAR(50),
    tag_qualificacao VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS pf_emails (
    id SERIAL PRIMARY KEY,
    cpf_ref VARCHAR(20) REFERENCES pf_dados(cpf) ON DELETE CASCADE,
    email VARCHAR(150)
);

CREATE TABLE IF NOT EXISTS pf_enderecos (
    id SERIAL PRIMARY KEY,
    cpf_ref VARCHAR(20) REFERENCES pf_dados(cpf) ON DELETE CASCADE,
    rua VARCHAR(255),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf VARCHAR(5),
    cep VARCHAR(20)
);

-- 3. DADOS PROFISSIONAIS (Novas Planilhas)

-- Tabela de Referência (Apenas armazena a lista para seleção)
CREATE TABLE IF NOT EXISTS pf_referencias (
    id SERIAL PRIMARY KEY,
    tipo VARCHAR(50), -- 'CONVENIO'
    nome VARCHAR(100),
    UNIQUE(tipo, nome)
);

-- Dados Profissionais (Emprego e Renda)
CREATE TABLE IF NOT EXISTS pf_emprego_renda (
    id SERIAL PRIMARY KEY,
    cpf_ref VARCHAR(20) REFERENCES pf_dados(cpf) ON DELETE CASCADE,
    convenio VARCHAR(100),
    matricula VARCHAR(100) UNIQUE, 
    dados_extras TEXT,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Contratos e Financiamentos (Vinculado à Matrícula)
CREATE TABLE IF NOT EXISTS pf_contratos (
    id SERIAL PRIMARY KEY,
    matricula_ref VARCHAR(100) REFERENCES pf_emprego_renda(matricula) ON DELETE CASCADE,
    contrato VARCHAR(100),
    dados_extras TEXT,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Carga inicial básica de referências (opcional, para a lista não vir vazia)
INSERT INTO pf_referencias (tipo, nome) VALUES ('CONVENIO', 'INSS'), ('CONVENIO', 'SIAPE'), ('CONVENIO', 'FGTS') ON CONFLICT DO NOTHING;