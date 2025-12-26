/* =============================================================================
CRIAÇÃO DE TABELA: CONTRATOS CLT (DADOS EMPREGATÍCIOS DETALHADOS)
SCHEMA: ADMIN
=============================================================================

1. NOME DA TABELA: admin.pf_contratos_clt
2. RELACIONAMENTO: 
   - A coluna 'matricula_ref' é vinculada à 'matricula' da tabela 'public.pf_emprego_renda'.
   - Isso garante que os dados CLT só sejam inseridos se existir um emprego cadastrado.

=============================================================================
*/

CREATE TABLE IF NOT EXISTS admin.pf_contratos_clt (
    id SERIAL PRIMARY KEY,
    
    -- VÍNCULO COM EMPREGO/RENDA (Tabela Pública)
    matricula_ref VARCHAR(100) NOT NULL,

    -- DADOS DA EMPRESA
    nome_convenio VARCHAR(150),
    cnpj_nome VARCHAR(255),
    cnpj_numero VARCHAR(30),
    qtd_funcionarios INTEGER,
    
    -- DADOS DA ABERTURA DA EMPRESA
    data_abertura_empresa DATE,
    tempo_abertura_anos INTEGER, -- Armazena o cálculo "YY" (anos)
    
    -- CLASSIFICAÇÃO (CNAE)
    cnae_nome VARCHAR(255),
    cnae_codigo VARCHAR(50),
    
    -- DADOS DA ADMISSÃO
    data_admissao DATE,
    tempo_admissao_anos INTEGER, -- Armazena o cálculo "YY" (anos)
    
    -- CARGO (CBO)
    cbo_codigo VARCHAR(50),
    cbo_nome VARCHAR(255),
    
    -- DADOS DO INÍCIO
    data_inicio_emprego DATE,
    tempo_inicio_emprego_anos INTEGER, -- Armazena o cálculo "YY" (anos)

    -- CONTROLES DE SISTEMA
    importacao_id INTEGER, -- Para rastrear de qual planilha veio
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- CRIAÇÃO DA CHAVE ESTRANGEIRA (VÍNCULO ENTRE SCHEMAS)
    CONSTRAINT fk_clt_matricula
        FOREIGN KEY (matricula_ref)
        REFERENCES public.pf_emprego_renda (matricula)
        ON DELETE CASCADE
);