/* =============================================================================
CRIAÇÃO DE TABELA: CLIENTE_CNPJ
SCHEMA: ADMIN
============================================================================= */

CREATE TABLE IF NOT EXISTS admin.cliente_cnpj (
    -- Identificador único padrão
    id SERIAL PRIMARY KEY,

    -- 3.1 CNPJ (Formatado com pontuação)
    -- VARCHAR(20) é suficiente para "XX.XXX.XXX/XXXX-XX" (18 caracteres)
    cnpj VARCHAR(20) UNIQUE, 

    -- 3.2 NOME EMPRESA (Texto Curto)
    nome_empresa VARCHAR(255),

    -- Colunas de controle padrão do sistema (Recomendado manter)
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);