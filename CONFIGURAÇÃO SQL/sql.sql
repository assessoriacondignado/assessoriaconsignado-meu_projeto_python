-- Criação da tabela para Exportação Ampla (Processos Complexos)
CREATE TABLE IF NOT EXISTS banco_pf.pf_campanhas_exportacao (
    id SERIAL PRIMARY KEY,
    nome_campanha VARCHAR(150) NOT NULL,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    objetivo TEXT,
    funcao_codigo VARCHAR(100) NOT NULL, -- Nome da função Python que será chamada
    status VARCHAR(20) DEFAULT 'ATIVO'
);

-- Comentário de ajuda para organização
COMMENT ON COLUMN banco_pf.pf_campanhas_exportacao.funcao_codigo IS 'Nome da função interna no modulo_pf_exportacao.py';