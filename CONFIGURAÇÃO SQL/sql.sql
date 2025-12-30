CREATE TABLE IF NOT EXISTS banco_pf.pf_tipos_exportacao (
    id SERIAL PRIMARY KEY,
    nome_exportacao VARCHAR(150) NOT NULL,
    colunas_exportacao TEXT, -- Guardará o JSON com a lista e ordem das colunas
    modulo_vinculado VARCHAR(50), -- 'CAMPANHA' ou 'PESQUISA_AMPLA'
    descricao TEXT,
    status VARCHAR(20) DEFAULT 'ATIVO',
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);