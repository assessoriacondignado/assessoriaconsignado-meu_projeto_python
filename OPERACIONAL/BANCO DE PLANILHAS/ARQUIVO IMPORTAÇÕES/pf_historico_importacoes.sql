CREATE TABLE IF NOT EXISTS pf_historico_importacoes (
    id SERIAL PRIMARY KEY,
    nome_arquivo VARCHAR(255),
    data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    qtd_novos INTEGER DEFAULT 0,
    qtd_atualizados INTEGER DEFAULT 0,
    qtd_erros INTEGER DEFAULT 0,
    caminho_arquivo_original TEXT,
    caminho_arquivo_erro TEXT
);