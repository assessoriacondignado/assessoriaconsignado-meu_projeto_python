-- Criação da tabela de histórico de importações no schema sistema_consulta
CREATE TABLE IF NOT EXISTS sistema_consulta.sistema_consulta_importacao (
    id SERIAL PRIMARY KEY,
    nome_arquivo TEXT,
    data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    qtd_novos TEXT,        -- Solicitado como texto curto
    qtd_atualizados TEXT,  -- Solicitado como texto curto
    qtd_erros TEXT,        -- Solicitado como texto curto
    caminho_arquivo_original TEXT,
    caminho_arquivo_erro TEXT
);