DROP TABLE IF EXISTS banco_pf.pf_modelos_filtro_fixo;

CREATE TABLE IF NOT EXISTS banco_pf.pf_modelos_filtro_fixo (
    id SERIAL PRIMARY KEY,
    nome_modelo VARCHAR(150) NOT NULL,
    tabela_alvo VARCHAR(100) NOT NULL,
    coluna_alvo TEXT NOT NULL, -- Agora é TEXT para suportar lista (JSON)
    resumo TEXT,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);