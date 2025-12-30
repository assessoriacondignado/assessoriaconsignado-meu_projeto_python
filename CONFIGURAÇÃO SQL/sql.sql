-- Criação da tabela para salvar os Modelos de Filtro Fixo
CREATE TABLE IF NOT EXISTS banco_pf.pf_modelos_filtro_fixo (
    id SERIAL PRIMARY KEY,
    nome_modelo VARCHAR(150) NOT NULL,
    tabela_alvo VARCHAR(100) NOT NULL, -- Ex: pf_dados
    coluna_alvo VARCHAR(100) NOT NULL, -- Ex: id_campanha
    resumo TEXT,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);