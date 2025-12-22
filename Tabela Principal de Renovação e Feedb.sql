-- Tabela Principal de Renovação e Feedback
CREATE TABLE IF NOT EXISTS renovacao_feedback (
    id SERIAL PRIMARY KEY,
    id_pedido INTEGER REFERENCES pedidos(id) ON DELETE CASCADE,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_previsao DATE,
    observacao TEXT,
    status VARCHAR(50) DEFAULT 'Entrada', -- Entrada, Em Análise, Concluído, etc.
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Histórico de Status
CREATE TABLE IF NOT EXISTS renovacao_feedback_historico (
    id SERIAL PRIMARY KEY,
    id_rf INTEGER REFERENCES renovacao_feedback(id) ON DELETE CASCADE,
    status_novo VARCHAR(50),
    observacao TEXT,
    data_mudanca TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Configurações de Mensagens
CREATE TABLE IF NOT EXISTS config_renovacao_feedback (
    id SERIAL PRIMARY KEY,
    msg_entrada TEXT,
    msg_processamento TEXT,
    msg_concluido TEXT
);

INSERT INTO config_renovacao_feedback (id) VALUES (1) ON CONFLICT (id) DO NOTHING;