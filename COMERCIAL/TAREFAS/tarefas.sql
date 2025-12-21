-- 1. Tabela Principal de Tarefas
CREATE TABLE IF NOT EXISTS tarefas (
    id SERIAL PRIMARY KEY,
    id_pedido INTEGER REFERENCES pedidos(id) ON DELETE CASCADE, -- Conexão com Pedidos
    data_previsao DATE, -- Serve para Entrega (Produto) ou Início (Serviço)
    observacao_tarefa TEXT,
    status VARCHAR(50) DEFAULT 'Solicitado',
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);