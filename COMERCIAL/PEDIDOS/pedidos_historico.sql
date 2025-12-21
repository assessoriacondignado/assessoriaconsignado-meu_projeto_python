CREATE TABLE IF NOT EXISTS pedidos_historico (
    id SERIAL PRIMARY KEY,
    id_pedido INTEGER REFERENCES pedidos(id) ON DELETE CASCADE,
    status_novo VARCHAR(50),
    observacao TEXT,
    data_mudanca TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);