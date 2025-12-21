-- 2. Histórico de Status da Tarefa
CREATE TABLE IF NOT EXISTS tarefas_historico (
    id SERIAL PRIMARY KEY,
    id_tarefa INTEGER REFERENCES tarefas(id) ON DELETE CASCADE,
    status_novo VARCHAR(50),
    observacao TEXT,
    data_mudanca TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);