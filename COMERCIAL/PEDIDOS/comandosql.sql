-- Adiciona colunas para vínculo direto na tabela de Tarefas
ALTER TABLE tarefas 
ADD COLUMN IF NOT EXISTS id_cliente INTEGER REFERENCES admin.clientes(id),
ADD COLUMN IF NOT EXISTS id_produto INTEGER REFERENCES produtos_servicos(id);

-- (Opcional) Se quiser garantir integridade, crie os índices
CREATE INDEX IF NOT EXISTS idx_tarefas_cliente ON tarefas(id_cliente);
CREATE INDEX IF NOT EXISTS idx_tarefas_produto ON tarefas(id_produto);