-- 1. Adicionar as colunas que faltam na tabela tarefas
ALTER TABLE tarefas ADD COLUMN id_cliente INTEGER;
ALTER TABLE tarefas ADD COLUMN id_produto INTEGER;

-- 2. Criar as conexões (Chaves Estrangeiras) para garantir que o cliente exista
-- ATENÇÃO: Se o seu banco não usa o esquema 'admin', remova o 'admin.' antes de 'clientes'
ALTER TABLE tarefas ADD CONSTRAINT fk_tarefas_cliente FOREIGN KEY (id_cliente) REFERENCES admin.clientes(id);
ALTER TABLE tarefas ADD CONSTRAINT fk_tarefas_produto FOREIGN KEY (id_produto) REFERENCES produtos_servicos(id);