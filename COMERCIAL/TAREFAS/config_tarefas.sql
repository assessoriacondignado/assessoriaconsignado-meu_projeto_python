-- 3. Configuração de Mensagens Específicas para Tarefas
CREATE TABLE IF NOT EXISTS config_tarefas (
    id SERIAL PRIMARY KEY,
    grupo_aviso_id VARCHAR(100),
    
    -- Modelos de Mensagem (Status da Tarefa)
    msg_solicitado TEXT,
    msg_registro TEXT,
    msg_entregue TEXT,
    msg_em_processamento TEXT,
    msg_em_execucao TEXT,
    msg_pendente TEXT,
    msg_cancelado TEXT
);