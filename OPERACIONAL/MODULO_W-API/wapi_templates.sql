CREATE TABLE IF NOT EXISTS wapi_templates (
    id SERIAL PRIMARY KEY,
    modulo VARCHAR(50) NOT NULL, -- Ex: PEDIDOS, TAREFAS, RENOVACAO
    chave_status VARCHAR(50) NOT NULL, -- Ex: criacao, pago, pendente
    conteudo_mensagem TEXT,
    UNIQUE(modulo, chave_status)
);