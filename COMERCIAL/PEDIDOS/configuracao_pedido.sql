-- 2. Tabela de Configurações de Pedidos (Mensagens e Grupo)
CREATE TABLE IF NOT EXISTS config_pedidos (
    id SERIAL PRIMARY KEY,
    grupo_aviso_id VARCHAR(100), -- ID do Grupo no WhatsApp para avisos internos
    
    -- Modelos de Mensagem para o Cliente
    msg_criacao TEXT,
    msg_pago TEXT,
    msg_registrar TEXT,
    msg_pendente TEXT,
    msg_cancelado TEXT
);

-- Insere uma linha padrão de configuração para não dar erro
INSERT INTO config_pedidos (id, grupo_aviso_id) 
VALUES (1, '') 
ON CONFLICT (id) DO NOTHING;