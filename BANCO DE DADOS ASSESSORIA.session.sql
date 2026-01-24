-- 1. Colunas para segurança na tabela de usuários
ALTER TABLE admin.clientes_usuarios 
ADD COLUMN IF NOT EXISTS senha_hash VARCHAR(255),
ADD COLUMN IF NOT EXISTS bloqueado_ate TIMESTAMP WITHOUT TIME ZONE,
ADD COLUMN IF NOT EXISTS tempo_sessao_padrao INTEGER DEFAULT 60;

-- 2. Tabela para controlar Sessão Única (Token)
CREATE TABLE IF NOT EXISTS admin.sessoes_ativas (
    token VARCHAR(255) PRIMARY KEY,
    id_usuario INTEGER,
    data_inicio TIMESTAMP DEFAULT NOW(),
    ultimo_clique TIMESTAMP DEFAULT NOW()
);