-- 1. Adiciona campos de segurança na tabela existente
ALTER TABLE clientes_usuarios 
ADD COLUMN IF NOT EXISTS senha VARCHAR(100),
ADD COLUMN IF NOT EXISTS hierarquia VARCHAR(50) DEFAULT 'Cliente',
ADD COLUMN IF NOT EXISTS usuario_pai INTEGER;

-- 2. Cria tabela de Logs de Acesso (Histórico 30 dias)
CREATE TABLE IF NOT EXISTS logs_acesso (
    id SERIAL PRIMARY KEY,
    id_usuario INTEGER,
    nome_usuario VARCHAR(200),
    ip_acesso VARCHAR(50),
    local_acesso VARCHAR(100),
    data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Cria tabela de Permissões
CREATE TABLE IF NOT EXISTS permissoes (
    id SERIAL PRIMARY KEY,
    id_usuario INTEGER REFERENCES clientes_usuarios(id),
    modulo VARCHAR(50), -- ex: 'COMERCIAL', 'FINANCEIRO'
    acesso BOOLEAN DEFAULT FALSE,
    UNIQUE(id_usuario, modulo) -- Evita duplicatas
);

-- 4. CRIA O SEU USUÁRIO ADMIN NO BANCO (Para você não ficar trancado fora!)
-- Senha inicial: 1234
INSERT INTO clientes_usuarios (nome, cpf, email, senha, hierarquia, telefone)
VALUES ('Administrador', '00000000000', 'admin', '1234', 'Admin', '000000000')
ON CONFLICT (cpf) DO NOTHING;
ALTER TABLE clientes_usuarios ADD COLUMN IF NOT EXISTS id_grupo_whats TEXT;
ALTER TABLE clientes_usuarios ADD COLUMN IF NOT EXISTS hierarquia TEXT;

-- 1. Cria a regra de e-mail único para permitir o funcionamento do "ON CONFLICT"
ALTER TABLE clientes_usuarios ADD CONSTRAINT unique_email_user UNIQUE (email);

-- 2. Adiciona a coluna CPF na tabela de clientes (que está em falta no seu print)
ALTER TABLE admin.clientes ADD COLUMN IF NOT EXISTS cpf TEXT;