-- Adiciona coluna para vincular o ID do usuário de login ao cadastro do cliente
ALTER TABLE admin.clientes ADD COLUMN IF NOT EXISTS id_usuario_vinculo INTEGER REFERENCES clientes_usuarios(id) ON DELETE SET NULL;

-- Adiciona coluna de status no cliente
ALTER TABLE admin.clientes ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'ATIVO';