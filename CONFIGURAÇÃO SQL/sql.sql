ALTER TABLE cliente.cliente_carteira_lista
ADD COLUMN IF NOT EXISTS cpf_usuario VARCHAR(20),
ADD COLUMN IF NOT EXISTS nome_usuario VARCHAR(255);