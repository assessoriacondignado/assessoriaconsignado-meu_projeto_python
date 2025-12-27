-- 1. Criação da Tabela de Registro de Números (Conciliação)
CREATE TABLE IF NOT EXISTS wapi_numeros (
    id SERIAL PRIMARY KEY,
    telefone VARCHAR(30) UNIQUE NOT NULL,
    id_cliente INTEGER REFERENCES admin.clientes(id) ON DELETE SET NULL,
    nome_cliente VARCHAR(255), -- Cache do nome para facilitar visualização
    data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_ultima_interacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Atualização da Tabela de Logs (Para histórico vinculado)
ALTER TABLE wapi_logs 
ADD COLUMN IF NOT EXISTS id_cliente INTEGER REFERENCES admin.clientes(id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS nome_cliente VARCHAR(255);

-- 3. Cria índices para performance
CREATE INDEX IF NOT EXISTS idx_wapi_numeros_tel ON wapi_numeros(telefone);
CREATE INDEX IF NOT EXISTS idx_wapi_logs_tel ON wapi_logs(telefone);