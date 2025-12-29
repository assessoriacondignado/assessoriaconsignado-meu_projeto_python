CREATE TABLE IF NOT EXISTS banco_pf.cpf_convenio (
    id SERIAL PRIMARY KEY,
    convenio VARCHAR(100),
    cpf_ref VARCHAR(11) NOT NULL, -- Limite de 11 caracteres
    
    -- Regra de Validação (CHECK CONSTRAINT)
    -- Garante que o CPF tenha apenas números e exatamente 11 dígitos
    CONSTRAINT check_cpf_formato CHECK (cpf_ref ~ '^[0-9]{11}$')
);

-- Opcional: Cria um índice para deixar a busca por CPF muito mais rápida
CREATE INDEX IF NOT EXISTS idx_cpf_convenio_ref ON banco_pf.cpf_convenio(cpf_ref);