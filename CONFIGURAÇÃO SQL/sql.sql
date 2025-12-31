-- TABELAS PARA GESTÃO FINANCEIRA DE CLIENTES (FATOR CONFERI)

-- 1. Carteira do Cliente (Saldo e Configurações)
CREATE TABLE IF NOT EXISTS conexoes.fator_cliente_carteira (
    id SERIAL PRIMARY KEY,
    id_cliente_admin INTEGER REFERENCES admin.clientes(id), -- Vínculo com cadastro principal
    nome_cliente VARCHAR(255),
    custo_por_consulta NUMERIC(10, 2) DEFAULT 0.50,
    saldo_atual NUMERIC(10, 2) DEFAULT 0.00,
    status VARCHAR(20) DEFAULT 'ATIVO',
    data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_cliente_fator UNIQUE (id_cliente_admin)
);

-- 2. Histórico de Transações (Extrato)
CREATE TABLE IF NOT EXISTS conexoes.fator_cliente_transacoes (
    id SERIAL PRIMARY KEY,
    id_carteira INTEGER REFERENCES conexoes.fator_cliente_carteira(id) ON DELETE CASCADE,
    tipo VARCHAR(20), -- 'CREDITO' (Recarga) ou 'DEBITO' (Consulta)
    valor NUMERIC(10, 2),
    saldo_anterior NUMERIC(10, 2),
    saldo_novo NUMERIC(10, 2),
    motivo VARCHAR(255), -- Ex: "Recarga via PIX", "Consulta CPF 123..."
    data_transacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario_responsavel VARCHAR(100) -- Quem fez a operação (Sistema ou Admin)
);