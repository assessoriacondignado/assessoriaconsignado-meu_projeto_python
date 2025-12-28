CREATE TABLE IF NOT EXISTS banco_pf.pf_modelos_exportacao (
    id SERIAL PRIMARY KEY,
    nome_modelo VARCHAR(100) NOT NULL,
    tipo_processamento VARCHAR(50), -- Ex: 'SIMPLES', 'CONTRATOS_DETALHADO'
    colunas_visiveis TEXT,          -- JSON ou Lista de colunas para o simples
    descricao TEXT,
    data_criacao DATE DEFAULT CURRENT_DATE,
    status VARCHAR(20) DEFAULT 'ATIVO'
);

-- Inserção de modelos padrão iniciais
INSERT INTO banco_pf.pf_modelos_exportacao (nome_modelo, tipo_processamento, descricao)
VALUES 
('Padrão - Lista Simples', 'SIMPLES', 'Exporta dados básicos (Nome, CPF, Telefone)'),
('Completo - Contratos e Vínculos', 'CONTRATOS_DETALHADO', 'Gera relatório com 1 linha por contrato, agrupando telefones e endereços.')
ON CONFLICT DO NOTHING;