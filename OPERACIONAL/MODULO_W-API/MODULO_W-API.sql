-- CRIAÇÃO DAS TABELAS DO MÓDULO W-API

-- 1. Tabela de Instâncias (Conexão com a API)
CREATE TABLE IF NOT EXISTS wapi_instancias (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    api_instance_id VARCHAR(100) NOT NULL UNIQUE,
    api_token VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'Desconectado',
    tipo VARCHAR(50) DEFAULT 'W-API',
    data_vencimento DATE
);

-- 2. Tabela de Modelos de Mensagem (Templates)
CREATE TABLE IF NOT EXISTS wapi_modelos (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100),
    objetivo VARCHAR(100), -- Ex: Vendas, Cobrança
    conteudo TEXT NOT NULL
);

-- 3. Tabela de Logs (Histórico de Envios e Recebimentos)
CREATE TABLE IF NOT EXISTS wapi_logs (
    id SERIAL PRIMARY KEY,
    data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    instance_id VARCHAR(100),
    telefone VARCHAR(20),
    nome_contato VARCHAR(100),
    mensagem TEXT,
    tipo VARCHAR(20), -- 'ENVIADA' ou 'RECEBIDA'
    status VARCHAR(50), -- 'Sucesso', 'Erro'
    cpf_cliente VARCHAR(20)
);

-- 4. Configurações do Chatbot (Para guardar o JSON dos blocos e horários)
CREATE TABLE IF NOT EXISTS wapi_chatbot_config (
    instance_id VARCHAR(100) PRIMARY KEY,
    ativo BOOLEAN DEFAULT FALSE,
    json_agendamento JSONB, -- Horários de funcionamento
    json_fluxo JSONB,       -- Blocos do robô (Menu, Perguntas)
    mensagem_offline TEXT,
    grupo_aviso VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS wapi_logs (
    id SERIAL PRIMARY KEY,
    data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    instancia TEXT,
    destinatario TEXT,
    mensagem TEXT,
    status TEXT
);
-- Adiciona colunas extras caso elas não existam na tabela wapi_logs
ALTER TABLE wapi_logs ADD COLUMN IF NOT EXISTS tipo VARCHAR(20) DEFAULT 'ENVIADA';
ALTER TABLE wapi_logs ADD COLUMN IF NOT EXISTS nome_contato VARCHAR(100);