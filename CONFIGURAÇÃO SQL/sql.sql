-- Criação do Schema (caso não exista)
CREATE SCHEMA IF NOT EXISTS conexoes;

-- 1. Tabela de Registro de Consultas (Log detalhado)
CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_registo_consulta (
    id SERIAL PRIMARY KEY,
    data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tipo_consulta VARCHAR(50), -- Ex: SIMPLES, COMPLETA
    cpf_consultado VARCHAR(20),
    id_usuario INTEGER, -- ID do usuário do sistema
    nome_usuario VARCHAR(255),
    valor_pago NUMERIC(10, 2), -- Custo da consulta
    caminho_json TEXT, -- Caminho do arquivo salvo na pasta CONEXÕES/JSON
    status_api VARCHAR(50) -- SUCESSO, ERRO, NAO_ENCONTRADO
);

-- 2. Tabela de Histórico de Saldo
CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_registro_de_saldo (
    id SERIAL PRIMARY KEY,
    data_consulta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valor_saldo NUMERIC(10, 2),
    observacao TEXT
);

-- 3. Tabela de Parâmetros e Regras
CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_parametros (
    id SERIAL PRIMARY KEY,
    nome_parametro VARCHAR(100) NOT NULL, -- Ex: SALDO_MINIMO_ALERTA
    valor_parametro TEXT,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    observacao TEXT, -- Tooltip/Descrição
    status VARCHAR(20) DEFAULT 'ATIVO' -- ATIVO, INATIVO
);

-- 4. Tabela de Clientes (Controle de Custo Personalizado)
-- Baseado na lógica do App Script de cobrar valores diferentes por cliente
CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_clientes_custo (
    id SERIAL PRIMARY KEY,
    id_cliente INTEGER, -- Vinculo com admin.clientes
    nome_cliente VARCHAR(255),
    custo_consulta NUMERIC(10, 2) DEFAULT 0.00,
    status VARCHAR(20) DEFAULT 'ATIVO'
);

-- Inserção de Parâmetros Padrão (Exemplo)
INSERT INTO conexoes.fatorconferi_parametros (nome_parametro, valor_parametro, observacao, status)
VALUES 
('ALERTA_SALDO_BAIXO', '5.00', 'Envia aviso quando o saldo da API for menor que o valor', 'ATIVO'),
('BLOQUEAR_CONSULTA_SEM_SALDO', 'TRUE', 'Impede consultas se o saldo estimado for insuficiente', 'ATIVO')
ON CONFLICT DO NOTHING;