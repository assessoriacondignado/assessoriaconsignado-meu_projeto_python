-- Criação das novas tabelas no schema 'permissão'

CREATE TABLE IF NOT EXISTS permissão.permissão_usuario_cheve (
    id SERIAL PRIMARY KEY,
    chave VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS permissão.permissão_usuario_categoria (
    id SERIAL PRIMARY KEY,
    categoria VARCHAR(255)
);