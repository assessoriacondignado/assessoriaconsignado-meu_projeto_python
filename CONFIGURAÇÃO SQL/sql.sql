-- 1. Cria o Schema
CREATE SCHEMA IF NOT EXISTS permissão;

-- 2. Cria a Tabela
CREATE TABLE IF NOT EXISTS permissão.permissão_grupo_nivel (
    id SERIAL PRIMARY KEY,
    nivel VARCHAR(255)
);
DROP TABLE IF EXISTS conexoes.fatorconferi_origem_consulta;