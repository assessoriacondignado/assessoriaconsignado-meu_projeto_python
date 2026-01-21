CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_conexao_tabelas (
    id SERIAL PRIMARY KEY,
    tabela_referencia TEXT,
    tabela_referencia_coluna TEXT,
    jason_api_fatorconferi_coluna TEXT
);