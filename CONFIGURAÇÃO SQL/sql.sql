-- 1. Criação da Tabela de Valores
CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_valor_da_consulta (
    id SERIAL PRIMARY KEY,
    valor_da_consulta NUMERIC(10, 2),
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Inserção de valor inicial (R$ 0,50) caso a tabela esteja vazia
INSERT INTO conexoes.fatorconferi_valor_da_consulta (valor_da_consulta)
SELECT 0.50
WHERE NOT EXISTS (SELECT 1 FROM conexoes.fatorconferi_valor_da_consulta);