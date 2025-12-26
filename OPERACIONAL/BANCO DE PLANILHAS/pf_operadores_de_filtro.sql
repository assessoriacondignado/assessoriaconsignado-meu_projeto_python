-- 1. Criação da Tabela
CREATE TABLE IF NOT EXISTS pf_operadores_de_filtro (
    id SERIAL PRIMARY KEY,
    tipo VARCHAR(20), -- texto, numero, data
    nome VARCHAR(50),
    simbolo VARCHAR(10),
    descricao VARCHAR(100),
    UNIQUE(tipo, simbolo)
);

-- 2. Inserção dos Operadores Padrão (Apenas se não existirem)
INSERT INTO pf_operadores_de_filtro (tipo, nome, simbolo, descricao) VALUES
    -- Operadores de Texto
    ('texto', 'Começa com', '=>', 'Busca registros que iniciam com o valor'),
    ('texto', 'Contém', '<=>', 'Busca o valor em qualquer parte do texto'),
    ('texto', 'Igual', '=', 'Exatamente igual'),
    ('texto', 'Seleção', 'o', 'Pesquisa múltipla (separe por vírgula)'),
    ('texto', 'Diferente', '≠', 'Diferente de'),
    ('texto', 'Não Contém', '<≠>', 'Exclui resultados que tenham essa palavra'),
    ('texto', 'Vazio', '∅', 'Campo não preenchido'),

    -- Operadores Numéricos
    ('numero', 'Igual', '=', 'Valor exato'),
    ('numero', 'Maior', '>', 'Maior que'),
    ('numero', 'Menor', '<', 'Menor que'),
    ('numero', 'Maior Igual', '≥', 'Maior ou igual a'),
    ('numero', 'Menor Igual', '≤', 'Menor ou igual a'),
    ('numero', 'Diferente', '≠', 'Diferente do valor'),
    ('numero', 'Vazio', '∅', 'Sem valor numérico'),

    -- Operadores de Data
    ('data', 'Igual', '=', 'Data exata'),
    ('data', 'A Partir', '≥', 'Desta data em diante'),
    ('data', 'Até', '≤', 'Até esta data'),
    ('data', 'Vazio', '∅', 'Sem data')
ON CONFLICT (tipo, simbolo) DO NOTHING;