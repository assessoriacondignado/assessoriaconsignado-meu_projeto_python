-- =================================================================
-- PADRONIZAÇÃO FINANCEIRA SCHEMA ADMIN -> NUMERIC(15, 2)
-- =================================================================

-- 1. Tabela PEDIDOS
ALTER TABLE admin.pedidos
    ALTER COLUMN valor_unitario TYPE NUMERIC(15, 2) 
    USING (REPLACE(CAST(valor_unitario AS TEXT), ',', '.')::NUMERIC(15, 2)),

    ALTER COLUMN valor_total TYPE NUMERIC(15, 2) 
    USING (REPLACE(CAST(valor_total AS TEXT), ',', '.')::NUMERIC(15, 2)),

    ALTER COLUMN custo_carteira TYPE NUMERIC(15, 2) 
    USING (REPLACE(CAST(custo_carteira AS TEXT), ',', '.')::NUMERIC(15, 2));

-- 2. Tabela PRODUTOS E SERVIÇOS
ALTER TABLE admin.produtos_servicos
    ALTER COLUMN preco TYPE NUMERIC(15, 2) 
    USING (REPLACE(CAST(preco AS TEXT), ',', '.')::NUMERIC(15, 2));