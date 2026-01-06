-- 1. Adiciona a coluna 'origem_custo' (Texto) na tabela de PEDIDOS
ALTER TABLE pedidos 
ADD COLUMN IF NOT EXISTS origem_custo TEXT;

-- 2. Ajusta a tabela de CUSTOS para aceitar TEXTO na coluna 'origem_custo'
-- (Isso é importante pois antes estava como NUMERIC e daria erro ao tentar salvar "Fator")
ALTER TABLE cliente.valor_custo_carteira_cliente 
ALTER COLUMN origem_custo TYPE TEXT USING origem_custo::TEXT;