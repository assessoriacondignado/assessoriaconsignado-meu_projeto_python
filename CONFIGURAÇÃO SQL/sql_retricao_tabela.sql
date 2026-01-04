SELECT
    tc.constraint_name, 
    tc.constraint_type, -- PRIMARY KEY, FOREIGN KEY, UNIQUE
    kcu.column_name 
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
WHERE 
    tc.table_schema = 'banco_pf' -- Seu schema
    AND tc.table_name = 'pf_dados'; -- Sua tabela