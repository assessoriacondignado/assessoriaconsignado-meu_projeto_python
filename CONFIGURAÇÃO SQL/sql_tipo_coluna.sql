SELECT 
    column_name, 
    data_type, 
    character_maximum_length, 
    is_nullable, 
    column_default
FROM 
    information_schema.columns
WHERE 
    table_schema = 'banco_pf'  -- Coloque o seu schema aqui (ex: 'admin', 'public')
    AND table_name = 'pf_dados'; -- Coloque o nome da tabela aqui