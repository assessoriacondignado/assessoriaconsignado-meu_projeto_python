SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_schema = 'banco_pf' 
AND table_name = 'pf_endereco'
AND column_name IN ('cpf');