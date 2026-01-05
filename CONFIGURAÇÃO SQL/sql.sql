SELECT d.nome, d.cpf, e.email 
FROM banco_pf.pf_dados d
JOIN banco_pf.pf_emails e ON d.cpf = e.cpf
WHERE d.nome ILIKE '%NOME DO CLIENTE%';