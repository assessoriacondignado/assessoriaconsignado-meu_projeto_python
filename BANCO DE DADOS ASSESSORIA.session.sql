SELECT 
    id, 
    nome, 
    id_grupo_whats, 
    LENGTH(id_grupo_whats) as tamanho_string
FROM admin.clientes 
WHERE id_grupo_whats LIKE '%120363237636078252%';