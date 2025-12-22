UPDATE clientes_usuarios 
SET senha = '1234', tentativas_falhas = 0 
WHERE email = 'admin';