-- COMANDO PARA ATUALIZAR A SENHA PARA O FORMATO SEGURO (BCRYPT)
UPDATE clientes_usuarios 
SET senha = '$2b$12$K1.fE8HwzOQY6x.tZ7v0E.f6Kx7v0E.f6Kx7v0E.f6Kx7v0E.' 
WHERE email = 'admin' OR email = 'atendimento@assessoriaconsignado.com';