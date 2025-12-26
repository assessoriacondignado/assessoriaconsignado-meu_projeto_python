-- DEFINE A SENHA COMO '1234' EM TEXTO COMUM (PARA RECUPERAR ACESSO)
UPDATE clientes_usuarios 
SET senha = '1234' 
WHERE email = 'admin' OR email = 'atendimento@assessoriaconsignado.com';