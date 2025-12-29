-- 1. Remove qualquer coisa que não seja número do campo de vínculo (pontos, traços, espaços)
UPDATE banco_pf.pf_emprego_renda
SET cpf_ref = REGEXP_REPLACE(cpf_ref, '[^0-9]', '', 'g');

-- 2. (Opcional) Se o seu padrão é SEM zero à esquerda, rode este também:
-- Isso transforma "0123" em "123" para bater com a lógica do sistema
UPDATE banco_pf.pf_emprego_renda
SET cpf_ref = TRIM(LEADING '0' FROM cpf_ref);