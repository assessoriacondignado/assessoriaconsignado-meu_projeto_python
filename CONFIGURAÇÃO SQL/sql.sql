-- Garante tipos TEXT para listas longas
ALTER TABLE permissão.permissão_usuario_regras_nível 
ALTER COLUMN nivel TYPE TEXT,
ALTER COLUMN chave TYPE TEXT,
ALTER COLUMN caminho_bloqueio TYPE TEXT;