-- Sincroniza a sequência da tabela de TELEFONES
SELECT setval(
    pg_get_serial_sequence('sistema_consulta.sistema_consulta_dados_cadastrais_telefone', 'id'),
    COALESCE((SELECT MAX(id) FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone) + 1, 1),
    false
);