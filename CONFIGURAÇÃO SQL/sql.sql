-- 1. Cria a tabela com o nome CORRETO no schema 'conexoes'
CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_origem_consulta_fator (
    id SERIAL PRIMARY KEY,
    origem VARCHAR(50) UNIQUE NOT NULL, -- WEB USUÁRIO, API, LOTE
    descricao TEXT
);

-- 2. Insere os dados padrão
INSERT INTO conexoes.fatorconferi_origem_consulta_fator (origem, descricao) VALUES
('WEB USUÁRIO', 'Consulta manual realizada pelo usuário logado no painel'),
('API FATOR CONFERI', 'Consulta via integração de API (Chave do Usuário)'),
('LOTE', 'Consulta massiva via processamento de arquivos')
ON CONFLICT (origem) DO NOTHING;