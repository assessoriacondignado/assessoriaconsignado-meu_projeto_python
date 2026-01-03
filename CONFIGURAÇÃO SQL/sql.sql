CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_ambiente_consulta (
    id SERIAL PRIMARY KEY,
    ambiente VARCHAR(255),
    origem VARCHAR(255)
);