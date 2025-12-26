/* =============================================================================
GUIA DE CRIAÇÃO DE NOVAS TABELAS (VIA COMANDO SQL)
=============================================================================

1. ESCOLHA O LOCAL (SCHEMA):
   - O padrão é 'public' (acessível por todo o sistema).
   - Se for algo restrito ou administrativo, use 'admin'.
   - Exemplo: CREATE TABLE public.nome_tabela OU CREATE TABLE admin.nome_tabela

2. TIPOS DE DADOS / FORMATAÇÃO (Copie o tipo desejado):
   - TEXT               -> Texto livre (longo ou curto, ideal para observações)
   - VARCHAR(255)       -> Texto médio (padrão para Nomes, E-mails, Ruas)
   - VARCHAR(50)        -> Texto curto (padrão para Status, Categorias, Códigos)
   - VARCHAR(20)        -> Texto muito curto (padrão para CPF, Telefone, CEP)
   - DATE               -> Data apenas (DD/MM/AAAA) - Ex: Nascimentos, Vencimentos
   - TIMESTAMP          -> Data e Hora (DD/MM/AAAA HH:MM:SS) - Ex: Registros de log
   - INTEGER            -> Número inteiro (sem vírgula) - Ex: Quantidade, Idade
   - NUMERIC(10,2)      -> Número decimal/Moeda (ex: 1500.50) - Ex: Preços, Salários
   - BOOLEAN            -> Sim ou Não (True/False) - Ex: Ativo, Pago, Verificado
   - SERIAL             -> Contador automático (Exclusivo para o ID)

=============================================================================
*/

-- COMANDO MODELO (Substitua os termos entre aspas ou os nomes de exemplo)
-- Dica: Use nomes sem espaços e sem acentos (use_underline_para_separar)

CREATE TABLE IF NOT EXISTS admin.pf_contratos_SIAPE(
    -- Coluna Obrigatória (Identificador Único)
    id SERIAL PRIMARY KEY,

    -- COLOQUE SUAS NOVAS COLUNAS AQUI:
    Matricula_ref VARCHAR(50),
    Data_atualização DATE,
    
    -- Colunas de Controle (Recomendado manter para auditoria)
    observacao TEXT,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- EXEMPLO PRÁTICO (Para criar uma tabela de Histórico de Viagens):
-- =============================================================================
/*
CREATE TABLE IF NOT EXISTS public.pf_viagens_cliente (
    id SERIAL PRIMARY KEY,
    cpf_cliente VARCHAR(20),      -- Para vincular com a pessoa física
    destino VARCHAR(100),
    data_ida DATE,
    data_volta DATE,
    valor_pacote NUMERIC(10,2),
    pacote_pago BOOLEAN DEFAULT FALSE,
    detalhes_voo TEXT,
    data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
*/