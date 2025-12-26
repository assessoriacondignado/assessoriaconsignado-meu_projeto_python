/* =============================================================================
GUIA DE CRIAÇÃO DE COLUNAS (MODULO PESSOA FÍSICA)
=============================================================================

1. TABELAS DISPONÍVEIS (Escolha qual tabela vai receber a coluna):
   - pf_dados           (Dados Pessoais: Nome, CPF, RG, Mãe, etc.)
   - pf_telefones       (Lista de Telefones)
   - pf_emails          (Lista de E-mails)
   - pf_enderecos       (Lista de Endereços)
   - pf_emprego_renda   (Dados Profissionais, Convênio, Matrícula)
   - pf_contratos       (Contratos vinculados à matrícula)

2. TIPOS DE DADOS / FORMATAÇÃO (Copie o tipo desejado):
   - TEXT               -> Texto livre (longo ou curto)
   - VARCHAR(50)        -> Texto curto (limite de 50 caracteres)
   - VARCHAR(255)       -> Texto médio (padrão para nomes, endereços)
   - DATE               -> Data apenas (DD/MM/AAAA)
   - TIMESTAMP          -> Data e Hora (DD/MM/AAAA HH:MM:SS)
   - INTEGER            -> Número inteiro (sem vírgula)
   - NUMERIC(10,2)      -> Número decimal/Moeda (ex: 1500.50)
   - BOOLEAN            -> Sim ou Não (True/False)
3. FUNÇÃO: CRIAR COLUNAS TENTRO DA TABELA

=============================================================================
*/

-- COMANDO PARA ADICIONAR COLUNA (Substitua os termos entre aspas)
-- Exemplo: Quero adicionar "nome_conjuge" na tabela "pf_dados" como texto.

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS Pis VARCHAR(50);

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  Nome_Empresa VARCHAR(50);

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  CNPJ_empresa VARCHAR(50);

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  CNAE_empresa_codigo VARCHAR(50));

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  CNAE_empresa_nome VARCHAR(50);

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  Data_AberturaEmpresa DATE;

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  QNT_funcionário_empresa INTEGER;

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  Tempo_abertura INTEGER;

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  CBO_funcionario_codigo VARCHAR(50));

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  CBO_funcionario_nome VARCHAR(50);

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  Data_inicio_emprego DATE;

ALTER TABLE admin.pf_contatos_clt 
ADD COLUMN IF NOT EXISTS  Tempo_inicio_emprego INTEGER;


-- EXEMPLOS PRÁTICOS (Pode descomentar e rodar):

-- 1. Adicionar data de admissão no emprego:
-- ALTER TABLE pf_emprego_renda ADD COLUMN data_admissao DATE;

-- 2. Adicionar limite de cartão de crédito (moeda):
-- ALTER TABLE pf_dados ADD COLUMN limite_credito NUMERIC(10,2);

-- 3. Adicionar campo de observação extra nos telefones:
-- ALTER TABLE pf_telefones ADD COLUMN obs_telefone TEXT;

-- 4. Adicionar check de 'Cliente VIP' (Sim/Não):
-- ALTER TABLE pf_dados ADD COLUMN cliente_vip BOOLEAN DEFAULT FALSE;