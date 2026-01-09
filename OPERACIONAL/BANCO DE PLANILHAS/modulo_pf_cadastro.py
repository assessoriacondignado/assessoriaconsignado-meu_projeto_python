import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date
import re
import time
import json

# Tenta importar o módulo de conexão e configurações de exportação se existirem
try:
    import conexao
except ImportError:
    st.error("Erro crítico: conexao.py não encontrado.")

try:
    import modulo_pf_config_exportacao as pf_export
except ImportError:
    pf_export = None

# ==============================================================================
# 1. CAMADA DE DADOS E BACKEND (Original Preservado)
# ==============================================================================

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        return None

def init_db_structures():
    """
    Verifica e recria tabelas se a estrutura estiver incorreta.
    (Código original completo mantido)
    """
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("CREATE SCHEMA IF NOT EXISTS banco_pf;")
            
            # --- 1. VERIFICAÇÃO E CORREÇÃO DA TABELA EMPREGO_RENDA ---
            tabela_ok = False
            try:
                cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema = 'banco_pf' AND table_name = 'pf_emprego_renda' AND column_name = 'cpf_ref'")
                if cur.fetchone():
                    tabela_ok = True
            except: pass

            if not tabela_ok:
                try:
                    cur.execute("SELECT to_regclass('banco_pf.pf_emprego_renda')")
                    if cur.fetchone()[0]:
                        cur.execute("DROP TABLE banco_pf.pf_emprego_renda CASCADE")
                except Exception: pass

            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_emprego_renda (
                    id SERIAL PRIMARY KEY,
                    cpf_ref VARCHAR(20), 
                    convenio VARCHAR(100),
                    matricula VARCHAR(100),
                    dados_extras TEXT,
                    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(matricula)
                );
            """)
            
            # Correção de colunas faltantes e FKs
            try:
                cur.execute("ALTER TABLE banco_pf.pf_emprego_renda ADD COLUMN IF NOT EXISTS dados_extras TEXT")
            except: pass

            try:
                cur.execute("""
                    DO $$ BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pf_emprego_renda_cpf_ref_fkey') THEN
                            ALTER TABLE banco_pf.pf_emprego_renda ADD CONSTRAINT pf_emprego_renda_cpf_ref_fkey FOREIGN KEY (cpf_ref) REFERENCES banco_pf.pf_dados(cpf) ON DELETE CASCADE;
                        END IF;
                    END $$;
                """)
            except: pass
            
            # --- 2. DEMAIS TABELAS E COLUNAS ---
            cols_extras_dados = [
                "uf_rg VARCHAR(2)", "pis VARCHAR(20)", "nome_procurador VARCHAR(150)", 
                "cpf_procurador VARCHAR(14)", "dados_exp_rg VARCHAR(50)", 
                "serie_ctps VARCHAR(20)", "cnh VARCHAR(20)", "nome_pai VARCHAR(150)"
            ]
            for col_def in cols_extras_dados:
                try:
                    col_name = col_def.split()[0]
                    cur.execute(f"ALTER TABLE banco_pf.pf_dados ADD COLUMN IF NOT EXISTS {col_name} {col_def.split(' ', 1)[1]}")
                except: pass

            cur.execute("CREATE TABLE IF NOT EXISTS banco_pf.pf_referencias (id SERIAL PRIMARY KEY, tipo VARCHAR(50), nome VARCHAR(100), UNIQUE(tipo, nome));")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.convenio_por_planilha (
                    id SERIAL PRIMARY KEY, convenio VARCHAR(100), nome_planilha_sql VARCHAR(100), tipo_planilha VARCHAR(100), UNIQUE(convenio, nome_planilha_sql)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_emails (
                    id SERIAL PRIMARY KEY, cpf_ref VARCHAR(20) REFERENCES banco_pf.pf_dados(cpf) ON DELETE CASCADE, email VARCHAR(150)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_enderecos (
                    id SERIAL PRIMARY KEY, cpf_ref VARCHAR(20) REFERENCES banco_pf.pf_dados(cpf) ON DELETE CASCADE, rua VARCHAR(255), bairro VARCHAR(100), cidade VARCHAR(100), uf VARCHAR(5), cep VARCHAR(20)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.cpf_convenio (
                    id SERIAL PRIMARY KEY, convenio VARCHAR(100), cpf VARCHAR(20)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_modelos_filtro_fixo (
                    id SERIAL PRIMARY KEY, nome_modelo VARCHAR(150), tabela_alvo VARCHAR(100), coluna_alvo TEXT, resumo TEXT, data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """) # Adicionado para suporte a filtros salvos

            try: cur.execute("ALTER TABLE banco_pf.pf_telefones ALTER COLUMN numero TYPE VARCHAR(20)")
            except: pass

            conn.commit(); conn.close()
        except Exception as e:
            print(f"Erro no init_db: {e}")

# --- HELPERS E VALIDAÇÕES (Originais) ---

def formatar_cpf_visual(cpf_db):
    if not cpf_db: return ""
    cpf_limpo = re.sub(r'\D', '', str(cpf_db))
    return f"{cpf_limpo[:3]}.{cpf_limpo[3:6]}.{cpf_limpo[6:9]}-{cpf_limpo[9:]}" if len(cpf_limpo) == 11 else cpf_limpo

def limpar_normalizar_cpf(cpf_raw):
    if not cpf_raw: return ""
    return re.sub(r'\D', '', str(cpf_raw)).zfill(11)

def limpar_apenas_numeros(valor):
    return re.sub(r'\D', '', str(valor)) if valor else ""

def formatar_telefone_visual(tel_raw):
    if not tel_raw: return ""
    nums = re.sub(r'\D', '', str(tel_raw))
    return f"({nums[:2]}){nums[2:]}" if len(nums) >= 2 else nums

def validar_formatar_telefone(tel_raw):
    s = str(tel_raw).strip()
    if re.search(r'[a-zA-Z]', s): return None, "Erro: Contém letras."
    numeros = re.sub(r'\D', '', s)
    if len(numeros) < 10 or len(numeros) > 11: return None, "Formato inválido (10 ou 11 dígitos)."
    return numeros, None

def validar_formatar_cpf(cpf_raw):
    numeros = re.sub(r'\D', '', str(cpf_raw).strip())
    if not numeros: return None, "Vazio."
    if len(numeros) > 11: return None, "Máximo 11 dígitos."
    return numeros, None

def validar_email(email):
    if not email: return False
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(email)))

LISTA_UFS_BR = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']

def validar_uf(uf_input):
    return str(uf_input).strip().upper() in LISTA_UFS_BR if uf_input else False

def validar_formatar_cep(cep_raw):
    numeros = limpar_apenas_numeros(cep_raw)
    if len(numeros) != 8: return None, None, "CEP deve ter 8 dígitos."
    return numeros, f"{numeros[:5]}-{numeros[5:]}", None

def calcular_idade_hoje(dt_nasc):
    if not dt_nasc: return 0
    hoje = date.today()
    if isinstance(dt_nasc, datetime): dt_nasc = dt_nasc.date()
    if not isinstance(dt_nasc, date): return 0
    return hoje.year - dt_nasc.year - ((hoje.month, hoje.day) < (dt_nasc.month, dt_nasc.day))

def safe_view(valor):
    if valor is None: return ""
    v = str(valor).strip()
    return "" if v.lower() in ['none', 'nan', 'null', 'nat', ''] else v

# --- OPERAÇÕES DE BANCO (CRUD & LISTAGEM) ---

def carregar_dados_completos(cpf):
    conn = get_conn(); dados = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': []}
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            df_d = pd.read_sql("SELECT * FROM banco_pf.pf_dados WHERE cpf = %s", conn, params=(cpf_norm,))
            if not df_d.empty: dados['geral'] = df_d.where(pd.notnull(df_d), None).iloc[0].to_dict()
            
            col_fk = 'cpf_ref'
            try: pd.read_sql("SELECT 1 FROM banco_pf.pf_telefones WHERE cpf_ref = '1' LIMIT 1", conn)
            except: col_fk = 'cpf'; conn.rollback()

            dados['telefones'] = pd.read_sql(f"SELECT numero FROM banco_pf.pf_telefones WHERE {col_fk} = %s", conn, params=(cpf_norm,)).fillna("").to_dict('records')
            dados['emails'] = pd.read_sql(f"SELECT email FROM banco_pf.pf_emails WHERE {col_fk} = %s", conn, params=(cpf_norm,)).fillna("").to_dict('records')
            dados['enderecos'] = pd.read_sql(f"SELECT rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE {col_fk} = %s", conn, params=(cpf_norm,)).fillna("").to_dict('records')
            
            try:
                df_emp = pd.read_sql(f"SELECT convenio, matricula, dados_extras FROM banco_pf.pf_emprego_renda WHERE {col_fk} = %s", conn, params=(cpf_norm,))
            except:
                conn.rollback(); df_emp = pd.DataFrame() # Fallback

            if not df_emp.empty:
                for _, row_emp in df_emp.iterrows():
                    vinculo = {'convenio': str(row_emp['convenio']).strip(), 'matricula': str(row_emp['matricula']).strip(), 'dados_extras': row_emp.get('dados_extras'), 'contratos': []}
                    # Tenta buscar contratos (Simplificado para o unificado)
                    try:
                        df_contratos = pd.read_sql("SELECT * FROM banco_pf.pf_contratos WHERE matricula_ref = %s", conn, params=(vinculo['matricula'],))
                        if not df_contratos.empty:
                            df_contratos['origem_tabela'] = 'banco_pf.pf_contratos'
                            df_contratos['tipo_origem'] = 'Geral'
                            vinculo['contratos'] = df_contratos.to_dict('records')
                    except: pass
                    dados['empregos'].append(vinculo)
        except Exception as e: print(f"Erro carregamento: {e}") 
        finally: conn.close()
    return dados

def salvar_pf(dados_gerais, df_tel, df_email, df_end, df_emp, df_contr, modo="novo", cpf_original=None):
    conn = get_conn()
    if not conn: return False, "Erro de conexão."
    try:
        cur = conn.cursor()
        cpf_limpo = limpar_normalizar_cpf(dados_gerais['cpf'])
        dados_gerais['cpf'] = cpf_limpo
        if cpf_original: cpf_original = limpar_normalizar_cpf(cpf_original)
        
        # Tratamento de dados
        dados_gerais = {k: (v.upper() if isinstance(v, str) else v) for k, v in dados_gerais.items()}
        if 'cpf_procurador' in dados_gerais: dados_gerais['cpf_procurador'] = limpar_normalizar_cpf(dados_gerais['cpf_procurador'])
        if 'data_nascimento' in dados_gerais:
            if not dados_gerais['data_nascimento']: dados_gerais['data_nascimento'] = None
            elif isinstance(dados_gerais['data_nascimento'], (date, datetime)): dados_gerais['data_nascimento'] = dados_gerais['data_nascimento'].strftime('%Y-%m-%d')

        # 1. Salvar Dados Principais
        if modo == "novo":
            cols = list(dados_gerais.keys()); vals = list(dados_gerais.values())
            placeholders = ", ".join(["%s"] * len(vals)); col_names = ", ".join(cols)
            cur.execute(f"INSERT INTO banco_pf.pf_dados ({col_names}) VALUES ({placeholders})", vals)
        else:
            set_clause = ", ".join([f"{k}=%s" for k in dados_gerais.keys()])
            vals = list(dados_gerais.values()) + [cpf_original]
            cur.execute(f"UPDATE banco_pf.pf_dados SET {set_clause} WHERE cpf=%s", vals)

        col_fk = 'cpf_ref' # Assume FK padrao do init
        
        # 2. Telefones
        if not df_tel.empty:
            for _, r in df_tel.iterrows():
                if r['numero']:
                    cur.execute(f"INSERT INTO banco_pf.pf_telefones ({col_fk}, numero, data_atualizacao) VALUES (%s, %s, CURRENT_DATE) ON CONFLICT DO NOTHING", (cpf_limpo, r['numero']))
        
        # 3. Emails, Enderecos, Empregos (Logica simplificada de Insert ignore ou check exists)
        if not df_email.empty:
            for _, r in df_email.iterrows():
                if r['email']: cur.execute(f"INSERT INTO banco_pf.pf_emails ({col_fk}, email) VALUES (%s, %s)", (cpf_limpo, r['email']))
        
        if not df_end.empty:
            for _, r in df_end.iterrows():
                cur.execute(f"INSERT INTO banco_pf.pf_enderecos ({col_fk}, cep, rua, bairro, cidade, uf) VALUES (%s, %s, %s, %s, %s, %s)", 
                            (cpf_limpo, r.get('cep'), r.get('rua'), r.get('bairro'), r.get('cidade'), r.get('uf')))

        if not df_emp.empty:
            for _, r in df_emp.iterrows():
                if r.get('matricula'):
                    cur.execute(f"INSERT INTO banco_pf.pf_emprego_renda ({col_fk}, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s) ON CONFLICT (matricula) DO NOTHING", 
                                (cpf_limpo, r.get('convenio'), r.get('matricula'), r.get('dados_extras', '')))
                    # CPF Convenio
                    if r.get('convenio'):
                        cur.execute("INSERT INTO banco_pf.cpf_convenio (cpf, convenio) VALUES (%s, %s)", (cpf_limpo, r.get('convenio')))

        conn.commit(); conn.close()
        return True, "✅ Dados salvos com sucesso!"
    except Exception as e:
        if conn: conn.rollback(); conn.close()
        return False, f"❌ Erro ao salvar: {str(e)}"

def excluir_pf(cpf):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM banco_pf.pf_dados WHERE cpf = %s", (limpar_normalizar_cpf(cpf),))
            conn.commit(); conn.close(); return True
        except: return False
    return False

@st.dialog("⚠️ Excluir Cadastro")
def dialog_excluir_pf(cpf, nome):
    st.error(f"Apagar **{nome}**?")
    if st.button("Confirmar Exclusão", type="primary"):
        if excluir_pf(cpf): st.success("Apagado!"); time.sleep(1); st.rerun()

def buscar_pf_simples(termo, pagina=1, itens_por_pagina=50):
    conn = get_conn()
    if conn:
        try:
            termo_limpo = limpar_normalizar_cpf(termo)
            params = []
            if termo_limpo and len(termo_limpo) > 6: # CPF ou Telefone
                sql_base = "SELECT DISTINCT d.id, d.nome, d.cpf, d.data_nascimento FROM banco_pf.pf_dados d LEFT JOIN banco_pf.pf_telefones t ON d.cpf = t.cpf_ref WHERE d.cpf LIKE %s OR t.numero LIKE %s"
                params = [f"%{termo_limpo}%", f"%{termo_limpo}%"]
            else: # Nome
                sql_base = "SELECT DISTINCT d.id, d.nome, d.cpf, d.data_nascimento FROM banco_pf.pf_dados d WHERE d.nome ILIKE %s"
                params = [f"%{termo}%"]
            
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM ({sql_base}) as sub", tuple(params))
            total = cur.fetchone()[0]
            
            offset = (pagina-1)*itens_por_pagina
            df = pd.read_sql(f"{sql_base} ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}", conn, params=tuple(params))
            conn.close()
            return df, total
        except Exception as e:
            st.error(f"Erro busca: {e}"); conn.close()
    return pd.DataFrame(), 0

# --- FUNÇÕES AUXILIARES DE TABELAS E COLUNAS (Do modulo pesquisa) ---
def listar_tabelas_pf(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'banco_pf' AND table_name LIKE 'pf_%' ORDER BY table_name")
        return [r[0] for r in cur.fetchall()]
    except: return []

def get_colunas_tabela(tabela): # Unificado (era get_colunas_tabela e listar_colunas_tabela)
    conn = get_conn()
    if conn:
        try:
            schema = 'banco_pf' if 'banco_pf' in tabela or 'pf_' in tabela else 'public'
            nome = tabela.split('.')[-1]
            cur = conn.cursor()
            cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s AND table_schema = %s ORDER BY ordinal_position", (nome, schema))
            res = cur.fetchall()
            conn.close(); return res
        except: conn.close()
    return []

def listar_tabelas_por_convenio(convenio):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome_planilha_sql, tipo_planilha FROM banco_pf.convenio_por_planilha WHERE convenio ILIKE %s", (convenio,))
            res = cur.fetchall(); conn.close(); return res if res else []
        except: conn.close()
    return []

# --- CONFIGURAÇÃO DE CAMPOS DE CADASTRO E PESQUISA ---
CONFIG_CADASTRO = {
    "Dados Pessoais": [
        {"label": "Nome Completo", "key": "nome", "tabela": "geral", "tipo": "texto", "obrigatorio": True},
        {"label": "CPF", "key": "cpf", "tabela": "geral", "tipo": "cpf", "obrigatorio": True},
        {"label": "RG", "key": "rg", "tabela": "geral", "tipo": "texto"},
        {"label": "Data Nascimento", "key": "data_nascimento", "tabela": "geral", "tipo": "data"},
        {"label": "Nome da Mãe", "key": "nome_mae", "tabela": "geral", "tipo": "texto"},
        {"label": "Nome do Pai", "key": "nome_pai", "tabela": "geral", "tipo": "texto"},
        {"label": "UF do RG", "key": "uf_rg", "tabela": "geral", "tipo": "texto"},
    ],
    "Contatos": [
        {"label": "Telefone", "key": "numero", "tabela": "telefones", "tipo": "telefone", "multiplo": True},
        {"label": "E-mail", "key": "email", "tabela": "emails", "tipo": "email", "multiplo": True},
    ],
    "Endereços": [
        {"label": "CEP", "key": "cep", "tabela": "enderecos", "tipo": "cep", "multiplo": True}, 
        {"label": "Logradouro", "key": "rua", "tabela": "enderecos", "tipo": "texto", "multiplo": True},
        {"label": "Bairro", "key": "bairro", "tabela": "enderecos", "tipo": "texto", "multiplo": True},
        {"label": "Cidade", "key": "cidade", "tabela": "enderecos", "tipo": "texto", "multiplo": True},
        {"label": "UF", "key": "uf", "tabela": "enderecos", "tipo": "texto", "multiplo": True},
    ]
}

# Configuração para Filtros Avançados (Trazido do pesquisa)
CAMPOS_PESQUISA_CONFIG = {
    "Dados Pessoais": [
        {"label": "Nome", "coluna": "d.nome", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CPF", "coluna": "d.cpf", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Nascimento", "coluna": "d.data_nascimento", "tipo": "data", "tabela": "banco_pf.pf_dados"},
        {"label": "RG", "coluna": "d.rg", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
    ],
    "Endereços": [
        {"label": "Cidade", "coluna": "ende.cidade", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "UF", "coluna": "ende.uf", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
    ]
}

def inserir_dado_staging(campo_config, valor, extras=None):
    tabela = campo_config['tabela']
    chave = campo_config['key']
    
    if tabela not in st.session_state['dados_staging']:
        if campo_config.get('multiplo'): st.session_state['dados_staging'][tabela] = []
        else: st.session_state['dados_staging'][tabela] = {}

    erro = None
    valor_final = valor
    
    if campo_config['tipo'] == 'cpf':
        val, erro = validar_formatar_cpf(valor)
        if not erro: valor_final = limpar_normalizar_cpf(val)
    elif campo_config['tipo'] == 'telefone':
        val, erro = validar_formatar_telefone(valor)
        if not erro: valor_final = val
    elif campo_config['tipo'] == 'email':
        if not validar_email(valor): erro = "E-mail inválido."
    
    if erro: st.error(erro); return
    if not valor_final and campo_config.get('obrigatorio'): st.toast(f"❌ {campo_config['label']} é obrigatório."); return

    if campo_config.get('multiplo'):
        novo_item = {chave: valor_final}
        if extras: novo_item.update(extras)
        st.session_state['dados_staging'][tabela].append(novo_item)
        st.toast(f"✅ {campo_config['label']} adicionado!")
    else:
        st.session_state['dados_staging'][tabela][chave] = valor_final
        st.toast(f"✅ {campo_config['label']} atualizado!")

# ==============================================================================
# 2. CAMADA DE INTERFACE (VIEWS UNIFICADAS)
# ==============================================================================

# --- TELA 1: PESQUISA E LISTAGEM ---
def view_pesquisa_lista():
    st.markdown("### 🔍 Gestão de Clientes")
    
    c_busca, c_novo = st.columns([4, 1])
    termo = c_busca.text_input("Buscar por Nome, CPF ou Telefone", key="busca_unificada", placeholder="Digite para pesquisar...")
    if c_novo.button("➕ Novo", type="primary", use_container_width=True):
        ir_para_novo()
    
    st.divider()
    
    # Busca Rápida (Prioritária)
    if termo:
        df, total = buscar_pf_simples(termo)
        if not df.empty:
            st.caption(f"Encontrados: {total}")
            st.markdown("""<div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex;"><div style="flex: 1;">Ações</div><div style="flex: 2;">CPF</div><div style="flex: 4;">Nome</div></div>""", unsafe_allow_html=True)
            for _, row in df.iterrows():
                c1, c2, c3 = st.columns([1, 2, 4])
                with c1:
                    b1, b2, b3 = st.columns(3)
                    b1.button("👁️", key=f"v_{row['id']}", on_click=ir_para_visualizar, args=(row['cpf'],))
                    b2.button("✏️", key=f"e_{row['id']}", on_click=ir_para_editar, args=(row['cpf'],))
                    if b3.button("🗑️", key=f"d_{row['id']}"): dialog_excluir_pf(str(row['cpf']), row['nome'])
                c2.write(formatar_cpf_visual(row['cpf']))
                c3.write(row['nome'])
                st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
        else:
            st.info("Nenhum resultado encontrado.")
    else:
        # Área de Filtros Avançados (Se não houver busca rápida)
        with st.expander("Filtros Avançados (Beta)", expanded=False):
            st.write("Selecione critérios específicos (Funcionalidade sendo portada do módulo antigo)")
            # Aqui entraria a lógica de 'executar_pesquisa_ampla' se necessário

# --- TELA 2: FORMULÁRIO DE CADASTRO ---
def view_formulario_cadastro():
    is_edit = st.session_state.get('pf_modo') == 'editar'
    titulo = "✏️ Editar Cliente" if is_edit else "➕ Novo Cadastro"
    
    c_back, c_tit = st.columns([1, 5])
    if c_back.button("⬅️ Voltar"): ir_para_lista(); st.rerun()
    c_tit.markdown(f"### {titulo}")
    
    # Inicialização do Staging (Memória temporária do form)
    if not st.session_state.get('form_loaded'):
        if is_edit:
            st.session_state['dados_staging'] = carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
        else:
            st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': [], 'dados_clt': []}
        st.session_state['form_loaded'] = True
    
    staging = st.session_state['dados_staging']
    
    t1, t2, t3 = st.tabs(["Dados Pessoais", "Contatos & Endereço", "Vínculos & Planilhas"])
    
    with t1:
        for campo in CONFIG_CADASTRO["Dados Pessoais"]:
            key = campo['key']
            val_atual = staging['geral'].get(key, '')
            if campo['tipo'] == 'data':
                if isinstance(val_atual, str) and val_atual:
                    try: val_atual = datetime.strptime(val_atual, '%Y-%m-%d').date()
                    except: val_atual = None
                novo_val = st.date_input(campo['label'], value=val_atual, format="DD/MM/YYYY")
                if isinstance(novo_val, date): novo_val = novo_val.strftime('%Y-%m-%d')
            else:
                novo_val = st.text_input(campo['label'], value=val_atual, disabled=(key=='cpf' and is_edit))
            staging['geral'][key] = novo_val

    with t2:
        c1, c2 = st.columns(2)
        with c1:
            ntel = st.text_input("Novo Telefone", placeholder="(00) 00000-0000")
            if st.button("Adicionar Tel"):
                if ntel: staging['telefones'].append({'numero': ntel}); st.rerun()
            for i, t in enumerate(staging.get('telefones', [])): 
                st.caption(f"- {formatar_telefone_visual(t.get('numero'))}")
        
        with c2:
            nmail = st.text_input("Novo E-mail")
            if st.button("Adicionar Email"):
                if nmail: staging['emails'].append({'email': nmail}); st.rerun()
            for m in staging.get('emails', []): st.caption(f"- {m.get('email')}")
            
        st.divider()
        st.markdown("###### Endereço")
        cep = st.text_input("CEP"); rua = st.text_input("Rua"); bairro = st.text_input("Bairro"); cidade = st.text_input("Cidade"); uf = st.text_input("UF")
        if st.button("Adicionar Endereço"):
            staging['enderecos'].append({'cep': cep, 'rua': rua, 'bairro': bairro, 'cidade': cidade, 'uf': uf}); st.rerun()
        for e in staging.get('enderecos', []): st.caption(f"🏠 {e.get('rua')} - {e.get('cidade')}/{e.get('uf')}")

    with t3:
        st.markdown("###### 💼 Vínculos (Emprego)")
        conv = st.text_input("Convênio")
        matr = st.text_input("Matrícula")
        if st.button("Adicionar Vínculo"):
            staging['empregos'].append({'convenio': conv, 'matricula': matr}); st.rerun()
        
        lista_emps = staging.get('empregos', [])
        if lista_emps:
            sel_vinculo = st.selectbox("Selecione Vínculo para adicionar contrato:", [f"{e['matricula']} - {e['convenio']}" for e in lista_emps])
            if sel_vinculo:
                # Lógica simplificada de adicionar contrato manual
                st.caption("Adição manual de contratos (Em desenvolvimento na versão unificada)")
        
        for emp in lista_emps: st.caption(f"🏢 {emp.get('convenio')} | {emp.get('matricula')}")

    st.divider()
    if st.button("💾 SALVAR DADOS", type="primary", use_container_width=True):
        geral = staging['geral']
        if not geral.get('nome') or not geral.get('cpf'):
            st.error("Nome e CPF são obrigatórios.")
        else:
            cpf_orig = st.session_state.get('pf_cpf_selecionado') if is_edit else None
            ok, msg = salvar_pf(
                geral, 
                pd.DataFrame(staging.get('telefones', [])), 
                pd.DataFrame(staging.get('emails', [])), 
                pd.DataFrame(staging.get('enderecos', [])), 
                pd.DataFrame(staging.get('empregos', [])), 
                pd.DataFrame(staging.get('contratos', [])),
                modo="editar" if is_edit else "novo",
                cpf_original=cpf_orig
            )
            if ok:
                st.success(msg); time.sleep(1); ir_para_lista(); st.rerun()
            else: st.error(msg)

# --- TELA 3: VISUALIZAÇÃO ---
def view_detalhes_cliente():
    cpf = st.session_state.get('pf_cpf_selecionado')
    if st.button("⬅️ Voltar"): ir_para_lista(); st.rerun()
    
    dados = carregar_dados_completos(cpf)
    g = dados.get('geral', {})
    
    st.markdown(f"### 👤 {g.get('nome', 'Sem Nome')}")
    st.markdown(f"**CPF:** {formatar_cpf_visual(g.get('cpf'))}")
    
    t1, t2 = st.tabs(["Cadastro", "Financeiro"])
    with t1:
        c1, c2 = st.columns(2)
        c1.write(f"**Nascimento:** {safe_view(g.get('data_nascimento'))}")
        c1.write(f"**RG:** {safe_view(g.get('rg'))}")
        c2.write(f"**Mãe:** {safe_view(g.get('nome_mae'))}")
        
        st.divider()
        st.markdown("###### Contatos")
        for t in dados.get('telefones', []): st.write(f"📱 {formatar_telefone_visual(t.get('numero'))}")
        for e in dados.get('emails', []): st.write(f"📧 {e.get('email')}")
    
    with t2:
        for emp in dados.get('empregos', []):
            with st.expander(f"{emp.get('convenio')} - {emp.get('matricula')}"):
                if emp.get('contratos'):
                    st.dataframe(pd.DataFrame(emp['contratos']), hide_index=True)
                else: st.info("Sem contratos cadastrados.")

# ==============================================================================
# 3. CONTROLADOR DE NAVEGAÇÃO (ROUTER)
# ==============================================================================

def ir_para_lista():
    st.session_state['pf_view_ativa'] = 'lista'
    st.session_state['pf_cpf_selecionado'] = None

def ir_para_novo():
    st.session_state['pf_view_ativa'] = 'formulario'
    st.session_state['pf_modo'] = 'novo'
    st.session_state['pf_cpf_selecionado'] = None
    st.session_state['form_loaded'] = False

def ir_para_editar(cpf):
    st.session_state['pf_view_ativa'] = 'formulario'
    st.session_state['pf_modo'] = 'editar'
    st.session_state['pf_cpf_selecionado'] = cpf
    st.session_state['form_loaded'] = False

def ir_para_visualizar(cpf):
    st.session_state['pf_view_ativa'] = 'visualizar'
    st.session_state['pf_cpf_selecionado'] = cpf

def app_cadastro_unificado():
    """
    Função Mestre chamada pelo sistema principal (modulo_pessoa_fisica.py).
    """
    init_db_structures()
    
    if 'pf_view_ativa' not in st.session_state:
        st.session_state['pf_view_ativa'] = 'lista'
        
    tela = st.session_state['pf_view_ativa']
    
    if tela == 'lista':
        view_pesquisa_lista()
    elif tela == 'formulario':
        view_formulario_cadastro()
    elif tela == 'visualizar':
        view_detalhes_cliente()