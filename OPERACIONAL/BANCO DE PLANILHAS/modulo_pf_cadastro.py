import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date
import re
import time

# Tenta importar o módulo de conexão
try:
    import conexao
except ImportError:
    st.error("Erro crítico: conexao.py não encontrado.")

# ==============================================================================
# 1. CAMADA DE DADOS E BACKEND (Conexão e SQL)
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
    """Verifica e recria tabelas se a estrutura estiver incorreta."""
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("CREATE SCHEMA IF NOT EXISTS banco_pf;")
            
            # --- 1. TABELA DE DADOS ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_dados (
                    id SERIAL PRIMARY KEY,
                    cpf VARCHAR(14) UNIQUE,
                    nome VARCHAR(255),
                    data_nascimento DATE,
                    rg VARCHAR(20),
                    uf_rg VARCHAR(2),
                    nome_mae VARCHAR(255),
                    nome_pai VARCHAR(255),
                    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # --- 2. TABELAS SATÉLITES ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_telefones (
                    id SERIAL PRIMARY KEY,
                    cpf_ref VARCHAR(20) REFERENCES banco_pf.pf_dados(cpf) ON DELETE CASCADE,
                    numero VARCHAR(20),
                    data_atualizacao DATE
                );
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_emails (
                    id SERIAL PRIMARY KEY,
                    cpf_ref VARCHAR(20) REFERENCES banco_pf.pf_dados(cpf) ON DELETE CASCADE,
                    email VARCHAR(150)
                );
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_enderecos (
                    id SERIAL PRIMARY KEY,
                    cpf_ref VARCHAR(20) REFERENCES banco_pf.pf_dados(cpf) ON DELETE CASCADE,
                    rua VARCHAR(255),
                    bairro VARCHAR(100),
                    cidade VARCHAR(100),
                    uf VARCHAR(5),
                    cep VARCHAR(20)
                );
            """)

            # --- 3. VÍNCULOS ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_emprego_renda (
                    id SERIAL PRIMARY KEY,
                    cpf_ref VARCHAR(20) REFERENCES banco_pf.pf_dados(cpf) ON DELETE CASCADE,
                    convenio VARCHAR(100),
                    matricula VARCHAR(100) UNIQUE,
                    dados_extras TEXT,
                    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            conn.commit()
            conn.close()
        except Exception as e:
            pass

# --- HELPERS DE FORMATAÇÃO ---
def limpar_normalizar_cpf(cpf_raw):
    if not cpf_raw: return ""
    return re.sub(r'\D', '', str(cpf_raw)).zfill(11)

def formatar_cpf_visual(cpf_db):
    if not cpf_db: return ""
    c = limpar_normalizar_cpf(cpf_db)
    return f"{c[:3]}.{c[3:6]}.{c[6:9]}-{c[9:]}"

def formatar_telefone_visual(tel_raw):
    if not tel_raw: return ""
    nums = re.sub(r'\D', '', str(tel_raw))
    return f"({nums[:2]}) {nums[2:]}" if len(nums) > 2 else nums

def safe_view(valor):
    if valor is None or str(valor).lower() in ['nan', 'none', 'nat', '']: return ""
    return str(valor)

# --- VALIDAÇÕES ---
def validar_formatar_cpf(cpf_raw):
    nums = re.sub(r'\D', '', str(cpf_raw))
    if not nums: return None, "Vazio"
    if len(nums) > 11: return None, "CPF inválido (muitos dígitos)"
    return nums, None

def validar_email(email):
    if not email: return False
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(email)))

def validar_formatar_telefone(tel_raw):
    nums = re.sub(r'\D', '', str(tel_raw))
    if len(nums) < 10: return None, "Telefone curto demais"
    return nums, None

def validar_formatar_cep(cep_raw):
    nums = re.sub(r'\D', '', str(cep_raw))
    if len(nums) != 8: return None, None, "CEP deve ter 8 dígitos"
    return nums, f"{nums[:5]}-{nums[5:]}", None

def validar_uf(uf):
    if not uf: return False
    return uf.upper() in ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']

def calcular_idade_hoje(dt_nasc):
    if not dt_nasc: return 0
    hj = date.today()
    if isinstance(dt_nasc, datetime): dt_nasc = dt_nasc.date()
    if not isinstance(dt_nasc, date): return 0
    return hj.year - dt_nasc.year - ((hj.month, hj.day) < (dt_nasc.month, dt_nasc.day))

# --- OPERAÇÕES DE BANCO (CRUD) ---

def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': []}
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            # Geral
            df = pd.read_sql("SELECT * FROM banco_pf.pf_dados WHERE cpf = %s", conn, params=(cpf_norm,))
            if not df.empty: dados['geral'] = df.iloc[0].to_dict()
            
            # Satélites
            col_fk = 'cpf_ref' 
            try: pd.read_sql("SELECT 1 FROM banco_pf.pf_telefones WHERE cpf_ref = '1' LIMIT 1", conn)
            except: col_fk = 'cpf'; conn.rollback()

            dados['telefones'] = pd.read_sql(f"SELECT * FROM banco_pf.pf_telefones WHERE {col_fk} = %s", conn, params=(cpf_norm,)).to_dict('records')
            dados['emails'] = pd.read_sql(f"SELECT * FROM banco_pf.pf_emails WHERE {col_fk} = %s", conn, params=(cpf_norm,)).to_dict('records')
            dados['enderecos'] = pd.read_sql(f"SELECT * FROM banco_pf.pf_enderecos WHERE {col_fk} = %s", conn, params=(cpf_norm,)).to_dict('records')
            
            # Vínculos
            try:
                df_emp = pd.read_sql(f"SELECT * FROM banco_pf.pf_emprego_renda WHERE {col_fk} = %s", conn, params=(cpf_norm,))
                for _, row in df_emp.iterrows():
                    vinculo = row.to_dict()
                    vinculo['contratos'] = []
                    if row.get('matricula'):
                        try:
                            # Tenta buscar contratos genéricos
                            ctrs = pd.read_sql("SELECT * FROM banco_pf.pf_contratos WHERE matricula_ref = %s", conn, params=(str(row['matricula']),))
                            if not ctrs.empty: 
                                ctrs['tipo_origem'] = 'Geral'
                                vinculo['contratos'] = ctrs.to_dict('records')
                        except: pass
                    dados['empregos'].append(vinculo)
            except: pass

        except Exception as e:
            print(f"Erro ao carregar: {e}")
        finally:
            conn.close()
    return dados

def salvar_pf(dados_gerais, df_tel, df_email, df_end, df_emp, df_contr, modo="novo", cpf_original=None):
    conn = get_conn()
    if not conn: return False, "Erro de conexão."
    try:
        cur = conn.cursor()
        cpf_limpo = limpar_normalizar_cpf(dados_gerais['cpf'])
        dados_gerais['cpf'] = cpf_limpo
        
        # 1. Tabela Principal
        cols = list(dados_gerais.keys())
        vals = list(dados_gerais.values())
        
        if modo == "novo":
            placeholders = ", ".join(["%s"] * len(vals))
            stmt = f"INSERT INTO banco_pf.pf_dados ({', '.join(cols)}) VALUES ({placeholders})"
            cur.execute(stmt, vals)
        else:
            set_clause = ", ".join([f"{k}=%s" for k in cols])
            vals.append(cpf_original)
            stmt = f"UPDATE banco_pf.pf_dados SET {set_clause} WHERE cpf=%s"
            cur.execute(stmt, vals)
        
        col_fk = 'cpf_ref' 
        try: cur.execute("SELECT 1 FROM banco_pf.pf_telefones WHERE cpf_ref = '1' LIMIT 1")
        except: col_fk = 'cpf'; conn.rollback(); cur = conn.cursor()

        # 2. Telefones
        if not df_tel.empty:
            for _, r in df_tel.iterrows():
                cur.execute(f"INSERT INTO banco_pf.pf_telefones ({col_fk}, numero, data_atualizacao) VALUES (%s, %s, CURRENT_DATE) ON CONFLICT DO NOTHING", (cpf_limpo, r['numero']))
        
        # 3. Emails
        if not df_email.empty:
            for _, r in df_email.iterrows():
                cur.execute(f"INSERT INTO banco_pf.pf_emails ({col_fk}, email) VALUES (%s, %s) ON CONFLICT DO NOTHING", (cpf_limpo, r['email']))

        # 4. Endereços
        if not df_end.empty:
            for _, r in df_end.iterrows():
                cur.execute(f"INSERT INTO banco_pf.pf_enderecos ({col_fk}, cep, rua, bairro, cidade, uf) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                            (cpf_limpo, r.get('cep'), r.get('rua'), r.get('bairro'), r.get('cidade'), r.get('uf')))

        # 5. Empregos
        if not df_emp.empty:
            for _, r in df_emp.iterrows():
                if r.get('matricula'):
                    cur.execute(f"INSERT INTO banco_pf.pf_emprego_renda ({col_fk}, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s) ON CONFLICT (matricula) DO NOTHING", 
                                (cpf_limpo, r.get('convenio'), r.get('matricula'), r.get('dados_extras', '')))
        
        conn.commit()
        conn.close()
        return True, "Salvo com sucesso!"
    except Exception as e:
        if conn: conn.close()
        return False, f"Erro SQL: {e}"

def excluir_pf(cpf):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM banco_pf.pf_dados WHERE cpf = %s", (limpar_normalizar_cpf(cpf),))
            conn.commit()
            conn.close()
            return True
        except: conn.close()
    return False

@st.dialog("Excluir")
def dialog_excluir_pf(cpf, nome):
    st.error(f"Tem certeza que deseja excluir {nome}?")
    if st.button("Confirmar Exclusão"):
        if excluir_pf(cpf):
            st.success("Excluído.")
            time.sleep(1)
            st.rerun()

def buscar_pf_simples(termo, pagina=1, itens=50):
    conn = get_conn()
    if conn:
        try:
            termo_limpo = limpar_normalizar_cpf(termo)
            if termo_limpo and len(termo_limpo) > 6:
                # Busca por CPF ou Telefone
                sql = "SELECT DISTINCT d.id, d.nome, d.cpf FROM banco_pf.pf_dados d LEFT JOIN banco_pf.pf_telefones t ON d.cpf = t.cpf_ref WHERE d.cpf LIKE %s OR t.numero LIKE %s"
                params = [f"%{termo_limpo}%", f"%{termo_limpo}%"]
            else:
                # Busca por Nome
                sql = "SELECT DISTINCT d.id, d.nome, d.cpf FROM banco_pf.pf_dados d WHERE d.nome ILIKE %s"
                params = [f"%{termo}%"]
            
            offset = (pagina-1)*itens
            df = pd.read_sql(f"{sql} ORDER BY d.nome LIMIT {itens} OFFSET {offset}", conn, params=tuple(params))
            conn.close()
            return df, 999 
        except Exception as e: 
            # Fallback se der erro na query (ex: tabelas ainda nao criadas)
            conn.close()
    return pd.DataFrame(), 0

# --- CONFIGURAÇÃO DE CAMPOS ---
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
    ]
}

def listar_tabelas_por_convenio(convenio):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome_planilha_sql, tipo_planilha FROM banco_pf.convenio_por_planilha WHERE convenio ILIKE %s", (convenio,))
            res = cur.fetchall()
            conn.close()
            return res if res else []
        except: conn.close()
    return []

def get_colunas_tabela(tabela):
    conn = get_conn()
    if conn:
        try:
            schema = 'banco_pf' if 'banco_pf' in tabela else 'public'
            nome = tabela.split('.')[-1]
            cur = conn.cursor()
            cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", (schema, nome))
            res = cur.fetchall()
            conn.close()
            return res
        except: conn.close()
    return []

def inserir_dado_staging(campo_config, valor, extras=None):
    tabela = campo_config['tabela']
    chave = campo_config['key']
    
    if tabela not in st.session_state['dados_staging']:
        if campo_config.get('multiplo'): st.session_state['dados_staging'][tabela] = []
        else: st.session_state['dados_staging'][tabela] = {}

    valor_final = valor
    if campo_config['tipo'] == 'cpf':
        valor_final = limpar_normalizar_cpf(valor)
    
    if campo_config.get('multiplo'):
        novo = {chave: valor_final}
        if extras: novo.update(extras)
        st.session_state['dados_staging'][tabela].append(novo)
    else:
        st.session_state['dados_staging'][tabela][chave] = valor_final
    
    st.toast(f"{campo_config['label']} atualizado!")

# ==============================================================================
# 2. CAMADA DE INTERFACE (VIEWS)
# ==============================================================================

# --- TELA 1: LISTAGEM E PESQUISA ---
def view_pesquisa_lista():
    st.markdown("### 🔍 Gestão de Clientes")
    
    c_busca, c_novo = st.columns([4, 1])
    termo = c_busca.text_input("Buscar por Nome, CPF ou Telefone", key="busca_unificada", placeholder="Digite para pesquisar...")
    if c_novo.button("➕ Novo", type="primary", use_container_width=True):
        ir_para_novo()
    
    st.divider()
    
    if termo:
        df, total = buscar_pf_simples(termo)
        if not df.empty:
            st.caption(f"Encontrados: {len(df)}")
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
        st.info("👆 Utilize o campo acima para pesquisar.")

# --- TELA 2: FORMULÁRIO DE CADASTRO/EDIÇÃO ---
def view_formulario_cadastro():
    is_edit = st.session_state.get('pf_modo') == 'editar'
    titulo = "✏️ Editar Cliente" if is_edit else "➕ Novo Cadastro"
    
    c_back, c_tit = st.columns([1, 5])
    if c_back.button("⬅️ Voltar"): ir_para_lista(); st.rerun()
    c_tit.markdown(f"### {titulo}")
    
    # Inicializa Staging
    if not st.session_state.get('form_loaded'):
        if is_edit:
            st.session_state['dados_staging'] = carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
        else:
            st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': []}
        st.session_state['form_loaded'] = True
    
    staging = st.session_state['dados_staging']
    
    t1, t2, t3 = st.tabs(["Dados Pessoais", "Contatos/Endereço", "Vínculos"])
    
    with t1:
        for campo in CONFIG_CADASTRO['Dados Pessoais']:
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
            st.markdown("###### 📞 Telefones")
            ntel = st.text_input("Novo Telefone")
            if st.button("Add Tel"):
                if ntel: staging['telefones'].append({'numero': ntel})
            for t in staging.get('telefones', []): st.caption(f"- {t.get('numero')}")
        
        with c2:
            st.markdown("###### 📧 E-mails")
            nmail = st.text_input("Novo E-mail")
            if st.button("Add Email"):
                if nmail: staging['emails'].append({'email': nmail})
            for m in staging.get('emails', []): st.caption(f"- {m.get('email')}")
            
        st.divider()
        st.markdown("###### 📍 Endereço")
        cep = st.text_input("CEP")
        rua = st.text_input("Rua")
        if st.button("Add Endereço"):
            staging['enderecos'].append({'cep': cep, 'rua': rua})
        for e in staging.get('enderecos', []): st.caption(f"- {e.get('rua')} (CEP: {e.get('cep')})")

    with t3:
        st.markdown("###### 💼 Emprego / Matrícula")
        conv = st.text_input("Convênio")
        matr = st.text_input("Matrícula")
        if st.button("Add Vínculo"):
            staging['empregos'].append({'convenio': conv, 'matricula': matr})
        for emp in staging.get('empregos', []): st.caption(f"- {emp.get('convenio')}: {emp.get('matricula')}")

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
                st.success(msg)
                time.sleep(1)
                ir_para_lista()
                st.rerun()
            else: st.error(msg)

# --- TELA 3: VISUALIZAÇÃO ---
def view_detalhes_cliente():
    cpf = st.session_state.get('pf_cpf_selecionado')
    if st.button("⬅️ Voltar"): ir_para_lista(); st.rerun()
    
    dados = carregar_dados_completos(cpf)
    g = dados.get('geral', {})
    
    st.markdown(f"### 👤 {g.get('nome', 'Sem Nome')}")
    st.markdown(f"**CPF:** {formatar_cpf_visual(g.get('cpf'))}")
    
    t1, t2 = st.tabs(["Resumo", "Financeiro"])
    with t1:
        st.write(f"**Nascimento:** {safe_view(g.get('data_nascimento'))}")
        st.write(f"**RG:** {safe_view(g.get('rg'))}")
        st.write(f"**Mãe:** {safe_view(g.get('nome_mae'))}")
        st.divider()
        st.write("📞 **Contatos:**")
        for t in dados.get('telefones', []): st.write(f"- {formatar_telefone_visual(t.get('numero'))}")
    
    with t2:
        for emp in dados.get('empregos', []):
            with st.expander(f"{emp.get('convenio')} - {emp.get('matricula')}"):
                if emp.get('contratos'):
                    st.dataframe(pd.DataFrame(emp['contratos']), hide_index=True)
                else:
                    st.info("Sem contratos.")

# ==============================================================================
# 3. CONTROLADOR DE ESTADO (ROUTER)
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
    Função Mestre chamada pelo sistema principal.
    Gerencia qual tela exibir com base no estado.
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