import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date
import re
import time

try:
    import conexao
except ImportError:
    st.error("Erro crítico: conexao.py não encontrado.")

# --- CONEXÃO E UTILS (BASE) ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        return None

def init_db_structures():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("CREATE SCHEMA IF NOT EXISTS banco_pf;")
            cur.execute("CREATE TABLE IF NOT EXISTS banco_pf.pf_referencias (id SERIAL PRIMARY KEY, tipo VARCHAR(50), nome VARCHAR(100), UNIQUE(tipo, nome));")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.convenio_por_planilha (
                    id SERIAL PRIMARY KEY,
                    convenio VARCHAR(100),
                    nome_planilha_sql VARCHAR(100),
                    tipo_planilha VARCHAR(100),
                    UNIQUE(convenio, nome_planilha_sql)
                );
            """)
            conn.commit(); conn.close()
        except: pass

# --- HELPERS DE FORMATAÇÃO E CÁLCULO ---

def formatar_cpf_visual(cpf_db):
    if not cpf_db: return ""
    return str(cpf_db).strip()

def limpar_normalizar_cpf(cpf_raw):
    if not cpf_raw: return ""
    return str(cpf_raw).strip()

def limpar_apenas_numeros(valor):
    if not valor: return ""
    return re.sub(r'\D', '', str(valor))

def validar_formatar_telefone(tel_raw):
    numeros = limpar_apenas_numeros(tel_raw)
    if len(numeros) == 10 or len(numeros) == 11: return numeros, None
    return None, "Telefone deve ter 10 ou 11 dígitos."

def validar_email(email):
    if not email: return False
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(regex, email))

def formatar_cnpj(valor):
    if not valor: return None
    numeros = re.sub(r'\D', '', str(valor))
    if not numeros: return None
    numeros = numeros.zfill(14)
    return f"{numeros[:2]}.{numeros[2:5]}.{numeros[5:8]}/{numeros[8:12]}-{numeros[12:]}"

def safe_view(valor):
    if valor is None: return ""
    v_str = str(valor).strip()
    if v_str.lower() in ['none', 'nan', 'null', 'nat', '']: return ""
    return v_str

# --- CARREGAMENTO DE DADOS ---
def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': []}
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)      
            params_busca = (cpf_norm,)
            
            df_d = pd.read_sql("SELECT * FROM banco_pf.pf_dados WHERE cpf = %s", conn, params=params_busca)
            if not df_d.empty: dados['geral'] = df_d.where(pd.notnull(df_d), None).iloc[0].to_dict()
            
            # Tabelas satélites
            col_fk = 'cpf_ref' 
            try: pd.read_sql("SELECT 1 FROM banco_pf.pf_telefones WHERE cpf_ref = '1' LIMIT 1", conn)
            except: col_fk = 'cpf'; conn.rollback()

            dados['telefones'] = pd.read_sql(f"SELECT numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE {col_fk} = %s", conn, params=params_busca).fillna("").to_dict('records')
            dados['emails'] = pd.read_sql(f"SELECT email FROM banco_pf.pf_emails WHERE {col_fk} = %s", conn, params=params_busca).fillna("").to_dict('records')
            dados['enderecos'] = pd.read_sql(f"SELECT rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE {col_fk} = %s", conn, params=params_busca).fillna("").to_dict('records')
            
            query_emp = f"SELECT convenio, matricula FROM banco_pf.pf_emprego_renda WHERE {col_fk} = %s"
            df_emp = pd.read_sql(query_emp, conn, params=params_busca)
            
            if not df_emp.empty:
                for _, row_emp in df_emp.iterrows():
                    conv_nome = str(row_emp['convenio']).strip() 
                    matricula = str(row_emp['matricula']).strip()
                    vinculo = {'convenio': conv_nome, 'matricula': matricula, 'contratos': []}
                    
                    try:
                        query_padrao = "SELECT * FROM banco_pf.pf_contratos WHERE matricula_ref = %s"
                        df_contratos = pd.read_sql(query_padrao, conn, params=(matricula,))
                        if not df_contratos.empty:
                            df_contratos['tipo_origem'] = 'Geral'
                            vinculo['contratos'] = df_contratos.to_dict('records')
                    except: pass
                    
                    dados['empregos'].append(vinculo)
        except Exception as e: print(f"Erro carregamento: {e}") 
        finally: conn.close()
    return dados

# --- CONFIGURAÇÃO DE CAMPOS ---
CONFIG_CADASTRO = {
    "Dados Pessoais": [
        {"label": "Nome Completo", "key": "nome", "tipo": "texto", "obrigatorio": True},
        {"label": "CPF", "key": "cpf", "tipo": "cpf", "obrigatorio": True},
        {"label": "RG", "key": "rg", "tipo": "texto"},
        {"label": "Data Nascimento", "key": "data_nascimento", "tipo": "data"},
        {"label": "Nome da Mãe", "key": "nome_mae", "tipo": "texto"},
    ]
}

def listar_tabelas_por_convenio(convenio):
    return [('banco_pf.pf_contratos', 'Contratos Gerais')]

def get_colunas_tabela(nome_tabela):
    return []

# --- INTERFACE PRINCIPAL (CORRIGIDA) ---
def interface_cadastro_pf():
    is_edit = st.session_state['pf_view'] == 'editar'
    
    cpf_formatado_titulo = ""
    if is_edit:
        raw_cpf = st.session_state.get('pf_cpf_selecionado', '')
        cpf_formatado_titulo = formatar_cpf_visual(raw_cpf)
    
    titulo = f"✏️ Editar: {cpf_formatado_titulo}" if is_edit else "➕ Novo Cadastro"
    st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'form_loaded': False}))
    st.markdown(f"### {titulo}")

    if 'dados_staging' not in st.session_state:
        st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': []}

    if is_edit and not st.session_state.get('form_loaded'):
        dados_db = carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
        st.session_state['dados_staging'] = dados_db
        st.session_state['form_loaded'] = True
    elif not is_edit and not st.session_state.get('form_loaded'):
        st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': []}
        st.session_state['form_loaded'] = True

    c_form, c_extra = st.columns([1.5, 1])

    with c_form:
        st.info("👤 Dados Pessoais")
        valores_form = {}
        for campo in CONFIG_CADASTRO["Dados Pessoais"]:
            key_widget = f"in_{campo['key']}"
            # CORREÇÃO: Usa get sem valor default para evitar string vazia em datas
            val_inicial = st.session_state['dados_staging']['geral'].get(campo['key'])
            
            if campo['tipo'] == 'data':
                # Garante que seja Date ou None (Evita string vazia)
                if val_inicial and isinstance(val_inicial, str):
                    try: 
                        # Tenta converter string para data
                        val_inicial = datetime.strptime(str(val_inicial)[:10], '%Y-%m-%d').date()
                    except: 
                        val_inicial = None
                
                # Se não for uma data válida, força None para o componente funcionar
                if not isinstance(val_inicial, (date, datetime)):
                    val_inicial = None

                valores_form[campo['key']] = st.date_input(campo['label'], value=val_inicial, format="DD/MM/YYYY", key=key_widget)
            else:
                # Para campos de texto, string vazia é aceitável
                val_texto = str(val_inicial) if val_inicial is not None else ""
                valores_form[campo['key']] = st.text_input(campo['label'], value=val_texto, key=key_widget)

    with c_extra:
        st.warning("📞 Contatos & Endereço (Opcional)")
        
        with st.expander("Telefones", expanded=False):
            novo_tel = st.text_input("Novo Telefone", key="new_tel")
            if st.button("Adicionar Tel"):
                if novo_tel: 
                    st.session_state['dados_staging']['telefones'].append({'numero': novo_tel, 'tag_whats': 'Não', 'tag_qualificacao': 'NC'})
                    st.rerun()
            for i, t in enumerate(st.session_state['dados_staging']['telefones']):
                c1, c2 = st.columns([4,1])
                c1.text(f"📱 {t['numero']}")
                if c2.button("X", key=f"del_tel_{i}"): 
                    st.session_state['dados_staging']['telefones'].pop(i); st.rerun()

        with st.expander("E-mails", expanded=False):
            novo_mail = st.text_input("Novo E-mail", key="new_mail")
            if st.button("Adicionar Email"):
                if novo_mail: 
                    st.session_state['dados_staging']['emails'].append({'email': novo_mail})
                    st.rerun()
            for i, m in enumerate(st.session_state['dados_staging']['emails']):
                c1, c2 = st.columns([4,1])
                c1.text(f"📧 {m['email']}")
                if c2.button("X", key=f"del_mail_{i}"): 
                    st.session_state['dados_staging']['emails'].pop(i); st.rerun()

    st.markdown("---")
    
    if st.button("💾 CONFIRMAR E SALVAR CADASTRO", type="primary", use_container_width=True):
        dados_finais = valores_form.copy()
        
        if not dados_finais.get('nome'):
            st.error("❌ O Nome é obrigatório.")
            return
        if not dados_finais.get('cpf'):
            st.error("❌ O CPF é obrigatório.")
            return
            
        st.session_state['dados_staging']['geral'] = dados_finais
        
        modo = "editar" if is_edit else "novo"
        cpf_orig = limpar_normalizar_cpf(st.session_state.get('pf_cpf_selecionado')) if is_edit else None
        
        sucesso, msg = salvar_pf_db(
            st.session_state['dados_staging'], 
            modo, 
            cpf_orig
        )
        
        if sucesso:
            st.success(f"✅ {msg}")
            time.sleep(1.5)
            st.session_state['pf_view'] = 'lista'
            st.session_state['form_loaded'] = False
            st.rerun()
        else:
            st.error(f"Erro ao salvar: {msg}")

# --- FUNÇÃO DE SALVAMENTO NO BANCO ---
def salvar_pf_db(dados_completos, modo, cpf_original=None):
    conn = get_conn()
    if not conn: return False, "Falha na conexão com o banco."
    
    try:
        cur = conn.cursor()
        geral = dados_completos['geral']
        cpf_limpo = limpar_normalizar_cpf(geral['cpf'])
        geral['cpf'] = cpf_limpo
        
        for k, v in geral.items():
            if isinstance(v, str): geral[k] = v.upper()

        cols = list(geral.keys())
        vals = list(geral.values())
        
        if modo == "novo":
            placeholders = ", ".join(["%s"] * len(vals))
            col_names = ", ".join(cols)
            query = f"INSERT INTO banco_pf.pf_dados ({col_names}) VALUES ({placeholders})"
            cur.execute(query, vals)
        else:
            set_clause = ", ".join([f"{k}=%s" for k in cols])
            vals_update = vals + [cpf_original]
            query = f"UPDATE banco_pf.pf_dados SET {set_clause} WHERE cpf=%s"
            cur.execute(query, vals_update)
            
            col_fk = 'cpf_ref'
            try: cur.execute("SELECT 1 FROM banco_pf.pf_telefones WHERE cpf_ref='1' LIMIT 1")
            except: col_fk = 'cpf'; conn.rollback(); cur = conn.cursor()
            
            cur.execute(f"DELETE FROM banco_pf.pf_telefones WHERE {col_fk} = %s", (cpf_limpo,))
            cur.execute(f"DELETE FROM banco_pf.pf_emails WHERE {col_fk} = %s", (cpf_limpo,))
            
        col_fk = 'cpf_ref'
        try: cur.execute("SELECT 1 FROM banco_pf.pf_telefones WHERE cpf_ref='1' LIMIT 1")
        except: col_fk = 'cpf'; conn.rollback(); cur = conn.cursor()
        
        for t in dados_completos['telefones']:
            cur.execute(f"INSERT INTO banco_pf.pf_telefones ({col_fk}, numero, tag_whats, tag_qualificacao, data_atualizacao) VALUES (%s, %s, %s, %s, %s)", 
                        (cpf_limpo, t['numero'], t.get('tag_whats'), t.get('tag_qualificacao'), date.today()))

        for m in dados_completos['emails']:
            cur.execute(f"INSERT INTO banco_pf.pf_emails ({col_fk}, email) VALUES (%s, %s)", 
                        (cpf_limpo, m['email']))

        conn.commit()
        conn.close()
        return True, "Cadastro salvo com sucesso!"
        
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return False, "Este CPF já está cadastrado no sistema."
    except Exception as e:
        if conn: conn.close()
        return False, str(e)

def excluir_pf(cpf):
    conn = get_conn()
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            cur = conn.cursor()
            cur.execute("DELETE FROM banco_pf.pf_dados WHERE cpf = %s", (cpf_norm,))
            conn.commit(); conn.close()
            return True
        except: return False
    return False

@st.dialog("⚠️ Excluir Cadastro")
def dialog_excluir_pf(cpf, nome):
    st.error(f"Apagar **{nome}**?")
    if st.button("Confirmar Exclusão", type="primary"):
        if excluir_pf(cpf): st.success("Apagado!"); time.sleep(1); st.rerun()

@st.dialog("👁️ Detalhes do Cliente", width="large")
def dialog_visualizar_cliente(cpf_cliente):
    cpf_vis = formatar_cpf_visual(cpf_cliente)
    dados = carregar_dados_completos(cpf_cliente)
    g = dados.get('geral', {})
    if not g: st.error("Cliente não encontrado."); return
    
    st.markdown(f"### 👤 {g.get('nome', 'Sem Nome')}")
    st.markdown(f"**CPF:** {cpf_vis}")
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    c1.write(f"**Mãe:** {safe_view(g.get('nome_mae'))}")
    c2.write(f"**Nascimento:** {safe_view(g.get('data_nascimento'))}")
    
    st.write("**Telefones:**")
    for t in dados.get('telefones', []):
        st.caption(f"📱 {t['numero']}")