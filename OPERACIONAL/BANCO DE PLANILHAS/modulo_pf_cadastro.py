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
            # Tabelas estruturais omitidas para brevidade, mas devem ser mantidas se não existirem
            # ... (Lógica de criação de tabelas pf_*)
            conn.commit()
            conn.close()
        except: pass

# --- HELPERS ---
def formatar_cpf_visual(cpf_db):
    if not cpf_db: return ""
    cpf_limpo = str(cpf_db).strip()
    cpf_full = cpf_limpo.zfill(11)
    return f"{cpf_full[:3]}.{cpf_full[3:6]}.{cpf_full[6:9]}-{cpf_full[9:]}"

def limpar_normalizar_cpf(cpf_raw):
    if not cpf_raw: return ""
    apenas_nums = re.sub(r'\D', '', str(cpf_raw))
    return apenas_nums.lstrip('0')

def limpar_apenas_numeros(valor):
    if not valor: return ""
    return re.sub(r'\D', '', str(valor))

def validar_formatar_telefone(tel_raw):
    numeros = limpar_apenas_numeros(tel_raw)
    if len(numeros) == 10 or len(numeros) == 11:
        return numeros, None
    return None, "Telefone deve ter 10 ou 11 dígitos."

def validar_email(email):
    if not email: return False
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(regex, email))

def validar_formatar_cep(cep_raw):
    numeros = limpar_apenas_numeros(cep_raw)
    if len(numeros) != 8: return None, "CEP deve ter 8 dígitos."
    return f"{numeros[:5]}-{numeros[5:]}", None

def converter_data_br_iso(valor):
    if not valor or pd.isna(valor): return None
    valor_str = str(valor).strip().split(' ')[0]
    formatos = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"]
    for fmt in formatos:
        try: return datetime.strptime(valor_str, fmt).strftime("%Y-%m-%d")
        except ValueError: continue
    return None

def buscar_referencias(tipo):
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT nome FROM pf_referencias WHERE tipo = %s ORDER BY nome", conn, params=(tipo,))
            conn.close()
            return df['nome'].tolist()
        except: conn.close()
    return []

# --- CRUD ---
def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {}
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            df_d = pd.read_sql("SELECT * FROM pf_dados WHERE cpf = %s", conn, params=(cpf_norm,))
            if not df_d.empty: dados['geral'] = df_d.fillna("").iloc[0]
            else: dados['geral'] = None
            
            # Carrega tabelas relacionadas
            dados['telefones'] = pd.read_sql("SELECT numero, data_atualizacao, tag_whats, tag_qualificacao FROM pf_telefones WHERE cpf_ref = %s", conn, params=(cpf_norm,)).fillna("")
            dados['emails'] = pd.read_sql("SELECT email FROM pf_emails WHERE cpf_ref = %s", conn, params=(cpf_norm,)).fillna("")
            dados['enderecos'] = pd.read_sql("SELECT rua, bairro, cidade, uf, cep FROM pf_enderecos WHERE cpf_ref = %s", conn, params=(cpf_norm,)).fillna("")
            dados['empregos'] = pd.read_sql("SELECT id, convenio, matricula, dados_extras FROM pf_emprego_renda WHERE cpf_ref = %s", conn, params=(cpf_norm,)).fillna("")
            dados['contratos'] = pd.DataFrame()
            dados['dados_clt'] = pd.DataFrame() 

            if not dados['empregos'].empty:
                matr_list = tuple(dados['empregos']['matricula'].dropna().tolist())
                if matr_list:
                    placeholders = ",".join(["%s"] * len(matr_list))
                    q_contratos = f"SELECT matricula_ref, contrato, dados_extras FROM pf_contratos WHERE matricula_ref IN ({placeholders})"
                    dados['contratos'] = pd.read_sql(q_contratos, conn, params=matr_list).fillna("")
                    try:
                        q_clt = f"""SELECT matricula_ref, nome_convenio, cnpj_nome, cnpj_numero, cnae_nome, cnae_codigo, data_admissao, cbo_nome, cbo_codigo, qtd_funcionarios, data_abertura_empresa, tempo_abertura_anos, tempo_admissao_anos FROM admin.pf_contratos_clt WHERE matricula_ref IN ({placeholders})"""
                        dados['dados_clt'] = pd.read_sql(q_clt, conn, params=matr_list).fillna("")
                        for col in ['data_admissao', 'data_abertura_empresa']:
                            if col in dados['dados_clt'].columns: dados['dados_clt'][col] = pd.to_datetime(dados['dados_clt'][col], errors='coerce').dt.strftime('%d/%m/%Y')
                    except: pass
        except: pass
        finally: conn.close()
    return dados

def salvar_pf(dados_gerais, df_tel, df_email, df_end, df_emp, df_contr, modo="novo", cpf_original=None):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cpf_limpo = limpar_normalizar_cpf(dados_gerais['cpf'])
            dados_gerais['cpf'] = cpf_limpo
            if cpf_original: cpf_original = limpar_normalizar_cpf(cpf_original)
            dados_gerais = {k: (v.upper() if isinstance(v, str) else v) for k, v in dados_gerais.items()}

            if modo == "novo":
                cols = list(dados_gerais.keys()); vals = list(dados_gerais.values())
                placeholders = ", ".join(["%s"] * len(vals)); col_names = ", ".join(cols)
                cur.execute(f"INSERT INTO pf_dados ({col_names}) VALUES ({placeholders})", vals)
            else:
                set_clause = ", ".join([f"{k}=%s" for k in dados_gerais.keys()])
                vals = list(dados_gerais.values()) + [cpf_original]
                cur.execute(f"UPDATE pf_dados SET {set_clause} WHERE cpf=%s", vals)
            
            cpf_chave = dados_gerais['cpf']
            if modo == "editar":
                for tb in ['pf_telefones', 'pf_emails', 'pf_enderecos', 'pf_emprego_renda']:
                    cur.execute(f"DELETE FROM {tb} WHERE cpf_ref = %s", (cpf_chave,))
            
            def df_upper(df): return df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
            if not df_tel.empty:
                for _, r in df_upper(df_tel).iterrows(): cur.execute("INSERT INTO pf_telefones (cpf_ref, numero, tag_whats, tag_qualificacao, data_atualizacao) VALUES (%s, %s, %s, %s, %s)", (cpf_chave, r['numero'], r.get('tag_whats'), r.get('tag_qualificacao'), date.today()))
            if not df_email.empty:
                for _, r in df_upper(df_email).iterrows(): cur.execute("INSERT INTO pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, r['email']))
            if not df_end.empty:
                for _, r in df_upper(df_end).iterrows(): cur.execute("INSERT INTO pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, r['rua'], r['bairro'], r['cidade'], r['uf'], r['cep']))
            if not df_emp.empty:
                for _, r in df_upper(df_emp).iterrows(): cur.execute("INSERT INTO pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s)", (cpf_chave, r['convenio'], r['matricula'], r['dados_extras']))
            if not df_contr.empty:
                for _, r in df_upper(df_contr).iterrows():
                    cur.execute("SELECT 1 FROM pf_emprego_renda WHERE matricula=%s", (r['matricula_ref'],))
                    if cur.fetchone(): cur.execute("INSERT INTO pf_contratos (matricula_ref, contrato, dados_extras) VALUES (%s, %s, %s)", (r['matricula_ref'], r['contrato'], r['dados_extras']))

            conn.commit(); conn.close(); return True, "Salvo com sucesso!"
        except Exception as e: return False, str(e)
    return False, "Erro conexão"

def excluir_pf(cpf):
    conn = get_conn()
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            cur = conn.cursor()
            cur.execute("DELETE FROM pf_dados WHERE cpf = %s", (cpf_norm,))
            conn.commit(); conn.close()
            return True
        except: return False
    return False

@st.dialog("👁️ Detalhes do Cliente")
def dialog_visualizar_cliente(cpf_cliente):
    cpf_vis = formatar_cpf_visual(cpf_cliente)
    dados = carregar_dados_completos(cpf_cliente)
    g = dados.get('geral')
    if g is None: st.error("Cliente não encontrado."); return
    
    st.markdown(f"### 👤 {g['nome']}")
    st.markdown(f"**CPF:** {cpf_vis}")
    st.divider()
    t1, t2, t3 = st.tabs(["📋 Cadastro", "💼 Profissional & CLT", "📞 Contatos"])
    with t1:
        c1, c2 = st.columns(2)
        c1.write(f"**Nascimento:** {pd.to_datetime(g['data_nascimento']).strftime('%d/%m/%Y') if g['data_nascimento'] else '-'}")
        c1.write(f"**RG:** {g['rg']}"); c2.write(f"**PIS:** {g['pis']}")
        st.markdown("##### 🏠 Endereços")
        df_end = dados.get('enderecos')
        if not df_end.empty:
            for _, row in df_end.iterrows(): st.info(f"📍 {row['rua']}, {row['bairro']} - {row['cidade']}/{row['uf']}")
    with t2:
        df_emp = dados.get('empregos'); df_clt = dados.get('dados_clt')
        if not df_emp.empty:
            for _, row in df_emp.iterrows():
                with st.expander(f"🏢 {row['convenio']} | Matr: {row['matricula']}", expanded=True):
                    if not df_clt.empty:
                        vinc = df_clt[df_clt['matricula_ref'] == row['matricula']]
                        if not vinc.empty:
                            d = vinc.iloc[0]
                            st.write(f"**Empresa:** {d['cnpj_nome']}"); st.write(f"**Cargo:** {d['cbo_nome']}")
    with t3:
        df_tel = dados.get('telefones')
        if not df_tel.empty:
            for _, r in df_tel.iterrows(): st.write(f"📱 {r['numero']}")

@st.dialog("⚠️ Excluir Cadastro")
def dialog_excluir_pf(cpf, nome):
    st.error(f"Apagar **{nome}**?")
    if st.button("Confirmar Exclusão", type="primary"):
        if excluir_pf(cpf): st.success("Apagado!"); time.sleep(1); st.rerun()

# --- INTERFACE DE CADASTRO ---
def interface_cadastro_pf():
    is_edit = st.session_state['pf_view'] == 'editar'
    cpf_titulo = formatar_cpf_visual(st.session_state.get('pf_cpf_selecionado')) if is_edit else ""
    titulo = f"✏️ Editar: {cpf_titulo}" if is_edit else "➕ Novo Cadastro"
    
    st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'form_loaded': False}))
    st.markdown(f"### {titulo}")

    if is_edit and not st.session_state.get('form_loaded'):
        dados_db = carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
        st.session_state['dados_gerais_temp'] = dados_db.get('geral', {})
        st.session_state['temp_telefones'] = dados_db.get('telefones', pd.DataFrame()).to_dict('records')
        st.session_state['temp_emails'] = dados_db.get('emails', pd.DataFrame()).to_dict('records')
        st.session_state['temp_enderecos'] = dados_db.get('enderecos', pd.DataFrame()).to_dict('records')
        st.session_state['temp_empregos'] = dados_db.get('empregos', pd.DataFrame()).to_dict('records')
        st.session_state['temp_contratos'] = dados_db.get('contratos', pd.DataFrame()).to_dict('records')
        st.session_state['form_loaded'] = True
    elif not is_edit and not st.session_state.get('form_loaded'):
        st.session_state['dados_gerais_temp'] = {}
        st.session_state['temp_telefones'] = []
        st.session_state['temp_emails'] = []
        st.session_state['temp_enderecos'] = []
        st.session_state['temp_empregos'] = []
        st.session_state['temp_contratos'] = []
        st.session_state['form_loaded'] = True

    g = st.session_state.get('dados_gerais_temp', {})

    with st.form("form_cadastro_pf"):
        t1, t2 = st.tabs(["👤 Dados Pessoais", "📞 Contatos"])
        with t1:
            c1, c2, c3 = st.columns(3)
            nome = c1.text_input("Nome *", value=g.get('nome', ''))
            cpf = c2.text_input("CPF *", value=g.get('cpf', ''), disabled=is_edit)
            d_nasc = c3.date_input("Nascimento", value=pd.to_datetime(g.get('data_nascimento')).date() if g.get('data_nascimento') else None, format="DD/MM/YYYY")
        with t2:
            st.info("Para adicionar contatos detalhados, salve o cadastro primeiro ou use a interface completa.")
        
        if st.form_submit_button("💾 Salvar"):
            if nome and cpf:
                suc, msg = salvar_pf({'nome': nome, 'cpf': cpf, 'data_nascimento': d_nasc}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "editar" if is_edit else "novo", cpf if is_edit else None)
                if suc: st.success(msg); time.sleep(1); st.session_state['pf_view'] = 'lista'; st.rerun()
                else: st.error(msg)
            else: st.warning("Nome e CPF obrigatórios.")