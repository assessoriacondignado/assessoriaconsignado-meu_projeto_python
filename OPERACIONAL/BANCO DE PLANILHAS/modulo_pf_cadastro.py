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
            
            # Tabela de Referências Gerais
            cur.execute("CREATE TABLE IF NOT EXISTS banco_pf.pf_referencias (id SERIAL PRIMARY KEY, tipo VARCHAR(50), nome VARCHAR(100), UNIQUE(tipo, nome));")
            
            # Tabela de Mapeamento (Convênio -> Tabela SQL)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.convenio_por_planilha (
                    id SERIAL PRIMARY KEY,
                    convenio VARCHAR(100),
                    nome_planilha_sql VARCHAR(100),
                    UNIQUE(convenio, nome_planilha_sql)
                );
            """)
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

def validar_formatar_cpf(cpf_raw):
    numeros = limpar_apenas_numeros(cpf_raw)
    if len(numeros) != 11:
        return None, "CPF deve ter 11 dígitos."
    cpf_fmt = f"{numeros[:3]}.{numeros[3:6]}.{numeros[6:9]}-{numeros[9:]}"
    return cpf_fmt, None

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

def calcular_idade_hoje(dt_nasc):
    if not dt_nasc: return None
    hoje = date.today()
    if isinstance(dt_nasc, datetime): dt_nasc = dt_nasc.date()
    return hoje.year - dt_nasc.year - ((hoje.month, hoje.day) < (dt_nasc.month, dt_nasc.day))

def safe_view(valor):
    if valor is None: return ""
    v_str = str(valor).strip()
    if v_str.lower() in ['none', 'nan', 'null', 'nat', '']: return ""
    return v_str

# --- CARREGAMENTO INTELIGENTE ---
def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {
        'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 
        'empregos': [] 
    }
    
    if conn:
        try:
            # 1. Padronização do CPF para busca (Testa com e sem zeros à esquerda)
            cpf_norm = limpar_normalizar_cpf(cpf)      
            cpf_full = str(cpf_norm).zfill(11)         
            params_busca = (cpf_norm, cpf_full)
            
            # 2. Dados Gerais
            df_d = pd.read_sql("SELECT * FROM banco_pf.pf_dados WHERE cpf IN %s", conn, params=(params_busca,))
            if not df_d.empty: 
                dados['geral'] = df_d.where(pd.notnull(df_d), None).iloc[0].to_dict()
            
            # 3. Contatos e Endereços
            dados['telefones'] = pd.read_sql("SELECT numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf_ref IN %s", conn, params=(params_busca,)).fillna("").to_dict('records')
            dados['emails'] = pd.read_sql("SELECT email FROM banco_pf.pf_emails WHERE cpf_ref IN %s", conn, params=(params_busca,)).fillna("").to_dict('records')
            dados['enderecos'] = pd.read_sql("SELECT rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf_ref IN %s", conn, params=(params_busca,)).fillna("").to_dict('records')
            
            # 4. LÓGICA DE VÍNCULO (EMPREGO E RENDA)
            # Busca na tabela correta: banco_pf.pf_emprego_renda usando a coluna cpf_ref
            query_emp = """
                SELECT convenio, matricula, dados_extras 
                FROM banco_pf.pf_emprego_renda 
                WHERE cpf_ref IN %s
            """
            df_emp = pd.read_sql(query_emp, conn, params=(params_busca,))
            
            if not df_emp.empty:
                for _, row_emp in df_emp.iterrows():
                    conv_nome = str(row_emp['convenio']).strip().upper()
                    matricula = str(row_emp['matricula']).strip()
                    extras = row_emp['dados_extras']
                    
                    # Objeto de Vínculo
                    vinculo = {
                        'convenio': conv_nome,
                        'matricula': matricula,
                        'dados_extras': extras,
                        'contratos': []
                    }

                    # Identifica se há contratos detalhados em outras tabelas
                    query_map = "SELECT nome_planilha_sql FROM banco_pf.convenio_por_planilha WHERE UPPER(convenio) = %s"
                    df_map = pd.read_sql(query_map, conn, params=(conv_nome,))
                    
                    if not df_map.empty:
                        tabela_destino = df_map.iloc[0]['nome_planilha_sql']
                        try:
                            cur = conn.cursor()
                            # Verifica se a tabela existe antes de consultar
                            cur.execute("SELECT to_regclass(%s)", (tabela_destino,))
                            if cur.fetchone()[0]:
                                query_contratos = f"SELECT * FROM {tabela_destino} WHERE matricula_ref = %s"
                                df_contratos = pd.read_sql(query_contratos, conn, params=(matricula,))
                                if not df_contratos.empty:
                                    vinculo['contratos'] = df_contratos.to_dict('records')
                        except: pass
                    
                    # Adiciona à lista de empregos encontrados
                    dados['empregos'].append(vinculo)

        except Exception as e:
            print(f"Erro ao carregar dados completos: {e}") 
        finally: 
            conn.close()
            
    return dados

# --- FUNÇÕES DE SALVAMENTO ---
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
                cur.execute(f"INSERT INTO banco_pf.pf_dados ({col_names}) VALUES ({placeholders})", vals)
            else:
                set_clause = ", ".join([f"{k}=%s" for k in dados_gerais.keys()])
                vals = list(dados_gerais.values()) + [cpf_original]
                cur.execute(f"UPDATE banco_pf.pf_dados SET {set_clause} WHERE cpf=%s", vals)
            
            cpf_chave = dados_gerais['cpf']
            if modo == "editar":
                # Limpa dados antigos para recriar (estratégia de replace)
                for tb in ['banco_pf.pf_telefones', 'banco_pf.pf_emails', 'banco_pf.pf_enderecos', 'banco_pf.pf_emprego_renda']:
                    cur.execute(f"DELETE FROM {tb} WHERE cpf_ref = %s", (cpf_chave,))
            
            def df_upper(df): return df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
            if not df_tel.empty:
                for _, r in df_upper(df_tel).iterrows(): cur.execute("INSERT INTO banco_pf.pf_telefones (cpf_ref, numero, tag_whats, tag_qualificacao, data_atualizacao) VALUES (%s, %s, %s, %s, %s)", (cpf_chave, r['numero'], r.get('tag_whats'), r.get('tag_qualificacao'), date.today()))
            if not df_email.empty:
                for _, r in df_upper(df_email).iterrows(): cur.execute("INSERT INTO banco_pf.pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, r['email']))
            if not df_end.empty:
                for _, r in df_upper(df_end).iterrows(): cur.execute("INSERT INTO banco_pf.pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, r['rua'], r['bairro'], r['cidade'], r['uf'], r['cep']))
            if not df_emp.empty:
                for _, r in df_upper(df_emp).iterrows(): cur.execute("INSERT INTO banco_pf.pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s)", (cpf_chave, r['convenio'], r['matricula'], r['dados_extras']))
            
            if not df_contr.empty:
                df_padrao = df_contr[df_contr.get('origem_tabela', 'pf_contratos') == 'pf_contratos']
                if not df_padrao.empty:
                     cur.execute("DELETE FROM banco_pf.pf_contratos WHERE matricula_ref IN (SELECT matricula FROM banco_pf.pf_emprego_renda WHERE cpf_ref = %s)", (cpf_chave,))
                     for _, r in df_upper(df_padrao).iterrows():
                        cur.execute("SELECT 1 FROM banco_pf.pf_emprego_renda WHERE matricula=%s", (r['matricula_ref'],))
                        if cur.fetchone(): cur.execute("INSERT INTO banco_pf.pf_contratos (matricula_ref, contrato, dados_extras) VALUES (%s, %s, %s)", (r['matricula_ref'], r['contrato'], r['dados_extras']))

            conn.commit(); conn.close(); return True, "Salvo com sucesso!"
        except Exception as e: return False, str(e)
    return False, "Erro conexão"

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

# --- VISUALIZAÇÃO LUPA (POP-UP) ---
@st.dialog("👁️ Detalhes do Cliente")
def dialog_visualizar_cliente(cpf_cliente):
    cpf_vis = formatar_cpf_visual(cpf_cliente)
    dados = carregar_dados_completos(cpf_cliente)
    g = dados.get('geral', {})
    
    if not g: st.error("Cliente não encontrado."); return
    
    nome_display = g.get('nome') or "Nome não informado"
    st.markdown(f"### 👤 {nome_display}")
    st.markdown(f"**CPF:** {cpf_vis}")
    st.divider()
    
    t1, t2, t3 = st.tabs(["📋 Cadastro & Vínculos", "💼 Detalhes Financeiros", "📞 Contatos"])
    
    with t1:
        # DADOS PESSOAIS
        st.markdown("##### 🆔 Dados Pessoais")
        c1, c2 = st.columns(2)
        nasc = g.get('data_nascimento')
        idade = calcular_idade_hoje(nasc)
        txt_nasc = f"{nasc.strftime('%d/%m/%Y')} ({idade} anos)" if idade and isinstance(nasc, (date, datetime)) else safe_view(nasc)
        
        c1.write(f"**Nascimento:** {txt_nasc}")
        c1.write(f"**RG:** {safe_view(g.get('rg'))}")
        c2.write(f"**Mãe:** {safe_view(g.get('nome_mae'))}")
        
        st.markdown("---")
        
        # VÍNCULOS (CONVÊNIO + MATRÍCULA)
        st.markdown("##### 🔗 Vínculos Identificados")
        
        vinculos = dados.get('empregos', [])
        
        if vinculos:
            for v in vinculos:
                # --- VISUALIZAÇÃO FORMATADA: MATRICULA - CONVENIO ---
                # Exibe a matrícula em negrito seguido do convênio
                st.info(f"🆔 **{v['matricula']}** - {v['convenio']}")
                
                if v.get('dados_extras'):
                    st.caption(f"Obs: {safe_view(v['dados_extras'])}")
        else:
            st.warning("Nenhum vínculo (Convênio/Matrícula) localizado para este CPF.")

        st.markdown("---")
        st.markdown("##### 🏠 Endereços")
        for end in dados.get('enderecos', []):
            st.success(f"📍 {safe_view(end.get('rua'))}, {safe_view(end.get('bairro'))} - {safe_view(end.get('cidade'))}/{safe_view(end.get('uf'))}")

    with t2:
        # DETALHES DOS CONTRATOS (Vindo da tabela dinâmica)
        st.markdown("##### 💰 Contratos por Vínculo")
        if vinculos:
            for v in vinculos:
                contratos = v.get('contratos', [])
                with st.expander(f"{v['convenio']} ({len(contratos)} contratos)", expanded=True):
                    if contratos:
                        df_c = pd.DataFrame(contratos)
                        # Remove colunas técnicas e ajusta visualização
                        cols_hide = ['id', 'matricula_ref', 'importacao_id', 'data_criacao', 'data_atualizacao']
                        cols_show = [c for c in df_c.columns if c not in cols_hide]
                        st.dataframe(df_c[cols_show], hide_index=True, use_container_width=True)
                    else:
                        st.caption("Matrícula encontrada, mas sem contratos na tabela específica.")
        else:
            st.info("Sem dados financeiros disponíveis.")

    with t3:
        for t in dados.get('telefones', []): 
            st.write(f"📱 {safe_view(t.get('numero'))} ({safe_view(t.get('tag_whats'))})")
        for m in dados.get('emails', []): 
            st.write(f"📧 {safe_view(m.get('email'))}")

# --- CONFIGURAÇÃO E STAGING ---
CONFIG_CADASTRO = {
    "Dados Pessoais": [
        {"label": "Nome Completo", "key": "nome", "tabela": "geral", "tipo": "texto", "obrigatorio": True},
        {"label": "CPF", "key": "cpf", "tabela": "geral", "tipo": "cpf", "obrigatorio": True},
        {"label": "RG", "key": "rg", "tabela": "geral", "tipo": "texto"},
        {"label": "Data Nascimento", "key": "data_nascimento", "tabela": "geral", "tipo": "data"},
        {"label": "Nome da Mãe", "key": "nome_mae", "tabela": "geral", "tipo": "texto"},
        {"label": "Nome do Pai", "key": "nome_pai", "tabela": "geral", "tipo": "texto"},
        {"label": "PIS", "key": "pis", "tabela": "geral", "tipo": "texto"},
        {"label": "CNH", "key": "cnh", "tabela": "geral", "tipo": "texto"},
    ],
    "Contatos": [
        {"label": "Telefone", "key": "numero", "tabela": "telefones", "tipo": "telefone", "multiplo": True, "extras": ["tag_whats", "tag_qualificacao"]},
        {"label": "E-mail", "key": "email", "tabela": "emails", "tipo": "email", "multiplo": True},
    ],
    "Endereços": [
        {"label": "CEP", "key": "cep", "tabela": "enderecos", "tipo": "cep", "multiplo": True, "agrupado": True}, 
        {"label": "Logradouro", "key": "rua", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "Bairro", "key": "bairro", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "Cidade", "key": "cidade", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "UF", "key": "uf", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
    ],
    "Profissional": [
        {"label": "Convênio", "key": "convenio", "tabela": "empregos", "tipo": "texto", "multiplo": True, "agrupado": True},
        {"label": "Matrícula", "key": "matricula", "tabela": "empregos", "tipo": "texto", "multiplo": True, "vinculo": "convenio"},
        {"label": "Dados Extras", "key": "dados_extras", "tabela": "empregos", "tipo": "texto", "multiplo": True, "vinculo": "convenio"},
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
    elif campo_config['tipo'] == 'cep':
        val, erro = validar_formatar_cep(valor)
        if not erro: valor_final = val
    
    if erro: st.toast(f"❌ {erro}"); return
    if not valor_final and campo_config.get('obrigatorio'): st.toast(f"❌ O campo {campo_config['label']} é obrigatório."); return

    if campo_config.get('multiplo'):
        novo_item = {chave: valor_final}
        if extras: novo_item.update(extras)
        if isinstance(valor, dict): st.session_state['dados_staging'][tabela].append(valor)
        else: st.session_state['dados_staging'][tabela].append(novo_item)
        st.toast(f"✅ {campo_config['label']} adicionado!")
    else:
        st.session_state['dados_staging'][tabela][chave] = valor_final
        st.toast(f"✅ {campo_config['label']} atualizado!")

def interface_cadastro_pf():
    is_edit = st.session_state['pf_view'] == 'editar'
    cpf_titulo = formatar_cpf_visual(st.session_state.get('pf_cpf_selecionado')) if is_edit else ""
    titulo = f"✏️ Editar: {cpf_titulo}" if is_edit else "➕ Novo Cadastro"
    
    st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'form_loaded': False}))
    st.markdown(f"### {titulo}")

    if 'dados_staging' not in st.session_state:
        st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': [], 'dados_clt': []}

    if is_edit and not st.session_state.get('form_loaded'):
        dados_db = carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
        st.session_state['dados_staging'] = dados_db
        st.session_state['form_loaded'] = True
    elif not is_edit and not st.session_state.get('form_loaded'):
        st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': [], 'dados_clt': []}
        st.session_state['form_loaded'] = True

    c_builder, c_preview = st.columns([1.5, 3.5])

    with c_builder:
        st.markdown("#### 🏗️ Inserir Dados")
        with st.expander("Dados Pessoais", expanded=True):
            for campo in CONFIG_CADASTRO["Dados Pessoais"]:
                if is_edit and campo['key'] == 'cpf':
                    st.text_input(campo['label'], value=st.session_state['dados_staging']['geral'].get('cpf', ''), disabled=True)
                    continue
                if campo['tipo'] == 'data':
                    val = st.date_input(campo['label'], value=None, min_value=date(1900, 1, 1), max_value=date(2050, 12, 31), format="DD/MM/YYYY", key=f"in_{campo['key']}")
                    if st.button("Inserir", key=f"btn_{campo['key']}"): inserir_dado_staging(campo, val)
                else:
                    val = st.text_input(campo['label'], key=f"in_{campo['key']}")
                    if st.button("Inserir", key=f"btn_{campo['key']}"): inserir_dado_staging(campo, val)
        
        with st.expander("Contatos"):
            tel = st.text_input("Número", key="in_tel_num")
            c_w, c_q = st.columns(2)
            whats = c_w.selectbox("WhatsApp", ["Não", "Sim"], key="in_tel_w")
            qualif = c_q.selectbox("Qualif.", ["NC", "CONFIRMADO"], key="in_tel_q")
            if st.button("Inserir Telefone"):
                cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'numero'][0]
                inserir_dado_staging(cfg, tel, {'tag_whats': whats, 'tag_qualificacao': qualif})
            st.divider()
            mail = st.text_input("E-mail", key="in_mail")
            if st.button("Inserir E-mail"):
                cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'email'][0]
                inserir_dado_staging(cfg, mail)

        with st.expander("Endereço"):
            cep = st.text_input("CEP", key="in_end_cep")
            rua = st.text_input("Logradouro", key="in_end_rua")
            bairro = st.text_input("Bairro", key="in_end_bairro")
            c_cid, c_uf = st.columns([3, 1])
            cidade = c_cid.text_input("Cidade", key="in_end_cid")
            uf = c_uf.text_input("UF", key="in_end_uf")
            if st.button("Inserir Endereço"):
                obj_end = {'cep': cep, 'rua': rua, 'bairro': bairro, 'cidade': cidade, 'uf': uf}
                cfg = [c for c in CONFIG_CADASTRO["Endereços"] if c['key'] == 'cep'][0]
                inserir_dado_staging(cfg, obj_end)

        with st.expander("Profissional"):
            conv = st.text_input("Convênio", key="in_emp_conv")
            matr = st.text_input("Matrícula", key="in_emp_matr")
            extra = st.text_input("Dados Extras", key="in_emp_extra")
            if st.button("Inserir Vínculo"):
                obj_emp = {'convenio': conv, 'matricula': matr, 'dados_extras': extra}
                cfg = [c for c in CONFIG_CADASTRO["Profissional"] if c['key'] == 'convenio'][0]
                inserir_dado_staging(cfg, obj_emp)

            st.divider()
            st.caption("Adicionar Contrato (Manual)")
            matrs_disponiveis = [e.get('matricula') for e in st.session_state['dados_staging']['empregos'] if e.get('matricula')]
            if matrs_disponiveis:
                m_ref = st.selectbox("Vincular à Matrícula", matrs_disponiveis, key="in_ctr_ref")
                ctr_num = st.text_input("Nº Contrato", key="in_ctr_num")
                ctr_det = st.text_input("Detalhes", key="in_ctr_det")
                if st.button("Inserir Contrato"):
                    st.session_state['dados_staging']['contratos'].append({
                        'matricula_ref': m_ref, 'contrato': ctr_num, 'dados_extras': ctr_det, 'origem_tabela': 'pf_contratos'
                    })
                    st.toast("✅ Contrato adicionado!")
            else:
                st.info("Insira um vínculo profissional primeiro.")

    with c_preview:
        st.markdown("### 📋 Resumo do Cadastro")
        st.info("👤 Dados Pessoais")
        geral = st.session_state['dados_staging'].get('geral', {})
        if geral:
            cols = st.columns(3)
            idx = 0
            for k, v in geral.items():
                if v:
                    val_str = v.strftime('%d/%m/%Y') if isinstance(v, (date, datetime)) else str(v)
                    cols[idx%3].text_input(k.upper(), value=val_str, disabled=True, key=f"view_geral_{k}")
                    idx += 1
        else: st.caption("Nenhum dado pessoal inserido.")

        st.warning("📞 Contatos")
        tels = st.session_state['dados_staging'].get('telefones', [])
        mails = st.session_state['dados_staging'].get('emails', [])
        
        if tels:
            for i, t in enumerate(tels):
                c1, c2, c3 = st.columns([4, 2, 1])
                c1.write(f"📱 {t.get('numero')} ({t.get('tag_whats')})")
                if c3.button("🗑️", key=f"rm_tel_{i}"): 
                    st.session_state['dados_staging']['telefones'].pop(i); st.rerun()
        if mails:
            for i, m in enumerate(mails):
                c1, c2 = st.columns([6, 1])
                c1.write(f"📧 {m.get('email')}")
                if c2.button("🗑️", key=f"rm_mail_{i}"):
                    st.session_state['dados_staging']['emails'].pop(i); st.rerun()

        st.success("🏠 Endereços")
        ends = st.session_state['dados_staging'].get('enderecos', [])
        if ends:
            for i, e in enumerate(ends):
                c1, c2 = st.columns([6, 1])
                c1.write(f"📍 {e.get('rua')}, {e.get('bairro')} - {e.get('cidade')}/{e.get('uf')} ({e.get('cep')})")
                if c2.button("🗑️", key=f"rm_end_{i}"):
                    st.session_state['dados_staging']['enderecos'].pop(i); st.rerun()

        st.error("💼 Profissional & Contratos")
        emps = st.session_state['dados_staging'].get('empregos', [])
        all_ctrs = st.session_state['dados_staging'].get('contratos', [])
        
        if emps:
            for i, emp in enumerate(emps):
                matr = emp.get('matricula')
                with st.container(border=True):
                    c1, c2 = st.columns([6, 1])
                    c1.markdown(f"**{emp.get('convenio')}** | Matr: {matr}")
                    if c2.button("🗑️", key=f"rm_emp_{i}"):
                        st.session_state['dados_staging']['empregos'].pop(i); st.rerun()
                    
                    ctrs_vinc = [c for c in all_ctrs if c.get('matricula_ref') == matr]
                    if ctrs_vinc:
                        df_ctrs = pd.DataFrame(ctrs_vinc)
                        if 'origem_tabela' in df_ctrs.columns:
                            grupos = df_ctrs.groupby('origem_tabela')
                            for origem, grupo in grupos:
                                st.caption(f"📂 Fonte: {origem}")
                                st.dataframe(grupo.drop(columns=['matricula_ref', 'origem_tabela'], errors='ignore'), hide_index=True)
                        else:
                            st.table(df_ctrs[['contrato', 'dados_extras']])

        st.divider()
        if st.button("💾 CONFIRMAR E SALVAR", type="primary", use_container_width=True):
            staging = st.session_state['dados_staging']
            if not staging['geral'].get('nome') or not staging['geral'].get('cpf'):
                st.error("É necessário informar pelo menos Nome e CPF nos Dados Pessoais.")
            else:
                df_tel = pd.DataFrame(staging['telefones'])
                df_email = pd.DataFrame(staging['emails'])
                df_end = pd.DataFrame(staging['enderecos'])
                df_emp = pd.DataFrame(staging['empregos'])
                df_contr = pd.DataFrame(staging['contratos'])
                
                modo_salvar = "editar" if is_edit else "novo"
                cpf_orig = limpar_normalizar_cpf(st.session_state.get('pf_cpf_selecionado')) if is_edit else None
                
                sucesso, msg = salvar_pf(staging['geral'], df_tel, df_email, df_end, df_emp, df_contr, modo_salvar, cpf_orig)
                
                if sucesso:
                    st.success(msg)
                    time.sleep(1.5)
                    st.session_state['pf_view'] = 'lista'
                    st.session_state['form_loaded'] = False
                    st.rerun()
                else:
                    st.error(msg)