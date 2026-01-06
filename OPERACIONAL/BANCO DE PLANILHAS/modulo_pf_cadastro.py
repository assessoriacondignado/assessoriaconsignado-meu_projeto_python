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
    """
    Garante que as tabelas necessárias existam no banco.
    Adicionado: Criação da tabela pf_emprego_renda se não existir.
    """
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("CREATE SCHEMA IF NOT EXISTS banco_pf;")
            
            # Garante colunas na tabela principal
            cols_extras = [
                "uf_rg VARCHAR(2)", "pis VARCHAR(20)", "nome_procurador VARCHAR(150)", 
                "cpf_procurador VARCHAR(14)", "dados_exp_rg VARCHAR(50)", 
                "serie_ctps VARCHAR(20)", "cnh VARCHAR(20)", "nome_pai VARCHAR(150)"
            ]
            for col_def in cols_extras:
                try:
                    col_name = col_def.split()[0]
                    cur.execute(f"ALTER TABLE banco_pf.pf_dados ADD COLUMN IF NOT EXISTS {col_name} {col_def.split(' ', 1)[1]}")
                except: pass
            
            # Garante coluna numero como texto
            try:
                cur.execute("ALTER TABLE banco_pf.pf_telefones ALTER COLUMN numero TYPE VARCHAR(20)")
            except: pass

            # --- CORREÇÃO: GARANTIR TABELAS DE VÍNCULO ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_emprego_renda (
                    id SERIAL PRIMARY KEY,
                    cpf_ref VARCHAR(20) REFERENCES banco_pf.pf_dados(cpf) ON DELETE CASCADE,
                    convenio VARCHAR(100),
                    matricula VARCHAR(100),
                    dados_extras TEXT,
                    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(matricula)
                );
            """)

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
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS banco_pf.pf_emails (
                    id SERIAL PRIMARY KEY,
                    cpf_ref VARCHAR(20) REFERENCES banco_pf.pf_dados(cpf) ON DELETE CASCADE,
                    email VARCHAR(150)
                );
            """)
            
            # Endereços
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

            conn.commit()
            conn.close()
        except: pass

# --- HELPERS DE FORMATAÇÃO E CÁLCULO ---

def formatar_cpf_visual(cpf_db):
    if not cpf_db: return ""
    cpf_limpo = re.sub(r'\D', '', str(cpf_db))
    cpf_full = cpf_limpo.zfill(11)
    return f"{cpf_full[:3]}.{cpf_full[3:6]}.{cpf_full[6:9]}-{cpf_full[9:]}"

def limpar_normalizar_cpf(cpf_raw):
    if not cpf_raw: return ""
    s = str(cpf_raw).strip()
    apenas_nums = re.sub(r'\D', '', s)
    return apenas_nums.zfill(11)

def limpar_apenas_numeros(valor):
    if not valor: return ""
    return re.sub(r'\D', '', str(valor))

def formatar_telefone_visual(tel_raw):
    if not tel_raw: return ""
    nums = re.sub(r'\D', '', str(tel_raw))
    if len(nums) >= 2:
        return f"({nums[:2]}){nums[2:]}"
    return nums

def validar_formatar_telefone(tel_raw):
    s = str(tel_raw).strip()
    if re.search(r'[a-zA-Z]', s):
        return None, "Erro: Contém letras. Digite apenas números."
    
    numeros = re.sub(r'\D', '', s)
    if len(numeros) < 10 or len(numeros) > 11:
        return None, "⚠️ Erro: Formato inválido! Digite 10 ou 11 números."
    return numeros, None

def validar_formatar_cpf(cpf_raw):
    numeros = re.sub(r'\D', '', str(cpf_raw).strip())
    if not numeros: return None, "CPF inválido (vazio)."
    if len(numeros) > 11: return None, "CPF deve ter no máximo 11 dígitos."
    return numeros, None

def validar_email(email):
    if not email: return False
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(regex, email))

# --- NOVAS VALIDAÇÕES DE ENDEREÇO ---

LISTA_UFS_BR = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
    'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
    'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
]

def validar_uf(uf_input):
    """Verifica se a UF é válida no Brasil."""
    if not uf_input: return False
    return str(uf_input).strip().upper() in LISTA_UFS_BR

def validar_formatar_cep(cep_raw):
    """
    Valida e formata o CEP.
    Retorna: (cep_numerico, cep_visual, erro)
    """
    numeros = limpar_apenas_numeros(cep_raw)
    
    if len(numeros) != 8:
        return None, None, "CEP deve ter exatamente 8 dígitos."
    
    cep_visual = f"{numeros[:5]}-{numeros[5:]}"
    return numeros, cep_visual, None

# ------------------------------------

def formatar_cnpj(valor):
    if not valor: return None
    numeros = re.sub(r'\D', '', str(valor))
    if not numeros: return None
    numeros = numeros.zfill(14)
    return f"{numeros[:2]}.{numeros[2:5]}.{numeros[5:8]}/{numeros[8:12]}-{numeros[12:]}"

def converter_data_br_iso(valor):
    if not valor or pd.isna(valor): return None
    valor_str = str(valor).strip().split(' ')[0]
    formatos = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"]
    for fmt in formatos:
        try: return datetime.strptime(valor_str, fmt).strftime("%Y-%m-%d")
        except ValueError: continue
    return None

def calcular_idade_hoje(dt_nasc):
    if not dt_nasc: return 0
    hoje = date.today()
    if isinstance(dt_nasc, datetime): dt_nasc = dt_nasc.date()
    if not isinstance(dt_nasc, date): return 0
    return hoje.year - dt_nasc.year - ((hoje.month, hoje.day) < (dt_nasc.month, dt_nasc.day))

def safe_view(valor):
    if valor is None: return ""
    v_str = str(valor).strip()
    if v_str.lower() in ['none', 'nan', 'null', 'nat', '']: return ""
    return v_str

# --- CARREGAMENTO DE DADOS ---
def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {
        'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 
        'empregos': [], 'contratos': [] 
    }
    
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)      
            params_busca = (cpf_norm,)
            
            # 1. Dados Pessoais
            df_d = pd.read_sql("SELECT * FROM banco_pf.pf_dados WHERE cpf = %s", conn, params=params_busca)
            if not df_d.empty: dados['geral'] = df_d.where(pd.notnull(df_d), None).iloc[0].to_dict()
            
            # Satélites (Visualização)
            col_fk = 'cpf_ref' 
            try:
                pd.read_sql("SELECT 1 FROM banco_pf.pf_telefones WHERE cpf_ref = '1' LIMIT 1", conn)
            except:
                col_fk = 'cpf'
                conn.rollback()

            dados['telefones'] = pd.read_sql(f"SELECT numero FROM banco_pf.pf_telefones WHERE {col_fk} = %s", conn, params=params_busca).fillna("").to_dict('records')
            dados['emails'] = pd.read_sql(f"SELECT email FROM banco_pf.pf_emails WHERE {col_fk} = %s", conn, params=params_busca).fillna("").to_dict('records')
            # Busca Endereços
            dados['enderecos'] = pd.read_sql(f"SELECT rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE {col_fk} = %s", conn, params=params_busca).fillna("").to_dict('records')
            
            # Busca Vínculos (Emprego e Renda)
            query_emp = f"SELECT convenio, matricula, dados_extras FROM banco_pf.pf_emprego_renda WHERE {col_fk} = %s"
            try:
                df_emp = pd.read_sql(query_emp, conn, params=params_busca)
            except:
                conn.rollback()
                df_emp = pd.DataFrame()
            
            if not df_emp.empty:
                for _, row_emp in df_emp.iterrows():
                    conv_nome = str(row_emp['convenio']).strip() 
                    matricula = str(row_emp['matricula']).strip()
                    vinculo = {'convenio': conv_nome, 'matricula': matricula, 'dados_extras': row_emp.get('dados_extras'), 'contratos': []}

                    query_map = "SELECT nome_planilha_sql, tipo_planilha FROM banco_pf.convenio_por_planilha WHERE convenio ILIKE %s"
                    cur = conn.cursor()
                    cur.execute(query_map, (conv_nome,))
                    tabelas_mapeadas = cur.fetchall()
                    
                    if tabelas_mapeadas:
                        for tabela_destino, tipo_destino in tabelas_mapeadas:
                            try:
                                cur.execute("SELECT to_regclass(%s)", (tabela_destino,))
                                if cur.fetchone()[0]:
                                    colunas_tb = get_colunas_tabela(tabela_destino)
                                    nomes_cols = [c[0] for c in colunas_tb]
                                    col_chave = 'matricula' if 'matricula' in nomes_cols else 'matricula_ref'
                                    query_contratos = f"SELECT * FROM {tabela_destino} WHERE {col_chave} = %s"
                                    df_contratos = pd.read_sql(query_contratos, conn, params=(matricula,))
                                    if not df_contratos.empty:
                                        df_contratos = df_contratos.astype(object).where(pd.notnull(df_contratos), None)
                                        df_contratos['origem_tabela'] = tabela_destino
                                        df_contratos['tipo_origem'] = tipo_destino 
                                        vinculo['contratos'].extend(df_contratos.to_dict('records'))
                            except: conn.rollback()
                    else:
                        # Fallback para tabela padrao antiga se houver
                        try:
                            query_padrao = "SELECT * FROM banco_pf.pf_contratos WHERE matricula_ref = %s"
                            df_contratos = pd.read_sql(query_padrao, conn, params=(matricula,))
                            if not df_contratos.empty:
                                df_contratos['origem_tabela'] = 'banco_pf.pf_contratos'
                                df_contratos['tipo_origem'] = 'Geral'
                                vinculo['contratos'] = df_contratos.to_dict('records')
                        except: pass
                    
                    dados['empregos'].append(vinculo)
        except Exception as e: print(f"Erro carregamento: {e}") 
        finally: conn.close()
    return dados

# --- FUNÇÕES AUXILIARES ---
def listar_tabelas_por_convenio(convenio):
    conn = get_conn()
    tabelas = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome_planilha_sql, tipo_planilha FROM banco_pf.convenio_por_planilha WHERE convenio ILIKE %s", (convenio,))
            tabelas = cur.fetchall()
            conn.close()
        except: conn.close()
    if not tabelas: tabelas = [('banco_pf.pf_contratos', 'Contratos Gerais')]
    return tabelas

def get_colunas_tabela(nome_tabela_completo):
    conn = get_conn()
    colunas = []
    if conn:
        try:
            if '.' in nome_tabela_completo: schema, tabela = nome_tabela_completo.split('.')
            else: schema, tabela = 'public', nome_tabela_completo
            cur = conn.cursor()
            query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s AND table_schema = %s ORDER BY ordinal_position"
            cur.execute(query, (tabela, schema))
            colunas = cur.fetchall()
            conn.close()
        except: conn.close()
    return colunas

# --- CONFIGURAÇÃO VISUAL ---
CONFIG_CADASTRO = {
    "Dados Pessoais": [
        {"label": "Nome Completo", "key": "nome", "tabela": "geral", "tipo": "texto", "obrigatorio": True},
        {"label": "CPF", "key": "cpf", "tabela": "geral", "tipo": "cpf", "obrigatorio": True},
        # Campos abaixo só aparecem no modo EDITAR
        {"label": "RG", "key": "rg", "tabela": "geral", "tipo": "texto"},
        {"label": "Data Nascimento", "key": "data_nascimento", "tabela": "geral", "tipo": "data"},
        {"label": "Nome da Mãe", "key": "nome_mae", "tabela": "geral", "tipo": "texto"},
        {"label": "Nome do Pai", "key": "nome_pai", "tabela": "geral", "tipo": "texto"},
        {"label": "UF do RG", "key": "uf_rg", "tabela": "geral", "tipo": "texto"},
        {"label": "Dados Exp. RG", "key": "dados_exp_rg", "tabela": "geral", "tipo": "texto"},
        {"label": "PIS", "key": "pis", "tabela": "geral", "tipo": "texto"},
        {"label": "CNH", "key": "cnh", "tabela": "geral", "tipo": "texto"},
        {"label": "Série CTPS", "key": "serie_ctps", "tabela": "geral", "tipo": "texto"},
        # Procurador
        {"label": "Nome Procurador", "key": "nome_procurador", "tabela": "geral", "tipo": "texto"},
        {"label": "CPF Procurador", "key": "cpf_procurador", "tabela": "geral", "tipo": "cpf"}, 
    ],
    "Contatos": [
        {"label": "Telefone", "key": "numero", "tabela": "telefones", "tipo": "telefone", "multiplo": True},
        {"label": "E-mail", "key": "email", "tabela": "emails", "tipo": "email", "multiplo": True},
    ],
    "Endereços": [
        {"label": "CEP", "key": "cep", "tabela": "enderecos", "tipo": "cep", "multiplo": True, "agrupado": True}, 
        {"label": "Logradouro", "key": "rua", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "Bairro", "key": "bairro", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "Cidade", "key": "cidade", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "UF", "key": "uf", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
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
        # Aplica regra de validação de CPF (para Titular e Procurador)
        val, erro = validar_formatar_cpf(valor)
        if not erro: valor_final = limpar_normalizar_cpf(val)
    elif campo_config['tipo'] == 'telefone':
        val, erro = validar_formatar_telefone(valor)
        if not erro: valor_final = val
    elif campo_config['tipo'] == 'email':
        if not validar_email(valor): erro = "E-mail inválido."
    # CEP AGORA TRATADO DIRETAMENTE NA INTERFACE
    
    if erro: st.error(erro); return
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

# --- INTERFACE PRINCIPAL ---
def interface_cadastro_pf():
    init_db_structures()
    is_edit = st.session_state['pf_view'] == 'editar'
    
    cpf_formatado_titulo = ""
    if is_edit:
        raw_cpf = st.session_state.get('pf_cpf_selecionado', '')
        cpf_formatado_titulo = formatar_cpf_visual(raw_cpf)
    
    titulo = f"✏️ Editar: {cpf_formatado_titulo}" if is_edit else "➕ Novo Cadastro"
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

    c_builder, c_preview = st.columns([3, 2])

    with c_builder:
        st.markdown("#### 🏗️ Inserir Dados")
        with st.expander("Dados Pessoais", expanded=True):
            
            # MENSAGEM INFORMATIVA NO MODO NOVO
            if not is_edit:
                st.info("ℹ️ Para cadastrar dados complementares (RG, Filiação, Procurador, etc.), salve o Nome e CPF primeiro e depois edite o registro.")

            for campo in CONFIG_CADASTRO["Dados Pessoais"]:
                # REGRA DE FLUXO: Se for novo cadastro, só mostra NOME e CPF
                if not is_edit and campo['key'] not in ['nome', 'cpf']:
                    continue

                # Se estiver editando, bloqueia o CPF (chave primária)
                if is_edit and campo['key'] == 'cpf':
                    c_lab, c_inp = st.columns([1.2, 3.5])
                    c_lab.markdown(f"**{campo['label']}:**")
                    val_atual = st.session_state['dados_staging']['geral'].get('cpf', '')
                    c_inp.text_input("CPF Display", value=formatar_cpf_visual(val_atual), disabled=True, label_visibility="collapsed")
                    continue
                
                c_lbl, c_inp, c_btn = st.columns([1.2, 2.5, 1.0])
                c_lbl.markdown(f"**{campo['label']}:**")
                with c_inp:
                    if campo['tipo'] == 'data':
                        # VISUALIZAÇÃO: Calendário (DD/MM/YYYY)
                        val_pre = st.session_state['dados_staging']['geral'].get(campo['key'])
                        if isinstance(val_pre, str):
                            # Tenta converter string YYYY-MM-DD para data object
                            try: val_pre = datetime.strptime(val_pre, '%Y-%m-%d').date()
                            except: val_pre = None
                        
                        val = st.date_input("Data", value=val_pre, min_value=date(1900, 1, 1), max_value=date(2050, 12, 31), format="DD/MM/YYYY", key=f"in_{campo['key']}", label_visibility="collapsed")
                    else:
                        val_pre = st.session_state['dados_staging']['geral'].get(campo['key'], '')
                        val = st.text_input("Texto", value=val_pre, label_visibility="collapsed", key=f"in_{campo['key']}")
                
                with c_btn:
                    if st.button("Inserir", key=f"btn_{campo['key']}", type="primary", use_container_width=True): 
                        inserir_dado_staging(campo, val)
        
        # --- BLOCO CONTATOS (REGRAS DE INCLUSÃO) ---
        with st.expander("Contatos"):
            # 1. TELEFONES E EMAILS (Regra: Apenas na Edição)
            if not is_edit:
                st.info("🚫 A inclusão de telefones e e-mails é permitida apenas no modo 'Editar', após salvar o cadastro inicial.")
            else:
                # --- ÁREA DE TELEFONES ---
                c_tel_in, c_tel_btn = st.columns([4, 2])
                with c_tel_in: 
                    tel = st.text_input("Número", key="in_tel_num", placeholder="Ex: (82)999025155")
                with c_tel_btn:
                    st.write(""); st.write("") 
                    if st.button("Inserir Telefone", type="primary", use_container_width=True):
                        cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'numero'][0]
                        inserir_dado_staging(cfg, tel, None)
                
                st.divider()

                # --- ÁREA DE E-MAILS (COM VALIDAÇÃO) ---
                st.markdown("##### 📧 Cadastro de E-mail")
                c_mail_in, c_mail_btn = st.columns([5, 2])
                with c_mail_in: 
                    mail = st.text_input("E-mail", key="in_mail", placeholder="exemplo@email.com")
                with c_mail_btn:
                    st.write(""); st.write("")
                    if st.button("Inserir E-mail", type="primary", use_container_width=True):
                        if validar_email(mail):
                            emails_atuais = [e['email'] for e in st.session_state['dados_staging'].get('emails', [])]
                            if mail in emails_atuais:
                                st.warning("⚠️ Este e-mail já está na lista deste cliente.")
                            else:
                                cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'email'][0]
                                inserir_dado_staging(cfg, mail)
                                st.success("E-mail validado e adicionado!")
                        else:
                            st.error("⚠️ Formato de e-mail inválido.")

        # --- BLOCO ENDEREÇO (REGRAS DE INCLUSÃO ATUALIZADAS) ---
        with st.expander("Endereço"):
            if not is_edit:
                st.info("🚫 A inclusão de endereços é permitida apenas no modo 'Editar', após salvar o cadastro inicial.")
            else:
                st.markdown("##### 📍 Cadastro de Endereço")
                
                c_cep, c_rua = st.columns([1.5, 3.5])
                with c_cep: 
                    cep = st.text_input("CEP", key="in_end_cep", placeholder="00000-000")
                with c_rua: 
                    rua = st.text_input("Logradouro", key="in_end_rua", placeholder="Rua, Av, etc.")
                
                c_bai, c_cid, c_uf = st.columns([2, 2, 1])
                with c_bai: bairro = st.text_input("Bairro", key="in_end_bairro")
                with c_cid: cidade = st.text_input("Cidade", key="in_end_cid")
                with c_uf: 
                    uf_digitada = st.text_input("UF", key="in_end_uf", placeholder="UF", max_chars=2).upper()
                
                if st.button("Inserir Endereço", type="primary", use_container_width=True):
                    # 1. VALIDAÇÃO DE CEP
                    cep_num, cep_vis, erro_cep = validar_formatar_cep(cep)
                    
                    # 2. VALIDAÇÃO DE UF
                    erro_uf = None
                    if not validar_uf(uf_digitada):
                        erro_uf = f"UF inválida: '{uf_digitada}'. Use siglas (ex: SP, MG, BA)."
                    
                    if erro_cep:
                        st.error(erro_cep)
                    elif erro_uf:
                        st.error(erro_uf)
                    elif not rua:
                        st.warning("O campo Logradouro é obrigatório.")
                    else:
                        # 3. VALIDAÇÃO DE DUPLICIDADE VISUAL
                        ends_atuais = st.session_state['dados_staging'].get('enderecos', [])
                        duplicado = False
                        for e in ends_atuais:
                            if e.get('cep') == cep_num and e.get('rua') == rua:
                                duplicado = True
                                break
                        
                        if duplicado:
                            st.warning("⚠️ Este endereço já está na lista deste cliente.")
                        else:
                            obj_end = {
                                'cep': cep_num, # Salva só números (para o banco)
                                'rua': rua, 
                                'bairro': bairro, 
                                'cidade': cidade, 
                                'uf': uf_digitada
                            }
                            
                            if 'enderecos' not in st.session_state['dados_staging']:
                                st.session_state['dados_staging']['enderecos'] = []
                                
                            st.session_state['dados_staging']['enderecos'].append(obj_end)
                            
                            cfg_dummy = [c for c in CONFIG_CADASTRO["Endereços"] if c['key'] == 'cep'][0]
                            st.toast(f"✅ Endereço adicionado! (CEP: {cep_vis})")
                            st.success("Endereço validado e incluído na lista temporária.")

        with st.expander("Emprego e Renda (Vínculo)"):
            c_conv, c_matr, c_btn_emp = st.columns([3, 3, 2])
            with c_conv: conv = st.text_input("Convênio", key="in_emp_conv", placeholder="Ex: INSS")
            with c_matr: matr = st.text_input("Matrícula", key="in_emp_matr")
            with c_btn_emp:
                st.write(""); st.write("")
                if st.button("Inserir Vínculo", type="primary", use_container_width=True):
                    if conv and matr:
                        obj_emp = {'convenio': conv.upper(), 'matricula': matr, 'dados_extras': ''}
                        if 'empregos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['empregos'] = []
                        st.session_state['dados_staging']['empregos'].append(obj_emp)
                        st.toast("✅ Vínculo adicionado!")
                        st.rerun()
                    else: st.warning("Campos obrigatórios.")

        with st.expander("Contratos / Planilhas"):
            lista_empregos = st.session_state['dados_staging'].get('empregos', [])
            if not lista_empregos:
                st.info("Insira um vínculo em 'Emprego e Renda' primeiro.")
            else:
                opcoes_matr = [f"{e['matricula']} - {e['convenio']}" for e in lista_empregos]
                sel_vinculo = st.selectbox("Vincular à Matrícula:", opcoes_matr, key="sel_vinc_contr")
                idx_vinc = opcoes_matr.index(sel_vinculo)
                dados_vinc = lista_empregos[idx_vinc]
                tabelas_destino = listar_tabelas_por_convenio(dados_vinc['convenio'])
                
                if not tabelas_destino: st.warning(f"Sem planilhas configuradas para {dados_vinc['convenio']}.")
                for nome_tabela, tipo_tabela in tabelas_destino:
                    st.markdown("---")
                    st.markdown(f"###### 📝 {tipo_tabela or 'Dados'} ({nome_tabela})")
                    sufixo = f"{nome_tabela}_{idx_vinc}"
                    colunas_banco = get_colunas_tabela(nome_tabela)
                    campos_ignorados = ['id', 'matricula_ref', 'matricula', 'convenio', 'tipo_planilha', 'importacao_id', 'data_criacao', 'data_atualizacao', 'cpf_ref']
                    inputs_gerados = {}
                    mapa_calculo_datas = {'tempo_abertura_anos': 'data_abertura_empresa', 'tempo_admissao_anos': 'data_admissao', 'tempo_inicio_emprego_anos': 'data_inicio_emprego'}
                    datas_preenchidas = {} 

                    cols_ui = st.columns(2)
                    for idx_col, (col_nome, col_tipo) in enumerate(colunas_banco):
                        if col_nome in campos_ignorados: continue
                        label_fmt = col_nome.replace('_', ' ').title()
                        with cols_ui[idx_col % 2]:
                            key_input = f"inp_{col_nome}_{sufixo}"
                            if col_nome in mapa_calculo_datas:
                                col_data_ref = mapa_calculo_datas[col_nome]
                                valor_data = datas_preenchidas.get(col_data_ref)
                                anos_calc = calcular_idade_hoje(valor_data) if valor_data else 0
                                val = st.number_input(label_fmt, value=anos_calc, disabled=True, key=key_input)
                            elif 'date' in col_tipo.lower() or 'data' in col_nome.lower():
                                val = st.date_input(label_fmt, value=None, format="DD/MM/YYYY", key=key_input)
                                datas_preenchidas[col_nome] = val
                            else:
                                val = st.text_input(label_fmt, key=key_input)
                            inputs_gerados[col_nome] = val
                    
                    if st.button(f"Inserir em {tipo_tabela or nome_tabela}", key=f"btn_save_{sufixo}", type="primary"):
                        nomes_cols_tabela = [c[0] for c in colunas_banco]
                        # Ajuste flexível para chaves estrangeiras
                        if 'matricula' in nomes_cols_tabela: inputs_gerados['matricula'] = dados_vinc['matricula']
                        elif 'matricula_ref' in nomes_cols_tabela: inputs_gerados['matricula_ref'] = dados_vinc['matricula']
                        
                        if 'convenio' in nomes_cols_tabela: inputs_gerados['convenio'] = dados_vinc['convenio']
                        if 'tipo_planilha' in nomes_cols_tabela and tipo_tabela: inputs_gerados['tipo_planilha'] = tipo_tabela
                        
                        inputs_gerados['origem_tabela'] = nome_tabela
                        inputs_gerados['tipo_origem'] = tipo_tabela
                        
                        if 'contratos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['contratos'] = []
                        st.session_state['dados_staging']['contratos'].append(inputs_gerados)
                        st.toast(f"✅ {tipo_tabela} adicionado!")

    with c_preview:
        st.markdown("### 📋 Resumo")
        st.info("👤 Dados Pessoais")
        geral = st.session_state['dados_staging'].get('geral', {})
        if geral:
            cols = st.columns(2)
            idx = 0
            for k, v in geral.items():
                if v:
                    val_str = v.strftime('%d/%m/%Y') if isinstance(v, (date, datetime)) else str(v)
                    if k == 'cpf' or k == 'cpf_procurador': val_str = formatar_cpf_visual(val_str)
                    cols[idx%2].text_input(k.replace('_', ' ').upper(), value=val_str, disabled=True, key=f"view_geral_{k}")
                    idx += 1
        
        st.warning("📞 Contatos")
        tels = st.session_state['dados_staging'].get('telefones', [])
        if tels:
            for i, t in enumerate(tels):
                c1, c2 = st.columns([5, 1])
                val_view = formatar_telefone_visual(t.get('numero'))
                c1.write(f"📱 **{val_view}**")
                if c2.button("🗑️", key=f"rm_tel_{i}"):
                    st.session_state['dados_staging']['telefones'].pop(i); st.rerun()
        
        mails = st.session_state['dados_staging'].get('emails', [])
        if mails:
            for i, m in enumerate(mails):
                c1, c2 = st.columns([5, 1])
                c1.write(f"📧 **{m.get('email')}**")
                if c2.button("🗑️", key=f"rm_mail_{i}"):
                    st.session_state['dados_staging']['emails'].pop(i); st.rerun()
        
        if not tels and not mails: st.caption("Nenhum contato.")

        st.warning("📍 Endereços")
        ends = st.session_state['dados_staging'].get('enderecos', [])
        if ends:
            for i, e in enumerate(ends):
                c1, c2 = st.columns([5, 1])
                # Formata CEP para visualização
                _, cep_fmt, _ = validar_formatar_cep(e.get('cep'))
                c1.write(f"🏠 **{e.get('rua')}** - {e.get('bairro')} | {e.get('cidade')}/{e.get('uf')} (CEP: {cep_fmt})")
                if c2.button("🗑️", key=f"rm_end_{i}"):
                    st.session_state['dados_staging']['enderecos'].pop(i); st.rerun()
        else: st.caption("Nenhum endereço.")
        
        st.warning("💼 Vínculos (Emprego)")
        emps = st.session_state['dados_staging'].get('empregos', [])
        if emps:
            for i, emp in enumerate(emps):
                c1, c2 = st.columns([5, 1])
                c1.write(f"🏢 **{emp.get('convenio')}** | Mat: {emp.get('matricula')}")
                if c2.button("🗑️", key=f"rm_emp_{i}"):
                    st.session_state['dados_staging']['empregos'].pop(i); st.rerun()
        else: st.caption("Nenhum vínculo inserido.")

        st.success("📝 Dados Financeiros / Planilhas")
        ctrs = st.session_state['dados_staging'].get('contratos', [])
        if ctrs:
            for i, c in enumerate(ctrs):
                c1, c2 = st.columns([5, 1])
                origem_nome = c.get('tipo_origem') or c.get('origem_tabela', 'Dado')
                chaves = [k for k in c.keys() if k not in ['origem_tabela', 'tipo_origem', 'matricula_ref', 'matricula', 'convenio', 'tipo_planilha']]
                display_txt = f"[{origem_nome}] "
                if len(chaves) > 0: display_txt += f"{c[chaves[0]]} "
                ref_matr = c.get('matricula') or c.get('matricula_ref')
                c1.write(f"📌 {display_txt} (Ref: {ref_matr})")
                if c2.button("🗑️", key=f"rm_ctr_{i}"):
                    st.session_state['dados_staging']['contratos'].pop(i); st.rerun()
        else: st.caption("Nenhum vínculo inserido.")

        st.divider()
        
        if st.button("💾 CONFIRMAR E SALVAR", type="primary", use_container_width=True):
            staging = st.session_state['dados_staging']
            if not staging['geral'].get('nome') or not staging['geral'].get('cpf'):
                st.error("Nome e CPF são obrigatórios.")
            else:
                df_tel = pd.DataFrame(staging['telefones'])
                df_email = pd.DataFrame(staging['emails'])
                df_end = pd.DataFrame(staging['enderecos'])
                df_emp = pd.DataFrame(staging['empregos'])
                df_contr = pd.DataFrame(staging['contratos'])
                
                modo_salvar = "editar" if is_edit else "novo"
                cpf_orig = limpar_normalizar_cpf(st.session_state.get('pf_cpf_selecionado')) if is_edit else None
                
                # --- CHAMADA CORRIGIDA DA FUNÇÃO DE SALVAR ---
                sucesso, msg = salvar_pf(staging['geral'], df_tel, df_email, df_end, df_emp, df_contr, modo_salvar, cpf_orig)
                if sucesso:
                    st.success(msg)
                    time.sleep(1.5)
                    st.session_state['pf_view'] = 'lista'
                    st.session_state['form_loaded'] = False
                    st.rerun()
                else: st.error(msg)
    
    st.markdown(f"<div style='text-align: right; color: gray; font-size: 0.8em; margin-top: 20px;'>código atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>", unsafe_allow_html=True)

# --- FUNÇÕES DE SALVAMENTO E EXCLUSÃO (CORRIGIDA) ---
def salvar_pf(dados_gerais, df_tel, df_email, df_end, df_emp, df_contr, modo="novo", cpf_original=None):
    """
    Realiza a inserção no banco pf_dados e em TODAS as tabelas satélites.
    CORREÇÃO: Incluída lógica para pf_emprego_renda e contratos dinâmicos.
    """
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            
            # --- PREPARAÇÃO CPF ---
            cpf_limpo = limpar_normalizar_cpf(dados_gerais['cpf'])
            dados_gerais['cpf'] = cpf_limpo
            if cpf_original: cpf_original = limpar_normalizar_cpf(cpf_original)
            
            dados_gerais = {k: (v.upper() if isinstance(v, str) else v) for k, v in dados_gerais.items()}
            
            if 'cpf_procurador' in dados_gerais and dados_gerais['cpf_procurador']:
                dados_gerais['cpf_procurador'] = limpar_normalizar_cpf(dados_gerais['cpf_procurador'])

            # Tratamento de Data de Nascimento para o Banco
            if 'data_nascimento' in dados_gerais:
                if isinstance(dados_gerais['data_nascimento'], (date, datetime)):
                     dados_gerais['data_nascimento'] = dados_gerais['data_nascimento'].strftime('%Y-%m-%d')
                elif not dados_gerais['data_nascimento']:
                     dados_gerais['data_nascimento'] = None

            # 1. SALVAMENTO NA TABELA PRINCIPAL (pf_dados)
            if modo == "novo":
                cols = list(dados_gerais.keys()); vals = list(dados_gerais.values())
                placeholders = ", ".join(["%s"] * len(vals)); col_names = ", ".join(cols)
                cur.execute(f"INSERT INTO banco_pf.pf_dados ({col_names}) VALUES ({placeholders})", vals)
            else:
                set_clause = ", ".join([f"{k}=%s" for k in dados_gerais.keys()])
                vals = list(dados_gerais.values()) + [cpf_original]
                cur.execute(f"UPDATE banco_pf.pf_dados SET {set_clause} WHERE cpf=%s", vals)
            
            # 2. SALVAMENTO DE TELEFONES
            col_fk = 'cpf_ref'
            try: cur.execute("SELECT 1 FROM banco_pf.pf_telefones WHERE cpf_ref = '1' LIMIT 1")
            except: col_fk = 'cpf'; conn.rollback(); cur = conn.cursor()

            if not df_tel.empty:
                for _, r in df_tel.iterrows():
                    num_novo = r['numero']
                    if num_novo:
                        cur.execute(f"SELECT 1 FROM banco_pf.pf_telefones WHERE {col_fk} = %s AND numero = %s", (cpf_limpo, num_novo))
                        if not cur.fetchone():
                            cur.execute(f"INSERT INTO banco_pf.pf_telefones ({col_fk}, numero, data_atualizacao) VALUES (%s, %s, CURRENT_DATE)", (cpf_limpo, num_novo))

            # 3. SALVAMENTO DE E-MAILS
            col_fk_email = 'cpf_ref'
            try: cur.execute("SELECT 1 FROM banco_pf.pf_emails WHERE cpf_ref = '1' LIMIT 1")
            except: col_fk_email = 'cpf'; conn.rollback(); cur = conn.cursor()

            if not df_email.empty:
                for _, r in df_email.iterrows():
                    email_novo = r['email']
                    if email_novo:
                        cur.execute(f"SELECT 1 FROM banco_pf.pf_emails WHERE {col_fk_email} = %s AND email = %s", (cpf_limpo, email_novo))
                        if not cur.fetchone():
                            cur.execute(f"INSERT INTO banco_pf.pf_emails ({col_fk_email}, email) VALUES (%s, %s)", (cpf_limpo, email_novo))
            
            # 4. SALVAMENTO DE ENDEREÇOS
            col_fk_end = 'cpf_ref'
            try: cur.execute("SELECT 1 FROM banco_pf.pf_enderecos WHERE cpf_ref = '1' LIMIT 1")
            except: col_fk_end = 'cpf'; conn.rollback(); cur = conn.cursor()

            if not df_end.empty:
                for _, r in df_end.iterrows():
                    if r.get('rua') or r.get('cep'):
                        cep_limpo_end = limpar_apenas_numeros(r.get('cep'))
                        rua_val = r.get('rua')
                        cur.execute(f"SELECT 1 FROM banco_pf.pf_enderecos WHERE {col_fk_end} = %s AND cep = %s AND rua = %s", (cpf_limpo, cep_limpo_end, rua_val))
                        if not cur.fetchone():
                            cur.execute(f"""
                                INSERT INTO banco_pf.pf_enderecos ({col_fk_end}, cep, rua, bairro, cidade, uf) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (cpf_limpo, cep_limpo_end, rua_val, r.get('bairro'), r.get('cidade'), r.get('uf')))

            # 5. SALVAMENTO DE EMPREGO E RENDA (NOVA IMPLEMENTAÇÃO)
            if not df_emp.empty:
                # Verifica a tabela pf_emprego_renda
                try:
                    for _, r in df_emp.iterrows():
                        matr = r.get('matricula')
                        conv = r.get('convenio')
                        if matr and conv:
                            # Tenta inserir se não existir a matrícula
                            cur.execute("SELECT 1 FROM banco_pf.pf_emprego_renda WHERE matricula = %s", (matr,))
                            if not cur.fetchone():
                                cur.execute("""
                                    INSERT INTO banco_pf.pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras)
                                    VALUES (%s, %s, %s, %s)
                                """, (cpf_limpo, conv, matr, r.get('dados_extras', '')))
                except Exception as e_emp:
                    # Em caso de erro na tabela, não aborta tudo, mas loga. (Ou relança se crítico)
                    raise e_emp

            # 6. SALVAMENTO DE CONTRATOS (NOVA IMPLEMENTAÇÃO)
            if not df_contr.empty:
                for _, r in df_contr.iterrows():
                    tabela_destino = r.get('origem_tabela')
                    if not tabela_destino: continue
                    
                    # Limpa metadados do dict para ficar só com colunas do banco
                    dados_clean = {k: v for k, v in r.items() if k not in ['origem_tabela', 'tipo_origem']}
                    if not dados_clean: continue
                    
                    cols = list(dados_clean.keys())
                    vals = list(dados_clean.values())
                    placeholders = ", ".join(["%s"] * len(vals))
                    col_names = ", ".join(cols)
                    
                    # Inserção direta (assumindo que o usuário quer adicionar novo registro)
                    # Idealmente teria verificação de duplicidade, mas depende da chave da tabela destino
                    cur.execute(f"INSERT INTO {tabela_destino} ({col_names}) VALUES ({placeholders})", vals)

            # --- COMMIT FINAL ---
            conn.commit() 
            conn.close()
            return True, "✅ Dados salvos com sucesso!"
            
        except Exception as e: 
            if conn: 
                conn.rollback()
                conn.close()
            # Retorna o erro real para o usuário ver
            return False, f"❌ Erro ao salvar: {str(e)}"
    return False, "Erro de conexão com o banco."

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
    
    st.markdown("""<style>.compact-header { margin-bottom: -15px; } .stMarkdown hr { margin-top: 5px; margin-bottom: 5px; }</style>""", unsafe_allow_html=True)
    st.markdown(f"<h3 class='compact-header'>👤 {g.get('nome', 'Nome não informado')}</h3>", unsafe_allow_html=True)
    st.markdown(f"**CPF:** {cpf_vis}")
    st.write("") 
    
    t1, t2, t3 = st.tabs(["📋 Cadastro & Vínculos", "💼 Detalhes Financeiros", "📞 Contatos & Endereços"])
    with t1:
        c1, c2 = st.columns(2)
        nasc = g.get('data_nascimento')
        txt_nasc = nasc.strftime('%d/%m/%Y') if nasc and isinstance(nasc, (date, datetime)) else safe_view(nasc)
        c1.write(f"**Nascimento:** {txt_nasc}"); c1.write(f"**RG:** {safe_view(g.get('rg'))}"); c2.write(f"**Mãe:** {safe_view(g.get('nome_mae'))}")
        
        demais_campos = {k: v for k, v in g.items() if k not in ['data_nascimento', 'rg', 'nome_mae', 'id', 'cpf', 'nome', 'importacao_id', 'id_campanha', 'data_criacao']}
        if demais_campos:
            st.markdown("---"); st.markdown("##### 📌 Outras Informações")
            col_iter = st.columns(3); idx = 0
            for k, v in demais_campos.items(): 
                val_display = safe_view(v)
                if 'cpf' in k: val_display = formatar_cpf_visual(val_display)
                col_iter[idx % 3].write(f"**{k.replace('_', ' ').title()}:** {val_display}"); idx += 1
        
        st.divider(); st.markdown("##### 🔗 Vínculos")
        for v in dados.get('empregos', []): st.info(f"🆔 **{v['matricula']}** - {v['convenio'].upper()}")
        if not dados.get('empregos'): st.warning("Nenhum vínculo localizado.")
            
    with t2:
        st.markdown("##### 💰 Detalhes Financeiros & Contratos")
        for v in dados.get('empregos', []):
            ctrs = v.get('contratos', [])
            if ctrs:
                tipo_display = v.get('contratos')[0].get('tipo_origem') or 'Detalhes'
                with st.expander(f"📂 {v['convenio'].upper()} | {tipo_display} | Matr: {v['matricula']}", expanded=True):
                    df_ex = pd.DataFrame(ctrs)
                    cols_drop = ['id', 'matricula_ref', 'importacao_id', 'data_criacao', 'data_atualizacao', 'origem_tabela', 'tipo_origem']
                    st.dataframe(df_ex.drop(columns=cols_drop, errors='ignore'), hide_index=True, use_container_width=True)
            else: st.caption(f"Sem contratos detalhados para {v['convenio']}.")
    with t3:
        for t in dados.get('telefones', []): 
            st.write(f"📱 {formatar_telefone_visual(t.get('numero'))}")
        for m in dados.get('emails', []): 
            st.write(f"📧 {safe_view(m.get('email'))}")
        
        st.divider()
        st.markdown("##### 📍 Endereços")
        for end in dados.get('enderecos', []): 
            # Validação e formatação de visualização de CEP
            _, cep_view, _ = validar_formatar_cep(end.get('cep'))
            cep_view = cep_view if cep_view else end.get('cep')
            st.success(f"🏠 {safe_view(end.get('rua'))}, {safe_view(end.get('bairro'))} - {safe_view(end.get('cidade'))}/{safe_view(end.get('uf'))} (CEP: {cep_view})")
    
    st.markdown(f"<div style='text-align: right; color: gray; font-size: 0.8em; margin-top: 10px;'>código atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>", unsafe_allow_html=True)