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
            conn.commit()
            conn.close()
        except: pass

# --- HELPERS DE FORMATAÇÃO E CÁLCULO ---

def formatar_cpf_visual(cpf_db):
    """
    Formata o CPF armazenado (sem zeros) para visualização (com zeros e pontuação).
    """
    if not cpf_db: return ""
    cpf_limpo = str(cpf_db).strip()
    # Adiciona zeros à esquerda para visualização (padrão 11 dígitos)
    cpf_full = cpf_limpo.zfill(11)
    return f"{cpf_full[:3]}.{cpf_full[3:6]}.{cpf_full[6:9]}-{cpf_full[9:]}"

def limpar_normalizar_cpf(cpf_raw):
    """
    Regra 3.1.1 e 3.1.3:
    - Remove espaços (.strip)
    - Remove caracteres não numéricos (pontuação, letras)
    - Remove zeros à esquerda (.lstrip('0'))
    """
    if not cpf_raw: return ""
    # Remove espaços
    s = str(cpf_raw).strip()
    # Remove não números (Regra 3.1.4 - não aceita letras/especiais)
    apenas_nums = re.sub(r'\D', '', s)
    # Remove zeros à esquerda (Regra 3.1.1)
    return apenas_nums.lstrip('0')

def limpar_apenas_numeros(valor):
    if not valor: return ""
    return re.sub(r'\D', '', str(valor))

def validar_formatar_telefone(tel_raw):
    numeros = limpar_apenas_numeros(tel_raw)
    # Aceita fixo (10) ou celular (11)
    if len(numeros) == 10 or len(numeros) == 11:
        return numeros, None
    return None, "Telefone deve ter 10 ou 11 dígitos."

def validar_formatar_cpf(cpf_raw):
    """
    Validação de entrada na Tela (Regra 3.1.2 e 3.1.4)
    - Deve aceitar entrada com ou sem zeros.
    - O importante é que seja numérico e tenha tamanho razoável (até 11 dígitos).
    """
    # Limpa para verificar o conteúdo real
    numeros = re.sub(r'\D', '', str(cpf_raw).strip())
    
    # Se estiver vazio, erro
    if not numeros:
        return None, "CPF inválido (vazio)."
    
    # Verifica tamanho máximo (CPF tem max 11 dígitos se considerar zeros)
    if len(numeros) > 11:
        return None, "CPF deve ter no máximo 11 dígitos."
    
    # A Regra 3.1.2 diz "sem o zero na frente: aceita".
    # Portanto, não podemos exigir len == 11. Se o usuário digitar "123" (CPF antigo/baixo ou erro),
    # o sistema aceita e formata. A validação de existência real ocorre em outros níveis se necessário.
    
    # Retorna o valor limpo (sem zeros) para consistência com limpar_normalizar_cpf depois
    return numeros, None

def validar_email(email):
    if not email: return False
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(regex, email))

def validar_formatar_cep(cep_raw):
    numeros = limpar_apenas_numeros(cep_raw)
    if len(numeros) != 8: return None, "CEP deve ter 8 dígitos."
    return f"{numeros[:5]}-{numeros[5:]}", None

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
            # Normaliza o CPF de entrada para buscar no banco (sem zeros)
            cpf_norm = limpar_normalizar_cpf(cpf)      
            
            # Prepara parâmetro de busca. O banco armazena sem zeros (pf_dados).
            # Por segurança, mantemos compatibilidade caso haja lixo, mas o foco é cpf_norm.
            params_busca = (cpf_norm,)
            
            # 1. Dados Pessoais
            df_d = pd.read_sql("SELECT * FROM banco_pf.pf_dados WHERE cpf = %s", conn, params=params_busca)
            if not df_d.empty: dados['geral'] = df_d.where(pd.notnull(df_d), None).iloc[0].to_dict()
            
            # Tabelas satélites (FK cpf_ref)
            # Obs: As tabelas satélites usam cpf_ref que referencia pf_dados(cpf).
            # Logo, o valor em cpf_ref também estará sem zeros.
            dados['telefones'] = pd.read_sql("SELECT numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf_ref = %s", conn, params=params_busca).fillna("").to_dict('records')
            dados['emails'] = pd.read_sql("SELECT email FROM banco_pf.pf_emails WHERE cpf_ref = %s", conn, params=params_busca).fillna("").to_dict('records')
            dados['enderecos'] = pd.read_sql("SELECT rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf_ref = %s", conn, params=params_busca).fillna("").to_dict('records')
            
            # Busca Vínculos
            # pf_emprego_renda também usa cpf_ref
            query_emp = """
                SELECT convenio, matricula 
                FROM banco_pf.pf_emprego_renda 
                WHERE cpf_ref = %s
            """
            df_emp = pd.read_sql(query_emp, conn, params=params_busca)
            
            if not df_emp.empty:
                for _, row_emp in df_emp.iterrows():
                    conv_nome = str(row_emp['convenio']).strip() 
                    matricula = str(row_emp['matricula']).strip()
                    
                    vinculo = {
                        'convenio': conv_nome,
                        'matricula': matricula,
                        'dados_extras': '',
                        'contratos': []
                    }

                    # Roteamento
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
                            except: pass
                    else:
                        try:
                            query_padrao = "SELECT * FROM banco_pf.pf_contratos WHERE matricula_ref = %s"
                            df_contratos = pd.read_sql(query_padrao, conn, params=(matricula,))
                            if not df_contratos.empty:
                                df_contratos['origem_tabela'] = 'banco_pf.pf_contratos'
                                df_contratos['tipo_origem'] = 'Geral'
                                vinculo['contratos'] = df_contratos.to_dict('records')
                        except: pass
                    
                    dados['empregos'].append(vinculo)

        except Exception as e:
            print(f"Erro carregamento: {e}") 
        finally: 
            conn.close()
            
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
    
    if not tabelas:
        tabelas = [('banco_pf.pf_contratos', 'Contratos Gerais')]
    return tabelas

def get_colunas_tabela(nome_tabela_completo):
    conn = get_conn()
    colunas = []
    if conn:
        try:
            if '.' in nome_tabela_completo:
                schema, tabela = nome_tabela_completo.split('.')
            else:
                schema, tabela = 'public', nome_tabela_completo

            cur = conn.cursor()
            query = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = %s
                ORDER BY ordinal_position
            """
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
        {"label": "RG", "key": "rg", "tabela": "geral", "tipo": "texto"},
        {"label": "Data Nascimento", "key": "data_nascimento", "tabela": "geral", "tipo": "data"},
        {"label": "Nome da Mãe", "key": "nome_mae", "tabela": "geral", "tipo": "texto"},
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
    
    # Validações específicas
    if campo_config['tipo'] == 'cpf':
        # Valida formato e aceita 3.1.2 (sem zero)
        val, erro = validar_formatar_cpf(valor)
        if not erro: 
            # Aplica limpeza final (Regra 3.1.1: Sem zeros na frente)
            valor_final = limpar_normalizar_cpf(val)
            
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

# --- INTERFACE PRINCIPAL ---
def interface_cadastro_pf():
    is_edit = st.session_state['pf_view'] == 'editar'
    
    # Visualização do título com zeros (Regra Visual)
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

    # --- COLUNA DA ESQUERDA (FORMULÁRIOS) ---
    with c_builder:
        st.markdown("#### 🏗️ Inserir Dados")
        
        # 1. DADOS PESSOAIS
        with st.expander("Dados Pessoais", expanded=True):
            for campo in CONFIG_CADASTRO["Dados Pessoais"]:
                # REGRA 3.1.5: Bloqueio do CPF na Edição
                if is_edit and campo['key'] == 'cpf':
                    c_lab, c_inp = st.columns([1.2, 3.5])
                    c_lab.markdown(f"**{campo['label']}:**")
                    
                    val_atual = st.session_state['dados_staging']['geral'].get('cpf', '')
                    # Exibe formatado para facilitar leitura
                    val_show = formatar_cpf_visual(val_atual)
                    
                    c_inp.text_input("CPF Display", value=val_show, disabled=True, label_visibility="collapsed")
                    continue
                
                c_lbl, c_inp, c_btn = st.columns([1.2, 2.5, 1.0])
                c_lbl.markdown(f"**{campo['label']}:**")
                with c_inp:
                    if campo['tipo'] == 'data':
                        val = st.date_input("Data", value=None, min_value=date(1900, 1, 1), max_value=date(2050, 12, 31), format="DD/MM/YYYY", key=f"in_{campo['key']}", label_visibility="collapsed")
                    else:
                        val = st.text_input("Texto", label_visibility="collapsed", key=f"in_{campo['key']}")
                
                with c_btn:
                    if st.button("Inserir", key=f"btn_{campo['key']}", type="primary", use_container_width=True): 
                        inserir_dado_staging(campo, val)
        
        # 2. CONTATOS
        with st.expander("Contatos"):
            c_tel_in, c_whats, c_qualif, c_tel_btn = st.columns([3, 1.5, 1.5, 2])
            with c_tel_in:
                tel = st.text_input("Número", key="in_tel_num", placeholder="Telefone")
            with c_whats:
                whats = st.selectbox("WhatsApp", ["Não", "Sim"], key="in_tel_w")
            with c_qualif:
                qualif = st.selectbox("Qualif.", ["NC", "CONFIRMADO"], key="in_tel_q")
            with c_tel_btn:
                st.write(""); st.write("") 
                if st.button("Inserir Telefone", type="primary", use_container_width=True):
                    cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'numero'][0]
                    inserir_dado_staging(cfg, tel, {'tag_whats': whats, 'tag_qualificacao': qualif})
            
            st.divider()
            c_mail_in, c_mail_btn = st.columns([5, 2])
            with c_mail_in:
                mail = st.text_input("E-mail", key="in_mail", placeholder="E-mail")
            with c_mail_btn:
                st.write(""); st.write("")
                if st.button("Inserir E-mail", type="primary", use_container_width=True):
                    cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'email'][0]
                    inserir_dado_staging(cfg, mail)

        # 3. ENDEREÇOS
        with st.expander("Endereço"):
            c_rua, c_cep = st.columns([1, 1])
            with c_rua: rua = st.text_input("Logradouro (Endereço)", key="in_end_rua")
            with c_cep: cep = st.text_input("CEP", key="in_end_cep")
            
            c_bai, c_cid, c_uf = st.columns([2, 2, 1])
            with c_bai: bairro = st.text_input("Bairro", key="in_end_bairro")
            with c_cid: cidade = st.text_input("Cidade", key="in_end_cid")
            with c_uf: uf = st.text_input("UF", key="in_end_uf")

            if st.button("Inserir Endereço", type="primary", use_container_width=True):
                obj_end = {'cep': cep, 'rua': rua, 'bairro': bairro, 'cidade': cidade, 'uf': uf}
                cfg = [c for c in CONFIG_CADASTRO["Endereços"] if c['key'] == 'cep'][0]
                inserir_dado_staging(cfg, obj_end)

        # 4. EMPREGO E RENDA
        with st.expander("Emprego e Renda (Vínculo)"):
            c_conv, c_matr, c_btn_emp = st.columns([3, 3, 2])
            with c_conv: conv = st.text_input("Convênio", key="in_emp_conv", placeholder="Ex: INSS")
            with c_matr: matr = st.text_input("Matrícula", key="in_emp_matr")
            with c_btn_emp:
                st.write(""); st.write("")
                if st.button("Inserir Vínculo", type="primary", use_container_width=True):
                    if conv and matr:
                        obj_emp = {'convenio': conv, 'matricula': matr, 'dados_extras': ''}
                        if 'empregos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['empregos'] = []
                        st.session_state['dados_staging']['empregos'].append(obj_emp)
                        st.toast("✅ Vínculo adicionado!")
                        st.rerun()
                    else: st.warning("Campos obrigatórios.")

        # 5. CONTRATOS / PLANILHAS
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
                
                if not tabelas_destino:
                    st.warning(f"Sem planilhas configuradas para {dados_vinc['convenio']}.")
                
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
                        if 'matricula' in nomes_cols_tabela: inputs_gerados['matricula'] = dados_vinc['matricula']
                        elif 'matricula_ref' in nomes_cols_tabela: inputs_gerados['matricula_ref'] = dados_vinc['matricula']
                        if 'convenio' in nomes_cols_tabela: inputs_gerados['convenio'] = dados_vinc['convenio']
                        if 'tipo_planilha' in nomes_cols_tabela and tipo_tabela: inputs_gerados['tipo_planilha'] = tipo_tabela
                        inputs_gerados['origem_tabela'] = nome_tabela
                        inputs_gerados['tipo_origem'] = tipo_tabela
                        
                        if 'contratos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['contratos'] = []
                        st.session_state['dados_staging']['contratos'].append(inputs_gerados)
                        st.toast(f"✅ {tipo_tabela} adicionado!")

    # --- COLUNA DA DIREITA (RESUMO E SALVAR) ---
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
                    # Exibição do CPF no resumo também formatada
                    if k == 'cpf': val_str = formatar_cpf_visual(val_str)
                    cols[idx%2].text_input(k.upper(), value=val_str, disabled=True, key=f"view_geral_{k}")
                    idx += 1
        
        st.warning("💼 Vínculos (Emprego)")
        emps = st.session_state['dados_staging'].get('empregos', [])
        if emps:
            for i, emp in enumerate(emps):
                c1, c2 = st.columns([5, 1])
                c1.write(f"🏢 **{emp.get('convenio')}** | Mat: {emp.get('matricula')}")
                if c2.button("🗑️", key=f"rm_emp_{i}"):
                    st.session_state['dados_staging']['empregos'].pop(i); st.rerun()
        else:
            st.caption("Nenhum vínculo inserido.")

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
        else:
            st.caption("Nenhum vínculo inserido.")

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
                
                sucesso, msg = salvar_pf(staging['geral'], df_tel, df_email, df_end, df_emp, df_contr, modo_salvar, cpf_orig)
                
                if sucesso:
                    st.success(msg)
                    time.sleep(1.5)
                    st.session_state['pf_view'] = 'lista'
                    st.session_state['form_loaded'] = False
                    st.rerun()
                else:
                    st.error(msg)
    
    st.markdown(f"<div style='text-align: right; color: gray; font-size: 0.8em; margin-top: 20px;'>código atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>", unsafe_allow_html=True)

# --- FUNÇÕES DE SALVAMENTO E EXCLUSÃO ---
def salvar_pf(dados_gerais, df_tel, df_email, df_end, df_emp, df_contr, modo="novo", cpf_original=None):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            
            # Limpeza final do CPF antes de salvar (Garante sem zeros)
            cpf_limpo = limpar_normalizar_cpf(dados_gerais['cpf'])
            dados_gerais['cpf'] = cpf_limpo
            
            # Se for edição, cpf_original também deve estar limpo para achar no banco
            if cpf_original: cpf_original = limpar_normalizar_cpf(cpf_original)
            
            # UPPERCASE
            dados_gerais = {k: (v.upper() if isinstance(v, str) else v) for k, v in dados_gerais.items()}

            if modo == "novo":
                cols = list(dados_gerais.keys()); vals = list(dados_gerais.values())
                placeholders = ", ".join(["%s"] * len(vals)); col_names = ", ".join(cols)
                cur.execute(f"INSERT INTO banco_pf.pf_dados ({col_names}) VALUES ({placeholders})", vals)
            else:
                set_clause = ", ".join([f"{k}=%s" for k in dados_gerais.keys()])
                vals = list(dados_gerais.values()) + [cpf_original]
                cur.execute(f"UPDATE banco_pf.pf_dados SET {set_clause} WHERE cpf=%s", vals)
            
            # CPF chave para as tabelas satélites
            cpf_chave = dados_gerais['cpf']
            
            # REGRA 3.1.6: Exclusão em Cascata na Edição
            # Remove dados antigos para inserir os novos da tela
            if modo == "editar":
                tabelas_ref = ['banco_pf.pf_telefones', 'banco_pf.pf_emails', 'banco_pf.pf_enderecos']
                # pf_telefones, emails e enderecos usam cpf_ref ou cpf?
                # No script de criação (banco_pf.sql), usa cpf_ref.
                # No entanto, em sistemas legados às vezes varia. O código original usava 'cpf' no DELETE.
                # Vou usar 'cpf_ref' conforme o SQL padrão, mas se o banco original tiver 'cpf', ajustar.
                # Baseado no arquivo banco_pf.sql fornecido: "cpf_ref VARCHAR(20) REFERENCES pf_dados(cpf)"
                # Então o campo correto é cpf_ref.
                
                # Mas, espere: o código original fornecido no prompt usava "DELETE FROM ... WHERE cpf = %s".
                # Se o banco SQL diz cpf_ref, isso daria erro.
                # Vou manter a lógica do original mas corrigir para cpf_ref caso o SQL seja o mandante.
                # Como o banco_pf.sql tem cpf_ref, usarei cpf_ref para garantir.
                
                # CORREÇÃO: Verifiquei o SQL. pf_telefones tem cpf_ref. pf_emails tem cpf_ref. pf_enderecos tem cpf_ref.
                # O código anterior estava possivelmente errado ou o banco permite. Vou usar cpf_ref.
                for tb in tabelas_ref: 
                    cur.execute(f"DELETE FROM {tb} WHERE cpf_ref = %s", (cpf_chave,))
            
            def df_upper(df): return df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
            
            # Inserção (Satélites usam cpf_ref)
            if not df_tel.empty:
                for _, r in df_upper(df_tel).iterrows(): cur.execute("INSERT INTO banco_pf.pf_telefones (cpf_ref, numero, tag_whats, tag_qualificacao, data_atualizacao) VALUES (%s, %s, %s, %s, %s)", (cpf_chave, r['numero'], r.get('tag_whats'), r.get('tag_qualificacao'), date.today()))
            if not df_email.empty:
                for _, r in df_upper(df_email).iterrows(): cur.execute("INSERT INTO banco_pf.pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, r['email']))
            if not df_end.empty:
                for _, r in df_upper(df_end).iterrows(): cur.execute("INSERT INTO banco_pf.pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, r['rua'], r['bairro'], r['cidade'], r['uf'], r['cep']))
            
            # Emprego (Lógica de Upsert por Matrícula)
            if not df_emp.empty:
                for _, r in df_upper(df_emp).iterrows():
                    matr = r['matricula']
                    # pf_emprego_renda usa cpf_ref
                    cur.execute("SELECT 1 FROM banco_pf.pf_emprego_renda WHERE matricula = %s", (matr,))
                    if cur.fetchone():
                        cur.execute("UPDATE banco_pf.pf_emprego_renda SET cpf_ref = %s, convenio = %s, data_atualizacao = %s WHERE matricula = %s", (cpf_chave, r['convenio'], datetime.now(), matr))
                    else:
                        cur.execute("INSERT INTO banco_pf.pf_emprego_renda (cpf_ref, convenio, matricula, data_atualizacao) VALUES (%s, %s, %s, %s)", (cpf_chave, r['convenio'], matr, datetime.now()))
            
            # Contratos
            if not df_contr.empty:
                for _, r in df_upper(df_contr).iterrows():
                    tabela = r.get('origem_tabela', 'banco_pf.pf_contratos')
                    r_dict = r.to_dict()
                    r_dict.pop('origem_tabela', None)
                    r_dict.pop('tipo_origem', None)
                    final_dict = {}
                    for k, v in r_dict.items():
                        if pd.isna(v) or v == "": continue
                        if 'cnpj' in k.lower(): final_dict[k] = formatar_cnpj(v)
                        else: final_dict[k] = v
                    if not final_dict: continue
                    cols = list(final_dict.keys()); vals = list(final_dict.values())
                    placeholders = ", ".join(["%s"] * len(vals)); col_names = ", ".join(cols)
                    try: cur.execute(f"INSERT INTO {tabela} ({col_names}) VALUES ({placeholders})", vals)
                    except Exception as e_contr: print(f"Erro inserção dinâmica {tabela}: {e_contr}")

            conn.commit(); conn.close(); return True, "Salvo com sucesso!"
        except Exception as e: return False, str(e)
    return False, "Erro conexão"

def excluir_pf(cpf):
    conn = get_conn()
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            cur = conn.cursor()
            # Na exclusão, apaga o pai (pf_dados). O CASCADE do banco deve apagar filhos.
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
    
    t1, t2, t3 = st.tabs(["📋 Cadastro & Vínculos", "💼 Detalhes Financeiros", "📞 Contatos"])
    with t1:
        c1, c2 = st.columns(2)
        nasc = g.get('data_nascimento')
        idade = calcular_idade_hoje(nasc)
        txt_nasc = f"{nasc.strftime('%d/%m/%Y')} ({idade} anos)" if idade and isinstance(nasc, (date, datetime)) else safe_view(nasc)
        c1.write(f"**Nascimento:** {txt_nasc}"); c1.write(f"**RG:** {safe_view(g.get('rg'))}"); c2.write(f"**Mãe:** {safe_view(g.get('nome_mae'))}")
        demais_campos = {k: v for k, v in g.items() if k not in ['data_nascimento', 'rg', 'nome_mae', 'id', 'cpf', 'nome', 'importacao_id', 'id_campanha', 'data_criacao']}
        if demais_campos:
            st.markdown("---"); st.markdown("##### 📌 Outras Informações")
            col_iter = st.columns(3); idx = 0
            for k, v in demais_campos.items(): col_iter[idx % 3].write(f"**{k.replace('_', ' ').title()}:** {safe_view(v)}"); idx += 1
        st.divider(); st.markdown("##### 🔗 Vínculos")
        for v in dados.get('empregos', []): st.info(f"🆔 **{v['matricula']}** - {v['convenio'].upper()}")
        if not dados.get('empregos'): st.warning("Nenhum vínculo localizado.")
        st.divider(); st.markdown("##### 🏠 Endereços")
        for end in dados.get('enderecos', []): st.success(f"📍 {safe_view(end.get('rua'))}, {safe_view(end.get('bairro'))} - {safe_view(end.get('cidade'))}/{safe_view(end.get('uf'))}")
            
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
        for t in dados.get('telefones', []): st.write(f"📱 {safe_view(t.get('numero'))} ({safe_view(t.get('tag_whats'))})")
        for m in dados.get('emails', []): st.write(f"📧 {safe_view(m.get('email'))}")
    
    st.markdown(f"<div style='text-align: right; color: gray; font-size: 0.8em; margin-top: 10px;'>código atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>", unsafe_allow_html=True)