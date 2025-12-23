import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date
import io
import time
import math
import re
import os

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
BASE_DIR_IMPORTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ARQUIVO IMPORTAÇÕES")
if not os.path.exists(BASE_DIR_IMPORTS):
    os.makedirs(BASE_DIR_IMPORTS)

try:
    import conexao
except ImportError:
    st.error("Erro crítico: conexao.py não encontrado.")

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        return None

# --- INICIALIZAÇÃO DO BANCO (AUTO-MIGRATE) ---
def init_db_structures():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pf_historico_importacoes (
                    id SERIAL PRIMARY KEY,
                    nome_arquivo VARCHAR(255),
                    data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    qtd_novos INTEGER DEFAULT 0,
                    qtd_atualizados INTEGER DEFAULT 0,
                    qtd_erros INTEGER DEFAULT 0,
                    caminho_arquivo_original TEXT,
                    caminho_arquivo_erro TEXT
                );
            """)
            cur.execute("ALTER TABLE pf_dados ADD COLUMN IF NOT EXISTS importacao_id INTEGER REFERENCES pf_historico_importacoes(id);")
            conn.commit()
            conn.close()
        except: pass

# --- FUNÇÕES AUXILIARES ---
def calcular_idade_completa(data_nasc):
    if not data_nasc: return "", "", ""
    hoje = date.today()
    if isinstance(data_nasc, datetime): data_nasc = data_nasc.date()
    anos = hoje.year - data_nasc.year - ((hoje.month, hoje.day) < (data_nasc.month, data_nasc.day))
    meses = (hoje.year - data_nasc.year) * 12 + hoje.month - data_nasc.month
    dias = (hoje - data_nasc).days
    return anos, meses, dias

def buscar_referencias(tipo):
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT nome FROM pf_referencias WHERE tipo = %s ORDER BY nome", conn, params=(tipo,))
            conn.close()
            return df['nome'].tolist()
        except: conn.close()
    return []

def limpar_normalizar_cpf(cpf_raw):
    if not cpf_raw: return ""
    apenas_nums = re.sub(r'\D', '', str(cpf_raw))
    if not apenas_nums: return ""
    return apenas_nums.zfill(11)

def verificar_cpf_existente(cpf_normalizado):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome FROM pf_dados WHERE cpf = %s", (cpf_normalizado,))
            res = cur.fetchone()
            conn.close()
            return res[0] if res else None
        except: conn.close()
    return None

def converter_data_br_iso(valor):
    if not valor or pd.isna(valor): return None
    valor_str = str(valor).strip()
    formatos = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]
    for fmt in formatos:
        try: return datetime.strptime(valor_str, fmt).strftime("%Y-%m-%d")
        except ValueError: continue
    return None

# --- FUNÇÕES DE BUSCA ---
def buscar_pf_simples(termo, filtro_importacao_id=None):
    conn = get_conn()
    if conn:
        try:
            termo_limpo = re.sub(r'\D', '', termo)
            param_nome = f"%{termo}%"
            param_num = f"%{termo_limpo}%"
            
            sql_base = """
                SELECT d.id, d.nome, d.cpf, d.data_nascimento 
                FROM pf_dados d
                LEFT JOIN pf_telefones t ON d.cpf = t.cpf_ref
            """
            
            if filtro_importacao_id:
                sql_where = " WHERE d.importacao_id = %s "
                params = [filtro_importacao_id]
                if termo:
                   sql_where += " AND (d.cpf ILIKE %s OR d.nome ILIKE %s OR t.numero ILIKE %s) "
                   params.extend([param_num, param_nome, param_num])
                params = tuple(params)
            else:
                sql_where = " WHERE d.cpf ILIKE %s OR d.nome ILIKE %s OR t.numero ILIKE %s "
                params = (param_num, param_nome, param_num)
            
            query = f"{sql_base} {sql_where} GROUP BY d.id ORDER BY d.nome ASC LIMIT 50"
            df = pd.read_sql(query, conn, params=params)
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def buscar_opcoes_filtro(coluna, tabela):
    conn = get_conn()
    opcoes = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT DISTINCT {coluna} FROM {tabela} WHERE {coluna} IS NOT NULL ORDER BY {coluna}")
            opcoes = [r[0] for r in cur.fetchall() if r[0]]
            conn.close()
        except: pass
    return opcoes

def executar_pesquisa_ampla(filtros, pagina=1, itens_por_pagina=30):
    conn = get_conn()
    if conn:
        try:
            sql = "SELECT DISTINCT d.id, d.nome, d.cpf, d.rg, d.data_nascimento FROM pf_dados d "
            joins = []
            conditions = []
            params = []

            if filtros.get('nome'):
                conditions.append("d.nome ILIKE %s")
                params.append(f"%{filtros['nome']}%")
            if filtros.get('cpf'):
                cpf_norm = limpar_normalizar_cpf(filtros['cpf'])
                conditions.append("d.cpf = %s") 
                params.append(cpf_norm)
            if filtros.get('rg'):
                conditions.append("d.rg ILIKE %s")
                params.append(f"%{filtros['rg']}%")
            if filtros.get('nascimento'):
                conditions.append("d.data_nascimento = %s")
                params.append(filtros['nascimento'])

            if any(k in filtros for k in ['uf', 'cidade', 'bairro', 'rua']):
                joins.append("JOIN pf_enderecos end ON d.cpf = end.cpf_ref")
                if filtros.get('uf'):
                    conditions.append("end.uf = %s")
                    params.append(filtros['uf'])
                if filtros.get('cidade'):
                    conditions.append("end.cidade ILIKE %s")
                    params.append(f"%{filtros['cidade']}%")
                if filtros.get('bairro'):
                    conditions.append("end.bairro ILIKE %s")
                    params.append(f"%{filtros['bairro']}%")
                if filtros.get('rua'):
                    conditions.append("end.rua ILIKE %s")
                    params.append(f"%{filtros['rua']}%")

            if filtros.get('ddd') or filtros.get('telefone'):
                joins.append("JOIN pf_telefones tel ON d.cpf = tel.cpf_ref")
            if filtros.get('ddd'):
                conditions.append("SUBSTRING(REGEXP_REPLACE(tel.numero, '[^0-9]', '', 'g'), 1, 2) = %s")
                params.append(filtros['ddd'])
            if filtros.get('telefone'):
                tel_clean = re.sub(r'\D', '', filtros['telefone'])
                conditions.append("tel.numero LIKE %s")
                params.append(f"%{tel_clean}%")

            if filtros.get('email'):
                joins.append("JOIN pf_emails em ON d.cpf = em.cpf_ref")
                conditions.append("em.email ILIKE %s")
                params.append(f"%{filtros['email']}%")

            if any(k in filtros for k in ['convenio', 'matricula', 'contrato']):
                if filtros.get('contrato'):
                    joins.append("JOIN pf_emprego_renda emp ON d.cpf = emp.cpf_ref")
                    joins.append("JOIN pf_contratos ctr ON emp.matricula = ctr.matricula_ref")
                    conditions.append("ctr.contrato ILIKE %s")
                    params.append(f"%{filtros['contrato']}%")
                else:
                    joins.append("JOIN pf_emprego_renda emp ON d.cpf = emp.cpf_ref")
                if filtros.get('convenio'):
                    conditions.append("emp.convenio = %s")
                    params.append(filtros['convenio'])
                if filtros.get('matricula'):
                    conditions.append("emp.matricula ILIKE %s")
                    params.append(f"%{filtros['matricula']}%")

            joins = list(set(joins))
            sql_joins = " ".join(joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            full_sql = f"{sql} {sql_joins} {sql_where} ORDER BY d.nome"
            
            count_sql = f"SELECT COUNT(DISTINCT d.id) FROM pf_dados d {sql_joins} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total_registros = cur.fetchone()[0]
            
            offset = (pagina - 1) * itens_por_pagina
            pag_sql = f"{full_sql} LIMIT {itens_por_pagina} OFFSET {offset}"
            
            df = pd.read_sql(pag_sql, conn, params=tuple(params))
            conn.close()
            return df, total_registros
        except: return pd.DataFrame(), 0
    return pd.DataFrame(), 0

# --- FUNÇÕES CRUD (RESTORED FULL LOGIC) ---
def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {}
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            df_d = pd.read_sql("SELECT * FROM pf_dados WHERE cpf = %s", conn, params=(cpf_norm,))
            dados['geral'] = df_d.iloc[0] if not df_d.empty else None
            dados['telefones'] = pd.read_sql("SELECT numero, data_atualizacao, tag_whats, tag_qualificacao FROM pf_telefones WHERE cpf_ref = %s", conn, params=(cpf_norm,))
            dados['emails'] = pd.read_sql("SELECT email FROM pf_emails WHERE cpf_ref = %s", conn, params=(cpf_norm,))
            dados['enderecos'] = pd.read_sql("SELECT rua, bairro, cidade, uf, cep FROM pf_enderecos WHERE cpf_ref = %s", conn, params=(cpf_norm,))
            dados['empregos'] = pd.read_sql("SELECT id, convenio, matricula, dados_extras FROM pf_emprego_renda WHERE cpf_ref = %s", conn, params=(cpf_norm,))
            
            if not dados['empregos'].empty:
                matr_list = tuple(dados['empregos']['matricula'].dropna().tolist())
                if matr_list:
                    placeholders = ",".join(["%s"] * len(matr_list))
                    q_contratos = f"SELECT matricula_ref, contrato, dados_extras FROM pf_contratos WHERE matricula_ref IN ({placeholders})"
                    dados['contratos'] = pd.read_sql(q_contratos, conn, params=matr_list)
                else: dados['contratos'] = pd.DataFrame()
            else: dados['contratos'] = pd.DataFrame()
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
                cols = list(dados_gerais.keys())
                vals = list(dados_gerais.values())
                placeholders = ", ".join(["%s"] * len(vals))
                col_names = ", ".join(cols)
                cur.execute(f"INSERT INTO pf_dados ({col_names}) VALUES ({placeholders})", vals)
            else:
                set_clause = ", ".join([f"{k}=%s" for k in dados_gerais.keys()])
                vals = list(dados_gerais.values()) + [cpf_original]
                cur.execute(f"UPDATE pf_dados SET {set_clause} WHERE cpf=%s", vals)
            
            cpf_chave = dados_gerais['cpf']
            
            if modo == "editar":
                # Limpa filhos para recriar (Estratégia Full Refresh)
                cur.execute("DELETE FROM pf_telefones WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_emails WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_enderecos WHERE cpf_ref = %s", (cpf_chave,))
                # Para empregos e contratos, deletamos apenas se for reescrever tudo. 
                # Aqui vamos simplificar deletando para garantir integridade com o DF enviado.
                cur.execute("DELETE FROM pf_contratos WHERE matricula_ref IN (SELECT matricula FROM pf_emprego_renda WHERE cpf_ref = %s)", (cpf_chave,))
                cur.execute("DELETE FROM pf_emprego_renda WHERE cpf_ref = %s", (cpf_chave,))
            
            def df_upper(df): return df.applymap(lambda x: x.upper() if isinstance(x, str) else x)

            # Salvar Telefones
            if not df_tel.empty:
                for _, row in df_upper(df_tel).iterrows():
                    if row.get('numero'): 
                        cur.execute("INSERT INTO pf_telefones (cpf_ref, numero, tag_whats) VALUES (%s, %s, %s)", (cpf_chave, row['numero'], row.get('tag_whats')))
            
            # Salvar Emails
            if not df_email.empty:
                for _, row in df_upper(df_email).iterrows():
                    if row.get('email'): 
                        cur.execute("INSERT INTO pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, row['email']))
            
            # Salvar Endereços
            if not df_end.empty:
                for _, row in df_upper(df_end).iterrows():
                    if row.get('rua') or row.get('cidade'): 
                        cur.execute("INSERT INTO pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, row['rua'], row.get('bairro'), row.get('cidade'), row.get('uf'), row.get('cep')))
            
            # Salvar Empregos
            if not df_emp.empty:
                for _, row in df_upper(df_emp).iterrows():
                    if row.get('convenio') and row.get('matricula'):
                        try: 
                            cur.execute("INSERT INTO pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s)", (cpf_chave, row.get('convenio'), row.get('matricula'), row.get('dados_extras')))
                        except: pass

            # Salvar Contratos
            if not df_contr.empty:
                for _, row in df_upper(df_contr).iterrows():
                    if row.get('matricula_ref'):
                        # Verifica se matrícula existe antes de inserir contrato
                        cur.execute("SELECT 1 FROM pf_emprego_renda WHERE matricula = %s", (row.get('matricula_ref'),))
                        if cur.fetchone():
                            cur.execute("INSERT INTO pf_contratos (matricula_ref, contrato, dados_extras) VALUES (%s, %s, %s)", (row.get('matricula_ref'), row.get('contrato'), row.get('dados_extras')))

            conn.commit()
            conn.close()
            return True, "Salvo com sucesso!"
        except psycopg2.IntegrityError:
            conn.rollback()
            return False, "Erro: CPF já cadastrado."
        except Exception as e: return False, str(e)
    return False, "Erro de conexão"

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

def exportar_dados(lista_cpfs):
    conn = get_conn()
    if conn and lista_cpfs:
        lista_norm = [limpar_normalizar_cpf(c) for c in lista_cpfs]
        placeholders = ",".join(["%s"] * len(lista_norm))
        # Query simplificada para exportação
        query = f"SELECT * FROM pf_dados WHERE cpf IN ({placeholders})"
        df = pd.read_sql(query, conn, params=tuple(lista_norm))
        conn.close()
        return df
    return pd.DataFrame()

# --- FUNÇÕES DE IMPORTAÇÃO ---
def get_table_columns(table_name):
    conn = get_conn()
    cols = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
            cols = cur.fetchall()
            conn.close()
        except: pass
    return cols

def add_column_to_table(table_name, col_name, col_type):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS \"{col_name}\" {col_type}")
            conn.commit(); conn.close()
            return True
        except: return False
    return False

# --- INTERFACE ---
@st.dialog("⚠️ Excluir Cadastro")
def dialog_excluir_pf(cpf, nome):
    st.error(f"Apagar **{nome}**?")
    st.warning("Esta ação é irreversível.")
    c1, c2 = st.columns(2)
    if c1.button("Confirmar Exclusão", type="primary"):
        if excluir_pf(cpf): st.success("Apagado!"); time.sleep(1); st.rerun()
    if c2.button("Cancelar"): st.rerun()

def app_pessoa_fisica():
    init_db_structures()
    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'pf_cpf_selecionado' not in st.session_state: st.session_state['pf_cpf_selecionado'] = None
    if 'import_step' not in st.session_state: st.session_state['import_step'] = 1
    if 'import_stats' not in st.session_state: st.session_state['import_stats'] = {}
    if 'filtro_importacao_id' not in st.session_state: st.session_state['filtro_importacao_id'] = None
    
    # Inicializa contadores de campos dinâmicos
    for k in ['count_tel', 'count_email', 'count_end', 'count_emp', 'count_ctr']:
        if k not in st.session_state: st.session_state[k] = 1

    # ==========================
    # 1. PESQUISA AMPLA
    # ==========================
    if st.session_state['pf_view'] == 'pesquisa_ampla':
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        st.markdown("### 🔎 Pesquisa Ampla")
        with st.form("form_pesquisa_ampla", enter_to_submit=False):
            t1, t2, t3, t4, t5 = st.tabs(["Identificação", "Endereço", "Contatos", "Profissional", "Contratos"])
            filtros = {}
            with t1:
                c1, c2, c3, c4 = st.columns(4)
                filtros['nome'] = c1.text_input("Nome")
                filtros['cpf'] = c2.text_input("CPF")
                filtros['rg'] = c3.text_input("RG")
                filtros['nascimento'] = c4.date_input("Nascimento", value=None, format="DD/MM/YYYY")
            with t2:
                c_uf, c_cid, c_bai, c_rua = st.columns([1, 3, 3, 3])
                lista_ufs = buscar_opcoes_filtro('uf', 'pf_enderecos')
                sel_uf = c_uf.selectbox("UF", [""] + lista_ufs)
                if sel_uf: filtros['uf'] = sel_uf
                filtros['cidade'] = c_cid.text_input("Cidade")
                filtros['bairro'] = c_bai.text_input("Bairro")
                filtros['rua'] = c_rua.text_input("Rua")
            with t3:
                c_ddd, c_tel, c_email = st.columns([0.5, 1.5, 4])
                filtros['ddd'] = c_ddd.text_input("DDD", max_chars=2)
                filtros['telefone'] = c_tel.text_input("Telefone", max_chars=9)
                filtros['email'] = c_email.text_input("E-mail")
            with t4:
                c_conv, c_matr = st.columns(2)
                lista_conv = buscar_referencias('CONVENIO')
                sel_conv = c_conv.selectbox("Convênio", [""] + lista_conv)
                if sel_conv: filtros['convenio'] = sel_conv
                filtros['matricula'] = c_matr.text_input("Matrícula")
            with t5:
                filtros['contrato'] = st.text_input("Número do Contrato")
            btn_pesquisar = st.form_submit_button("Pesquisar")

        if btn_pesquisar:
            filtros_limpos = {k: v for k, v in filtros.items() if v}
            st.session_state['filtros_ativos'] = filtros_limpos
            st.session_state['pesquisa_pag'] = 1
        
        if 'filtros_ativos' in st.session_state and st.session_state['filtros_ativos']:
            pag_atual = st.session_state['pesquisa_pag']
            df_res, total = executar_pesquisa_ampla(st.session_state['filtros_ativos'], pag_atual)
            st.divider()
            st.write(f"**Resultados Encontrados:** {total}")
            if not df_res.empty:
                df_res.insert(0, "Selecionar", False)
                edited_df = st.data_editor(df_res, column_config={"Selecionar": st.column_config.CheckboxColumn(required=True)}, disabled=df_res.columns.drop("Selecionar"), hide_index=True, use_container_width=True)
                
                total_pags = math.ceil(total / 30)
                col_p1, col_p2, col_p3 = st.columns([1, 2, 1])
                with col_p1:
                    if pag_atual > 1 and st.button("⬅️ Anterior"): st.session_state['pesquisa_pag'] -= 1; st.rerun()
                with col_p2:
                    st.markdown(f"<div style='text-align:center'>Página {pag_atual} de {total_pags}</div>", unsafe_allow_html=True)
                with col_p3:
                    if pag_atual < total_pags and st.button("Próxima ➡️"): st.session_state['pesquisa_pag'] += 1; st.rerun()

                subset_selecionado = edited_df[edited_df["Selecionar"] == True]
                if not subset_selecionado.empty:
                    st.divider()
                    registro = subset_selecionado.iloc[0]
                    c1, c2 = st.columns(2)
                    if c1.button("✏️ Editar"): 
                        st.session_state['pf_view'] = 'editar'; st.session_state['pf_cpf_selecionado'] = registro['cpf']; st.rerun()
                    if c2.button("🗑️ Excluir"): dialog_excluir_pf(registro['cpf'], registro['nome'])
            else: st.warning("Nenhum registro encontrado.")

    # ==========================
    # 2. HISTÓRICO DE IMPORTAÇÕES
    # ==========================
    elif st.session_state['pf_view'] == 'historico_importacao':
        st.button("⬅️ Voltar para Lista", on_click=lambda: st.session_state.update({'pf_view': 'importacao', 'import_step': 1}))
        st.markdown("### 📜 Histórico de Importações")
        conn = get_conn()
        if conn:
            df_hist = pd.read_sql("SELECT * FROM pf_historico_importacoes ORDER BY data_importacao DESC", conn)
            conn.close()
            if not df_hist.empty:
                for _, row in df_hist.iterrows():
                    data_fmt = row['data_importacao'].strftime("%d/%m/%Y %H:%M")
                    with st.expander(f"{data_fmt} - {row['nome_arquivo']}"):
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Novos", row['qtd_novos'])
                        c2.metric("Atualizados", row['qtd_atualizados'])
                        c3.metric("Erros", row['qtd_erros'])
                        col_btns = c4.columns(2)
                        if col_btns[0].button("🔎", key=f"src_{row['id']}", help="Ver clientes desta importação"):
                            st.session_state['pf_view'] = 'lista'
                            st.session_state['filtro_importacao_id'] = row['id']
                            st.rerun()
                        if row['qtd_erros'] > 0 and row['caminho_arquivo_erro']:
                            if os.path.exists(row['caminho_arquivo_erro']):
                                with open(row['caminho_arquivo_erro'], "rb") as f:
                                    col_btns[1].download_button("📥 Erros", f, file_name=os.path.basename(row['caminho_arquivo_erro']), key=f"dw_{row['id']}")
            else: st.info("Nenhum histórico.")

    # ==========================
    # 3. MODO IMPORTAÇÃO
    # ==========================
    elif st.session_state['pf_view'] == 'importacao':
        c_cancel, c_hist = st.columns([1, 4])
        c_cancel.button("⬅️ Cancelar", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'import_step': 1}))
        c_hist.button("📜 Ver Histórico Importação", on_click=lambda: st.session_state.update({'pf_view': 'historico_importacao'}))
        st.divider()
        st.markdown("""<style>section[data-testid="stFileUploader"] div[role="button"] > div > div > span {display: none;} section[data-testid="stFileUploader"] div[role="button"] > div > div::after {content: "Arraste o arquivo CSV aqui"; font-size: 1rem; visibility: visible; display: block;} section[data-testid="stFileUploader"] div[role="button"] > div > div > small {display: none;} section[data-testid="stFileUploader"] div[role="button"] > div > div::before {content: "Limite 200MB • CSV"; font-size: 0.8em; visibility: visible; display: block; margin-bottom: 10px;}</style>""", unsafe_allow_html=True)
        
        opcoes_tabelas = ["Dados Cadastrais (pf_dados)", "Telefones (pf_telefones)", "Emails (pf_emails)", "Endereços (pf_enderecos)", "Emprego/Renda (pf_emprego_renda)", "Contratos (pf_contratos)"]
        mapa_real = {"Dados Cadastrais (pf_dados)": "pf_dados", "Telefones (pf_telefones)": "pf_telefones", "Emails (pf_emails)": "pf_emails", "Endereços (pf_enderecos)": "pf_enderecos", "Emprego/Renda (pf_emprego_renda)": "pf_emprego_renda", "Contratos (pf_contratos)": "pf_contratos"}

        if st.session_state['import_step'] == 1:
            st.markdown("### 📤 Etapa 1: Upload")
            sel_amigavel = st.selectbox("Selecione a Tabela de Destino", opcoes_tabelas)
            st.session_state['import_table'] = mapa_real[sel_amigavel]
            uploaded_file = st.file_uploader("Carregar Arquivo CSV", type=['csv'])
            if uploaded_file:
                try:
                    uploaded_file.seek(0)
                    try:
                        df = pd.read_csv(uploaded_file, sep=';')
                        if len(df.columns) <= 1: uploaded_file.seek(0); df = pd.read_csv(uploaded_file, sep=',')
                    except: uploaded_file.seek(0); df = pd.read_csv(uploaded_file, sep=',')
                    st.session_state['import_df'] = df
                    st.session_state['uploaded_file_name'] = uploaded_file.name
                    st.success(f"Carregado! {len(df)} linhas.")
                    if st.button("Ir para Mapeamento", type="primary"):
                        st.session_state['csv_map'] = {col: None for col in df.columns}
                        st.session_state['current_csv_idx'] = 0
                        st.session_state['import_step'] = 2
                        st.rerun()
                except Exception as e: st.error(f"Erro: {e}")

        elif st.session_state['import_step'] == 2:
            st.markdown("### 🔗 Etapa 2: Mapeamento Visual")
            df = st.session_state['import_df']
            csv_cols = list(df.columns)
            table_name = st.session_state['import_table']
            db_cols_info = get_table_columns(table_name)
            ignore_db = ['id', 'data_criacao', 'data_atualizacao', 'cpf_ref', 'matricula_ref', 'importacao_id']
            db_fields = [c[0] for c in db_cols_info if c[0] not in ignore_db]

            c_l, c_r = st.columns([1, 2])
            with c_l:
                for idx, col in enumerate(csv_cols):
                    mapped = st.session_state['csv_map'].get(col)
                    txt = f"{idx+1}. {col} -> {'✅ '+mapped if mapped else '❓'}"
                    if idx == st.session_state['current_csv_idx']: st.info(txt, icon="👉")
                    else: 
                        if st.button(txt, key=f"s_{idx}"): st.session_state['current_csv_idx'] = idx; st.rerun()
            with c_r:
                cols_b = st.columns(4)
                if cols_b[0].button("🚫 IGNORAR", type="secondary"):
                    curr = csv_cols[st.session_state['current_csv_idx']]
                    st.session_state['csv_map'][curr] = "IGNORAR"
                    if st.session_state['current_csv_idx'] < len(csv_cols) - 1: st.session_state['current_csv_idx'] += 1
                    st.rerun()
                for i, field in enumerate(db_fields):
                    if cols_b[(i+1)%4].button(f"📌 {field}", key=f"m_{field}"):
                        curr = csv_cols[st.session_state['current_csv_idx']]
                        st.session_state['csv_map'][curr] = field
                        if st.session_state['current_csv_idx'] < len(csv_cols) - 1: st.session_state['current_csv_idx'] += 1
                        st.rerun()

            st.divider()
            if st.button("🚀 INICIAR IMPORTAÇÃO", type="primary"):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                orig_name = st.session_state.get('uploaded_file_name', 'importacao')
                safe_name = f"{os.path.splitext(orig_name)[0]}_{timestamp}.csv"
                path_orig = os.path.join(BASE_DIR_IMPORTS, safe_name)
                df.to_csv(path_orig, index=False, sep=';')
                
                conn = get_conn()
                import_id = None
                if conn:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO pf_historico_importacoes (nome_arquivo, caminho_arquivo_original) VALUES (%s, %s) RETURNING id", (orig_name, path_orig))
                    import_id = cur.fetchone()[0]
                    conn.commit()
                
                final_map = {k: v for k, v in st.session_state['csv_map'].items() if v and v != "IGNORAR"}
                qtd_novos = 0; qtd_atualizados = 0; qtd_erros = 0; erros_list = []
                
                if not final_map: st.error("Mapeie pelo menos uma coluna."); st.stop()
                if conn:
                    cur = conn.cursor()
                    progress = st.progress(0)
                    for idx, row in df.iterrows():
                        try:
                            dados_row = {}
                            for csv_c, db_c in final_map.items():
                                val = row[csv_c]
                                if db_c in ['data_nascimento', 'data_exp_rg', 'data_criacao', 'data_atualizacao']:
                                    conv_val = converter_data_br_iso(val)
                                    if conv_val: val = conv_val
                                if pd.isna(val): val = None
                                dados_row[db_c] = val
                            
                            if 'cpf' in dados_row: dados_row['cpf'] = limpar_normalizar_cpf(dados_row['cpf'])
                            pk_field = 'cpf' if 'cpf' in dados_row else ('matricula' if 'matricula' in dados_row else None)
                            
                            if pk_field and dados_row.get(pk_field):
                                set_clause = ", ".join([f"{k}=%s" for k in dados_row.keys()])
                                values = list(dados_row.values()) + [dados_row[pk_field]]
                                cur.execute(f"UPDATE {table_name} SET {set_clause}, importacao_id={import_id} WHERE {pk_field}=%s", values)
                                if cur.rowcount > 0: qtd_atualizados += 1
                                else:
                                    cols = list(dados_row.keys())
                                    vals = list(dados_row.values())
                                    placeholders = ", ".join(["%s"] * len(vals))
                                    cur.execute(f"INSERT INTO {table_name} ({', '.join(cols)}, importacao_id) VALUES ({placeholders}, {import_id})", vals)
                                    qtd_novos += 1
                            else:
                                cols = list(dados_row.keys())
                                vals = list(dados_row.values())
                                placeholders = ", ".join(["%s"] * len(vals))
                                cur.execute(f"INSERT INTO {table_name} ({', '.join(cols)}, importacao_id) VALUES ({placeholders}, {import_id})", vals)
                                qtd_novos += 1
                        except Exception as e:
                            qtd_erros += 1; erros_list.append(f"Linha {idx+2}: {str(e)}")
                        if idx % 10 == 0: progress.progress((idx+1)/len(df))
                    conn.commit()
                    path_erro = None
                    if erros_list:
                        name_erro = f"{os.path.splitext(orig_name)[0]}_{timestamp}_ERRO.txt"
                        path_erro = os.path.join(BASE_DIR_IMPORTS, name_erro)
                        with open(path_erro, "w", encoding="utf-8") as f: f.write("\n".join(erros_list))
                    
                    cur.execute("UPDATE pf_historico_importacoes SET qtd_novos=%s, qtd_atualizados=%s, qtd_erros=%s, caminho_arquivo_erro=%s WHERE id=%s", (qtd_novos, qtd_atualizados, qtd_erros, path_erro, import_id))
                    conn.commit(); cur.close(); conn.close()
                    st.session_state['import_stats'] = {'novos': qtd_novos, 'atualizados': qtd_atualizados, 'erros': qtd_erros, 'path_erro': path_erro, 'sample': df.head(5)}
                    st.session_state['import_step'] = 3; st.rerun()

        elif st.session_state['import_step'] == 3:
            st.markdown("### ✅ Etapa 3: Resultado da Importação")
            stats = st.session_state.get('import_stats', {})
            c1, c2, c3 = st.columns(3)
            c1.metric("Novos", stats.get('novos', 0)); c2.metric("Atualizados", stats.get('atualizados', 0)); c3.metric("Erros", stats.get('erros', 0), delta_color="inverse")
            st.markdown("#### Amostra"); st.dataframe(stats.get('sample', pd.DataFrame()))
            if stats.get('erros', 0) > 0 and stats.get('path_erro'):
                with open(stats['path_erro'], "rb") as f: st.download_button("⚠️ Baixar Erros (.txt)", f, file_name="erros.txt")
            if st.button("Concluir"): st.session_state['pf_view'] = 'lista'; st.session_state['import_step'] = 1; st.rerun()

    # ==========================
    # 4. MODO LISTA (INICIAL)
    # ==========================
    elif st.session_state['pf_view'] == 'lista':
        # Reinicia contadores ao voltar para a lista
        for k in ['count_tel', 'count_email', 'count_end', 'count_emp', 'count_ctr']:
            st.session_state[k] = 1

        filtro_imp = st.session_state.get('filtro_importacao_id')
        c1, c2 = st.columns([2, 2])
        with c2: 
            label_busca = "🔎 Pesquisar Rápida" + (" (Filtrado)" if filtro_imp else "")
            busca = st.text_input(label_busca, key="pf_busca")
        if filtro_imp and st.button("❌ Limpar Filtro"): st.session_state['filtro_importacao_id'] = None; st.rerun()
            
        # BOTÕES UNIFICADOS NO TOPO DA LISTA (Layout solicitado)
        col_b1, col_b2, col_b3 = st.columns([1, 1, 1])
        if col_b1.button("➕ Novo", type="primary", use_container_width=True): st.session_state.update({'pf_view': 'novo'}); st.rerun()
        if col_b2.button("🔍 Pesquisa Ampla", type="primary", use_container_width=True): st.session_state.update({'pf_view': 'pesquisa_ampla'}); st.rerun()
        if col_b3.button("📥 Importar", type="primary", use_container_width=True): st.session_state.update({'pf_view': 'importacao', 'import_step': 1}); st.rerun()

        if busca or filtro_imp:
            df_lista = buscar_pf_simples(busca, filtro_imp)
            if not df_lista.empty:
                df_lista.insert(0, "Selecionar", False)
                # Configuração de colunas: CPF Largo, Resto Compacto
                cfg_cols = {
                    "Selecionar": st.column_config.CheckboxColumn(required=True, width="small"),
                    "cpf": st.column_config.TextColumn("CPF", width="large"),
                    "nome": st.column_config.TextColumn("Nome", width="medium"),
                    "data_nascimento": st.column_config.DateColumn("Nascimento", format="DD/MM/YYYY", width="small"),
                    "id": st.column_config.NumberColumn("ID", width="small")
                }
                edited_df = st.data_editor(df_lista, column_config=cfg_cols, disabled=df_lista.columns.drop("Selecionar"), hide_index=True, use_container_width=True)
                
                subset = edited_df[edited_df["Selecionar"] == True]
                if not subset.empty:
                    st.divider()
                    registro = subset.iloc[0]
                    c1, c2 = st.columns(2)
                    if c1.button("✏️ Editar", use_container_width=True): st.session_state.update({'pf_view': 'editar', 'pf_cpf_selecionado': registro['cpf']}); st.rerun()
                    if c2.button("🗑️ Excluir", use_container_width=True): dialog_excluir_pf(registro['cpf'], registro['nome'])
            else: st.warning("Sem resultados.")
        else: st.info("Use a pesquisa para ver cadastros.")

    # ==========================
    # 5. MODO NOVO / EDITAR
    # ==========================
    elif st.session_state['pf_view'] in ['novo', 'editar']:
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        modo = st.session_state['pf_view']
        cpf_atual = st.session_state['pf_cpf_selecionado']
        dados_db = carregar_dados_completos(cpf_atual) if modo == 'editar' and cpf_atual else {}
        geral = dados_db.get('geral')
        
        # Lógica de Pré-carregamento dos campos dinâmicos na edição
        if modo == 'editar' and cpf_atual and f"edit_ready_{cpf_atual}" not in st.session_state:
            df_t = dados_db.get('telefones')
            if df_t is not None and not df_t.empty:
                st.session_state['count_tel'] = len(df_t)
                for idx, row in df_t.iterrows():
                    st.session_state[f"new_tel_n_{idx}"] = row['numero']
                    st.session_state[f"new_tel_t_{idx}"] = row['tag_whats']
            
            df_m = dados_db.get('emails')
            if df_m is not None and not df_m.empty:
                st.session_state['count_email'] = len(df_m)
                for idx, row in df_m.iterrows():
                    st.session_state[f"new_email_{idx}"] = row['email']

            df_e = dados_db.get('enderecos')
            if df_e is not None and not df_e.empty:
                st.session_state['count_end'] = len(df_e)
                for idx, row in df_e.iterrows():
                    st.session_state[f"end_rua_{idx}"] = row['rua']
                    st.session_state[f"end_bairro_{idx}"] = row['bairro']
                    st.session_state[f"end_cid_{idx}"] = row['cidade']
                    st.session_state[f"end_uf_{idx}"] = row['uf']
                    st.session_state[f"end_cep_{idx}"] = row['cep']

            df_em = dados_db.get('empregos')
            if df_em is not None and not df_em.empty:
                st.session_state['count_emp'] = len(df_em)
                for idx, row in df_em.iterrows():
                    st.session_state[f"emp_conv_{idx}"] = row['convenio']
                    st.session_state[f"emp_matr_{idx}"] = row['matricula']
                    st.session_state[f"emp_ext_{idx}"] = row['dados_extras']

            df_c = dados_db.get('contratos')
            if df_c is not None and not df_c.empty:
                st.session_state['count_ctr'] = len(df_c)
                for idx, row in df_c.iterrows():
                    st.session_state[f"ctr_mref_{idx}"] = row['matricula_ref']
                    st.session_state[f"ctr_num_{idx}"] = row['contrato']

            st.session_state[f"edit_ready_{cpf_atual}"] = True
            st.rerun()

        st.markdown(f"### {geral['nome'] if geral is not None else 'Novo Cadastro'}")
        
        with st.form("form_pf", enter_to_submit=False):
            t1, t2, t3, t4, t5, t6 = st.tabs(["👤 Dados Pessoais", "📞 Telefones", "📧 Emails", "🏠 Endereços", "💼 Emprego/Renda", "📄 Contratos"])
            
            with t1:
                c1, c2, c3 = st.columns(3)
                nome = st.text_input("Nome *", value=geral['nome'] if geral else "").upper()
                cpf = st.text_input("CPF *", value=geral['cpf'] if geral else "")
                val_nasc = None
                if geral is not None and geral['data_nascimento']:
                    try: val_nasc = pd.to_datetime(geral['data_nascimento']).date()
                    except: pass
                nasc = c3.date_input("Nascimento", value=val_nasc, format="DD/MM/YYYY")
                rg = st.text_input("RG", value=geral['rg'] if geral is not None else "").upper()

            collected_tels = []
            collected_emails = []
            collected_ends = []
            collected_emps = []
            collected_ctrs = []

            with t2:
                for i in range(st.session_state['count_tel']):
                    c_num, c_tag, c_vazio = st.columns([2, 1, 4])
                    val_num = c_num.text_input(f"Telefone {i+1} (DDD+9 digitos)", key=f"new_tel_n_{i}", max_chars=11)
                    val_tag = c_tag.selectbox(f"WhatsApp? {i+1}", ["Sim", "Não"], key=f"new_tel_t_{i}")
                    if val_num: collected_tels.append({"numero": val_num, "tag_whats": val_tag})
                if st.form_submit_button("➕ Adicionar Telefone"): st.session_state['count_tel'] += 1; st.rerun()

            with t3:
                for i in range(st.session_state['count_email']):
                    ce1, ce2 = st.columns([4, 2])
                    val_email = ce1.text_input(f"E-mail {i+1}", key=f"new_email_{i}")
                    if val_email: collected_emails.append({"email": val_email})
                if st.form_submit_button("➕ Adicionar E-mail"): st.session_state['count_email'] += 1; st.rerun()

            with t4:
                for i in range(st.session_state['count_end']):
                    st.markdown(f"**Endereço {i+1}**")
                    c_rua, c_bairro = st.columns(2)
                    c_cid, c_uf, c_cep = st.columns([3, 1, 1.5])
                    rua = c_rua.text_input("Rua", key=f"end_rua_{i}")
                    bairro = c_bairro.text_input("Bairro", key=f"end_bairro_{i}")
                    cidade = c_cid.text_input("Cidade", key=f"end_cid_{i}")
                    uf = c_uf.text_input("UF", key=f"end_uf_{i}", max_chars=2)
                    cep = c_cep.text_input("CEP", key=f"end_cep_{i}")
                    if rua or cidade: collected_ends.append({"rua": rua, "bairro": bairro, "cidade": cidade, "uf": uf, "cep": cep})
                    st.divider()
                if st.form_submit_button("➕ Adicionar Endereço"): st.session_state['count_end'] += 1; st.rerun()

            with t5:
                lista_conv = buscar_referencias('CONVENIO')
                st.markdown("##### Dados Profissionais")
                for i in range(st.session_state['count_emp']):
                    c_conv, c_matr = st.columns(2)
                    conv = c_conv.selectbox(f"Convênio {i+1}", [""] + lista_conv, key=f"emp_conv_{i}")
                    matr = c_matr.text_input(f"Matrícula {i+1}", key=f"emp_matr_{i}")
                    extras = st.text_area(f"Dados Extras {i+1}", height=60, key=f"emp_ext_{i}")
                    if conv and matr: collected_emps.append({"convenio": conv, "matricula": matr, "dados_extras": extras})
                    st.divider()
                if st.form_submit_button("➕ Adicionar Emprego"): st.session_state['count_emp'] += 1; st.rerun()

            with t6:
                st.markdown("##### Contratos")
                for i in range(st.session_state['count_ctr']):
                    c_mref, c_ctr = st.columns(2)
                    mref = c_mref.text_input(f"Matrícula Referência {i+1}", key=f"ctr_mref_{i}")
                    ctr = c_ctr.text_input(f"Contrato {i+1}", key=f"ctr_num_{i}")
                    if ctr: collected_ctrs.append({"matricula_ref": mref, "contrato": ctr, "dados_extras": ""})
                if st.form_submit_button("➕ Adicionar Contrato"): st.session_state['count_ctr'] += 1; st.rerun()

            st.markdown("---")
            confirmar = st.form_submit_button("💾 Salvar Tudo")

        if confirmar:
            if nome and cpf:
                cpf_limpo_verif = limpar_normalizar_cpf(cpf)
                
                # Regra de Duplicidade
                if modo == 'novo':
                    nome_existente = verificar_cpf_existente(cpf_limpo_verif)
                    if nome_existente:
                        st.error(f"⚠️ CPF já cadastrado: {nome_existente}")
                        if st.button("🔄 Editar existente?"):
                            st.session_state['pf_view'] = 'editar'; st.session_state['pf_cpf_selecionado'] = cpf_limpo_verif; st.rerun()
                        st.stop() 

                dg = {"cpf": cpf, "nome": nome, "data_nascimento": nasc, "rg": rg}
                
                # Conversão das Listas Coletadas para DataFrames (formato esperado pelo salvar_pf)
                df_final_tel = pd.DataFrame(collected_tels) if collected_tels else pd.DataFrame()
                df_final_email = pd.DataFrame(collected_emails) if collected_emails else pd.DataFrame()
                df_final_end = pd.DataFrame(collected_ends) if collected_ends else pd.DataFrame()
                df_final_emp = pd.DataFrame(collected_emps) if collected_emps else pd.DataFrame()
                df_final_ctr = pd.DataFrame(collected_ctrs) if collected_ctrs else pd.DataFrame()

                ok, msg = salvar_pf(dg, df_final_tel, df_final_email, df_final_end, df_final_emp, df_final_ctr, modo, cpf_atual)
                if ok: 
                    st.success(msg)
                    st.session_state['pf_view'] = 'lista'
                    time.sleep(1); st.rerun()
                else: st.error(msg)
            else: st.warning("Nome e CPF obrigatórios.")
    
    st.caption("Atualizado em: 23/12/2025 17:15")

if __name__ == "__main__":
    app_pessoa_fisica()