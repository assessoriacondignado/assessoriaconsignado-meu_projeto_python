import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date, timedelta
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
                    caminho_arquivo_erro TEXT,
                    usuario_responsavel VARCHAR(100)
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
    """Remove não-numeros e remove zeros a esquerda"""
    if not cpf_raw: return ""
    apenas_nums = re.sub(r'\D', '', str(cpf_raw))
    if not apenas_nums: return ""
    return apenas_nums.lstrip('0')

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
    valor_str = valor_str.split(' ')[0] # Remove horas
    formatos = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"]
    for fmt in formatos:
        try: return datetime.strptime(valor_str, fmt).strftime("%Y-%m-%d")
        except ValueError: continue
    return None

# --- IMPORTAÇÃO RÁPIDA (BULK OTIMIZADO) ---
def processar_importacao_lote(conn, df, table_name, mapping, import_id):
    cur = conn.cursor()
    try:
        df_proc = df.rename(columns=mapping)
        cols_db = list(mapping.values())
        df_proc = df_proc[cols_db].copy()
        
        df_proc['importacao_id'] = import_id

        if 'cpf' in df_proc.columns:
            df_proc['cpf'] = df_proc['cpf'].astype(str).apply(limpar_normalizar_cpf)
        
        cols_data = ['data_nascimento', 'data_exp_rg', 'data_criacao', 'data_atualizacao']
        for col in cols_data:
            if col in df_proc.columns:
                df_proc[col] = df_proc[col].apply(converter_data_br_iso)

        erros = []
        if table_name == 'pf_dados':
            invalidos = df_proc[df_proc['cpf'] == ""]
            if not invalidos.empty:
                for idx, row in invalidos.iterrows():
                    erros.append(f"Linha {idx}: CPF inválido ou vazio.")
            df_proc = df_proc[df_proc['cpf'] != ""]

        staging_table = f"staging_import_{import_id}"
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")
        
        output = io.StringIO()
        df_proc.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        
        cols_order = list(df_proc.columns)
        cur.copy_expert(f"COPY {staging_table} ({', '.join(cols_order)}) FROM STDIN WITH CSV DELIMITER E'\t' NULL '\\N'", output)
        
        pk_field = 'cpf' if 'cpf' in df_proc.columns else ('matricula' if 'matricula' in df_proc.columns else None)
        qtd_novos = 0
        qtd_atualizados = 0
        
        if pk_field:
            sql_update = f"UPDATE {table_name} t SET {', '.join([f'{c} = s.{c}' for c in cols_order if c != pk_field])} FROM {staging_table} s WHERE t.{pk_field} = s.{pk_field}"
            cur.execute(sql_update)
            qtd_atualizados = cur.rowcount
            
            sql_insert = f"INSERT INTO {table_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s WHERE NOT EXISTS (SELECT 1 FROM {table_name} t WHERE t.{pk_field} = s.{pk_field})"
            cur.execute(sql_insert)
            qtd_novos = cur.rowcount
        else:
            sql_insert = f"INSERT INTO {table_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table}"
            cur.execute(sql_insert)
            qtd_novos = cur.rowcount

        return qtd_novos, qtd_atualizados, erros
        
    except Exception as e:
        raise e

# --- FUNÇÕES DE BUSCA (CORRIGIDA) ---
def buscar_pf_simples(termo, filtro_importacao_id=None):
    conn = get_conn()
    if conn:
        try:
            # Limpa o termo: remove tudo que não é dígito e zeros à esquerda
            termo_limpo = re.sub(r'\D', '', termo).lstrip('0')
            param_nome = f"%{termo}%"
            
            sql_base = "SELECT d.id, d.nome, d.cpf, d.data_nascimento FROM pf_dados d LEFT JOIN pf_telefones t ON d.cpf = t.cpf_ref"
            
            conditions = []
            params = []

            # Filtro de Importação
            if filtro_importacao_id:
                conditions.append("d.importacao_id = %s")
                params.append(filtro_importacao_id)
            
            # Filtro de Texto (Nome, CPF ou Telefone)
            if termo:
                # Se tiver números, busca em CPF/Telefone também
                if termo_limpo:
                    param_num = f"%{termo_limpo}%"
                    sub_cond = ["d.nome ILIKE %s", "d.cpf ILIKE %s", "t.numero ILIKE %s"]
                    sub_params = [param_nome, param_num, param_num]
                    # Adiciona grupo OR (Importante parenteses)
                    conditions.append(f"({' OR '.join(sub_cond)})")
                    params.extend(sub_params)
                else:
                    # Se só tiver letras, busca APENAS no Nome (Evita erro de %% em campos numéricos)
                    conditions.append("d.nome ILIKE %s")
                    params.append(param_nome)
            
            # Monta Query Final
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            query = f"{sql_base} {sql_where} GROUP BY d.id ORDER BY d.nome ASC LIMIT 50"
            
            df = pd.read_sql(query, conn, params=tuple(params))
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
            
            if filtros.get('importacao_id'):
                conditions.append("d.importacao_id = %s")
                params.append(filtros['importacao_id'])

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
                tel_clean = re.sub(r'\D', '', filtros['telefone']).lstrip('0')
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

# --- FUNÇÕES CRUD ---
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
                cur.execute("DELETE FROM pf_telefones WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_emails WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_enderecos WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_contratos WHERE matricula_ref IN (SELECT matricula FROM pf_emprego_renda WHERE cpf_ref = %s)", (cpf_chave,))
                cur.execute("DELETE FROM pf_emprego_renda WHERE cpf_ref = %s", (cpf_chave,))
            
            def df_upper(df): return df.applymap(lambda x: x.upper() if isinstance(x, str) else x)

            if not df_tel.empty:
                for _, row in df_upper(df_tel).iterrows():
                    if row.get('numero'): cur.execute("INSERT INTO pf_telefones (cpf_ref, numero, tag_whats) VALUES (%s, %s, %s)", (cpf_chave, row['numero'], row.get('tag_whats')))
            
            if not df_email.empty:
                for _, row in df_upper(df_email).iterrows():
                    if row.get('email'): cur.execute("INSERT INTO pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, row['email']))
            
            if not df_end.empty:
                for _, row in df_upper(df_end).iterrows():
                    if row.get('rua') or row.get('cidade'): cur.execute("INSERT INTO pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, row['rua'], row.get('bairro'), row.get('cidade'), row.get('uf'), row.get('cep')))
            
            if not df_emp.empty:
                for _, row in df_upper(df_emp).iterrows():
                    if row.get('convenio') and row.get('matricula'):
                        try: cur.execute("INSERT INTO pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s)", (cpf_chave, row.get('convenio'), row.get('matricula'), row.get('dados_extras')))
                        except: pass

            if not df_contr.empty:
                for _, row in df_upper(df_contr).iterrows():
                    if row.get('matricula_ref'):
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

# --- DIALOG: VISUALIZAR CLIENTE ---
@st.dialog("👁️ Detalhes do Cliente")
def dialog_visualizar_cliente(cpf_cliente):
    dados = carregar_dados_completos(cpf_cliente)
    g = dados.get('geral')
    
    if g is None:
        st.error("Cliente não encontrado.")
        return

    with st.expander("👤 Dados Cadastrais", expanded=True):
        c1, c2 = st.columns(2)
        c1.write(f"**Nome:** {g['nome']}")
        c2.write(f"**CPF:** {g['cpf']}")
        c3, c4 = st.columns(2)
        dt_nasc = pd.to_datetime(g['data_nascimento']).strftime('%d/%m/%Y') if g['data_nascimento'] else "-"
        c3.write(f"**Nascimento:** {dt_nasc}")
        c4.write(f"**RG:** {g['rg']}")
    
    with st.expander("📞 Telefones"):
        df_tel = dados.get('telefones')
        if not df_tel.empty:
            for _, row in df_tel.iterrows():
                st.write(f"📱 {row['numero']} (WhatsApp: {row['tag_whats']})")
        else: st.info("Sem telefones cadastrados.")

    with st.expander("📧 E-mails"):
        df_email = dados.get('emails')
        if not df_email.empty:
            for _, row in df_email.iterrows():
                st.write(f"✉️ {row['email']}")
        else: st.info("Sem e-mails cadastrados.")

    with st.expander("🏠 Endereços"):
        df_end = dados.get('enderecos')
        if not df_end.empty:
            for _, row in df_end.iterrows():
                st.write(f"📍 {row['rua']}, {row['bairro']} - {row['cidade']}/{row['uf']} (CEP: {row['cep']})")
        else: st.info("Sem endereços cadastrados.")

    with st.expander("💼 Emprego, Renda e Contratos"):
        df_emp = dados.get('empregos')
        df_contr = dados.get('contratos')
        if not df_emp.empty:
            for _, row in df_emp.iterrows():
                st.markdown(f"**Convênio:** {row['convenio']} | **Matrícula:** {row['matricula']}")
                if row['dados_extras']: st.caption(f"Extras: {row['dados_extras']}")
                contratos_vinculados = df_contr[df_contr['matricula_ref'] == row['matricula']]
                if not contratos_vinculados.empty:
                    st.write("📄 *Contratos:*")
                    for _, c in contratos_vinculados.iterrows():
                        st.write(f"- {c['contrato']}")
                st.divider()
        else: st.info("Sem dados profissionais.")

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
    
    for k in ['count_tel', 'count_email', 'count_end', 'count_emp', 'count_ctr']:
        if k not in st.session_state: st.session_state[k] = 1

    # ==========================
    # 1. PESQUISA AMPLA
    # ==========================
    if st.session_state['pf_view'] == 'pesquisa_ampla':
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        st.markdown("### 🔎 Pesquisa Ampla")
        with st.form("form_pesquisa_ampla", enter_to_submit=False):
            t1, t2, t3, t4, t5, t6 = st.tabs(["Identificação", "Endereço", "Contatos", "Profissional", "Contratos", "Origem"])
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
            with t6:
                conn = get_conn()
                opcoes_imp = {}
                if conn:
                    try:
                        df_imp = pd.read_sql("SELECT id, nome_arquivo, data_importacao FROM pf_historico_importacoes ORDER BY id DESC", conn)
                        for _, row in df_imp.iterrows():
                             dt = row['data_importacao'].strftime('%d/%m/%Y %H:%M')
                             label = f"{dt} - {row['nome_arquivo']} (ID: {row['id']})"
                             opcoes_imp[label] = row['id']
                    except: pass
                    conn.close()
                sel_imp = st.selectbox("Filtrar por Importação", [""] + list(opcoes_imp.keys()))
                if sel_imp: filtros['importacao_id'] = opcoes_imp[sel_imp]

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
                # CONFIGURAÇÃO DE COLUNAS (RG e DATA ocultos)
                cfg_cols = {
                    "Selecionar": st.column_config.CheckboxColumn(required=True, width="small"),
                    "cpf": st.column_config.TextColumn("CPF", width="large"),
                    "nome": st.column_config.TextColumn("Nome", width="medium"),
                    "id": st.column_config.NumberColumn("Código", width="small"),
                    "rg": None,
                    "data_nascimento": None
                }
                edited_df = st.data_editor(df_res, column_config=cfg_cols, disabled=df_res.columns.drop("Selecionar"), hide_index=True, use_container_width=True)
                subset = edited_df[edited_df["Selecionar"] == True]
                if not subset.empty:
                    st.divider()
                    registro = subset.iloc[0]
                    c1, c2, c3 = st.columns(3)
                    if c1.button("👁️ Ver", use_container_width=True): dialog_visualizar_cliente(registro['cpf'])
                    if c2.button("✏️ Editar", use_container_width=True): st.session_state.update({'pf_view': 'editar', 'pf_cpf_selecionado': registro['cpf']}); st.rerun()
                    if c3.button("🗑️ Excluir", use_container_width=True): dialog_excluir_pf(registro['cpf'], registro['nome'])
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
    # 3. MODO IMPORTAÇÃO (BULK)
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
            if st.button("🚀 INICIAR IMPORTAÇÃO (BULK)", type="primary"):
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
                
                if not final_map: st.error("Mapeie pelo menos uma coluna."); st.stop()

                if conn:
                    with st.spinner("Processando em lote... Aguarde alguns instantes."):
                        try:
                            novos, atualizados, erros_list = processar_importacao_lote(conn, df, table_name, final_map, import_id)
                            conn.commit()
                            
                            path_erro = None
                            if erros_list:
                                name_erro = f"{os.path.splitext(orig_name)[0]}_{timestamp}_ERRO.txt"
                                path_erro = os.path.join(BASE_DIR_IMPORTS, name_erro)
                                with open(path_erro, "w", encoding="utf-8") as f: f.write("\n".join(erros_list))
                            
                            cur = conn.cursor()
                            cur.execute("UPDATE pf_historico_importacoes SET qtd_novos=%s, qtd_atualizados=%s, qtd_erros=%s, caminho_arquivo_erro=%s WHERE id=%s", (novos, atualizados, len(erros_list), path_erro, import_id))
                            conn.commit(); cur.close(); conn.close()
                            
                            st.session_state['import_stats'] = {'novos': novos, 'atualizados': atualizados, 'erros': len(erros_list), 'path_erro': path_erro, 'sample': df.head(5)}
                            st.session_state['import_step'] = 3; st.rerun()
                        except Exception as e:
                            st.error(f"Erro Crítico na Importação: {e}")
                            if conn: conn.close()

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
    # 5. MODO NOVO / EDITAR (INSERIDO AQUI)
    # ==========================
    elif st.session_state['pf_view'] in ['novo', 'editar']:
        is_edit = st.session_state['pf_view'] == 'editar'
        titulo = f"✏️ Editar Cadastro: {st.session_state['pf_cpf_selecionado']}" if is_edit else "➕ Novo Cadastro"
        
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        st.markdown(f"### {titulo}")

        # Dados Iniciais (Vazio ou Carregado)
        dados_db = {}
        if is_edit:
            dados_db = carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
        
        # Extrair DataFrames ou criar vazios com segurança
        g = dados_db.get('geral')
        if g is None: g = {}
        
        df_tel_db = dados_db.get('telefones', pd.DataFrame(columns=['numero', 'tag_whats', 'tag_qualificacao']))
        df_email_db = dados_db.get('emails', pd.DataFrame(columns=['email']))
        df_end_db = dados_db.get('enderecos', pd.DataFrame(columns=['rua', 'bairro', 'cidade', 'uf', 'cep']))
        df_emp_db = dados_db.get('empregos', pd.DataFrame(columns=['convenio', 'matricula', 'dados_extras']))
        df_contr_db = dados_db.get('contratos', pd.DataFrame(columns=['matricula_ref', 'contrato', 'dados_extras']))

        # FORMULÁRIO PRINCIPAL
        with st.form("form_cadastro_pf"):
            t1, t2, t3, t4 = st.tabs(["👤 Dados Pessoais", "📞 Contatos e Endereço", "💼 Profissional", "📄 Contratos"])
            
            with t1:
                c1, c2, c3 = st.columns(3)
                nome = c1.text_input("Nome Completo *", value=g.get('nome', ''))
                # CPF desabilitado na edição para não quebrar integridade (chave primária/estrangeira)
                cpf = c2.text_input("CPF *", value=g.get('cpf', ''), disabled=is_edit) 
                
                val_nasc = None
                if g.get('data_nascimento'):
                    try: val_nasc = pd.to_datetime(g['data_nascimento']).date()
                    except: val_nasc = None
                d_nasc = c3.date_input("Data Nascimento", value=val_nasc, format="DD/MM/YYYY")
                
                c4, c5, c6 = st.columns(3)
                rg = c4.text_input("RG", value=g.get('rg', ''))
                cnh = c5.text_input("CNH", value=g.get('cnh', ''))
                pis = c6.text_input("PIS", value=g.get('pis', ''))
                
                c7, c8 = st.columns(2)
                nome_mae = c7.text_input("Nome da Mãe", value=g.get('nome_mae', ''))
                nome_pai = c8.text_input("Nome do Pai", value=g.get('nome_pai', ''))

            with t2:
                st.write("📞 **Telefones**")
                # Data Editor permite adicionar/remover linhas dinamicamente
                edt_tel = st.data_editor(df_tel_db, num_rows="dynamic", use_container_width=True, key="ed_tel")
                
                st.divider()
                st.write("📧 **E-mails**")
                edt_email = st.data_editor(df_email_db, num_rows="dynamic", use_container_width=True, key="ed_email")
                
                st.divider()
                st.write("🏠 **Endereços**")
                edt_end = st.data_editor(df_end_db, num_rows="dynamic", use_container_width=True, key="ed_end")

            with t3:
                st.write("💼 **Emprego e Renda**")
                edt_emp = st.data_editor(df_emp_db, num_rows="dynamic", use_container_width=True, key="ed_emp")

            with t4:
                st.write("📄 **Contratos Ativos**")
                st.info("Importante: A 'Matrícula Ref' deve ser igual a uma Matrícula cadastrada na aba Profissional.")
                edt_ctr = st.data_editor(df_contr_db, num_rows="dynamic", use_container_width=True, key="ed_ctr")

            st.markdown("---")
            col_b1, col_b2 = st.columns([1, 5])
            if col_b1.form_submit_button("💾 SALVAR DADOS", type="primary"):
                # Validação Básica
                if not nome or not cpf:
                    st.error("Nome e CPF são obrigatórios.")
                else:
                    # Montar Dicionário Geral
                    dados_gerais = {
                        'cpf': cpf, 'nome': nome, 'data_nascimento': d_nasc,
                        'rg': rg, 'cnh': cnh, 'pis': pis,
                        'nome_mae': nome_mae, 'nome_pai': nome_pai
                    }
                    
                    modo_salvar = "editar" if is_edit else "novo"
                    cpf_orig = st.session_state.get('pf_cpf_selecionado') if is_edit else None
                    
                    sucesso, msg = salvar_pf(dados_gerais, edt_tel, edt_email, edt_end, edt_emp, edt_ctr, modo_salvar, cpf_orig)
                    
                    if sucesso:
                        st.success(msg)
                        time.sleep(1)
                        st.session_state['pf_view'] = 'lista'
                        st.rerun()
                    else:
                        st.error(msg)

    # ==========================
    # 6. MODO LISTA (INICIAL)
    # ==========================
    elif st.session_state['pf_view'] == 'lista':
        # Reinicia contadores
        for k in ['count_tel', 'count_email', 'count_end', 'count_emp', 'count_ctr']:
            st.session_state[k] = 1

        filtro_imp = st.session_state.get('filtro_importacao_id')
        c1, c2 = st.columns([2, 2])
        with c2: 
            label_busca = "🔎 Pesquisar Rápida" + (" (Filtrado)" if filtro_imp else "")
            busca = st.text_input(label_busca, key="pf_busca")
        if filtro_imp and st.button("❌ Limpar Filtro"): st.session_state['filtro_importacao_id'] = None; st.rerun()
            
        col_b1, col_b2, col_b3 = st.columns([1, 1, 1])
        if col_b1.button("➕ Novo", type="primary", use_container_width=True): st.session_state.update({'pf_view': 'novo'}); st.rerun()
        if col_b2.button("🔍 Pesquisa Ampla", type="primary", use_container_width=True): st.session_state.update({'pf_view': 'pesquisa_ampla'}); st.rerun()
        if col_b3.button("📥 Importar", type="primary", use_container_width=True): st.session_state.update({'pf_view': 'importacao', 'import_step': 1}); st.rerun()

        if busca or filtro_imp:
            df_lista = buscar_pf_simples(busca, filtro_imp)
            if not df_lista.empty:
                df_lista.insert(0, "Selecionar", False)
                # Configuração de colunas: CPF Largo, Resto Compacto. Ocultar RG/Data
                cfg_cols = {
                    "Selecionar": st.column_config.CheckboxColumn(required=True, width="small"),
                    "cpf": st.column_config.TextColumn("CPF", width="large"),
                    "nome": st.column_config.TextColumn("Nome", width="medium"),
                    "id": st.column_config.NumberColumn("Código", width="small"),
                    # OCULTAR COLUNAS (AJUSTE SOLICITADO)
                    "rg": None,
                    "data_nascimento": None
                }
                edited_df = st.data_editor(df_lista, column_config=cfg_cols, disabled=df_lista.columns.drop("Selecionar"), hide_index=True, use_container_width=True)
                subset = edited_df[edited_df["Selecionar"] == True]
                if not subset.empty:
                    st.divider()
                    registro = subset.iloc[0]
                    c1, c2, c3 = st.columns(3)
                    if c1.button("👁️ Ver", use_container_width=True): dialog_visualizar_cliente(registro['cpf'])
                    if c2.button("✏️ Editar", use_container_width=True): st.session_state.update({'pf_view': 'editar', 'pf_cpf_selecionado': registro['cpf']}); st.rerun()
                    if c3.button("🗑️ Excluir", use_container_width=True): dialog_excluir_pf(registro['cpf'], registro['nome'])
            else: st.warning("Sem resultados.")
        else: st.info("Use a pesquisa para ver cadastros.")
    
    # RODAPÉ - Fuso Horário Brasília (GMT-3)
    br_time = datetime.now() - timedelta(hours=3)
    st.caption(f"Atualizado  em: {br_time.strftime('%d/%m/%Y %H:%M')}")

if __name__ == "__main__":
    app_pessoa_fisica()