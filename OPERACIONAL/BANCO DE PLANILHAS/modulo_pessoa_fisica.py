import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date
import io
import time
import math
import re

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

# --- FUNÇÕES AUXILIARES GERAIS ---
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
        except: 
            conn.close()
    return []

def limpar_normalizar_cpf(cpf_raw):
    """Remove não-numeros e garante 11 dígitos com zeros a esquerda"""
    if not cpf_raw: return ""
    apenas_nums = re.sub(r'\D', '', str(cpf_raw))
    if not apenas_nums: return ""
    return apenas_nums.zfill(11)

def verificar_cpf_existente(cpf_normalizado):
    """Verifica se o CPF já existe e retorna o Nome do titular se encontrar"""
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome FROM pf_dados WHERE cpf = %s", (cpf_normalizado,))
            res = cur.fetchone()
            conn.close()
            return res[0] if res else None
        except:
            conn.close()
    return None

# --- FUNÇÕES DE BUSCA (SIMPLES E AMPLA) ---
def buscar_pf_simples(termo):
    conn = get_conn()
    if conn:
        try:
            # Tenta normalizar o termo caso seja um CPF digitado parcialmente
            termo_limpo = re.sub(r'\D', '', termo)
            param = f"%{termo}%"
            
            query = """
                SELECT d.id, d.nome, d.cpf, d.data_nascimento 
                FROM pf_dados d
                LEFT JOIN pf_telefones t ON d.cpf = t.cpf_ref
                WHERE d.cpf ILIKE %s OR d.nome ILIKE %s OR t.numero ILIKE %s
                GROUP BY d.id
                ORDER BY d.nome ASC
                LIMIT 50
            """
            df = pd.read_sql(query, conn, params=(f"%{termo_limpo}%", param, param))
            conn.close()
            return df
        except:
            conn.close()
    return pd.DataFrame()

def buscar_opcoes_filtro(coluna, tabela):
    """Busca opções únicas para dropdowns da pesquisa ampla"""
    conn = get_conn()
    opcoes = []
    if conn:
        try:
            query = f"SELECT DISTINCT {coluna} FROM {tabela} WHERE {coluna} IS NOT NULL ORDER BY {coluna}"
            cur = conn.cursor()
            cur.execute(query)
            res = cur.fetchall()
            opcoes = [r[0] for r in res if r[0]]
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

            # Identificação
            if filtros.get('nome'):
                conditions.append("d.nome ILIKE %s")
                params.append(f"%{filtros['nome']}%")
            if filtros.get('cpf'):
                cpf_norm = limpar_normalizar_cpf(filtros['cpf'])
                conditions.append("d.cpf = %s") # Busca exata pelo normalizado
                params.append(cpf_norm)
            if filtros.get('rg'):
                conditions.append("d.rg ILIKE %s")
                params.append(f"%{filtros['rg']}%")
            if filtros.get('nascimento'):
                conditions.append("d.data_nascimento = %s")
                params.append(filtros['nascimento'])

            # Endereço
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

            # Contatos
            if filtros.get('ddd'):
                joins.append("JOIN pf_telefones tel ON d.cpf = tel.cpf_ref")
                conditions.append("SUBSTRING(REGEXP_REPLACE(tel.numero, '[^0-9]', '', 'g'), 1, 2) = %s")
                params.append(filtros['ddd'])
            if filtros.get('email'):
                joins.append("JOIN pf_emails em ON d.cpf = em.cpf_ref")
                conditions.append("em.email ILIKE %s")
                params.append(f"%{filtros['email']}%")

            # Profissional
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

            # Montagem
            joins = list(set(joins))
            sql_joins = " ".join(joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            full_sql = f"{sql} {sql_joins} {sql_where} ORDER BY d.nome"
            
            # Contagem
            count_sql = f"SELECT COUNT(DISTINCT d.id) FROM pf_dados d {sql_joins} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total_registros = cur.fetchone()[0]
            
            # Paginação
            offset = (pagina - 1) * itens_por_pagina
            pag_sql = f"{full_sql} LIMIT {itens_por_pagina} OFFSET {offset}"
            
            df = pd.read_sql(pag_sql, conn, params=tuple(params))
            conn.close()
            return df, total_registros
        except Exception as e:
            st.error(f"Erro na pesquisa: {e}")
            return pd.DataFrame(), 0
    return pd.DataFrame(), 0

# --- FUNÇÕES CRUD ---
def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {}
    if conn:
        try:
            # Garante busca pelo normalizado
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
            
            # Normalização de CPF para garantir a regra de 11 dígitos com zeros a esquerda
            cpf_limpo = limpar_normalizar_cpf(dados_gerais['cpf'])
            dados_gerais['cpf'] = cpf_limpo
            
            # Normaliza o cpf original se vier
            if cpf_original:
                cpf_original = limpar_normalizar_cpf(cpf_original)

            # Converte dados gerais para UPPER
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
                # Limpa filhos para recriar
                cur.execute("DELETE FROM pf_telefones WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_emails WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_enderecos WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_emprego_renda WHERE cpf_ref = %s", (cpf_chave,))
            
            # Helper para UPPER em Dataframes
            def df_upper(df):
                return df.applymap(lambda x: x.upper() if isinstance(x, str) else x)

            for _, row in df_upper(df_tel).iterrows():
                if row.get('numero'): cur.execute("INSERT INTO pf_telefones (cpf_ref, numero, data_atualizacao, tag_whats, tag_qualificacao) VALUES (%s, %s, %s, %s, %s)", (cpf_chave, row['numero'], row.get('data_atualizacao'), row.get('tag_whats'), row.get('tag_qualificacao')))
            for _, row in df_upper(df_email).iterrows():
                if row.get('email'): cur.execute("INSERT INTO pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, row['email']))
            for _, row in df_upper(df_end).iterrows():
                if row.get('rua') or row.get('cidade'): cur.execute("INSERT INTO pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, row['rua'], row.get('bairro'), row.get('cidade'), row.get('uf'), row.get('cep')))
            
            if not df_emp.empty:
                for _, row in df_upper(df_emp).iterrows():
                    conv = row.get('convenio')
                    matr = row.get('matricula')
                    if conv:
                        try:
                            cur.execute("INSERT INTO pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s)", (cpf_chave, conv, matr, row.get('dados_extras')))
                        except: pass

            if not df_contr.empty:
                for _, row in df_upper(df_contr).iterrows():
                    matr_ref = row.get('matricula_ref')
                    if matr_ref:
                        cur.execute("SELECT 1 FROM pf_emprego_renda WHERE matricula = %s", (matr_ref,))
                        if cur.fetchone():
                            cur.execute("INSERT INTO pf_contratos (matricula_ref, contrato, dados_extras) VALUES (%s, %s, %s)", (matr_ref, row.get('contrato'), row.get('dados_extras')))

            conn.commit()
            conn.close()
            return True, "Salvo com sucesso!"
        except psycopg2.IntegrityError:
            conn.rollback()
            return False, "Erro: CPF já cadastrado no sistema."
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
        query = f"""
            SELECT d.cpf, d.nome, d.data_nascimento, t.numero as telefone_principal, e.email
            FROM pf_dados d
            LEFT JOIN pf_telefones t ON d.cpf = t.cpf_ref
            LEFT JOIN pf_emails e ON d.cpf = e.cpf_ref
            WHERE d.cpf IN ({placeholders})
        """
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
            conn.commit()
            conn.close()
            return True
        except: return False
    return False

# --- INTERFACE ---
@st.dialog("⚠️ Excluir Cadastro")
def dialog_excluir_pf(cpf, nome):
    st.error(f"Apagar **{nome}**?")
    if st.button("Confirmar", type="primary"):
        if excluir_pf(cpf): st.success("Apagado!"); st.rerun()

@st.dialog("🖨️ Imprimir Dados")
def dialog_imprimir(dados):
    d = dados['geral']
    st.markdown(f"### Ficha Cadastral: {d['nome']}")
    st.markdown("---")
    st.write(f"**CPF:** {d['cpf']} | **RG:** {d['rg']}")
    st.button("Fechar")

def app_pessoa_fisica():
    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    # Inicialização de contadores para campos dinâmicos
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'pf_cpf_selecionado' not in st.session_state: st.session_state['pf_cpf_selecionado'] = None
    if 'import_step' not in st.session_state: st.session_state['import_step'] = 1
    if 'pesquisa_pag' not in st.session_state: st.session_state['pesquisa_pag'] = 1
    
    # Contadores para campos múltiplos
    if 'count_tel' not in st.session_state: st.session_state['count_tel'] = 1
    if 'count_email' not in st.session_state: st.session_state['count_email'] = 1
    if 'count_end' not in st.session_state: st.session_state['count_end'] = 1
    if 'count_emp' not in st.session_state: st.session_state['count_emp'] = 1
    if 'count_ctr' not in st.session_state: st.session_state['count_ctr'] = 1

    # ==========================
    # 1. MODO PESQUISA AMPLA
    # ==========================
    if st.session_state['pf_view'] == 'pesquisa_ampla':
        st.button("⬅️ Voltar para Lista", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        st.markdown("### 🔎 Pesquisa Ampla e Avançada")
        
        with st.form("form_pesquisa_ampla"):
            t1, t2, t3, t4, t5 = st.tabs(["Identificação", "Endereço", "Contatos", "Profissional", "Contratos"])
            filtros = {}
            
            with t1:
                c1, c2, c3, c4 = st.columns(4)
                filtros['nome'] = c1.text_input("Nome")
                filtros['cpf'] = c2.text_input("CPF")
                filtros['rg'] = c3.text_input("RG")
                filtros['nascimento'] = c4.date_input("Nascimento", value=None)
            
            with t2:
                c_uf, c_cid, c_bai, c_rua = st.columns(4)
                lista_ufs = buscar_opcoes_filtro('uf', 'pf_enderecos')
                sel_uf = c_uf.selectbox("UF", [""] + lista_ufs)
                if sel_uf: filtros['uf'] = sel_uf
                filtros['cidade'] = c_cid.text_input("Cidade")
                filtros['bairro'] = c_bai.text_input("Bairro")
                filtros['rua'] = c_rua.text_input("Rua")
            
            with t3:
                c_ddd, c_email = st.columns(2)
                filtros['ddd'] = c_ddd.text_input("DDD (2 dígitos)", max_chars=2)
                filtros['email'] = c_email.text_input("E-mail")
            
            with t4:
                c_conv, c_matr = st.columns(2)
                lista_conv = buscar_referencias('CONVENIO')
                sel_conv = c_conv.selectbox("Convênio", [""] + lista_conv)
                if sel_conv: filtros['convenio'] = sel_conv
                filtros['matricula'] = c_matr.text_input("Matrícula")
                
            with t5:
                filtros['contrato'] = st.text_input("Número do Contrato")
            
            btn_pesquisar = st.form_submit_button("🔎 Executar Pesquisa")
        
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
                st.dataframe(df_res, use_container_width=True, hide_index=True)
                
                total_pags = math.ceil(total / 30)
                col_p1, col_p2, col_p3 = st.columns([1, 2, 1])
                with col_p1:
                    if pag_atual > 1 and st.button("⬅️ Anterior"):
                        st.session_state['pesquisa_pag'] -= 1; st.rerun()
                with col_p2:
                    st.markdown(f"<div style='text-align:center'>Página {pag_atual} de {total_pags}</div>", unsafe_allow_html=True)
                with col_p3:
                    if pag_atual < total_pags and st.button("Próxima ➡️"):
                        st.session_state['pesquisa_pag'] += 1; st.rerun()
                
                col_sel = st.selectbox("Selecionar Cadastro para ver Detalhes", df_res['nome'].tolist())
                if st.button("Ver Cadastro Selecionado"):
                    cpf_sel = df_res[df_res['nome'] == col_sel].iloc[0]['cpf']
                    st.session_state['pf_view'] = 'editar'; st.session_state['pf_cpf_selecionado'] = cpf_sel; st.rerun()
            else: st.warning("Nenhum registro encontrado.")

    # ==========================
    # 2. MODO IMPORTAÇÃO
    # ==========================
    elif st.session_state['pf_view'] == 'importacao':
        st.button("⬅️ Cancelar Importação", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'import_step': 1}))
        st.divider()
        
        mapa_tabelas = {"Dados Cadastrais": "pf_dados", "Telefones": "pf_telefones", "Emails": "pf_emails", "Endereços": "pf_enderecos", "Emprego/Renda": "pf_emprego_renda", "Contratos": "pf_contratos"}

        if st.session_state['import_step'] == 1:
            st.markdown("### 📤 Etapa 1: Upload")
            sel_amigavel = st.selectbox("Selecione a Tabela de Destino", list(mapa_tabelas.keys()))
            st.session_state['import_table'] = mapa_tabelas[sel_amigavel]
            uploaded_file = st.file_uploader("Arquivo CSV", type=['csv'])
            
            if uploaded_file:
                try:
                    df = pd.read_csv(uploaded_file)
                    st.session_state['import_df'] = df
                    st.dataframe(df.head(10))
                    if st.button("Ir para Mapeamento", type="primary"):
                        st.session_state['import_step'] = 2; st.rerun()
                except Exception as e: st.error(f"Erro: {e}")

        elif st.session_state['import_step'] == 2:
            st.markdown("### 🔗 Etapa 2: Mapeamento")
            df = st.session_state['import_df']
            table_name = st.session_state['import_table']
            db_cols_info = get_table_columns(table_name)
            db_col_names = [c[0] for c in db_cols_info if c[0] not in ['id', 'data_criacao', 'data_atualizacao']]
            
            if table_name in ['pf_emprego_renda', 'pf_contratos']:
                with st.expander("✨ Criar Nova Coluna"):
                    new_col = st.text_input("Nome Coluna")
                    if st.button("Criar") and new_col:
                        clean = re.sub(r'[^a-zA-Z0-9_]', '', new_col).lower()
                        if add_column_to_table(table_name, clean, "VARCHAR(255)"): st.success("Criada!"); time.sleep(1); st.rerun()
            
            mapping = {}
            cols_csv = ["(Ignorar)"] + list(df.columns)
            for db_col in db_col_names:
                idx = 0
                for i, c in enumerate(cols_csv):
                    if c.lower() == db_col.lower(): idx = i
                mapping[db_col] = st.selectbox(f"Campo Banco: {db_col}", cols_csv, index=idx, key=f"map_{db_col}")
            
            if st.button("Finalizar Importação", type="primary"):
                conn = get_conn()
                if conn:
                    cur = conn.cursor()
                    valid_map = {k: v for k, v in mapping.items() if v != "(Ignorar)"}
                    for i, row in df.iterrows():
                        try:
                            cols = list(valid_map.keys())
                            vals = [row[valid_map[c]] for c in cols]
                            vals = [None if pd.isna(v) else v for v in vals]
                            placeholders = ", ".join(["%s"] * len(vals))
                            cur.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})", vals)
                        except: pass
                    conn.commit(); conn.close()
                    st.success("Concluído!"); st.session_state['pf_view'] = 'lista'; st.rerun()

    # ==========================
    # 3. MODO LISTA (INICIAL)
    # ==========================
    elif st.session_state['pf_view'] == 'lista':
        # Reset counters when returning to list
        st.session_state['count_tel'] = 1
        st.session_state['count_email'] = 1
        st.session_state['count_end'] = 1
        st.session_state['count_emp'] = 1
        st.session_state['count_ctr'] = 1

        c1, c2 = st.columns([2, 2])
        with c2: busca = st.text_input("🔎 Pesquisar Rápida (CPF / Nome / Telefone)", key="pf_busca")
        
        c_btn1, c_btn2, c_btn3 = st.columns([1, 1.5, 3.5])
        if c_btn1.button("➕ Novo", type="primary"):
            st.session_state['pf_view'] = 'novo'; st.session_state['pf_cpf_selecionado'] = None; st.rerun()
        if c_btn2.button("🔍 Pesquisa Ampla"):
            st.session_state['pf_view'] = 'pesquisa_ampla'; st.rerun()
        if c_btn3.button("📥 Importar"):
            st.session_state['pf_view'] = 'importacao'; st.session_state['import_step'] = 1; st.rerun()

        if busca:
            df_lista = buscar_pf_simples(busca)
            if not df_lista.empty:
                df_lista.insert(0, "Sel", False)
                edited_df = st.data_editor(df_lista, column_config={"Sel": st.column_config.CheckboxColumn(required=True)}, disabled=["id", "nome", "cpf"], hide_index=True, use_container_width=True)
                sel = edited_df[edited_df["Sel"]]["cpf"].tolist()
                if sel and st.button(f"📥 Exportar ({len(sel)})"):
                    csv = exportar_dados(sel).to_csv(index=False).encode('utf-8')
                    st.download_button("Baixar CSV", csv, "export_pf.csv", "text/csv")
                
                for i, row in df_lista.iterrows():
                    with st.expander(f"👤 {row['nome']} ({row['cpf']})"):
                        c1, c2 = st.columns(2)
                        if c1.button("✏️ Editar/Ver", key=f"e_{row['id']}"):
                            st.session_state['pf_view'] = 'editar'; st.session_state['pf_cpf_selecionado'] = row['cpf']; st.rerun()
                        if c2.button("🗑️ Excluir", key=f"d_{row['id']}"): dialog_excluir_pf(row['cpf'], row['nome'])
            else: st.warning("Sem resultados.")
        else: st.info("Use a pesquisa para ver os cadastros.")

    # ==========================
    # 4. MODO NOVO / EDITAR
    # ==========================
    elif st.session_state['pf_view'] in ['novo', 'editar']:
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        
        modo = st.session_state['pf_view']
        cpf_atual = st.session_state['pf_cpf_selecionado']
        dados_db = carregar_dados_completos(cpf_atual) if modo == 'editar' and cpf_atual else {}
        geral = dados_db.get('geral')
        
        st.markdown(f"### {geral['nome'] if geral is not None else 'Novo Cadastro'}")

        with st.form("form_pf", enter_to_submit=False):
            t1, t2, t3, t4, t5, t6 = st.tabs(["👤 Dados Pessoais", "📞 Telefones", "📧 Emails", "🏠 Endereços", "💼 Emprego/Renda", "📄 Contratos"])
            
            with t1:
                c1, c2, c3 = st.columns(3)
                nome = st.text_input("Nome *", value=geral['nome'] if geral is not None else "").upper()
                cpf = st.text_input("CPF *", value=geral['cpf'] if geral is not None else "")
                
                min_data = date(1900, 1, 1); max_data = date.today()
                val_nasc = None
                if geral is not None and geral['data_nascimento']:
                    try: val_nasc = pd.to_datetime(geral['data_nascimento']).date()
                    except: pass
                
                nasc = c3.date_input("Data Nascimento", value=val_nasc, min_value=min_data, max_value=max_data, format="DD/MM/YYYY")
                if nasc:
                    a, m, d = calcular_idade_completa(nasc)
                    st.caption(f"Idade: {a} anos, {m} meses, {d} dias")
                rg = st.text_input("RG", value=geral['rg'] if geral is not None else "").upper()

            # --- DADOS DE COLETA MANUAL (Substituindo data_editor) ---
            collected_tels = []
            collected_emails = []
            collected_ends = []
            collected_emps = []
            collected_ctrs = []

            with t2:
                # Se for EDITAR, carrega os dados do banco inicialmente (apenas visualização/edição manual se necessário)
                # NOTA: Para simplificar, no modo NOVO usamos inputs limpos. No modo EDITAR, 
                # a lógica ideal seria carregar e permitir adicionar mais. Aqui focamos na estrutura de inputs.
                
                if modo == 'editar':
                    # Mantém data_editor para edição em massa rápida se for editar
                    df_tel = dados_db.get('telefones')
                    cfg_tel = {
                        "numero": st.column_config.TextColumn("Telefone", help="Telefone (DDD+9 digitos)", width="medium", required=True, validate=r"^\d{11}$"),
                        "tag_whats": st.column_config.SelectboxColumn("WhatsApp?", options=["Sim", "Não"], required=True, width="small")
                    }
                    ed_tel = st.data_editor(df_tel, column_config=cfg_tel, num_rows="dynamic", use_container_width=True, key="editor_tel")
                else:
                    # MODO NOVO: Inputs Individuais
                    for i in range(st.session_state['count_tel']):
                        c_num, c_tag, c_vazio = st.columns([2, 1, 4])
                        val_num = c_num.text_input(f"Telefone {i+1} (DDD+9 digitos)", key=f"new_tel_n_{i}", max_chars=11)
                        val_tag = c_tag.selectbox(f"WhatsApp? {i+1}", ["Sim", "Não"], key=f"new_tel_t_{i}")
                        if val_num: collected_tels.append({"numero": val_num, "tag_whats": val_tag})
                    
                    if st.form_submit_button("➕ Adicionar Telefone"):
                        st.session_state['count_tel'] += 1
                        st.rerun()

            with t3:
                if modo == 'editar':
                    df_email = dados_db.get('emails')
                    ed_email = st.data_editor(df_email, num_rows="dynamic", use_container_width=True, key="editor_email")
                else:
                    for i in range(st.session_state['count_email']):
                        val_email = st.text_input(f"E-mail {i+1}", key=f"new_email_{i}")
                        if val_email: collected_emails.append({"email": val_email})
                    
                    if st.form_submit_button("➕ Adicionar E-mail"):
                        st.session_state['count_email'] += 1
                        st.rerun()

            with t4:
                if modo == 'editar':
                    df_end = dados_db.get('enderecos')
                    ed_end = st.data_editor(df_end, num_rows="dynamic", use_container_width=True, key="editor_end")
                else:
                    for i in range(st.session_state['count_end']):
                        st.markdown(f"**Endereço {i+1}**")
                        c_rua, c_bairro = st.columns(2)
                        c_cid, c_uf, c_cep = st.columns([2, 1, 1])
                        
                        rua = c_rua.text_input("Rua", key=f"end_rua_{i}")
                        bairro = c_bairro.text_input("Bairro", key=f"end_bairro_{i}")
                        cidade = c_cid.text_input("Cidade", key=f"end_cid_{i}")
                        uf = c_uf.text_input("UF", key=f"end_uf_{i}", max_chars=2)
                        cep = c_cep.text_input("CEP", key=f"end_cep_{i}")
                        
                        if rua or cidade: collected_ends.append({"rua": rua, "bairro": bairro, "cidade": cidade, "uf": uf, "cep": cep})
                        st.divider()
                    
                    if st.form_submit_button("➕ Adicionar Endereço"):
                        st.session_state['count_end'] += 1
                        st.rerun()

            with t5:
                lista_conv = buscar_referencias('CONVENIO')
                if modo == 'editar':
                    df_emp = dados_db.get('empregos')
                    col_emp = {"convenio": st.column_config.SelectboxColumn("Convênio", options=lista_conv, required=True), "matricula": st.column_config.TextColumn("Matrícula", required=True)}
                    ed_emp = st.data_editor(df_emp, column_config=col_emp, num_rows="dynamic", use_container_width=True, key="editor_emp")
                else:
                    st.markdown("##### Dados Profissionais")
                    for i in range(st.session_state['count_emp']):
                        c_conv, c_matr = st.columns(2)
                        conv = c_conv.selectbox(f"Convênio {i+1}", [""] + lista_conv, key=f"emp_conv_{i}")
                        matr = c_matr.text_input(f"Matrícula {i+1}", key=f"emp_matr_{i}")
                        extras = st.text_area(f"Dados Extras {i+1}", height=60, key=f"emp_ext_{i}")
                        if conv and matr: collected_emps.append({"convenio": conv, "matricula": matr, "dados_extras": extras})
                        st.divider()
                    
                    if st.form_submit_button("➕ Adicionar Emprego"):
                        st.session_state['count_emp'] += 1
                        st.rerun()

            with t6:
                if modo == 'editar':
                    df_ctr = dados_db.get('contratos')
                    ed_ctr = st.data_editor(df_ctr, num_rows="dynamic", use_container_width=True, key="editor_ctr")
                else:
                    st.markdown("##### Contratos")
                    for i in range(st.session_state['count_ctr']):
                        c_mref, c_ctr = st.columns(2)
                        mref = c_mref.text_input(f"Matrícula Referência {i+1}", key=f"ctr_mref_{i}")
                        ctr = c_ctr.text_input(f"Contrato {i+1}", key=f"ctr_num_{i}")
                        if ctr: collected_ctrs.append({"matricula_ref": mref, "contrato": ctr, "dados_extras": ""})
                    
                    if st.form_submit_button("➕ Adicionar Contrato"):
                        st.session_state['count_ctr'] += 1
                        st.rerun()

            st.markdown("---")
            confirmar = st.form_submit_button("💾 Salvar Tudo")

        if confirmar:
            if nome and cpf:
                
                # --- VERIFICAÇÃO DE DUPLICIDADE (NOVA REGRA) ---
                cpf_limpo_verif = limpar_normalizar_cpf(cpf)
                
                # Só verifica se estiver no modo NOVO e clicou em salvar
                if modo == 'novo':
                    nome_existente = verificar_cpf_existente(cpf_limpo_verif)
                    if nome_existente:
                        st.error(f"⚠️ Este CPF já está cadastrado para: **{nome_existente}**")
                        st.warning("Não é permitido cadastros duplicados.")
                        
                        col_dup1, col_dup2 = st.columns(2)
                        if col_dup1.button("🔄 Editar cadastro existente?"):
                            st.session_state['pf_view'] = 'editar'
                            st.session_state['pf_cpf_selecionado'] = cpf_limpo_verif
                            st.rerun()
                        # Interrompe a execução para não salvar duplicado
                        st.stop() 

                # --- SE PASSOU NA VERIFICAÇÃO, PROSSEGUE ---
                dg = {"cpf": cpf, "nome": nome, "data_nascimento": nasc, "rg": rg}
                
                # Prepara DataFrames para a função de salvamento
                if modo == 'novo':
                    df_final_tel = pd.DataFrame(collected_tels) if collected_tels else pd.DataFrame(columns=["numero", "tag_whats"])
                    df_final_email = pd.DataFrame(collected_emails) if collected_emails else pd.DataFrame(columns=["email"])
                    df_final_end = pd.DataFrame(collected_ends) if collected_ends else pd.DataFrame(columns=["rua", "bairro", "cidade", "uf", "cep"])
                    df_final_emp = pd.DataFrame(collected_emps) if collected_emps else pd.DataFrame(columns=["convenio", "matricula", "dados_extras"])
                    df_final_ctr = pd.DataFrame(collected_ctrs) if collected_ctrs else pd.DataFrame(columns=["matricula_ref", "contrato", "dados_extras"])
                else:
                    # No modo edição, usamos os data_editors
                    df_final_tel = ed_tel
                    df_final_email = ed_email
                    df_final_end = ed_end
                    df_final_emp = ed_emp
                    df_final_ctr = ed_ctr

                ok, msg = salvar_pf(dg, df_final_tel, df_final_email, df_final_end, df_final_emp, df_final_ctr, modo, cpf_atual)
                if ok: 
                    st.success(msg)
                    # Reset counters
                    st.session_state['count_tel'] = 1
                    st.session_state['count_email'] = 1
                    st.session_state['count_end'] = 1
                    st.session_state['count_emp'] = 1
                    st.session_state['count_ctr'] = 1
                    st.session_state['pf_view'] = 'lista'
                    time.sleep(1)
                    st.rerun()
                else: st.error(msg)
            else: st.warning("Nome e CPF obrigatórios.")

if __name__ == "__main__":
    app_pessoa_fisica() 