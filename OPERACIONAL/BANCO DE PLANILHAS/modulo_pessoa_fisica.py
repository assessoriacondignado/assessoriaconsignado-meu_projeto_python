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

# --- FUNÇÕES DE BUSCA SIMPLES ---
def buscar_pf_simples(termo):
    conn = get_conn()
    if conn:
        try:
            query = """
                SELECT d.id, d.nome, d.cpf, d.data_nascimento 
                FROM pf_dados d
                LEFT JOIN pf_telefones t ON d.cpf = t.cpf_ref
                WHERE d.cpf ILIKE %s OR d.nome ILIKE %s OR t.numero ILIKE %s
                GROUP BY d.id
                ORDER BY d.nome ASC
                LIMIT 50
            """
            param = f"%{termo}%"
            df = pd.read_sql(query, conn, params=(param, param, param))
            conn.close()
            return df
        except:
            conn.close()
    return pd.DataFrame()

# --- FUNÇÕES DE PESQUISA AMPLA ---
def buscar_opcoes_filtro(coluna, tabela, filtro_pai=None, valor_pai=None):
    """Busca opções únicas para dropdowns (Ex: UFs, Cidades dado uma UF)"""
    conn = get_conn()
    opcoes = []
    if conn:
        try:
            where_clause = ""
            params = []
            if filtro_pai and valor_pai:
                where_clause = f"WHERE {filtro_pai} = %s"
                params.append(valor_pai)
            
            query = f"SELECT DISTINCT {coluna} FROM {tabela} {where_clause} ORDER BY {coluna}"
            cur = conn.cursor()
            cur.execute(query, tuple(params))
            res = cur.fetchall()
            opcoes = [r[0] for r in res if r[0]]
            conn.close()
        except: pass
    return options if 'options' in locals() else opcoes

def executar_pesquisa_ampla(filtros, pagina=1, itens_por_pagina=30):
    conn = get_conn()
    if conn:
        try:
            # Base da Query
            sql = "SELECT DISTINCT d.id, d.nome, d.cpf, d.rg, d.data_nascimento FROM pf_dados d "
            joins = []
            conditions = []
            params = []

            # Filtros de Identificação
            if filtros.get('nome'):
                conditions.append("d.nome ILIKE %s")
                params.append(f"%{filtros['nome']}%")
            if filtros.get('cpf'):
                conditions.append("d.cpf ILIKE %s")
                params.append(f"%{filtros['cpf']}%")
            if filtros.get('rg'):
                conditions.append("d.rg ILIKE %s")
                params.append(f"%{filtros['rg']}%")
            if filtros.get('nascimento'):
                conditions.append("d.data_nascimento = %s")
                params.append(filtros['nascimento'])

            # Filtros de Endereço (Tabela pf_enderecos)
            if any(k in filtros for k in ['uf', 'cidade', 'bairro', 'rua', 'cep']):
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

            # Filtros de Contato (Tabelas pf_telefones e pf_emails)
            if filtros.get('ddd'):
                # Lógica para extrair os 2 primeiros digitos numericos
                joins.append("JOIN pf_telefones tel ON d.cpf = tel.cpf_ref")
                conditions.append("SUBSTRING(REGEXP_REPLACE(tel.numero, '[^0-9]', '', 'g'), 1, 2) = %s")
                params.append(filtros['ddd'])
            
            if filtros.get('email'):
                joins.append("JOIN pf_emails em ON d.cpf = em.cpf_ref")
                conditions.append("em.email ILIKE %s")
                params.append(f"%{filtros['email']}%")

            # Filtros Profissionais/Contratos
            if any(k in filtros for k in ['convenio', 'matricula', 'contrato']):
                # Se tem contrato, faz join com contratos também
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

            # Montagem Final
            # Remove duplicatas dos joins
            joins = list(set(joins))
            sql_joins = " ".join(joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            full_sql = f"{sql} {sql_joins} {sql_where} ORDER BY d.nome"
            
            # Contagem Total (para paginação)
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

def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {}
    if conn:
        try:
            df_d = pd.read_sql("SELECT * FROM pf_dados WHERE cpf = %s", conn, params=(cpf,))
            dados['geral'] = df_d.iloc[0] if not df_d.empty else None
            dados['telefones'] = pd.read_sql("SELECT numero, data_atualizacao, tag_whats, tag_qualificacao FROM pf_telefones WHERE cpf_ref = %s", conn, params=(cpf,))
            dados['emails'] = pd.read_sql("SELECT email FROM pf_emails WHERE cpf_ref = %s", conn, params=(cpf,))
            dados['enderecos'] = pd.read_sql("SELECT rua, bairro, cidade, uf, cep FROM pf_enderecos WHERE cpf_ref = %s", conn, params=(cpf,))
            dados['empregos'] = pd.read_sql("SELECT id, convenio, matricula, dados_extras FROM pf_emprego_renda WHERE cpf_ref = %s", conn, params=(cpf,))
            
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
                cur.execute("DELETE FROM pf_emprego_renda WHERE cpf_ref = %s", (cpf_chave,))
            
            for _, row in df_tel.iterrows():
                if row.get('numero'): cur.execute("INSERT INTO pf_telefones (cpf_ref, numero, data_atualizacao, tag_whats, tag_qualificacao) VALUES (%s, %s, %s, %s, %s)", (cpf_chave, row['numero'], row.get('data_atualizacao'), row.get('tag_whats'), row.get('tag_qualificacao')))
            for _, row in df_email.iterrows():
                if row.get('email'): cur.execute("INSERT INTO pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, row['email']))
            for _, row in df_end.iterrows():
                if row.get('rua') or row.get('cidade'): cur.execute("INSERT INTO pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, row['rua'], row.get('bairro'), row.get('cidade'), row.get('uf'), row.get('cep')))
            
            if not df_emp.empty:
                for _, row in df_emp.iterrows():
                    conv = row.get('convenio')
                    matr = row.get('matricula')
                    if conv:
                        try:
                            cur.execute("INSERT INTO pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s)", (cpf_chave, conv, matr, row.get('dados_extras')))
                        except: pass

            if not df_contr.empty:
                for _, row in df_contr.iterrows():
                    matr_ref = row.get('matricula_ref')
                    if matr_ref:
                        cur.execute("SELECT 1 FROM pf_emprego_renda WHERE matricula = %s", (matr_ref,))
                        if cur.fetchone():
                            cur.execute("INSERT INTO pf_contratos (matricula_ref, contrato, dados_extras) VALUES (%s, %s, %s)", (matr_ref, row.get('contrato'), row.get('dados_extras')))

            conn.commit()
            conn.close()
            return True, "Salvo com sucesso!"
        except Exception as e: return False, str(e)
    return False, "Erro de conexão"

def excluir_pf(cpf):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM pf_dados WHERE cpf = %s", (cpf,))
            conn.commit(); conn.close()
            return True
        except: return False
    return False

def exportar_dados(lista_cpfs):
    conn = get_conn()
    if conn and lista_cpfs:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        query = f"""
            SELECT d.cpf, d.nome, d.data_nascimento, t.numero as telefone_principal, e.email
            FROM pf_dados d
            LEFT JOIN pf_telefones t ON d.cpf = t.cpf_ref
            LEFT JOIN pf_emails e ON d.cpf = e.cpf_ref
            WHERE d.cpf IN ({placeholders})
        """
        df = pd.read_sql(query, conn, params=tuple(lista_cpfs))
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
        except Exception as e: 
            return False
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
    
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'pf_cpf_selecionado' not in st.session_state: st.session_state['pf_cpf_selecionado'] = None
    if 'import_step' not in st.session_state: st.session_state['import_step'] = 1
    if 'pesquisa_pag' not in st.session_state: st.session_state['pesquisa_pag'] = 1

    # ==========================
    # MODO PESQUISA AMPLA
    # ==========================
    if st.session_state['pf_view'] == 'pesquisa_ampla':
        st.button("⬅️ Voltar para Lista", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        st.markdown("### 🔎 Pesquisa Ampla e Avançada")
        
        with st.form("form_pesquisa_ampla"):
            # ABAS DE FILTRO
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
                filtros['ddd'] = c_ddd.text_input("DDD (2 dígitos)", max_chars=2, help="Ex: 11, 21")
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
                    if pag_atual > 1:
                        if st.button("⬅️ Anterior"):
                            st.session_state['pesquisa_pag'] -= 1
                            st.rerun()
                with col_p2:
                    st.markdown(f"<div style='text-align:center'>Página {pag_atual} de {total_pags}</div>", unsafe_allow_html=True)
                with col_p3:
                    if pag_atual < total_pags:
                        if st.button("Próxima ➡️"):
                            st.session_state['pesquisa_pag'] += 1
                            st.rerun()
                
                st.markdown("##### Ações Rápidas")
                col_sel = st.selectbox("Selecionar Cadastro para ver Detalhes", df_res['nome'].tolist())
                if st.button("Ver Cadastro Selecionado"):
                    cpf_sel = df_res[df_res['nome'] == col_sel].iloc[0]['cpf']
                    st.session_state['pf_view'] = 'editar'
                    st.session_state['pf_cpf_selecionado'] = cpf_sel
                    st.rerun()
            else:
                st.warning("Nenhum registro encontrado com esses filtros.")

    # --- MODO IMPORTAÇÃO ---
    elif st.session_state['pf_view'] == 'importacao':
        st.button("⬅️ Cancelar Importação", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'import_step': 1}))
        st.divider()
        
        mapa_tabelas = {
            "Dados Cadastrais": "pf_dados",
            "Telefones": "pf_telefones",
            "Emails": "pf_emails",
            "Endereços": "pf_enderecos",
            "Emprego/Renda": "pf_emprego_renda",
            "Contratos": "pf_contratos"
        }

        if st.session_state['import_step'] == 1:
            st.markdown("### 📤 Etapa 1: Upload do Arquivo")
            sel_amigavel = st.selectbox("Selecione a Tabela de Destino", list(mapa_tabelas.keys()))
            st.session_state['import_table'] = mapa_tabelas[sel_amigavel]
            
            uploaded_file = st.file_uploader("Selecione o arquivo CSV", type=['csv'])
            
            if uploaded_file:
                try:
                    df = pd.read_csv(uploaded_file)
                    st.session_state['import_df'] = df
                    st.write("🔎 **Pré-visualização (10 linhas):**")
                    st.dataframe(df.head(10))
                    
                    linhas_erro = []
                    for idx, row in df.iterrows():
                        if row.isnull().all(): linhas_erro.append(f"Linha {idx+2}: Linha vazia.")
                    
                    if linhas_erro:
                        st.error(f"{len(linhas_erro)} erros encontrados.")
                        txt_erros = "\n".join(linhas_erro)
                        st.download_button("Baixar Relatório de Erros", txt_erros, "erros_importacao.txt")
                    else:
                        st.success("Arquivo validado. Nenhuma linha vazia detectada.")
                    
                    if st.button("Confirmar e Ir para Mapeamento", type="primary"):
                        st.session_state['import_step'] = 2
                        st.rerun()
                except Exception as e:
                    st.error(f"Erro ao ler arquivo: {e}")

        elif st.session_state['import_step'] == 2:
            st.markdown("### 🔗 Etapa 2: Mapeamento de Colunas")
            df = st.session_state['import_df']
            table_name = st.session_state['import_table']
            
            db_cols_info = get_table_columns(table_name)
            db_col_names = [c[0] for c in db_cols_info if c[0] not in ['id', 'data_criacao', 'data_atualizacao']]
            
            if table_name in ['pf_emprego_renda', 'pf_contratos']:
                with st.expander("✨ Criar Nova Coluna no Banco"):
                    new_col_name = st.text_input("Nome da Nova Coluna (sem espaços)")
                    new_col_type = st.selectbox("Tipo", ["VARCHAR(255)", "NUMERIC", "DATE"])
                    if st.button("Criar Coluna"):
                        if new_col_name:
                            clean_name = re.sub(r'[^a-zA-Z0-9_]', '', new_col_name).lower()
                            if add_column_to_table(table_name, clean_name, new_col_type):
                                st.success(f"Coluna '{clean_name}' criada!")
                                time.sleep(1); st.rerun()
                            else: st.error("Erro ao criar coluna.")
            
            mapping = {}
            st.write("Relacione as colunas do CSV com as do Banco de Dados (Deixe 'Ignorar' para pular)")
            cols_csv = ["(Ignorar)"] + list(df.columns)
            
            for db_col in db_col_names:
                idx_default = 0
                for i, csv_c in enumerate(cols_csv):
                    if csv_c.lower() == db_col.lower(): idx_default = i
                mapping[db_col] = st.selectbox(f"Campo Banco: **{db_col}**", cols_csv, index=idx_default, key=f"map_{db_col}")
            
            if st.button("Finalizar Importação", type="primary"):
                st.markdown("### ⚙️ Processando...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                conn = get_conn()
                if conn:
                    cur = conn.cursor()
                    total_rows = len(df)
                    count_novo = 0
                    count_atualizado = 0
                    valid_map = {k: v for k, v in mapping.items() if v != "(Ignorar)"}
                    
                    for i, row in df.iterrows():
                        if i % 500 == 0 or i == total_rows - 1:
                            progress_bar.progress(int((i + 1) / total_rows * 100))
                            status_text.text(f"Processando linha {i+1} de {total_rows}...")
                        try:
                            cols = list(valid_map.keys())
                            vals = [row[valid_map[c]] for c in cols]
                            vals = [None if pd.isna(v) else v for v in vals]
                            
                            pk = 'cpf' if table_name == 'pf_dados' else 'matricula' if table_name == 'pf_emprego_renda' else None
                            
                            if pk and pk in cols:
                                pk_val = vals[cols.index(pk)]
                                cur.execute(f"SELECT 1 FROM {table_name} WHERE {pk} = %s", (pk_val,))
                                exists = cur.fetchone()
                                if exists:
                                    set_clause = ", ".join([f"{c}=%s" for c in cols])
                                    cur.execute(f"UPDATE {table_name} SET {set_clause} WHERE {pk}=%s", vals + [pk_val])
                                    count_atualizado += 1
                                else:
                                    placeholders = ", ".join(["%s"] * len(vals))
                                    cur.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})", vals)
                                    count_novo += 1
                            else:
                                placeholders = ", ".join(["%s"] * len(vals))
                                cur.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})", vals)
                                count_novo += 1
                        except Exception as e: print(f"Erro linha {i}: {e}")
                            
                    conn.commit(); conn.close()
                    st.success("Importação Concluída!")
                    st.info(f"Resumo: {count_novo} novos registros, {count_atualizado} atualizados.")
                    if st.button("Voltar para Lista"):
                        st.session_state['pf_view'] = 'lista'; st.rerun()

    # --- MODO LISTA (PADRÃO) ---
    elif st.session_state['pf_view'] == 'lista':
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

    # --- MODO NOVO / EDITAR ---
    elif st.session_state['pf_view'] in ['novo', 'editar']:
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        
        modo = st.session_state['pf_view']
        cpf_atual = st.session_state['pf_cpf_selecionado']
        dados_db = carregar_dados_completos(cpf_atual) if modo == 'editar' and cpf_atual else {}
        geral = dados_db.get('geral')
        
        st.markdown(f"### {geral['nome'] if geral is not None else 'Novo Cadastro'}")

        with st.form("form_pf"):
            t1, t2, t3, t4, t5, t6 = st.tabs(["Dados Pessoais", "Telefones", "Emails", "Endereços", "💼 Emprego/Renda", "📄 Contratos"])
            
            with t1:
                c1, c2, c3 = st.columns(3)
                nome = c1.text_input("Nome *", value=geral['nome'] if geral is not None else "")
                cpf = c2.text_input("CPF *", value=geral['cpf'] if geral is not None else "")
                
                # --- AJUSTE: DATA DE NASCIMENTO ---
                min_data = date(1900, 1, 1)
                max_data = date.today()
                
                val_nasc = None
                if geral is not None and geral['data_nascimento']:
                    try: val_nasc = pd.to_datetime(geral['data_nascimento']).date()
                    except: val_nasc = None

                nasc = c3.date_input("Data Nascimento", value=val_nasc, min_value=min_data, max_value=max_data, format="DD/MM/YYYY")
                
                if nasc:
                    a, m, d = calcular_idade_completa(nasc)
                    st.caption(f"Idade: {a} anos, {m} meses, {d} dias")
                
                rg = st.text_input("RG", value=geral['rg'] if geral is not None else "")

            with t2:
                df_tel = dados_db.get('telefones') if modo=='editar' else pd.DataFrame(columns=["numero", "tag_whats"])
                ed_tel = st.data_editor(df_tel, num_rows="dynamic", use_container_width=True)

            with t3:
                df_email = dados_db.get('emails') if modo=='editar' else pd.DataFrame(columns=["email"])
                ed_email = st.data_editor(df_email, num_rows="dynamic", use_container_width=True)

            with t4:
                df_end = dados_db.get('enderecos') if modo=='editar' else pd.DataFrame(columns=["rua", "cidade", "uf", "cep"])
                ed_end = st.data_editor(df_end, num_rows="dynamic", use_container_width=True)

            with t5:
                st.markdown("##### Dados Profissionais")
                lista_convenios = buscar_referencias('CONVENIO')
                df_emp = dados_db.get('empregos') if modo=='editar' else pd.DataFrame(columns=["convenio", "matricula", "dados_extras"])
                col_config_emp = {
                    "convenio": st.column_config.SelectboxColumn("Convênio", options=lista_convenios, required=True),
                    "matricula": st.column_config.TextColumn("Matrícula", required=True)
                }
                ed_emp = st.data_editor(df_emp, column_config=col_config_emp, num_rows="dynamic", use_container_width=True)

            with t6:
                st.markdown("##### Contratos e Financiamentos")
                df_contr = dados_db.get('contratos') if modo=='editar' else pd.DataFrame(columns=["matricula_ref", "contrato", "dados_extras"])
                ed_contr = st.data_editor(df_contr, num_rows="dynamic", use_container_width=True)

            st.markdown("---")
            confirmar = st.form_submit_button("💾 Salvar Tudo")

        if confirmar:
            if nome and cpf:
                dg = {"cpf": cpf, "nome": nome, "data_nascimento": nasc, "rg": rg}
                ok, msg = salvar_pf(dg, ed_tel, ed_email, ed_end, ed_emp, ed_contr, modo, cpf_atual)
                if ok: st.success(msg); st.session_state['pf_view'] = 'lista'; st.rerun()
                else: st.error(msg)
            else: st.warning("Nome e CPF obrigatórios.")

if __name__ == "__main__":
    app_pessoa_fisica()