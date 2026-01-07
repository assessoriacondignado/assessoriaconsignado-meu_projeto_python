import streamlit as st
import pandas as pd
import psycopg2
import re
import time
from datetime import datetime, date, timedelta

# Tenta importar conexao
try:
    import conexao
except ImportError:
    st.error("Erro: conexao.py não encontrado na raiz.")

# --- FUNÇÕES DE CONEXÃO E UTILITÁRIOS ---

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        print(f"Erro conexão: {e}")
        return None

def sanitizar_nome_tabela(nome):
    s = str(nome).lower().strip()
    s = re.sub(r'[^a-z0-9]', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.strip('_')

# --- FUNÇÕES DE ESTRUTURA (DDL) ---

def garantir_tabela_extrato_geral():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cliente.extrato_carteira_por_produto (
                    id SERIAL PRIMARY KEY,
                    id_cliente INTEGER,
                    data_lancamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tipo_lancamento VARCHAR(50),
                    produto_vinculado VARCHAR(255),
                    origem_lancamento VARCHAR(100),
                    valor_lancado NUMERIC(10, 2),
                    saldo_anterior NUMERIC(10, 2),
                    saldo_novo NUMERIC(10, 2),
                    nome_usuario VARCHAR(255)
                );
            """)
            conn.commit(); conn.close()
        except: 
            if conn: conn.close()

def garantir_tabela_custo_carteira():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cliente.valor_custo_carteira_cliente (
                    id SERIAL PRIMARY KEY,
                    cpf_cliente VARCHAR(20),
                    nome_cliente VARCHAR(255),
                    nome_carteira VARCHAR(255),
                    custo_carteira NUMERIC(10, 2),
                    cpf_usuario VARCHAR(20),
                    nome_usuario VARCHAR(255),
                    origem_custo VARCHAR(100),
                    data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit(); conn.close()
        except: 
            if conn: conn.close()

def garantir_tabela_config_carteiras():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cliente.carteiras_config (
                    id SERIAL PRIMARY KEY,
                    id_produto INTEGER,
                    nome_produto VARCHAR(255),
                    nome_carteira VARCHAR(255),
                    nome_tabela_transacoes VARCHAR(255),
                    status VARCHAR(50) DEFAULT 'ATIVO',
                    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    origem_custo VARCHAR(100)
                );
            """)
            conn.commit(); conn.close()
        except: 
            if conn: conn.close()

# --- FUNÇÕES DE NEGÓCIO (FINANCEIRO) ---

def listar_origens_para_selecao():
    conn = get_conn()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT origem FROM conexoes.fatorconferi_origem_consulta_fator ORDER BY origem ASC")
        res = [row[0] for row in cur.fetchall()]
        conn.close(); return res
    except:
        if conn: conn.close()
        return []

def listar_produtos_para_selecao():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, nome FROM produtos_servicos WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_usuarios_para_selecao():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, nome, cpf FROM clientes_usuarios WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_clientes_para_selecao():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, nome, cpf FROM admin.clientes ORDER BY nome", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

# -- Carteiras Config --

def salvar_nova_carteira_sistema(id_prod, nome_prod, nome_carteira, status, origem_custo):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        sufixo = sanitizar_nome_tabela(nome_carteira)
        nome_tab = f"cliente.transacoes_{sufixo}"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {nome_tab} (id SERIAL PRIMARY KEY, cpf_cliente VARCHAR(20), nome_cliente VARCHAR(255), motivo VARCHAR(255), origem_lancamento VARCHAR(100), data_transacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP, tipo_lancamento VARCHAR(50), valor NUMERIC(10, 2), saldo_anterior NUMERIC(10, 2), saldo_novo NUMERIC(10, 2))")
        cur.execute("INSERT INTO cliente.carteiras_config (id_produto, nome_produto, nome_carteira, nome_tabela_transacoes, status, origem_custo) VALUES (%s, %s, %s, %s, %s, %s)", (id_prod, nome_prod, nome_carteira, nome_tab, status, origem_custo))
        conn.commit(); conn.close(); return True
    except Exception as e:
        print(e)
        if conn: conn.close()
        return False

def listar_todas_carteiras_ativas():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, nome_carteira, nome_tabela_transacoes FROM cliente.carteiras_config WHERE status = 'ATIVO' ORDER BY nome_carteira", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_carteiras_config():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT * FROM cliente.carteiras_config ORDER BY id DESC", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def atualizar_carteira_config(id_conf, status, nome_carteira=None, origem_custo=None):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE cliente.carteiras_config 
            SET status = %s, nome_carteira = %s, origem_custo = %s 
            WHERE id = %s
        """, (status, nome_carteira, origem_custo, id_conf))
        conn.commit(); conn.close()
        return True
    except Exception as e: 
        print(e)
        conn.close(); return False

def excluir_carteira_config(id_conf, nome_tabela):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM cliente.carteiras_config WHERE id = %s", (id_conf,))
            conn.commit(); conn.close()
            return True
        except: conn.close(); return False
    return False

# -- Custo Carteira Cliente --

def listar_cliente_carteira_lista():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        query = """
            SELECT 
                l.id, l.cpf_cliente, l.nome_cliente, l.nome_carteira, 
                l.custo_carteira, l.cpf_usuario, l.nome_usuario, l.origem_custo,
                c.nome_tabela_transacoes
            FROM cliente.valor_custo_carteira_cliente l
            LEFT JOIN cliente.carteiras_config c ON l.nome_carteira = c.nome_carteira
            ORDER BY l.nome_cliente
        """
        df = pd.read_sql(query, conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def salvar_cliente_carteira_lista(cpf, nome, carteira, custo, origem_custo):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cpf_limpo = re.sub(r'\D', '', str(cpf))
        query_vinculo = """
            SELECT u.cpf, u.nome FROM admin.clientes c
            JOIN clientes_usuarios u ON c.id_usuario_vinculo = u.id
            WHERE regexp_replace(c.cpf, '[^0-9]', '', 'g') = %s LIMIT 1
        """
        cur.execute(query_vinculo, (cpf_limpo,))
        res_v = cur.fetchone()
        cpf_u, nome_u = (res_v[0], res_v[1]) if res_v else (None, None)
        
        cur.execute("""
            INSERT INTO cliente.valor_custo_carteira_cliente (cpf_cliente, nome_cliente, nome_carteira, custo_carteira, cpf_usuario, nome_usuario, origem_custo) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (cpf, nome, carteira, custo, cpf_u, nome_u, origem_custo))
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

def atualizar_cliente_carteira_lista(id_reg, cpf, nome, carteira, custo, cpf_u, nome_u, origem_custo):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE cliente.valor_custo_carteira_cliente 
            SET cpf_cliente=%s, nome_cliente=%s, nome_carteira=%s, custo_carteira=%s, cpf_usuario=%s, nome_usuario=%s, origem_custo=%s 
            WHERE id=%s
        """, (cpf, nome, carteira, custo, cpf_u, nome_u, origem_custo, id_reg))
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

def excluir_cliente_carteira_lista(id_reg):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM cliente.valor_custo_carteira_cliente WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

# -- Transações e Tabelas Dinâmicas --

def listar_tabelas_transacao_reais():
    conn = get_conn()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'cliente' AND table_name LIKE 'transacoes_%%' ORDER BY table_name")
        res = [row[0] for row in cur.fetchall()]
        conn.close(); return res
    except:
        if conn: conn.close()
        return []

def carregar_dados_tabela_dinamica(nome_tabela):
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql(f"SELECT * FROM cliente.{nome_tabela} ORDER BY id DESC", conn)
        conn.close(); return df
    except:
        if conn: conn.close()
        return pd.DataFrame()

def salvar_alteracoes_tabela_dinamica(nome_tabela, df_original, df_editado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        ids_originais = set(df_original['id'].dropna().astype(int).tolist())
        
        ids_editados_atuais = set()
        for _, row in df_editado.iterrows():
            if pd.notna(row.get('id')) and row.get('id') != '':
                try: ids_editados_atuais.add(int(row['id']))
                except: pass

        ids_del = ids_originais - ids_editados_atuais
        if ids_del:
            ids_str = ",".join(map(str, ids_del))
            cur.execute(f"DELETE FROM cliente.{nome_tabela} WHERE id IN ({ids_str})")

        for index, row in df_editado.iterrows():
            colunas_db = [c for c in row.index if c not in ['id', 'data_transacao']]
            valores = [row[c] for c in colunas_db]
            row_id = row.get('id')
            eh_novo = pd.isna(row_id) or row_id == '' or row_id is None
            
            if eh_novo:
                cols_str = ", ".join(colunas_db)
                placeholders = ", ".join(["%s"] * len(colunas_db))
                cur.execute(f"INSERT INTO cliente.{nome_tabela} ({cols_str}) VALUES ({placeholders})", valores)
            elif int(row_id) in ids_originais:
                set_clause = ", ".join([f"{c} = %s" for c in colunas_db])
                valores_update = valores + [int(row_id)]
                cur.execute(f"UPDATE cliente.{nome_tabela} SET {set_clause} WHERE id = %s", valores_update)
        
        conn.commit(); conn.close(); return True
    except Exception as e:
        st.error(f"Erro ao salvar tabela: {e}"); 
        if conn: conn.close()
        return False

def buscar_transacoes_carteira_filtrada(nome_tabela_sql, cpf_cliente, data_ini, data_fim):
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        dt_ini_str = data_ini.strftime('%Y-%m-%d 00:00:00')
        dt_fim_str = data_fim.strftime('%Y-%m-%d 23:59:59')
        query = f"SELECT id, data_transacao, motivo, tipo_lancamento, valor, saldo_novo, origem_lancamento FROM {nome_tabela_sql} WHERE cpf_cliente = %s AND data_transacao BETWEEN %s AND %s ORDER BY data_transacao DESC"
        df = pd.read_sql(query, conn, params=(str(cpf_cliente), dt_ini_str, dt_fim_str))
        conn.close(); return df
    except:
        if conn: conn.close()
        return pd.DataFrame()

def realizar_lancamento_manual(tabela_sql, cpf_cliente, nome_cliente, tipo_lanc, valor, motivo):
    conn = get_conn()
    if not conn: return False, "Erro conexão"
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT saldo_novo FROM {tabela_sql} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_cliente,))
        res = cur.fetchone()
        saldo_anterior = float(res[0]) if res else 0.0
        valor = float(valor)
        saldo_novo = saldo_anterior - valor if tipo_lanc == "DEBITO" else saldo_anterior + valor
        
        # Inserção na tabela individual
        query = f"INSERT INTO {tabela_sql} (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao) VALUES (%s, %s, %s, 'MANUAL', %s, %s, %s, %s, NOW())"
        cur.execute(query, (cpf_cliente, nome_cliente, motivo, tipo_lanc, valor, saldo_anterior, saldo_novo))
        
        # Inserção na tabela unificada
        cur.execute("SELECT id FROM admin.clientes WHERE cpf = %s LIMIT 1", (cpf_cliente,))
        res_cli = cur.fetchone()
        if res_cli:
            id_cliente = res_cli[0]
            cur.execute("""
                INSERT INTO cliente.extrato_carteira_por_produto 
                (id_cliente, tipo_lancamento, produto_vinculado, origem_lancamento, valor_lancado, saldo_anterior, saldo_novo)
                VALUES (%s, %s, %s, 'MANUAL', %s, %s, %s)
            """, (str(id_cliente), tipo_lanc, motivo, valor, saldo_anterior, saldo_novo))

        conn.commit(); conn.close(); return True, "Sucesso"
    except Exception as e:
        if conn: conn.close()
        return False, str(e)

def atualizar_transacao_dinamica(nome_tabela, id_transacao, novo_motivo, novo_valor, novo_tipo):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        query = f"UPDATE {nome_tabela} SET motivo = %s, valor = %s, tipo_lancamento = %s WHERE id = %s"
        cur.execute(query, (novo_motivo, float(novo_valor), novo_tipo, id_transacao))
        conn.commit(); conn.close()
        return True
    except: conn.close(); return False

def excluir_transacao_dinamica(nome_tabela, id_transacao):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        query = f"DELETE FROM {nome_tabela} WHERE id = %s"
        cur.execute(query, (id_transacao,))
        conn.commit(); conn.close()
        return True
    except: conn.close(); return False

# --- DIALOGS ---

@st.dialog("✏️ Editar Carteira Cliente")
def dialog_editar_cart_lista(dados):
    st.write(f"Editando: **{dados['nome_cliente']}**")
    
    df_users = listar_usuarios_para_selecao()
    opcoes_usuarios = [""] + df_users.apply(lambda x: f"{x['nome']} | CPF: {x['cpf']}", axis=1).tolist()
    
    idx_atual = 0
    if dados['nome_usuario']:
        match = [i for i, s in enumerate(opcoes_usuarios) if dados['nome_usuario'] in s]
        if match: idx_atual = match[0]
        
    lista_origens = listar_origens_para_selecao()
    idx_origem = 0
    valor_atual_origem = dados.get('origem_custo')
    if valor_atual_origem and valor_atual_origem in lista_origens:
        idx_origem = lista_origens.index(valor_atual_origem)
    opcoes_origem = [""] + lista_origens

    with st.form("f_ed_cl"):
        n_cpf = st.text_input("CPF Cliente", value=dados['cpf_cliente'])
        n_nome = st.text_input("Nome Cliente", value=dados['nome_cliente'])
        n_cart = st.text_input("Nome Carteira", value=dados['nome_carteira'])
        
        n_origem_custo = st.selectbox("Origem do Custo", options=opcoes_origem, index=idx_origem + 1 if valor_atual_origem else 0)
        n_custo = st.number_input("Custo Carteira (R$)", value=float(dados['custo_carteira'] or 0.0), step=0.01)
        
        sel_user = st.selectbox("Usuário Vinculado", options=opcoes_usuarios, index=idx_atual)
        
        if st.form_submit_button("Salvar"):
            cpf_u_final = None
            nome_u_final = None
            if sel_user:
                partes = sel_user.split(" | CPF: ")
                nome_u_final = partes[0]
                cpf_u_final = partes[1] if len(partes) > 1 else None

            if atualizar_cliente_carteira_lista(dados['id'], n_cpf, n_nome, n_cart, n_custo, cpf_u_final, nome_u_final, n_origem_custo):
                st.success("Atualizado!"); st.rerun()
            else: st.error("Erro.")

@st.dialog("✏️ Editar Configuração da Carteira")
def dialog_editar_carteira_config(dados):
    st.write(f"Editando: **{dados['nome_carteira']}**")
    lista_origens = listar_origens_para_selecao()
    with st.form("form_edit_cart_conf"):
        n_nome = st.text_input("Nome da Carteira", value=dados['nome_carteira'])
        n_status = st.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if dados['status'] == "ATIVO" else 1)
        
        idx_org = 0
        valor_org = dados.get('origem_custo')
        if valor_org and valor_org in lista_origens: idx_org = lista_origens.index(valor_org)
        
        n_origem = st.selectbox("Origem Custo (Tabela Fator)", options=[""] + lista_origens, index=idx_org + 1 if valor_org else 0)
        
        if st.form_submit_button("Salvar Alterações"):
            if atualizar_carteira_config(dados['id'], n_status, n_nome, n_origem):
                st.success("Atualizado!"); time.sleep(1); st.rerun()
            else: st.error("Erro ao atualizar.")

@st.dialog("💰 Lançamento Manual")
def dialog_lancamento_manual(tabela_sql, cpf_cliente, nome_cliente, tipo_lanc):
    titulo = "Crédito (Aporte)" if tipo_lanc == "CREDITO" else "Débito (Cobrança)"
    st.markdown(f"### {titulo}")
    st.write(f"Cliente: **{nome_cliente}**")
    
    with st.form("form_lanc_manual"):
        valor = st.number_input("Valor (R$)", min_value=0.01, step=1.00)
        motivo = st.text_input("Motivo", value="Lançamento Manual")
        
        if st.form_submit_button("✅ Confirmar"):
            ok, msg = realizar_lancamento_manual(tabela_sql, cpf_cliente, nome_cliente, tipo_lanc, valor, motivo)
            if ok:
                st.success(msg)
                time.sleep(1.5)
                st.rerun()
            else:
                st.error(f"Erro: {msg}")

@st.dialog("✏️ Editar Lançamento")
def dialog_editar_lancamento_extrato(tabela_sql, transacao):
    st.write(f"Editando ID: {transacao['id']}")
    with st.form("form_edit_lanc"):
        n_motivo = st.text_input("Motivo", value=transacao['motivo'])
        c1, c2 = st.columns(2)
        n_tipo = c1.selectbox("Tipo", ["CREDITO", "DEBITO"], index=0 if transacao['tipo_lancamento'] == "CREDITO" else 1)
        n_valor = c2.number_input("Valor (R$)", value=float(transacao['valor']), step=0.01)
        
        if st.form_submit_button("💾 Salvar"):
            if atualizar_transacao_dinamica(tabela_sql, transacao['id'], n_motivo, n_valor, n_tipo):
                st.success("Atualizado!"); time.sleep(1); st.rerun()
            else: st.error("Erro.")

@st.dialog("🗑️ Excluir Lançamento")
def dialog_excluir_lancamento_extrato(tabela_sql, id_transacao):
    st.warning("Excluir este lançamento?")
    st.caption("Atenção: O saldo futuro não é recalculado automaticamente.")
    if st.button("🚨 Sim, Excluir"):
        if excluir_transacao_dinamica(tabela_sql, id_transacao):
            st.success("Excluído!"); time.sleep(1); st.rerun()
        else: st.error("Erro.")

# --- APP PRINCIPAL FINANCEIRO ---

def app_financeiro():
    # Garante que as tabelas base existam
    garantir_tabela_config_carteiras()
    garantir_tabela_extrato_geral()
    garantir_tabela_custo_carteira()

    st.markdown("## 💰 Módulo Financeiro")
    
    tab_custos, tab_config, tab_edit_tab, tab_rel = st.tabs([
        "💼 Custos de Carteira", 
        "⚙️ Config. Carteiras", 
        "📝 Editar Transações",
        "📊 Relatórios"
    ])

    # --- ABA CUSTOS DE CARTEIRA ---
    with tab_custos:
        with st.expander("➕ Adicionar Custo a Cliente", expanded=False):
            with st.container(border=True):
                c1, c2, c3, c4, c5 = st.columns([1.5, 2, 1.5, 1, 1])
                
                df_clis = listar_clientes_para_selecao()
                opcoes_cli = [""] + df_clis.apply(lambda x: f"{x['nome']} | CPF: {x['cpf']}", axis=1).tolist()
                n_sel = c2.selectbox("Cliente", options=opcoes_cli, key="n_sel_l")
                
                cpf_auto = n_sel.split(" | CPF: ")[1] if n_sel else ""
                n_cpf = c1.text_input("CPF", value=cpf_auto, key="n_cpf_l")
                
                df_c_at = listar_todas_carteiras_ativas()
                n_cart = c3.selectbox("Carteira", options=[""] + df_c_at['nome_carteira'].tolist(), key="n_cart_l")
                
                n_val = c4.number_input("Custo", key="n_val_l", step=0.01)
                
                orgs = listar_origens_para_selecao()
                n_org = st.selectbox("Origem Custo", options=[""] + orgs, key="n_org_l")
                
                if c5.button("Adicionar", key="add_cl_btn"):
                    if n_cpf and n_cart:
                        nome_cli_clean = n_sel.split(" | ")[0] if n_sel else ""
                        if salvar_cliente_carteira_lista(n_cpf, nome_cli_clean, n_cart, n_val, n_org): st.rerun()
                    else: st.warning("Preencha CPF e Carteira.")

        df_l = listar_cliente_carteira_lista()
        if not df_l.empty:
            st.markdown("""<div style="display: flex; font-weight: bold; background: #f0f2f6; padding: 8px; font-size: 0.85em;">
            <div style="flex: 2;">Cliente</div><div style="flex: 1.5;">CPF</div><div style="flex: 2;">Carteira</div>
            <div style="flex: 1;">Custo</div><div style="flex: 1.5;">Usuário</div><div style="flex: 1;">Ações</div></div>""", unsafe_allow_html=True)
            
            for _, r in df_l.iterrows():
                c1, c2, c3, c4, c5, c6 = st.columns([2, 1.5, 2, 1, 1.5, 1])
                c1.write(r['nome_cliente']); c2.write(r['cpf_cliente']); c3.write(r['nome_carteira'])
                c4.write(f"R$ {float(r['custo_carteira'] or 0):.2f}")
                c5.write(r.get('nome_usuario', '-'))
                with c6:
                    if st.button("✏️", key=f"ed_cl_{r['id']}"): dialog_editar_cart_lista(r)
                    if st.button("🗑️", key=f"de_cl_{r['id']}"): excluir_cliente_carteira_lista(r['id']); st.rerun()

    # --- ABA CONFIGURAÇÃO DE CARTEIRAS ---
    with tab_config:
        with st.expander("🆕 Criar Nova Carteira (Sistema)", expanded=False):
            st.info("Cria uma nova tabela SQL para armazenar as transações desta carteira.")
            df_pds = listar_produtos_para_selecao()
            if not df_pds.empty:
                with st.container(border=True):
                    cc1, cc2, cc3, cc4, cc5 = st.columns([2, 2, 2, 2, 2])
                    idx_p = cc1.selectbox("Produto Vinculado", range(len(df_pds)), format_func=lambda x: df_pds.iloc[x]['nome'])
                    n_cart_in = cc2.text_input("Nome Carteira", key="n_c_n")
                    orgs = listar_origens_para_selecao()
                    origem_cart_in = cc3.selectbox("Origem Custo", options=[""] + orgs, key="org_c_new")
                    stt_in = cc4.selectbox("Status", ["ATIVO", "INATIVO"], key="s_c_n")
                    
                    if cc5.button("Criar Carteira", key="b_c_c"):
                        if n_cart_in: 
                            if salvar_nova_carteira_sistema(int(df_pds.iloc[idx_p]['id']), df_pds.iloc[idx_p]['nome'], n_cart_in, stt_in, origem_cart_in):
                                st.success("Carteira Criada!"); time.sleep(1); st.rerun()

        st.divider()
        df_configs = listar_carteiras_config()
        if not df_configs.empty:
            st.markdown("""<div style="display: flex; font-weight: bold; background: #f0f2f6; padding: 8px; font-size: 0.9em;">
            <div style="flex: 2;">Produto</div><div style="flex: 2;">Carteira</div><div style="flex: 2;">Tabela SQL</div>
            <div style="flex: 2;">Origem</div><div style="flex: 1;">Status</div><div style="flex: 1;">Ações</div></div>""", unsafe_allow_html=True)
            
            for _, row in df_configs.iterrows():
                c1, c2, c3, c4, c5, c6 = st.columns([2, 2, 2, 2, 1, 1])
                c1.write(row['nome_produto']); c2.write(row['nome_carteira']); c3.code(row['nome_tabela_transacoes'])
                c4.write(row.get('origem_custo', '-')); c5.write(row['status'])
                with c6:
                    if st.button("✏️", key=f"ed_cc_{row['id']}"): dialog_editar_carteira_config(row)
                    if st.button("🗑️", key=f"del_cc_{row['id']}"): 
                        if excluir_carteira_config(row['id'], row['nome_tabela_transacoes']): st.rerun()

    # --- ABA EDIÇÃO DE TABELAS REAIS ---
    with tab_edit_tab:
        st.caption("Edição direta de lançamentos nas tabelas físicas das carteiras.")
        l_tabs = listar_tabelas_transacao_reais()
        if l_tabs:
            t_sel = st.selectbox("Selecione a Tabela", options=l_tabs, key="s_t_e_r")
            if t_sel:
                df_e = carregar_dados_tabela_dinamica(t_sel)
                if not df_e.empty:
                    df_res = st.data_editor(df_e, key=f"ed_{t_sel}", use_container_width=True, hide_index=True, num_rows="dynamic", disabled=["id", "data_transacao"])
                    if st.button("💾 Salvar Alterações na Tabela", key="b_s_p"):
                        if salvar_alteracoes_tabela_dinamica(t_sel, df_e, df_res): st.success("Atualizado!"); time.sleep(1); st.rerun()
                else: st.warning("Tabela vazia.")
        else: st.info("Nenhuma tabela de carteira encontrada.")

    # --- ABA RELATÓRIOS ---
    with tab_rel:
        st.markdown("### 📊 Extrato Unificado")
        conn = get_conn()
        try:
            df_clientes_opt = pd.read_sql("SELECT id, nome FROM admin.clientes ORDER BY nome", conn)
            df_prods = pd.read_sql("SELECT DISTINCT produto_vinculado FROM cliente.extrato_carteira_por_produto", conn)
            lista_produtos = df_prods['produto_vinculado'].dropna().tolist()
        except: 
            df_clientes_opt = pd.DataFrame()
            lista_produtos = []
        finally: 
            if conn: conn.close()

        with st.container(border=True):
            c1, c2, c3 = st.columns(3)
            cli_sel = c1.selectbox("Cliente", options=df_clientes_opt['id'], format_func=lambda x: df_clientes_opt[df_clientes_opt['id']==x]['nome'].values[0] if not df_clientes_opt.empty else "Vazio")
            dates = c2.date_input("Período", value=(date.today().replace(day=1), date.today()))
            prods = c3.multiselect("Filtrar Produtos", options=lista_produtos)
            btn_gerar = st.button("Gerar Relatório", type="primary", use_container_width=True)

        if btn_gerar and cli_sel:
            conn = get_conn()
            try:
                dt_ini, dt_fim = dates if len(dates) == 2 else (dates[0], dates[0])
                q = """
                    SELECT data_lancamento, produto_vinculado, origem_lancamento, tipo_lancamento, valor_lancado, saldo_novo, nome_usuario 
                    FROM cliente.extrato_carteira_por_produto 
                    WHERE id_cliente = %s AND data_lancamento BETWEEN %s AND %s
                """
                params = [str(cli_sel), f"{dt_ini} 00:00:00", f"{dt_fim} 23:59:59"]
                
                if prods:
                    q += f" AND produto_vinculado IN ({','.join(['%s']*len(prods))})"
                    params.extend(prods)
                
                q += " ORDER BY data_lancamento DESC"
                
                df_r = pd.read_sql(q, conn, params=params)
                
                # Busca Saldo Real Atual
                cur = conn.cursor()
                cur.execute("SELECT saldo_novo FROM cliente.extrato_carteira_por_produto WHERE id_cliente = %s ORDER BY id DESC LIMIT 1", (str(cli_sel),))
                res_s = cur.fetchone()
                saldo_real = float(res_s[0]) if res_s else 0.0
                
                if not df_r.empty:
                    df_r['data_lancamento'] = pd.to_datetime(df_r['data_lancamento']).dt.strftime('%d/%m/%Y %H:%M')
                    st.dataframe(df_r, use_container_width=True, hide_index=True)
                    
                    st.divider()
                    k1, k2, k3 = st.columns(3)
                    tot_c = df_r[df_r['tipo_lancamento']=='CREDITO']['valor_lancado'].sum()
                    tot_d = df_r[df_r['tipo_lancamento']=='DEBITO']['valor_lancado'].sum()
                    
                    k1.metric("Total Crédito (Período)", f"R$ {tot_c:,.2f}")
                    k2.metric("Total Débito (Período)", f"R$ {tot_d:,.2f}")
                    k3.metric("Saldo Atual do Cliente", f"R$ {saldo_real:,.2f}")
                else: st.warning("Sem dados no período.")
            except Exception as e: st.error(f"Erro: {e}")
            finally: conn.close()