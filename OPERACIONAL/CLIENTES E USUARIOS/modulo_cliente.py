import streamlit as st
import pandas as pd
import psycopg2
import bcrypt
import re
import time
from datetime import datetime, date, timedelta

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
    except: return None

# =============================================================================
# 1. FUNÇÕES AUXILIARES E DB (GERAL)
# =============================================================================

def formatar_cnpj(v):
    v = re.sub(r'\D', '', str(v))
    return f"{v[:2]}.{v[2:5]}.{v[5:8]}/{v[8:12]}-{v[12:]}" if len(v) == 14 else v

def limpar_formatacao_texto(texto):
    if not texto: return ""
    return str(texto).replace('*', '').strip()

def sanitizar_nome_tabela(nome):
    s = str(nome).lower().strip()
    s = re.sub(r'[^a-z0-9]', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.strip('_')

# --- AGRUPAMENTOS ---
def listar_agrupamentos(tipo):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        df = pd.read_sql(f"SELECT id, nome_agrupamento FROM {tabela} ORDER BY id", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def salvar_agrupamento(tipo, nome):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {tabela} (nome_agrupamento) VALUES (%s)", (nome,))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def excluir_agrupamento(tipo, id_agrup):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {tabela} WHERE id = %s", (id_agrup,))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def atualizar_agrupamento(tipo, id_agrup, novo_nome):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {tabela} SET nome_agrupamento = %s WHERE id = %s", (novo_nome, id_agrup))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

# --- CLIENTE CNPJ ---
def listar_cliente_cnpj():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, cnpj, nome_empresa FROM admin.cliente_cnpj ORDER BY nome_empresa", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def salvar_cliente_cnpj(cnpj, nome):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO admin.cliente_cnpj (cnpj, nome_empresa) VALUES (%s, %s)", (cnpj, nome))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def excluir_cliente_cnpj(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM admin.cliente_cnpj WHERE id = %s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def atualizar_cliente_cnpj(id_reg, cnpj, nome):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.cliente_cnpj SET cnpj=%s, nome_empresa=%s WHERE id=%s", (cnpj, nome, id_reg))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

# --- RELAÇÃO PEDIDO CARTEIRA ---
def listar_relacao_pedido_carteira():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, produto, nome_carteira FROM cliente.cliente_carteira_relacao_pedido_carteira ORDER BY id DESC", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def salvar_relacao_pedido_carteira(produto, carteira):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO cliente.cliente_carteira_relacao_pedido_carteira (produto, nome_carteira) VALUES (%s, %s)", (produto, carteira))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def atualizar_relacao_pedido_carteira(id_reg, produto, carteira):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE cliente.cliente_carteira_relacao_pedido_carteira SET produto=%s, nome_carteira=%s WHERE id=%s", (produto, carteira, id_reg))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def excluir_relacao_pedido_carteira(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM cliente.cliente_carteira_relacao_pedido_carteira WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

# --- CLIENTE CARTEIRA LISTA ---
def listar_cliente_carteira_lista():
    conn = get_conn()
    try:
        query = """
            SELECT 
                l.id, l.cpf_cliente, l.nome_cliente, l.nome_carteira, 
                l.custo_carteira, l.cpf_usuario, l.nome_usuario, l.origem_custo,
                c.nome_tabela_transacoes
            FROM cliente.cliente_carteira_lista l
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
            INSERT INTO cliente.cliente_carteira_lista (cpf_cliente, nome_cliente, nome_carteira, custo_carteira, cpf_usuario, nome_usuario, origem_custo) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (cpf, nome, carteira, custo, cpf_u, nome_u, origem_custo))
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

def atualizar_cliente_carteira_lista(id_reg, cpf, nome, carteira, custo, cpf_u, nome_u, origem_custo):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE cliente.cliente_carteira_lista 
            SET cpf_cliente=%s, nome_cliente=%s, nome_carteira=%s, custo_carteira=%s, cpf_usuario=%s, nome_usuario=%s, origem_custo=%s 
            WHERE id=%s
        """, (cpf, nome, carteira, custo, cpf_u, nome_u, origem_custo, id_reg))
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

def excluir_cliente_carteira_lista(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor(); cur.execute("DELETE FROM cliente.cliente_carteira_lista WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

def listar_origens_para_selecao():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT origem FROM conexoes.fatorconferi_origem_consulta_fator ORDER BY origem ASC")
        res = [row[0] for row in cur.fetchall()]
        conn.close(); return res
    except:
        if conn: conn.close()
        return []

def listar_usuarios_para_selecao():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome, cpf FROM clientes_usuarios WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_clientes_para_selecao():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome, cpf FROM admin.clientes ORDER BY nome", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

# =============================================================================
# 2. GESTÃO DE CARTEIRAS E TRANSAÇÕES (DINÂMICO)
# =============================================================================

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
            try:
                cur.execute("ALTER TABLE cliente.carteiras_config ADD COLUMN IF NOT EXISTS origem_custo VARCHAR(100)")
            except: pass
            
            conn.commit(); conn.close()
        except: 
            if conn: conn.close()

def listar_tabelas_transacao_reais():
    conn = get_conn()
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
    except:
        if conn: conn.close()
        return False

def listar_produtos_para_selecao():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome FROM produtos_servicos WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_todas_carteiras_ativas():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome_carteira, nome_tabela_transacoes FROM cliente.carteiras_config WHERE status = 'ATIVO' ORDER BY nome_carteira", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_carteiras_config():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT * FROM cliente.carteiras_config ORDER BY id DESC", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

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

def atualizar_carteira_config(id_conf, status, nome_carteira=None, origem_custo=None):
    conn = get_conn()
    if conn:
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
    return False

# --- FUNÇÕES DE EXTRATO/TRANSAÇÕES ---

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
        query = f"INSERT INTO {tabela_sql} (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao) VALUES (%s, %s, %s, 'MANUAL', %s, %s, %s, %s, NOW())"
        cur.execute(query, (cpf_cliente, nome_cliente, motivo, tipo_lanc, valor, saldo_anterior, saldo_novo))
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

# =============================================================================
# 3. USUÁRIOS E CLIENTES (VINCULOS E SEGURANÇA)
# =============================================================================

def hash_senha(senha):
    if senha.startswith('$2b$'): return senha
    return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def buscar_usuarios_disponiveis():
    conn = get_conn()
    try:
        query = "SELECT id, nome, email, cpf FROM clientes_usuarios WHERE id NOT IN (SELECT id_usuario_vinculo FROM admin.clientes WHERE id_usuario_vinculo IS NOT NULL) ORDER BY nome"
        df = pd.read_sql(query, conn); conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def vincular_usuario_cliente(id_cliente, id_usuario):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = %s WHERE id = %s", (int(id_usuario), int(id_cliente)))
        conn.commit(); conn.close(); return True, "Vinculado!"
    except Exception as e: 
        conn.close()
        return False, str(e)

def desvincular_usuario_cliente(id_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor(); cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = NULL WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def excluir_cliente_db(id_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor(); cur.execute("DELETE FROM admin.clientes WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def salvar_usuario_novo(nome, email, cpf, tel, senha, hierarquia, ativo):
    conn = get_conn()
    try:
        cur = conn.cursor(); senha_f = hash_senha(senha)
        cur.execute("INSERT INTO clientes_usuarios (nome, email, cpf, telefone, senha, hierarquia, ativo) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", (nome, email, cpf, tel, senha_f, hierarquia, ativo))
        nid = cur.fetchone()[0]; conn.commit(); conn.close(); return nid
    except: 
        if conn: conn.close()
        return None

# =============================================================================
# 4. DIALOGS
# =============================================================================

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

@st.dialog("🔗 Gestão de Acesso do Cliente")
def dialog_gestao_usuario_vinculo(dados_cliente):
    id_vinculo = dados_cliente.get('id_vinculo') or dados_cliente.get('id_usuario_vinculo')
    if id_vinculo:
        st.success("✅ Este cliente já possui um usuário vinculado.")
        conn = get_conn()
        df_u = pd.read_sql(f"SELECT nome, email, telefone, cpf FROM clientes_usuarios WHERE id = {id_vinculo}", conn); conn.close()
        if not df_u.empty:
            usr = df_u.iloc[0]
            st.write(f"**Nome:** {usr['nome']}"); st.write(f"**Login:** {usr['email']}"); st.write(f"**CPF:** {usr['cpf']}")
            st.markdown("---")
            if st.button("🔓 Desvincular Usuário", type="primary"):
                if desvincular_usuario_cliente(dados_cliente['id']): st.success("Desvinculado!"); time.sleep(1.5); st.rerun()
                else: st.error("Erro.")
        else:
            st.warning("Usuário vinculado não encontrado.")
            if st.button("Forçar Desvinculo"): desvincular_usuario_cliente(dados_cliente['id']); st.rerun()
    else:
        st.warning("⚠️ Este cliente não tem acesso ao sistema.")
        tab_novo, tab_existente = st.tabs(["✨ Criar Novo", "🔍 Vincular Existente"])
        with tab_novo:
            with st.form("form_cria_vincula"):
                u_email = st.text_input("Login (Email)", value=dados_cliente['email'])
                u_senha = st.text_input("Senha Inicial", value="1234")
                u_cpf = st.text_input("CPF", value=dados_cliente['cpf'])
                u_nome = st.text_input("Nome", value=limpar_formatacao_texto(dados_cliente['nome']))
                if st.form_submit_button("Criar e Vincular"):
                    novo_id = salvar_usuario_novo(u_nome, u_email, u_cpf, dados_cliente['telefone'], u_senha, 'Cliente', True)
                    if novo_id: 
                        ok, msg = vincular_usuario_cliente(dados_cliente['id'], novo_id)
                        if ok: st.success("Criado e vinculado!"); time.sleep(1); st.rerun()
                        else: st.error(f"Erro ao vincular: {msg}")
                    else: st.error("Erro ao criar usuário.")
        with tab_existente:
            df_livres = buscar_usuarios_disponiveis()
            if not df_livres.empty:
                opcoes = df_livres.apply(lambda x: f"{x['nome']} ({x['email']})", axis=1)
                idx_sel = st.selectbox("Selecione o Usuário", range(len(df_livres)), format_func=lambda x: opcoes[x])
                if st.button("Vincular Selecionado"):
                    ok, msg = vincular_usuario_cliente(dados_cliente['id'], df_livres.iloc[idx_sel]['id'])
                    if ok:
                        st.success("Vinculado com sucesso!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"Erro ao vincular: {msg}")
            else: st.info("Sem usuários livres.")

@st.dialog("🚨 Excluir Cliente")
def dialog_excluir_cliente(id_cli, nome):
    st.error(f"Excluir **{nome}**?"); st.warning("Apenas a ficha cadastral será apagada.")
    c1, c2 = st.columns(2)
    if c1.button("Sim, Excluir"):
        if excluir_cliente_db(id_cli): st.success("Removido."); time.sleep(1); st.session_state['view_cliente'] = 'lista'; st.rerun()
    if c2.button("Cancelar"): st.rerun()

@st.dialog("🔎 Histórico de Consultas", width="large")
def dialog_historico_consultas(cpf_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor(); cur.execute(f"SELECT id FROM clientes_usuarios WHERE cpf = '{cpf_cliente}'"); res = cur.fetchone()
        if res:
            df = pd.read_sql(f"SELECT data_hora, tipo_consulta, cpf_consultado, valor_pago FROM conexoes.fatorconferi_registo_consulta WHERE id_usuario = {res[0]} ORDER BY id DESC LIMIT 200", conn)
            st.dataframe(df, hide_index=True)
        else: st.warning("Nenhum usuário vinculado.")
    except: pass
    finally: conn.close()

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
        
        if st.form_submit_button("💾 Salvar Alterações"):
            if atualizar_transacao_dinamica(tabela_sql, transacao['id'], n_motivo, n_valor, n_tipo):
                st.success("Atualizado com sucesso!")
                time.sleep(1)
                st.rerun()
            else:
                st.error("Erro ao atualizar.")

@st.dialog("🗑️ Excluir Lançamento")
def dialog_excluir_lancamento_extrato(tabela_sql, id_transacao):
    st.warning("Tem certeza que deseja excluir este lançamento?")
    st.caption("Essa ação não recalcula o saldo das transações seguintes automaticamente.")
    
    col_sim, col_nao = st.columns(2)
    if col_sim.button("🚨 Sim, Excluir", use_container_width=True):
        if excluir_transacao_dinamica(tabela_sql, id_transacao):
            st.success("Excluído!")
            time.sleep(1)
            st.rerun()
        else:
            st.error("Erro ao excluir.")
            
    if col_nao.button("Cancelar", use_container_width=True):
        st.rerun()
        
@st.dialog("✏️ Editar Agrupamento")
def dialog_editar_agrupamento(tipo, id_agrup, nome_atual):
    st.caption(f"Editando: {nome_atual}")
    with st.form("form_edit_agrup"):
        novo_nome = st.text_input("Nome", value=nome_atual)
        if st.form_submit_button("💾 Salvar", use_container_width=True):
            if novo_nome:
                if atualizar_agrupamento(tipo, id_agrup, novo_nome):
                    st.success("Atualizado!"); time.sleep(0.5); st.rerun()
                else: st.error("Erro ao atualizar.")

@st.dialog("✏️ Editar Cliente CNPJ")
def dialog_editar_cliente_cnpj(id_reg, cnpj_atual, nome_atual):
    st.caption(f"Editando: {cnpj_atual}")
    with st.form("form_edit_cnpj"):
        n_cnpj = st.text_input("CNPJ", value=cnpj_atual)
        n_nome = st.text_input("Nome Empresa", value=nome_atual)
        if st.form_submit_button("💾 Salvar", use_container_width=True):
            if atualizar_cliente_cnpj(id_reg, n_cnpj, n_nome):
                st.success("Atualizado!"); time.sleep(0.5); st.rerun()
            else: st.error("Erro ao atualizar.")

@st.dialog("✏️ Editar Relação Pedido")
def dialog_editar_relacao_ped_cart(id_reg, prod_atual, cart_atual):
    st.caption("Editando Relação")
    with st.form("form_edit_rel"):
        n_p = st.text_input("Produto", value=prod_atual)
        n_c = st.text_input("Carteira", value=cart_atual)
        if st.form_submit_button("Salvar"):
            if atualizar_relacao_pedido_carteira(id_reg, n_p, n_c): st.success("Ok!"); st.rerun()

# =============================================================================
# NOVA SEÇÃO: FUNÇÕES PARA O SUBMENU PLANILHAS
# =============================================================================

def listar_tabelas_planilhas():
    """
    Lista tabelas do schema 'admin' que começam com 'cliente'
    E todas as tabelas do schema 'cliente'.
    """
    conn = get_conn()
    if not conn: return []
    try:
        cur = conn.cursor()
        query = """
            SELECT table_schema || '.' || table_name 
            FROM information_schema.tables 
            WHERE 
                (table_schema = 'admin' AND table_name LIKE 'cliente%')
                OR 
                (table_schema = 'cliente')
            ORDER BY table_schema, table_name;
        """
        cur.execute(query)
        res = [row[0] for row in cur.fetchall()]
        conn.close()
        return res
    except:
        if conn: conn.close()
        return []

def salvar_alteracoes_planilha_generica(nome_tabela_completo, df_original, df_editado):
    """
    Salva edições genéricas para tabelas completas (ex: admin.clientes)
    """
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        
        # Identifica IDs originais para saber o que foi excluído
        ids_originais = set()
        if 'id' in df_original.columns:
            ids_originais = set(df_original['id'].dropna().astype(int).tolist())
        
        ids_editados_atuais = set()
        for _, row in df_editado.iterrows():
            if 'id' in row and pd.notna(row['id']) and row['id'] != '':
                try: ids_editados_atuais.add(int(row['id']))
                except: pass

        # 1. DELETE (IDs que estavam no original mas não estão no editado)
        ids_del = ids_originais - ids_editados_atuais
        if ids_del:
            ids_str = ",".join(map(str, ids_del))
            cur.execute(f"DELETE FROM {nome_tabela_completo} WHERE id IN ({ids_str})")

        # 2. UPDATE e INSERT
        for index, row in df_editado.iterrows():
            colunas_db = [c for c in row.index if c not in ['data_criacao', 'data_registro']]
            
            row_id = row.get('id')
            eh_novo = pd.isna(row_id) or row_id == '' or row_id is None
            
            valores = [row[c] for c in colunas_db if c != 'id']
            
            if eh_novo:
                cols_str = ", ".join([c for c in colunas_db if c != 'id'])
                placeholders = ", ".join(["%s"] * len(valores))
                if cols_str:
                    cur.execute(f"INSERT INTO {nome_tabela_completo} ({cols_str}) VALUES ({placeholders})", valores)
            elif int(row_id) in ids_originais:
                set_clause = ", ".join([f"{c} = %s" for c in colunas_db if c != 'id'])
                valores_update = valores + [int(row_id)]
                if set_clause:
                    cur.execute(f"UPDATE {nome_tabela_completo} SET {set_clause} WHERE id = %s", valores_update)
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar tabela {nome_tabela_completo}: {e}")
        if conn: conn.close()
        return False

# =============================================================================
# 5. INTERFACE PRINCIPAL (ATUALIZADA)
# =============================================================================

def app_clientes():
    garantir_tabela_config_carteiras()
    st.markdown("## 👥 Central de Clientes e Usuários")
    
    # Adicionada a aba "Planilhas" ao final
    tab_cli, tab_user, tab_param, tab_carteira, tab_rel, tab_plan = st.tabs(["🏢 Clientes", "👤 Usuários", "⚙️ Parâmetros", "💼 Carteira", "📊 Relatórios", "📅 Planilhas"])

    # --- ABA CLIENTES ---
    with tab_cli:
        c1, c2 = st.columns([6, 1])
        filtro = c1.text_input("🔍 Buscar Cliente", placeholder="Nome, CPF ou Nome Empresa")
        if c2.button("➕ Novo", type="primary"): st.session_state['view_cliente'] = 'novo'; st.rerun()

        if st.session_state.get('view_cliente', 'lista') == 'lista':
            conn = get_conn()
            sql = "SELECT *, id_usuario_vinculo as id_vinculo FROM admin.clientes"
            if filtro: sql += f" WHERE nome ILIKE '%%{filtro}%%' OR cpf ILIKE '%%{filtro}%%' OR nome_empresa ILIKE '%%{filtro}%%'"
            sql += " ORDER BY id DESC LIMIT 50"
            df_cli = pd.read_sql(sql, conn); conn.close()

            if not df_cli.empty:
                st.markdown("""
                <div style="display:flex; font-weight:bold; color:#555; padding:8px; border-bottom:2px solid #ddd; margin-bottom:10px; background-color:#f8f9fa;">
                    <div style="flex:3;">Nome</div>
                    <div style="flex:2;">CPF</div>
                    <div style="flex:2;">Empresa</div>
                    <div style="flex:1;">Status</div>
                    <div style="flex:2; text-align:center;">Ações</div>
                </div>
                """, unsafe_allow_html=True)
                
                for _, row in df_cli.iterrows():
                    with st.container():
                        c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 1, 2])
                        c1.write(f"**{limpar_formatacao_texto(row['nome'])}**")
                        c2.write(row['cpf'] or "-")
                        c3.write(row['nome_empresa'] or "-")
                        cor_st = 'green' if row.get('status','ATIVO')=='ATIVO' else 'red'
                        c4.markdown(f":{cor_st}[{row.get('status','ATIVO')}]")
                        
                        with c5:
                            b1, b2, b3, b4 = st.columns(4)
                            if b1.button("✏️", key=f"e_{row['id']}", help="Editar Cadastro"): 
                                st.session_state.update({'view_cliente': 'editar', 'cli_id': row['id']}); st.rerun()
                            
                            if b2.button("📜", key=f"ext_{row['id']}", help="Ver Extrato"):
                                if st.session_state.get('extrato_expandido') == row['id']:
                                    st.session_state['extrato_expandido'] = None
                                else:
                                    st.session_state['extrato_expandido'] = row['id']
                                    st.session_state['pag_hist'] = 1 
                                st.rerun()
                                
                            if b3.button("🔗" if row['id_vinculo'] else "👤", key=f"u_{row['id']}", help="Acesso Usuário"): 
                                dialog_gestao_usuario_vinculo(row)
                                
                            if b4.button("🗑️", key=f"d_{row['id']}", help="Excluir"):
                                dialog_excluir_cliente(row['id'], row['nome'])
                        
                        st.markdown("<hr style='margin: 5px 0; border-color: #eee;'>", unsafe_allow_html=True)

                        # --- ÁREA EXPANSÍVEL DO EXTRATO ---
                        if st.session_state.get('extrato_expandido') == row['id']:
                            with st.container(border=True):
                                st.markdown(f"#### 📜 Extrato Financeiro: {row['nome']}")
                                st.caption(f"CPF: {row.get('cpf', '-')}")
                                
                                df_carteiras = listar_todas_carteiras_ativas()
                                
                                if not df_carteiras.empty:
                                    col_sel, col_btn_c, col_btn_d, col_vazio = st.columns([4, 1.5, 1.5, 3])
                                    
                                    opcoes_cart = df_carteiras.apply(lambda x: f"{x['nome_carteira']}", axis=1)
                                    idx_sel = col_sel.selectbox("Selecione a Carteira", range(len(df_carteiras)), format_func=lambda x: opcoes_cart[x], key=f"sel_cart_{row['id']}", label_visibility="visible")
                                    
                                    carteira_sel = df_carteiras.iloc[idx_sel]
                                    tabela_sql = carteira_sel['nome_tabela_transacoes']
                                    cpf_limpo = str(row.get('cpf', '')).strip()
                                    
                                    if col_btn_c.button("➕ Crédito", key=f"cred_{row['id']}"):
                                        dialog_lancamento_manual(tabela_sql, cpf_limpo, row['nome'], "CREDITO")
                                    
                                    if col_btn_d.button("➖ Débito", key=f"deb_{row['id']}"):
                                        dialog_lancamento_manual(tabela_sql, cpf_limpo, row['nome'], "DEBITO")
                                    
                                    st.write("") 
                                    fd1, fd2 = st.columns(2)
                                    data_ini = fd1.date_input("Data Inicial", value=date.today() - timedelta(days=30), key=f"ini_{row['id']}")
                                    data_fim = fd2.date_input("Data Final", value=date.today(), key=f"fim_{row['id']}")
                                    
                                    df_extrato = buscar_transacoes_carteira_filtrada(tabela_sql, cpf_limpo, data_ini, data_fim)
                                    
                                    if not df_extrato.empty:
                                        st.markdown("""
                                        <div style="display: flex; font-weight: bold; background-color: #e9ecef; padding: 5px; border-radius: 4px; font-size:0.9em; margin-top:10px;">
                                            <div style="flex: 2;">Data</div>
                                            <div style="flex: 3;">Motivo</div>
                                            <div style="flex: 1;">Tipo</div>
                                            <div style="flex: 1.5;">Valor</div>
                                            <div style="flex: 1.5;">Saldo</div>
                                            <div style="flex: 1; text-align: center;">Ações</div>
                                        </div>
                                        """, unsafe_allow_html=True)
                                        
                                        for _, tr in df_extrato.iterrows():
                                            tc1, tc2, tc3, tc4, tc5, tc6 = st.columns([2, 3, 1, 1.5, 1.5, 1])
                                            tc1.write(pd.to_datetime(tr['data_transacao']).strftime('%d/%m/%y %H:%M'))
                                            tc2.write(tr['motivo'])
                                            
                                            cor_t = "green" if tr['tipo_lancamento'] == 'CREDITO' else "red"
                                            tc3.markdown(f":{cor_t}[{tr['tipo_lancamento']}]")
                                            tc4.write(f"R$ {float(tr['valor']):.2f}")
                                            tc5.write(f"R$ {float(tr['saldo_novo']):.2f}")
                                            
                                            with tc6:
                                                bc1, bc2 = st.columns(2)
                                                if bc1.button("✏️", key=f"ed_tr_{tr['id']}", help="Editar Lançamento"):
                                                    dialog_editar_lancamento_extrato(tabela_sql, tr)
                                                if bc2.button("🗑️", key=f"del_tr_{tr['id']}", help="Excluir Lançamento"):
                                                    dialog_excluir_lancamento_extrato(tabela_sql, tr['id'])
                                            
                                            st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
                                    else:
                                        st.warning("Nenhuma movimentação encontrada no período.")
                                else:
                                    st.info("Nenhuma carteira configurada no sistema.")

            else: st.info("Nenhum cliente encontrado.")

        elif st.session_state['view_cliente'] in ['novo', 'editar']:
            st.markdown(f"### {'📝 Novo' if st.session_state['view_cliente']=='novo' else '✏️ Editar'}")
            
            # --- CARREGA DADOS PRÉVIOS (EDITAR) ---
            dados = {}
            if st.session_state['view_cliente'] == 'editar':
                conn = get_conn(); df = pd.read_sql(f"SELECT * FROM admin.clientes WHERE id = {st.session_state['cli_id']}", conn); conn.close()
                if not df.empty: dados = df.iloc[0]

            # --- CARREGA LISTAS PARA OS SELETORES ---
            df_empresas = listar_cliente_cnpj() # Tabela: admin.cliente_cnpj
            df_ag_cli = listar_agrupamentos("cliente")
            df_ag_emp = listar_agrupamentos("empresa")

            with st.form("form_cliente"):
                c1, c2, c3 = st.columns(3)
                nome = c1.text_input("Nome Completo *", value=limpar_formatacao_texto(dados.get('nome', '')))
                
                lista_empresas = df_empresas['nome_empresa'].unique().tolist()
                idx_emp = 0
                val_emp_atual = dados.get('nome_empresa', '')
                if val_emp_atual in lista_empresas: idx_emp = lista_empresas.index(val_emp_atual)
                
                nome_emp = c2.selectbox("Empresa (Selecionar)", options=[""] + lista_empresas, index=idx_emp + 1 if val_emp_atual else 0, help="Ao selecionar, o CNPJ será preenchido automaticamente ao salvar.")
                cnpj_display = dados.get('cnpj_empresa', '')
                c3.text_input("CNPJ (Vinculado)", value=cnpj_display, disabled=True, help="Este campo é atualizado automaticamente com base na Empresa selecionada.")

                c4, c5, c6, c7 = st.columns(4)
                email = c4.text_input("E-mail *", value=dados.get('email', ''))
                cpf = c5.text_input("CPF *", value=dados.get('cpf', ''))
                tel1 = c6.text_input("Telefone 1", value=dados.get('telefone', ''))
                tel2 = c7.text_input("Telefone 2", value=dados.get('telefone2', ''))
                
                c8, c9, c10 = st.columns([1, 1, 1])
                id_gp = c8.text_input("ID Grupo WhatsApp", value=dados.get('id_grupo_whats', ''))
                
                padrao_cli = []
                if dados.get('ids_agrupamento_cliente'):
                    try: padrao_cli = [int(x.strip()) for x in str(dados.get('ids_agrupamento_cliente')).split(',') if x.strip().isdigit()]
                    except: pass
                sel_ag_cli = c9.multiselect("Agrupamento Cliente", options=df_ag_cli['id'], format_func=lambda x: df_ag_cli[df_ag_cli['id']==x]['nome_agrupamento'].values[0] if not df_ag_cli[df_ag_cli['id']==x].empty else x, default=[x for x in padrao_cli if x in df_ag_cli['id'].values])

                padrao_emp = []
                if dados.get('ids_agrupamento_empresa'):
                    try: padrao_emp = [int(x.strip()) for x in str(dados.get('ids_agrupamento_empresa')).split(',') if x.strip().isdigit()]
                    except: pass
                sel_ag_emp = c10.multiselect("Agrupamento Empresa", options=df_ag_emp['id'], format_func=lambda x: df_ag_emp[df_ag_emp['id']==x]['nome_agrupamento'].values[0] if not df_ag_emp[df_ag_emp['id']==x].empty else x, default=[x for x in padrao_emp if x in df_ag_emp['id'].values])
                
                status_final = "ATIVO"
                if st.session_state['view_cliente'] == 'editar':
                    st.divider(); cs1, _ = st.columns([1, 4])
                    status_final = cs1.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if dados.get('status','ATIVO')=="ATIVO" else 1)

                st.markdown("<br>", unsafe_allow_html=True); ca = st.columns([1, 1, 4])
                
                if ca[0].form_submit_button("💾 Salvar"):
                    cnpj_final = ""
                    if nome_emp:
                        filtro_cnpj = df_empresas[df_empresas['nome_empresa'] == nome_emp]
                        if not filtro_cnpj.empty: cnpj_final = filtro_cnpj.iloc[0]['cnpj']
                    
                    str_ag_cli = ",".join(map(str, sel_ag_cli))
                    str_ag_emp = ",".join(map(str, sel_ag_emp))

                    conn = get_conn(); cur = conn.cursor()
                    if st.session_state['view_cliente'] == 'novo':
                        cur.execute("INSERT INTO admin.clientes (nome, nome_empresa, cnpj_empresa, email, cpf, telefone, telefone2, id_grupo_whats, ids_agrupamento_cliente, ids_agrupamento_empresa, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ATIVO')", (nome, nome_emp, cnpj_final, email, cpf, tel1, tel2, id_gp, str_ag_cli, str_ag_emp))
                    else:
                        cur.execute("UPDATE admin.clientes SET nome=%s, nome_empresa=%s, cnpj_empresa=%s, email=%s, cpf=%s, telefone=%s, telefone2=%s, id_grupo_whats=%s, ids_agrupamento_cliente=%s, ids_agrupamento_empresa=%s, status=%s WHERE id=%s", (nome, nome_emp, cnpj_final, email, cpf, tel1, tel2, id_gp, str_ag_cli, str_ag_emp, status_final, st.session_state['cli_id']))
                    conn.commit(); conn.close(); st.success("Salvo!"); time.sleep(1); st.session_state['view_cliente'] = 'lista'; st.rerun()
                
                if ca[1].form_submit_button("Cancelar"): st.session_state['view_cliente'] = 'lista'; st.rerun()

            if st.session_state['view_cliente'] == 'editar':
                st.markdown("---")
                if st.button("🗑️ Excluir Cliente", type="primary"): dialog_excluir_cliente(st.session_state['cli_id'], nome)

    # --- ABA USUÁRIOS ---
    with tab_user:
        st.markdown("### Gestão de Acesso")
        busca_user = st.text_input("Buscar Usuário", placeholder="Nome ou Email")
        conn = get_conn(); sql_u = "SELECT id, nome, email, hierarquia, ativo, telefone, id_grupo_whats FROM clientes_usuarios WHERE 1=1"
        if busca_user: sql_u += f" AND (nome ILIKE '%{busca_user}%' OR email ILIKE '%{busca_user}%')"
        sql_u += " ORDER BY id DESC"
        df_users = pd.read_sql(sql_u, conn); conn.close()
        for _, u in df_users.iterrows():
            with st.expander(f"{u['nome']} ({u['hierarquia']})"):
                with st.form(f"form_user_{u['id']}"):
                    c_n, c_e = st.columns(2); n_nome = c_n.text_input("Nome", value=u['nome']); n_mail = c_e.text_input("Email", value=u['email'])
                    c_h, c_s, c_a = st.columns(3); n_hier = c_h.selectbox("Nível", ["Cliente", "Admin", "Gerente"], index=["Cliente", "Admin", "Gerente"].index(u['hierarquia']) if u['hierarquia'] in ["Cliente", "Admin", "Gerente"] else 0); n_senha = c_s.text_input("Nova Senha", type="password"); n_ativo = c_a.checkbox("Ativo", value=u['ativo'])
                    if st.form_submit_button("Atualizar"):
                        conn = get_conn(); cur = conn.cursor()
                        if n_senha: cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, hierarquia=%s, senha=%s, ativo=%s WHERE id=%s", (n_nome, n_mail, n_hier, hash_senha(n_senha), n_ativo, u['id']))
                        else: cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, hierarquia=%s, ativo=%s WHERE id=%s", (n_nome, n_mail, n_hier, n_ativo, u['id']))
                        conn.commit(); conn.close(); st.success("Atualizado!"); st.rerun()

    # --- ABA PARÂMETROS ---
    with tab_param:
        
        # 1. AGRUPAMENTO CLIENTES
        with st.expander("🏷️ Agrupamento Clientes", expanded=False):
            st.markdown("<p style='color: lightblue; font-size: 12px; margin-bottom: 5px;'>Tabela SQL: admin.agrupamento_clientes</p>", unsafe_allow_html=True)
            with st.container(border=True):
                st.caption("Novo Item")
                c_in, c_bt = st.columns([5, 1])
                n_ac = c_in.text_input("Nome", key="in_ac", label_visibility="visible", placeholder="Digite o nome...")
                c_bt.write(""); c_bt.write("") 
                if c_bt.button("➕", key="add_ac", use_container_width=True):
                    if n_ac: salvar_agrupamento("cliente", n_ac); st.rerun()
            
            st.divider()
            df_ac = listar_agrupamentos("cliente")
            if not df_ac.empty:
                for _, r in df_ac.iterrows():
                    ca1, ca2, ca3 = st.columns([8, 1, 1]) 
                    ca1.markdown(f"**{r['id']}** | {r['nome_agrupamento']}")
                    if ca2.button("✏️", key=f"ed_ac_{r['id']}"): dialog_editar_agrupamento("cliente", r['id'], r['nome_agrupamento'])
                    if ca3.button("🗑️", key=f"del_ac_{r['id']}"): excluir_agrupamento("cliente", r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

        # 2. AGRUPAMENTO EMPRESAS
        with st.expander("🏢 Agrupamento Empresas", expanded=False):
            st.markdown("<p style='color: lightblue; font-size: 12px; margin-bottom: 5px;'>Tabela SQL: admin.agrupamento_empresas</p>", unsafe_allow_html=True)
            with st.container(border=True):
                st.caption("Novo Item")
                c_in, c_bt = st.columns([5, 1])
                n_ae = c_in.text_input("Nome", key="in_ae", label_visibility="visible", placeholder="Digite o nome...")
                c_bt.write(""); c_bt.write("")
                if c_bt.button("➕", key="add_ae", use_container_width=True):
                    if n_ae: salvar_agrupamento("empresa", n_ae); st.rerun()
            
            st.divider()
            df_ae = listar_agrupamentos("empresa")
            if not df_ae.empty:
                for _, r in df_ae.iterrows():
                    ca1, ca2, ca3 = st.columns([8, 1, 1])
                    ca1.markdown(f"**{r['id']}** | {r['nome_agrupamento']}")
                    if ca2.button("✏️", key=f"ed_ae_{r['id']}"): dialog_editar_agrupamento("empresa", r['id'], r['nome_agrupamento'])
                    if ca3.button("🗑️", key=f"del_ae_{r['id']}"): excluir_agrupamento("empresa", r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

        # 3. CLIENTE CNPJ
        with st.expander("💼 Cliente CNPJ", expanded=False):
            st.markdown("<p style='color: lightblue; font-size: 12px; margin-bottom: 5px;'>Tabela SQL: admin.cliente_cnpj</p>", unsafe_allow_html=True)
            with st.container(border=True):
                st.caption("Novo Cadastro")
                c_inp1, c_inp2, c_bt = st.columns([2, 3, 1])
                n_cnpj = c_inp1.text_input("CNPJ", key="n_cnpj", placeholder="00.000...", label_visibility="visible")
                n_emp = c_inp2.text_input("Nome Empresa", key="n_emp", placeholder="Razão Social", label_visibility="visible")
                c_bt.write(""); c_bt.write("")
                if c_bt.button("➕", key="add_cnpj", use_container_width=True):
                    if n_cnpj and n_emp: salvar_cliente_cnpj(n_cnpj, n_emp); st.rerun()
            
            st.divider()
            df_cnpj = listar_cliente_cnpj()
            if not df_cnpj.empty:
                for _, r in df_cnpj.iterrows():
                    cc1, cc2, cc3 = st.columns([8, 1, 1])
                    cc1.markdown(f"**{r['cnpj']}** | {r['nome_empresa']}")
                    if cc2.button("✏️", key=f"ed_cn_{r['id']}"): dialog_editar_cliente_cnpj(r['id'], r['cnpj'], r['nome_empresa'])
                    if cc3.button("🗑️", key=f"del_cn_{r['id']}"): excluir_cliente_cnpj(r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

        # 4. RELAÇÃO PEDIDO X CARTEIRA
        with st.expander("🔗 Relação Pedido/Carteira", expanded=False):
            st.markdown("<p style='color: lightblue; font-size: 12px; margin-bottom: 5px;'>Tabela SQL: cliente.cliente_carteira_relacao_pedido_carteira</p>", unsafe_allow_html=True)
            with st.container(border=True):
                st.caption("Novo Vínculo")
                c_rp1, c_rp2, c_bt = st.columns([2, 2, 1])
                n_prod = c_rp1.text_input("Nome Produto", key="n_prod_rel", placeholder="Ex: Produto A", label_visibility="visible")
                n_cart = c_rp2.text_input("Nome Carteira", key="n_cart_rel", placeholder="Ex: Carteira 2024", label_visibility="visible")
                
                c_bt.write(""); c_bt.write("") 
                if c_bt.button("➕", key="add_rel_pc", use_container_width=True):
                    if n_prod and n_cart: salvar_relacao_pedido_carteira(n_prod, n_cart); st.rerun()
            
            st.divider()
            df_rel_pc = listar_relacao_pedido_carteira()
            if not df_rel_pc.empty:
                for _, r in df_rel_pc.iterrows():
                    cc1, cc2, cc3 = st.columns([8, 1, 1])
                    cc1.markdown(f"**{r['produto']}** -> {r['nome_carteira']}")
                    if cc2.button("✏️", key=f"ed_rpc_{r['id']}"): dialog_editar_relacao_ped_cart(r['id'], r['produto'], r['nome_carteira'])
                    if cc3.button("🗑️", key=f"del_rpc_{r['id']}"): excluir_relacao_pedido_carteira(r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

        # 5. LISTA DE CARTEIRAS (ATUALIZADO COM SELETOR DE CLIENTE E ORIGEM)
        with st.expander("📂 Lista de Carteiras", expanded=False):
            st.markdown("<p style='color: lightblue; font-size: 12px; margin-bottom: 5px;'>Tabela SQL: cliente.cliente_carteira_lista</p>", unsafe_allow_html=True)
            with st.container(border=True):
                st.caption("Nova Carteira")
                c1, c2, c3, c4, c5 = st.columns([1.5, 2, 1.5, 1, 1])
                
                # --- MUDANÇA: Seletor de Cliente com CPF ---
                df_clis = listar_clientes_para_selecao()
                n_sel = c2.selectbox("Cliente", options=[""] + df_clis.apply(lambda x: f"{x['nome']} | CPF: {x['cpf']}", axis=1).tolist(), key="n_sel_l")
                cpf_auto = n_sel.split(" | CPF: ")[1] if n_sel else ""
                n_cpf = c1.text_input("CPF", value=cpf_auto, key="n_cpf_l")
                # -------------------------------------------
                
                # --- ATUALIZADO: Incluindo Produto na seleção ---
                df_prods = listar_produtos_para_selecao()
                n_prod_sel = c3.selectbox("Produto", options=[""] + df_prods['nome'].tolist(), key="n_prod_l")

                df_c_at = listar_todas_carteiras_ativas()
                n_cart = c3.selectbox("Carteira", options=[""] + df_c_at['nome_carteira'].tolist(), key="n_cart_l")
                
                n_val = c4.number_input("Custo", key="n_val_l", step=0.01)
                
                # --- MUDANÇA: Seletor de Origem de Custo ---
                orgs = listar_origens_para_selecao()
                n_org = st.selectbox("Origem Custo", options=[""] + orgs, key="n_org_l")
                # -------------------------------------------
                
                if c5.button("➕", key="add_cl_btn"):
                    if n_cpf and n_cart:
                        nome_cli_clean = n_sel.split(" | ")[0] if n_sel else ""
                        if salvar_cliente_carteira_lista(n_cpf, nome_cli_clean, n_cart, n_val, n_org): st.rerun()
                    else:
                        st.warning("Preencha CPF e Carteira.")

            df_l = listar_cliente_carteira_lista()
            if not df_l.empty:
                # ADICIONADA COLUNA CPF, PRODUTO E TABELA SQL NA VISUALIZAÇÃO
                st.markdown("""<div style="display: flex; font-weight: bold; background: #f0f2f6; padding: 8px; font-size: 0.85em;"><div style="flex: 2;">Cliente</div><div style="flex: 1.5;">CPF</div><div style="flex: 2;">Carteira</div><div style="flex: 2;">Tabela SQL</div><div style="flex: 1;">Custo</div><div style="flex: 1.5;">Origem</div><div style="flex: 1.5;">Usuário</div><div style="flex: 1; text-align: center;">Ações</div></div>""", unsafe_allow_html=True)
                for _, r in df_l.iterrows():
                    c1, c2, c3, c4, c5, c6, c7, c8 = st.columns([2, 1.5, 2, 2, 1, 1.5, 1.5, 1])
                    c1.write(r['nome_cliente'])
                    c2.write(r['cpf_cliente']) 
                    c3.write(r['nome_carteira'])
                    # Exibe Tabela SQL com caption
                    c4.caption(r.get('nome_tabela_transacoes', '-')) 
                    c5.write(f"R$ {float(r['custo_carteira'] or 0):.2f}")
                    c6.write(r.get('origem_custo', '-'))
                    c7.write(r.get('nome_usuario', '-'))
                    
                    with c8:
                        if st.button("✏️", key=f"ed_cl_{r['id']}"): dialog_editar_cart_lista(r)
                        if st.button("🗑️", key=f"de_cl_{r['id']}"): excluir_cliente_carteira_lista(r['id']); st.rerun()

        # 6. CONFIGURAÇÃO DE CARTEIRAS (NOVO BLOCO)
        with st.expander("⚙️ Configurações de Carteiras", expanded=False):
            st.markdown("<p style='color: lightblue; font-size: 12px; margin-bottom: 5px;'>Tabela SQL: cliente.carteiras_config</p>", unsafe_allow_html=True)
            st.info("Aqui você pode visualizar e editar as configurações de carteiras (tabela: cliente.carteiras_config).")
            df_configs = listar_carteiras_config()
            if not df_configs.empty:
                st.markdown("""<div style="display: flex; font-weight: bold; background: #f0f2f6; padding: 8px; font-size: 0.9em;"><div style="flex: 2;">Produto</div><div style="flex: 2;">Carteira</div><div style="flex: 2;">Tabela SQL</div><div style="flex: 2;">Origem Custo</div><div style="flex: 1;">Status</div><div style="flex: 1; text-align: center;">Ações</div></div>""", unsafe_allow_html=True)
                for _, row in df_configs.iterrows():
                    c1, c2, c3, c4, c5, c6 = st.columns([2, 2, 2, 2, 1, 1])
                    c1.write(row['nome_produto']); c2.write(row['nome_carteira']); c3.code(row['nome_tabela_transacoes']); c4.write(row.get('origem_custo', '-')); c5.write(row['status'])
                    with c6:
                        if st.button("✏️", key=f"ed_cc_{row['id']}"): dialog_editar_carteira_config(row)
                        if st.button("🗑️", key=f"del_cc_{row['id']}"): 
                            if excluir_carteira_config(row['id'], row['nome_tabela_transacoes']): st.rerun()
            else:
                st.info("Nenhuma carteira configurada no sistema.")

    with tab_carteira: # Gestão de Carteira e Tabelas Reais
        st.markdown("### 💼 Gestão de Carteira")
        
        # --- MUDANÇA: Nova Carteira com Origem Custo ---
        with st.expander("📂 Nova Carteira (Produtos)", expanded=False):
            st.info("Cria carteiras e tabelas automaticamente.")
            df_pds = listar_produtos_para_selecao()
            if not df_pds.empty:
                with st.container(border=True):
                    cc1, cc2, cc3, cc4, cc5 = st.columns([2, 2, 2, 2, 2])
                    idx_p = cc1.selectbox("Produto", range(len(df_pds)), format_func=lambda x: df_pds.iloc[x]['nome'])
                    n_cart_in = cc2.text_input("Nome Carteira", key="n_c_n")
                    
                    # --- Campo Origem Custo ---
                    orgs = listar_origens_para_selecao()
                    origem_cart_in = cc3.selectbox("Origem Custo", options=[""] + orgs, key="org_c_new")
                    # --------------------------
                    
                    stt_in = cc4.selectbox("Status", ["ATIVO", "INATIVO"], key="s_c_n")
                    
                    if cc5.button("💾 Criar", key="b_c_c"):
                        if n_cart_in: 
                            salvar_nova_carteira_sistema(int(df_pds.iloc[idx_p]['id']), df_pds.iloc[idx_p]['nome'], n_cart_in, stt_in, origem_cart_in)
                            st.rerun()

        st.divider()
        st.markdown("#### 📑 Edição de Conteúdo das Tabelas")
        st.caption("Selecione uma tabela física para editar os lançamentos diretamente.")
        
        l_tabs = listar_tabelas_transacao_reais()
        if l_tabs:
            t_sel = st.selectbox("Escolha a Tabela Física", options=l_tabs, key="s_t_e_r")
            if t_sel:
                df_e = carregar_dados_tabela_dinamica(t_sel)
                if not df_e.empty:
                    st.info(f"Editando: `cliente.{t_sel}`")
                    df_res = st.data_editor(
                        df_e, 
                        key=f"ed_{t_sel}", 
                        use_container_width=True, 
                        hide_index=True,
                        num_rows="dynamic",
                        disabled=["id", "data_transacao"]
                    )
                    if st.button("💾 Salvar Planilha", key="b_s_p"):
                        if salvar_alteracoes_tabela_dinamica(t_sel, df_e, df_res): st.success("Atualizado!"); time.sleep(1); st.rerun()
                else: st.warning("Tabela sem dados.")
        else: st.info("Nenhuma tabela de transação encontrada.")

    with tab_rel:
        st.markdown("### 📊 Relatórios")
        conn = get_conn(); opts = pd.read_sql("SELECT id, nome, cpf FROM admin.clientes ORDER BY nome", conn); conn.close()
        sel = st.selectbox("Cliente", opts['id'], format_func=lambda x: opts[opts['id']==x]['nome'].values[0])
        if sel:
            row = opts[opts['id']==sel].iloc[0]; st.divider(); c1, c2 = st.columns(2)
            with c1:
                st.info("💰 Saldo Fator")
                try:
                    conn = get_conn(); id_c = pd.read_sql(f"SELECT id FROM conexoes.fator_cliente_carteira WHERE id_cliente_admin = {sel}", conn).iloc[0]['id']
                    st.dataframe(pd.read_sql(f"SELECT data_transacao, tipo, valor, saldo_novo FROM conexoes.fator_cliente_transacoes WHERE id_carteira = {id_c} ORDER BY id DESC LIMIT 20", conn), hide_index=True)
                    conn.close()
                except: st.warning("Sem carteira.")
            with c2:
                st.info("🔎 Consultas"); 
                if st.button("Ver Histórico"): dialog_historico_consultas(row['cpf'])

    # =========================================================================
    # --- NOVA ABA: PLANILHAS ---
    # =========================================================================
    with tab_plan:
        st.markdown("### 📅 Gestão de Planilhas do Banco")
        st.caption("Visualização e edição direta de tabelas (Admin: 'cliente...' e Schema: 'cliente')")
        
        # 1. Carregar lista de tabelas disponíveis
        lista_tabelas = listar_tabelas_planilhas()
        
        if lista_tabelas:
            col_sel, col_info = st.columns([1, 2])
            tabela_selecionada = col_sel.selectbox("Selecione a Tabela", lista_tabelas)
            
            if tabela_selecionada:
                conn = get_conn()
                if conn:
                    try:
                        # Carregar dados
                        st.markdown(f"**Editando:** `{tabela_selecionada}`")
                        # Limite de segurança ou paginação pode ser adicionado se as tabelas forem gigantes
                        df_tabela = pd.read_sql(f"SELECT * FROM {tabela_selecionada} ORDER BY id DESC LIMIT 1000", conn)
                        conn.close()
                        
                        # Definir colunas travadas (normalmente ID não se edita)
                        cols_travadas = ["data_criacao", "data_registro"]
                        if 'id' in df_tabela.columns:
                            # Se quiser travar o ID: cols_travadas.append("id")
                            # Se quiser permitir criar linhas novas, o ID deve ser gerado pelo banco (SERIAL), 
                            # então deixamos o ID visível mas inativo para inserção manual geralmente, 
                            # ou ocultamos. O data_editor lida bem com IDs se configurado.
                            pass

                        # Editor
                        df_editado = st.data_editor(
                            df_tabela,
                            key=f"editor_planilha_{tabela_selecionada}",
                            num_rows="dynamic",
                            use_container_width=True,
                            disabled=cols_travadas
                        )
                        
                        # Botão Salvar
                        if st.button("💾 Salvar Alterações na Planilha", type="primary"):
                            with st.spinner("Salvando..."):
                                if salvar_alteracoes_planilha_generica(tabela_selecionada, df_tabela, df_editado):
                                    st.success("Tabela atualizada com sucesso!")
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("Erro ao salvar alterações. Verifique os logs.")
                    except Exception as e:
                        st.error(f"Erro ao ler tabela: {e}")
                        if conn: conn.close()
        else:
            st.warning("Nenhuma tabela encontrada com os critérios (Admin 'cliente...' ou schema 'Cliente').")

if __name__ == "__main__":
    app_clientes()