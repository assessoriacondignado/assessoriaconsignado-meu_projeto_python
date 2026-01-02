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
# 1. FUNÇÕES AUXILIARES E DB (ORIGINAIS MANTIDAS)
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
        df = pd.read_sql("SELECT id, cpf_cliente, nome_cliente, nome_carteira, custo_carteira, cpf_usuario, nome_usuario, origem_custo FROM cliente.cliente_carteira_lista ORDER BY nome_cliente", conn)
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
        for index, row in df_editado.iterrows():
            colunas = row.index.tolist()
            if 'id' in colunas: colunas.remove('id')
            set_clause = ", ".join([f"{col} = %s" for col in colunas])
            valores = [row[col] for col in colunas] + [row['id']]
            cur.execute(f"UPDATE cliente.{nome_tabela} SET {set_clause} WHERE id = %s", valores)
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

def salvar_nova_carteira_sistema(id_prod, nome_prod, nome_carteira, status):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        sufixo = sanitizar_nome_tabela(nome_carteira)
        nome_tab = f"cliente.transacoes_{sufixo}"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {nome_tab} (id SERIAL PRIMARY KEY, cpf_cliente VARCHAR(20), nome_cliente VARCHAR(255), motivo VARCHAR(255), origem_lancamento VARCHAR(100), data_transacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP, tipo_lancamento VARCHAR(50), valor NUMERIC(10, 2), saldo_anterior NUMERIC(10, 2), saldo_novo NUMERIC(10, 2))")
        cur.execute("INSERT INTO cliente.carteiras_config (id_produto, nome_produto, nome_carteira, nome_tabela_transacoes, status) VALUES (%s, %s, %s, %s, %s)", (id_prod, nome_prod, nome_carteira, nome_tab, status))
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

# =============================================================================
# 3. USUÁRIOS E CLIENTES (VÍNCULOS E SEGURANÇA)
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
        if conn: conn.close()
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
# 4. DIALOGS (MANTIDOS E ATUALIZADOS)
# =============================================================================

@st.dialog("✏️ Editar Carteira Cliente")
def dialog_editar_cart_lista(dados):
    st.write(f"Editando: **{dados['nome_cliente']}**")
    df_u = listar_usuarios_para_selecao()
    op_u = [""] + df_u.apply(lambda x: f"{x['nome']} | CPF: {x['cpf']}", axis=1).tolist()
    idx_u = 0
    if dados['nome_usuario']:
        match = [i for i, s in enumerate(op_u) if dados['nome_usuario'] in s]
        if match: idx_u = match[0]
    orgs = listar_origens_para_selecao()
    idx_org = (orgs.index(dados['origem_custo']) + 1) if dados.get('origem_custo') in orgs else 0
    with st.form("f_ed_cl"):
        n_cpf = st.text_input("CPF Cliente", value=dados['cpf_cliente'])
        n_nome = st.text_input("Nome Cliente", value=dados['nome_cliente'])
        n_cart = st.text_input("Nome Carteira", value=dados['nome_carteira'])
        n_org = st.selectbox("Origem Custo", options=[""] + orgs, index=idx_org)
        n_val = st.number_input("Custo (R$)", value=float(dados['custo_carteira'] or 0.0), step=0.01)
        s_u = st.selectbox("Usuário Vinculado", options=op_u, index=idx_u)
        if st.form_submit_button("Salvar"):
            u_cpf, u_nome = (None, None)
            if s_u: 
                p = s_u.split(" | CPF: ")
                u_nome, u_cpf = p[0], (p[1] if len(p) > 1 else None)
            if atualizar_cliente_carteira_lista(dados['id'], n_cpf, n_nome, n_cart, n_val, u_cpf, u_nome, n_org):
                st.success("Atualizado!"); st.rerun()

# =============================================================================
# 5. INTERFACE PRINCIPAL
# =============================================================================

def app_clientes():
    garantir_tabela_config_carteiras()
    st.markdown("## 👥 Central de Clientes e Usuários")
    tabs = st.tabs(["🏢 Clientes", "👤 Usuários", "⚙️ Parâmetros", "💼 Carteira", "📊 Relatórios"])

    with tabs[2]: # Parâmetros
        with st.expander("📂 Lista de Carteiras", expanded=False):
            with st.container(border=True):
                st.caption("Nova Carteira")
                c1, c2, c3, c4, c5 = st.columns([1.5, 2, 1.5, 1, 1])
                df_clis = listar_clientes_para_selecao()
                n_sel = c2.selectbox("Cliente", options=[""] + df_clis.apply(lambda x: f"{x['nome']} | CPF: {x['cpf']}", axis=1).tolist(), key="n_sel_l")
                cpf_auto = n_sel.split(" | CPF: ")[1] if n_sel else ""
                n_cpf = c1.text_input("CPF", value=cpf_auto, key="n_cpf_l")
                df_c_at = listar_todas_carteiras_ativas()
                n_cart = c3.selectbox("Carteira", options=[""] + df_c_at['nome_carteira'].tolist(), key="n_cart_l")
                n_val = c4.number_input("Custo", key="n_val_l")
                orgs = listar_origens_para_selecao()
                n_org = st.selectbox("Origem Custo", options=[""] + orgs, key="n_org_l")
                if c5.button("➕", key="add_cl_btn"):
                    if n_cpf and n_cart:
                        if salvar_cliente_carteira_lista(n_cpf, n_sel.split(" | ")[0], n_cart, n_val, n_org): st.rerun()

            df_l = listar_cliente_carteira_lista()
            if not df_l.empty:
                st.markdown("""<div style="display: flex; font-weight: bold; background: #f0f2f6; padding: 8px; font-size: 0.9em;"><div style="flex: 2;">Cliente</div><div style="flex: 2;">Carteira</div><div style="flex: 1;">Custo</div><div style="flex: 2;">Origem</div><div style="flex: 2;">Usuário</div><div style="flex: 1; text-align: center;">Ações</div></div>""", unsafe_allow_html=True)
                for _, r in df_l.iterrows():
                    cc1, cc2, cc3, cc4, cc5, cc6 = st.columns([2, 2, 1, 2, 2, 1])
                    cc1.write(r['nome_cliente']); cc2.write(r['nome_carteira']); cc3.write(f"R$ {float(r['custo_carteira'] or 0):.2f}"); cc4.write(r.get('origem_custo', '-')); cc5.write(r.get('nome_usuario', '-'))
                    with cc6:
                        if st.button("✏️", key=f"ed_cl_{r['id']}"): dialog_editar_cart_lista(r)
                        if st.button("🗑️", key=f"de_cl_{r['id']}"): excluir_cliente_carteira_lista(r['id']); st.rerun()

    with tabs[3]: # Carteira
        st.markdown("### 💼 Gestão de Carteira")
        with st.expander("📂 Nova Carteira (Produtos)", expanded=False):
            st.info("Cria carteiras e tabelas automaticamente.")
            df_pds = listar_produtos_para_selecao()
            if not df_pds.empty:
                with st.container(border=True):
                    cc1, cc2, cc3, cc4 = st.columns([3, 3, 2, 2])
                    idx_p = cc1.selectbox("Produto", range(len(df_pds)), format_func=lambda x: df_pds.iloc[x]['nome'])
                    n_cart_in = cc2.text_input("Nome Carteira", key="n_c_n")
                    stt_in = cc3.selectbox("Status", ["ATIVO", "INATIVO"], key="s_c_n")
                    if cc4.button("💾 Criar", key="b_c_c"):
                        if n_cart_in: salvar_nova_carteira_sistema(int(df_pds.iloc[idx_p]['id']), df_pds.iloc[idx_p]['nome'], n_cart_in, stt_in); st.rerun()

        st.divider()
        st.markdown("#### 📑 Edição de Conteúdo das Tabelas")
        l_tabs = listar_tabelas_transacao_reais()
        if l_tabs:
            t_sel = st.selectbox("Escolha a Tabela", options=l_tabs, key="s_t_e_r")
            if t_sel:
                df_e = carregar_dados_tabela_dinamica(t_sel)
                if not df_e.empty:
                    st.info(f"Editando: `cliente.{t_sel}`")
                    df_res = st.data_editor(df_e, key=f"ed_{t_sel}", use_container_width=True, hide_index=True, disabled=["id", "data_transacao"])
                    if st.button("💾 Salvar Planilha", key="b_s_p"):
                        if salvar_alteracoes_tabela_dinamica(t_sel, df_e, df_res): st.success("Atualizado!"); time.sleep(1); st.rerun()
        else: st.info("Sem tabelas.")

if __name__ == "__main__":
    app_clientes()