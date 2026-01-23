import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import pool
import time
import contextlib
import sys
import os

# ==============================================================================
# 0. CONFIGURAÇÃO DE CAMINHOS
# ==============================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    import conexao
except ImportError:
    st.error("Erro: conexao.py não encontrado na raiz.")
    conexao = None

# ==============================================================================
# 1. CONEXÃO BLINDADA (Connection Pool)
# ==============================================================================

@st.cache_resource
def get_pool():
    if not conexao: return None
    try:
        return psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=10, 
            host=conexao.host, port=conexao.port,
            database=conexao.database, user=conexao.user, password=conexao.password,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5
        )
    except Exception as e:
        st.error(f"Erro fatal no Pool de Conexão: {e}")
        return None

@contextlib.contextmanager
def get_db_connection():
    pool_obj = get_pool()
    if not pool_obj:
        yield None
        return
    
    conn = pool_obj.getconn()
    try:
        conn.rollback() # Health check
        yield conn
        pool_obj.putconn(conn)
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        try: pool_obj.putconn(conn, close=True)
        except: pass
        try:
            conn = pool_obj.getconn()
            yield conn
            pool_obj.putconn(conn)
        except Exception:
            yield None
    except Exception as e:
        pool_obj.putconn(conn)
        raise e

# =============================================================================
# 1. FUNÇÕES DE BANCO DE DADOS (PARÂMETROS)
# =============================================================================

# --- AGRUPAMENTOS ---
def listar_agrupamentos(tipo):
    with get_db_connection() as conn:
        if not conn: return pd.DataFrame()
        tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
        try:
            return pd.read_sql(f"SELECT id, nome_agrupamento FROM {tabela} ORDER BY id", conn)
        except: return pd.DataFrame()

def salvar_agrupamento(tipo, nome):
    with get_db_connection() as conn:
        if not conn: return False
        tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
        try:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {tabela} (nome_agrupamento) VALUES (%s)", (nome,))
            conn.commit()
            return True
        except: return False

def excluir_agrupamento(tipo, id_agrup):
    with get_db_connection() as conn:
        if not conn: return False
        tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {tabela} WHERE id = %s", (id_agrup,))
            conn.commit()
            return True
        except: return False

def atualizar_agrupamento(tipo, id_agrup, novo_nome):
    with get_db_connection() as conn:
        if not conn: return False
        tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
        try:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE {tabela} SET nome_agrupamento = %s WHERE id = %s", (novo_nome, id_agrup))
            conn.commit()
            return True
        except: return False

# --- CLIENTE CNPJ ---
def listar_cliente_cnpj():
    with get_db_connection() as conn:
        if not conn: return pd.DataFrame()
        try:
            return pd.read_sql("SELECT id, cnpj, nome_empresa FROM admin.cliente_cnpj ORDER BY nome_empresa", conn)
        except: return pd.DataFrame()

def salvar_cliente_cnpj(cnpj, nome):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO admin.cliente_cnpj (cnpj, nome_empresa) VALUES (%s, %s)", (cnpj, nome))
            conn.commit()
            return True
        except: return False

def excluir_cliente_cnpj(id_reg):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM admin.cliente_cnpj WHERE id = %s", (id_reg,))
            conn.commit()
            return True
        except: return False

def atualizar_cliente_cnpj(id_reg, cnpj, nome):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE admin.cliente_cnpj SET cnpj=%s, nome_empresa=%s WHERE id=%s", (cnpj, nome, id_reg))
            conn.commit()
            return True
        except: return False

# --- RELAÇÃO PEDIDO / CARTEIRA ---
def listar_relacao_pedido_carteira():
    with get_db_connection() as conn:
        if not conn: return pd.DataFrame()
        try:
            return pd.read_sql("SELECT id, produto, nome_carteira FROM cliente.cliente_carteira_relacao_pedido_carteira ORDER BY id DESC", conn)
        except: return pd.DataFrame()

def salvar_relacao_pedido_carteira(produto, carteira):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO cliente.cliente_carteira_relacao_pedido_carteira (produto, nome_carteira) VALUES (%s, %s)", (produto, carteira))
            conn.commit()
            return True
        except: return False

def atualizar_relacao_pedido_carteira(id_reg, produto, carteira):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE cliente.cliente_carteira_relacao_pedido_carteira SET produto=%s, nome_carteira=%s WHERE id=%s", (produto, carteira, id_reg))
            conn.commit()
            return True
        except: return False

def excluir_relacao_pedido_carteira(id_reg):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM cliente.cliente_carteira_relacao_pedido_carteira WHERE id=%s", (id_reg,))
            conn.commit()
            return True
        except: return False

# =============================================================================
# 2. DIALOGS DE EDIÇÃO
# =============================================================================

@st.dialog("✏️ Editar Agrupamento")
def dialog_editar_agrupamento(tipo, id_agrup, nome_atual):
    st.caption(f"Editando: {nome_atual} ({tipo})")
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
            if atualizar_relacao_pedido_carteira(id_reg, n_p, n_c): 
                st.success("Ok!"); st.rerun()

# =============================================================================
# 3. APP PRINCIPAL DO MÓDULO
# =============================================================================

def app_parametros():
    st.markdown("## ⚙️ Parâmetros Gerais")
    
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

if __name__ == "__main__":
    if get_pool():
        app_parametros()
    else:
        st.error("Erro crítico de conexão.")