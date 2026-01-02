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

# --- FUNÇÃO AUXILIAR: SANITIZAR NOME DE TABELA ---
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
        conn.close()
        return df
    except: conn.close(); return pd.DataFrame()

def salvar_agrupamento(tipo, nome):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {tabela} (nome_agrupamento) VALUES (%s)", (nome,))
        conn.commit(); conn.close()
        return True
    except: conn.close(); return False

def excluir_agrupamento(tipo, id_agrup):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {tabela} WHERE id = %s", (id_agrup,))
        conn.commit(); conn.close()
        return True
    except: conn.close(); return False

def atualizar_agrupamento(tipo, id_agrup, novo_nome):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {tabela} SET nome_agrupamento = %s WHERE id = %s", (novo_nome, id_agrup))
        conn.commit(); conn.close()
        return True
    except: conn.close(); return False

# --- CLIENTE CNPJ ---

def listar_cliente_cnpj():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, cnpj, nome_empresa FROM admin.cliente_cnpj ORDER BY nome_empresa", conn)
        conn.close()
        return df
    except: conn.close(); return pd.DataFrame()

def salvar_cliente_cnpj(cnpj, nome):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO admin.cliente_cnpj (cnpj, nome_empresa) VALUES (%s, %s)", (cnpj, nome))
        conn.commit(); conn.close()
        return True
    except: conn.close(); return False

def excluir_cliente_cnpj(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM admin.cliente_cnpj WHERE id = %s", (id_reg,))
        conn.commit(); conn.close()
        return True
    except: conn.close(); return False

def atualizar_cliente_cnpj(id_reg, cnpj, nome):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.cliente_cnpj SET cnpj=%s, nome_empresa=%s WHERE id=%s", (cnpj, nome, id_reg))
        conn.commit(); conn.close()
        return True
    except: conn.close(); return False

# --- RELAÇÃO PEDIDO CARTEIRA ---

def listar_relacao_pedido_carteira():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, produto, nome_carteira FROM cliente.cliente_carteira_relacao_pedido_carteira ORDER BY id DESC", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_relacao_pedido_carteira(produto, carteira):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO cliente.cliente_carteira_relacao_pedido_carteira (produto, nome_carteira) VALUES (%s, %s)", (produto, carteira))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def atualizar_relacao_pedido_carteira(id_reg, produto, carteira):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE cliente.cliente_carteira_relacao_pedido_carteira SET produto=%s, nome_carteira=%s WHERE id=%s", (produto, carteira, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_relacao_pedido_carteira(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM cliente.cliente_carteira_relacao_pedido_carteira WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

# --- CLIENTE CARTEIRA LISTA ---

def listar_cliente_carteira_lista():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, cpf_cliente, nome_cliente, nome_carteira, custo_carteira FROM cliente.cliente_carteira_lista ORDER BY nome_cliente", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_cliente_carteira_lista(cpf, nome, carteira, custo):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO cliente.cliente_carteira_lista (cpf_cliente, nome_cliente, nome_carteira, custo_carteira) 
            VALUES (%s, %s, %s, %s)
        """, (cpf, nome, carteira, custo))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def atualizar_cliente_carteira_lista(id_reg, cpf, nome, carteira, custo):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE cliente.cliente_carteira_lista 
            SET cpf_cliente=%s, nome_cliente=%s, nome_carteira=%s, custo_carteira=%s 
            WHERE id=%s
        """, (cpf, nome, carteira, custo, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_cliente_carteira_lista(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM cliente.cliente_carteira_lista WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

# =============================================================================
# --- FUNÇÕES: CARTEIRA CLIENTE (CONFIG E EXTRATO) ---
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
                    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit(); conn.close()
        except: conn.close()

def listar_produtos_para_selecao():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome FROM produtos_servicos WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close(); return df
    except: 
        conn.close(); return pd.DataFrame()

def listar_carteiras_config():
    garantir_tabela_config_carteiras()
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT * FROM cliente.carteiras_config ORDER BY id DESC", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def listar_todas_carteiras_ativas():
    garantir_tabela_config_carteiras()
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome_carteira, nome_tabela_transacoes FROM cliente.carteiras_config WHERE status = 'ATIVO' ORDER BY nome_carteira", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_nova_carteira_sistema(id_prod, nome_prod, nome_carteira, status):
    conn = get_conn()
    if not conn: return False, "Erro conexão"
    try:
        cur = conn.cursor()
        sufixo = sanitizar_nome_tabela(nome_carteira)
        nome_tabela_dinamica = f"cliente.transacoes_{sufixo}"
        
        sql_create = f"""
            CREATE TABLE IF NOT EXISTS {nome_tabela_dinamica} (
                id SERIAL PRIMARY KEY,
                cpf_cliente VARCHAR(20),
                nome_cliente VARCHAR(255),
                motivo VARCHAR(255),
                origem_lancamento VARCHAR(100),
                data_transacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tipo_lancamento VARCHAR(50),
                valor NUMERIC(10, 2),
                saldo_anterior NUMERIC(10, 2),
                saldo_novo NUMERIC(10, 2)
            );
        """
        cur.execute(sql_create)
        
        sql_insert = """
            INSERT INTO cliente.carteiras_config 
            (id_produto, nome_produto, nome_carteira, nome_tabela_transacoes, status)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(sql_insert, (id_prod, nome_prod, nome_carteira, nome_tabela_dinamica, status))
        conn.commit(); conn.close()
        return True, f"Carteira '{nome_carteira}' criada!"
    except Exception as e:
        conn.close(); return False, str(e)

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

def atualizar_carteira_config(id_conf, status):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE cliente.carteiras_config SET status = %s WHERE id = %s", (status, id_conf))
            conn.commit(); conn.close()
            return True
        except: conn.close(); return False
    return False

# --- FUNÇÃO ATUALIZADA: BUSCAR COM ID ---
def buscar_transacoes_carteira_filtrada(nome_tabela_sql, cpf_cliente, data_ini, data_fim):
    """Busca o extrato na tabela dinâmica específica da carteira com filtro de data"""
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        dt_ini_str = data_ini.strftime('%Y-%m-%d 00:00:00')
        dt_fim_str = data_fim.strftime('%Y-%m-%d 23:59:59')

        # SELECT INCLUI 'id'
        query = f"""
            SELECT id, data_transacao, motivo, tipo_lancamento, valor, saldo_novo, origem_lancamento 
            FROM {nome_tabela_sql} 
            WHERE cpf_cliente = %s 
              AND data_transacao BETWEEN %s AND %s
            ORDER BY data_transacao DESC
        """
        df = pd.read_sql(query, conn, params=(str(cpf_cliente), dt_ini_str, dt_fim_str))
        conn.close()
        return df
    except Exception as e:
        conn.close(); return pd.DataFrame()

# --- FUNÇÃO PARA LANÇAMENTO MANUAL (CRÉDITO/DÉBITO) ---
def realizar_lancamento_manual(tabela_sql, cpf_cliente, nome_cliente, tipo_lanc, valor, motivo):
    conn = get_conn()
    if not conn: return False, "Erro conexão"
    try:
        cur = conn.cursor()
        
        # 1. Busca saldo atual
        cur.execute(f"SELECT saldo_novo FROM {tabela_sql} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_cliente,))
        res = cur.fetchone()
        saldo_anterior = float(res[0]) if res else 0.0
        
        # 2. Calcula novo saldo
        valor = float(valor)
        if tipo_lanc == "DEBITO":
            saldo_novo = saldo_anterior - valor
        else:
            saldo_novo = saldo_anterior + valor
            
        # 3. Insere registro
        query = f"""
            INSERT INTO {tabela_sql} 
            (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao)
            VALUES (%s, %s, %s, 'MANUAL', %s, %s, %s, %s, NOW())
        """
        cur.execute(query, (cpf_cliente, nome_cliente, motivo, tipo_lanc, valor, saldo_anterior, saldo_novo))
        
        conn.commit(); conn.close()
        return True, "Lançamento realizado com sucesso!"
    except Exception as e:
        conn.close(); return False, str(e)

# --- NOVAS FUNÇÕES: EDITAR E EXCLUIR TRANSAÇÃO ---
def atualizar_transacao_dinamica(nome_tabela, id_transacao, novo_motivo, novo_valor, novo_tipo):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        # Nota: Alteração de valor histórico não recalcula saldos futuros automaticamente
        query = f"""
            UPDATE {nome_tabela} 
            SET motivo = %s, valor = %s, tipo_lancamento = %s
            WHERE id = %s
        """
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

# --- USUÁRIOS E CLIENTES (VINCULOS) ---

def buscar_usuarios_disponiveis():
    conn = get_conn()
    try:
        query = """
            SELECT id, nome, email, cpf 
            FROM clientes_usuarios 
            WHERE id NOT IN (SELECT id_usuario_vinculo FROM admin.clientes WHERE id_usuario_vinculo IS NOT NULL)
            ORDER BY nome
        """
        df = pd.read_sql(query, conn); conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def vincular_usuario_cliente(id_cliente, id_usuario):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = %s WHERE id = %s", (id_usuario, id_cliente))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def desvincular_usuario_cliente(id_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = NULL WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_cliente_db(id_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM admin.clientes WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

# =============================================================================
# 2. FUNÇÕES DE USUÁRIO
# =============================================================================

def hash_senha(senha):
    if senha.startswith('$2b$'): return senha
    return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def salvar_usuario_novo(nome, email, cpf, tel, senha, hierarquia, ativo):
    conn = get_conn()
    try:
        cur = conn.cursor()
        senha_final = hash_senha(senha)
        cur.execute("""
            INSERT INTO clientes_usuarios (nome, email, cpf, telefone, senha, hierarquia, ativo)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (nome, email, cpf, tel, senha_final, hierarquia, ativo))
        novo_id = cur.fetchone()[0]; conn.commit(); conn.close(); return novo_id
    except Exception as e: conn.close(); return None

# =============================================================================
# 3. DIALOGS (POP-UPS PADRONIZADOS)
# =============================================================================

@st.dialog("✏️ Editar")
def dialog_editar_agrupamento(tipo, id_agrup, nome_atual):
    st.caption(f"Editando: {nome_atual}")
    with st.form("form_edit_agrup"):
        novo_nome = st.text_input("Nome", value=nome_atual)
        if st.form_submit_button("💾 Salvar", use_container_width=True):
            if novo_nome:
                if atualizar_agrupamento(tipo, id_agrup, novo_nome):
                    st.success("Atualizado!"); time.sleep(0.5); st.rerun()
                else: st.error("Erro ao atualizar.")

@st.dialog("✏️ Editar")
def dialog_editar_cliente_cnpj(id_reg, cnpj_atual, nome_atual):
    st.caption(f"Editando: {cnpj_atual}")
    with st.form("form_edit_cnpj"):
        novo_cnpj = st.text_input("CNPJ", value=cnpj_atual)
        novo_nome = st.text_input("Nome Empresa", value=nome_atual)
        if st.form_submit_button("💾 Salvar", use_container_width=True):
            if atualizar_cliente_cnpj(id_reg, novo_cnpj, novo_nome):
                st.success("Atualizado!"); time.sleep(0.5); st.rerun()
            else: st.error("Erro ao atualizar.")

@st.dialog("✏️ Editar Relação")
def dialog_editar_relacao_ped_cart(id_reg, prod_atual, cart_atual):
    st.caption("Editando Relação")
    with st.form("form_edit_rel_pc"):
        n_prod = st.text_input("Nome Produto", value=prod_atual)
        n_cart = st.text_input("Nome Carteira", value=cart_atual)
        if st.form_submit_button("💾 Salvar"):
            if atualizar_relacao_pedido_carteira(id_reg, n_prod, n_cart):
                st.success("Atualizado!"); st.rerun()
            else: st.error("Erro.")

@st.dialog("✏️ Editar Carteira Cliente")
def dialog_editar_cart_lista(dados):
    st.caption(f"Editando: {dados['nome_cliente']}")
    with st.form("form_edit_cart_li"):
        n_cpf = st.text_input("CPF Cliente", value=dados['cpf_cliente'])
        n_nome = st.text_input("Nome Cliente", value=dados['nome_cliente'])
        n_cart = st.text_input("Nome Carteira", value=dados['nome_carteira'])
        n_custo = st.number_input("Custo Carteira (R$)", value=float(dados['custo_carteira'] or 0.0), step=0.01)
        if st.form_submit_button("💾 Salvar"):
            if atualizar_cliente_carteira_lista(dados['id'], n_cpf, n_nome, n_cart, n_custo):
                st.success("Atualizado!"); st.rerun()
            else: st.error("Erro.")

@st.dialog("✏️ Editar Config Carteira")
def dialog_editar_carteira_config(dados):
    st.write(f"Carteira: **{dados['nome_carteira']}**")
    st.caption(f"Tabela: {dados['nome_tabela_transacoes']}")
    with st.form("form_edit_cart_conf"):
        n_status = st.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if dados['status'] == "ATIVO" else 1)
        if st.form_submit_button("Salvar"):
            if atualizar_carteira_config(dados['id'], n_status):
                st.success("Status Atualizado!"); st.rerun()

@st.dialog("🔗 Gestão de Acesso do Cliente")
def dialog_gestao_usuario_vinculo(dados_cliente):
    id_vinculo = dados_cliente.get('id_vinculo') or dados_cliente.get('id_usuario_vinculo')
    if id_vinculo:
        st.success("✅ Este cliente já possui um usuário vinculado.")
        conn = get_conn()
        df_u = pd.read_sql(f"SELECT nome, email, telefone, cpf FROM clientes_usuarios WHERE id = {id_vinculo}", conn); conn.close()
        if not df_u.empty:
            usr = df_u.iloc[0]
            st.write(f"**Nome:** {usr['nome']}"); st.write