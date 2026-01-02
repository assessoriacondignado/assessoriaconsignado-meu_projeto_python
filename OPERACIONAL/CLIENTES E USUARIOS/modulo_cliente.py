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
        df = pd.read_sql("SELECT id, cpf_cliente, nome_cliente, nome_carteira, custo_carteira, cpf_usuario, nome_usuario FROM cliente.cliente_carteira_lista ORDER BY nome_cliente", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_cliente_carteira_lista(cpf, nome, carteira, custo):
    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # --- LÓGICA MELHORADA: Busca usuário vinculado ---
        cpf_user = None
        nome_user = None
        
        # Limpa o CPF para busca (apenas números)
        cpf_limpo = re.sub(r'\D', '', str(cpf))
        
        # Query que busca pelo CPF limpo OU pelo CPF formatado (com pontos)
        query_vinculo = """
            SELECT u.cpf, u.nome 
            FROM admin.clientes c
            JOIN clientes_usuarios u ON c.id_usuario_vinculo = u.id
            WHERE regexp_replace(c.cpf, '[^0-9]', '', 'g') = %s
            LIMIT 1
        """
        cur.execute(query_vinculo, (cpf_limpo,))
        res_vinculo = cur.fetchone()
        
        if res_vinculo:
            cpf_user = res_vinculo[0]
            nome_user = res_vinculo[1]
        
        cur.execute("""
            INSERT INTO cliente.cliente_carteira_lista 
            (cpf_cliente, nome_cliente, nome_carteira, custo_carteira, cpf_usuario, nome_usuario) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (cpf, nome, carteira, custo, cpf_user, nome_user))
        
        conn.commit(); conn.close(); return True
    except Exception as e: 
        print(e)
        if conn: conn.close()
        return False

def atualizar_cliente_carteira_lista(id_reg, cpf, nome, carteira, custo, cpf_user, nome_user):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE cliente.cliente_carteira_lista 
            SET cpf_cliente=%s, nome_cliente=%s, nome_carteira=%s, custo_carteira=%s, cpf_usuario=%s, nome_usuario=%s 
            WHERE id=%s
        """, (cpf, nome, carteira, custo, cpf_user, nome_user, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_cliente_carteira_lista(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM cliente.cliente_carteira_lista WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def listar_usuarios_para_selecao():
    """Retorna lista de usuários cadastrados para o dropdown"""
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome, cpf FROM clientes_usuarios WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close(); return df
    except: 
        conn.close(); return pd.DataFrame()

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
                    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    origem_custo VARCHAR(100)
                );
            """)
            conn.commit(); conn.close()
        except: conn.close()

def listar_origens_para_selecao():
    conn = get_conn()
    lista = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT origem FROM conexoes.fatorconferi_origem_consulta_fator ORDER BY origem ASC")
            lista = [row[0] for row in cur.fetchall()]
            conn.close()
        except:
            if conn: conn.close()
    return lista

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

# --- FUNÇÃO ATUALIZADA: BUSCAR COM ID ---
def buscar_transacoes_carteira_filtrada(nome_tabela_sql, cpf_cliente, data_ini, data_fim):
    """Busca o extrato na tabela dinâmica específica da carteira com filtro de data"""
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        dt_ini_str = data_ini.strftime('%Y-%m-%d 00:00:00')
        dt_fim_str = data_fim.strftime('%Y-%m-%d 23:59:59')

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
        cur.execute(f"SELECT saldo_novo FROM {tabela_sql} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_cliente,))
        res = cur.fetchone()
        saldo_anterior = float(res[0]) if res else 0.0
        
        valor = float(valor)
        if tipo_lanc == "DEBITO": saldo_novo = saldo_anterior - valor
        else: saldo_novo = saldo_anterior + valor
            
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
        cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = %s WHERE id = %s", (int(id_usuario), int(id_cliente)))
        conn.commit(); conn.close()
        return True, "Vinculado com sucesso!"
    except Exception as e: 
        conn.close()
        return False, str(e)

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
    st.write(f"Editando: **{dados['nome_cliente']}**")
    
    # 1. Carrega usuários disponíveis para o Selectbox
    df_users = listar_usuarios_para_selecao()
    opcoes_usuarios = [""] + df_users.apply(lambda x: f"{x['nome']} | CPF: {x['cpf']}", axis=1).tolist()
    
    # 2. Encontra o índice atual (se já tiver vínculo)
    idx_atual = 0
    if dados['nome_usuario']:
        match = [i for i, s in enumerate(opcoes_usuarios) if dados['nome_usuario'] in s]
        if match: idx_atual = match[0]

    with st.form("form_edit_cart_li"):
        n_cpf = st.text_input("CPF Cliente", value=dados['cpf_cliente'])
        n_nome = st.text_input("Nome Cliente", value=dados['nome_cliente'])
        n_cart = st.text_input("Nome Carteira", value=dados['nome_carteira'])
        n_custo = st.number_input("Custo Carteira (R$)", value=float(dados['custo_carteira'] or 0.0), step=0.01)
        
        # 3. Selectbox de Usuários
        sel_user = st.selectbox("Usuário Vinculado", options=opcoes_usuarios, index=idx_atual)
        
        if st.form_submit_button("💾 Salvar"):
            # Extrai Nome e CPF do selecionado
            cpf_u_final = None
            nome_u_final = None
            
            if sel_user:
                # Formato: "Nome | CPF: 123"
                partes = sel_user.split(" | CPF: ")
                nome_u_final = partes[0]
                cpf_u_final = partes[1] if len(partes) > 1 else None

            if atualizar_cliente_carteira_lista(dados['id'], n_cpf, n_nome, n_cart, n_custo, cpf_u_final, nome_u_final):
                st.success("Atualizado!"); st.rerun()
            else: st.error("Erro.")

@st.dialog("✏️ Editar Configuração da Carteira")
def dialog_editar_carteira_config(dados):
    st.write(f"Editando: **{dados['nome_carteira']}**")
    lista_origens = listar_origens_para_selecao()
    
    with st.form("form_edit_cart_conf"):
        n_nome = st.text_input("Nome da Carteira", value=dados['nome_carteira'])
        n_status = st.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if dados['status'] == "ATIVO" else 1)
        
        idx_origem = 0
        valor_atual_origem = dados.get('origem_custo')
        if valor_atual_origem and valor_atual_origem in lista_origens:
            idx_origem = lista_origens.index(valor_atual_origem)
        opcoes_origem = [""] + lista_origens
        n_origem = st.selectbox("Origem Custo (Tabela Fator)", options=opcoes_origem, index=idx_origem + 1 if valor_atual_origem else 0)
        
        st.info("⚠️ Alterar o nome da carteira aqui não renomeia a tabela do banco, apenas o rótulo.")
        
        if st.form_submit_button("Salvar Alterações"):
            if atualizar_carteira_config(dados['id'], n_status, n_nome, n_origem):
                st.success("Atualizado com sucesso!"); time.sleep(1); st.rerun()
            else:
                st.error("Erro ao atualizar.")

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

# --- NOVO DIALOG: LANÇAMENTO MANUAL ---
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

# --- NOVOS DIALOGS: EDIÇÃO E EXCLUSÃO DE EXTRATO ---
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

# =============================================================================
# 4. INTERFACE PRINCIPAL
# =============================================================================

def app_clientes():
    st.markdown("## 👥 Central de Clientes e Usuários")
    
    # Atualizado: Aba 3 renomeada para "Parâmetros"
    tab_cli, tab_user, tab_param, tab_carteira, tab_rel = st.tabs(["🏢 Clientes", "👤 Usuários", "⚙️ Parâmetros", "💼 Carteira", "📊 Relatórios"])

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
                # Cabeçalho Fixo (Layout similar ao Fator Conferi)
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
                            # Botão Ver/Editar
                            if b1.button("✏️", key=f"e_{row['id']}", help="Editar Cadastro"): 
                                st.session_state.update({'view_cliente': 'editar', 'cli_id': row['id']}); st.rerun()
                            
                            # Botão Extrato
                            if b2.button("📜", key=f"ext_{row['id']}", help="Ver Extrato"):
                                if st.session_state.get('extrato_expandido') == row['id']:
                                    st.session_state['extrato_expandido'] = None
                                else:
                                    st.session_state['extrato_expandido'] = row['id']
                                    st.session_state['pag_hist'] = 1 
                                st.rerun()
                                
                            # Botão Vínculo Usuário
                            if b3.button("🔗" if row['id_vinculo'] else "👤", key=f"u_{row['id']}", help="Acesso Usuário"): 
                                dialog_gestao_usuario_vinculo(row)
                                
                            # Botão Excluir (Opcional, se quiser manter no grid)
                            if b4.button("🗑️", key=f"d_{row['id']}", help="Excluir"):
                                dialog_excluir_cliente(row['id'], row['nome'])
                        
                        st.markdown("<hr style='margin: 5px 0; border-color: #eee;'>", unsafe_allow_html=True)

                        # --- ÁREA EXPANSÍVEL DO EXTRATO ---
                        if st.session_state.get('extrato_expandido') == row['id']:
                            with st.container(border=True):
                                st.markdown(f"#### 📜 Extrato Financeiro: {row['nome']}")
                                st.caption(f"CPF: {row.get('cpf', '-')}")
                                
                                # Busca Carteiras Ativas
                                df_carteiras = listar_todas_carteiras_ativas()
                                
                                if not df_carteiras.empty:
                                    col_sel, col_btn_c, col_btn_d, col_vazio = st.columns([4, 1.5, 1.5, 3])
                                    
                                    opcoes_cart = df_carteiras.apply(lambda x: f"{x['nome_carteira']}", axis=1)
                                    idx_sel = col_sel.selectbox("Selecione a Carteira", range(len(df_carteiras)), format_func=lambda x: opcoes_cart[x], key=f"sel_cart_{row['id']}", label_visibility="visible")
                                    
                                    # Carrega dados da carteira selecionada
                                    carteira_sel = df_carteiras.iloc[idx_sel]
                                    tabela_sql = carteira_sel['nome_tabela_transacoes']
                                    cpf_limpo = str(row.get('cpf', '')).strip()
                                    
                                    # --- BOTÕES DE LANÇAMENTO ---
                                    if col_btn_c.button("➕ Crédito", key=f"cred_{row['id']}"):
                                        dialog_lancamento_manual(tabela_sql, cpf_limpo, row['nome'], "CREDITO")
                                    
                                    if col_btn_d.button("➖ Débito", key=f"deb_{row['id']}"):
                                        dialog_lancamento_manual(tabela_sql, cpf_limpo, row['nome'], "DEBITO")
                                    
                                    # --- FILTROS E EXTRATO ---
                                    st.write("") # Espaço
                                    fd1, fd2 = st.columns(2)
                                    data_ini = fd1.date_input("Data Inicial", value=date.today() - timedelta(days=30), key=f"ini_{row['id']}")
                                    data_fim = fd2.date_input("Data Final", value=date.today(), key=f"fim_{row['id']}")
                                    
                                    df_extrato = buscar_transacoes_carteira_filtrada(tabela_sql, cpf_limpo, data_ini, data_fim)
                                    
                                    if not df_extrato.empty:
                                        # Cabeçalho Visual
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
                                            # ATUALIZADO: Incluindo coluna de Ações
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
        conn = get_conn(); sql_u = "SELECT id, nome, email, hierarquia, ativo FROM clientes_usuarios WHERE 1=1"
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

        # 5. LISTA DE CARTEIRAS (ATUALIZADO)
        with st.expander("📂 Lista de Carteiras", expanded=False):
            with st.container(border=True):
                st.caption("Nova Carteira")
                c_lc1, c_lc2, c_lc3, c_lc4, c_bt = st.columns([1.5, 2, 1.5, 1, 1])
                n_cpf = c_lc1.text_input("CPF Cliente", key="n_cpf_l", label_visibility="visible")
                n_nome = c_lc2.text_input("Nome Cliente", key="n_nome_l", label_visibility="visible")
                
                # --- MUDANÇA AQUI: SELECTBOX COM CARTEIRAS ATIVAS ---
                df_cart_ativas = listar_todas_carteiras_ativas()
                opcoes_cart_ativas = df_cart_ativas['nome_carteira'].tolist() if not df_cart_ativas.empty else []
                n_carteira = c_lc3.selectbox("Nome Carteira", options=[""] + opcoes_cart_ativas, key="n_cart_l", label_visibility="visible")
                # ----------------------------------------------------
                
                n_custo = c_lc4.number_input("Custo (R$)", key="n_custo_l", step=0.01, label_visibility="visible")
                
                c_bt.write(""); c_bt.write("") 
                if c_bt.button("➕", key="add_cart_l", use_container_width=True):
                    if n_cpf and n_nome and n_carteira: 
                        salvar_cliente_carteira_lista(n_cpf, n_nome, n_carteira, n_custo); st.rerun()
                    else:
                        st.warning("Preencha CPF, Nome e Selecione a Carteira.")
            
            st.divider()
            df_cart_l = listar_cliente_carteira_lista()
            
            # [MUDANÇA VISUAL]: Adicionado cabeçalho e novas colunas
            if not df_cart_l.empty:
                st.markdown("""
                <div style="display: flex; font-weight: bold; background: #f0f2f6; padding: 8px; font-size: 0.9em;">
                    <div style="flex: 2;">Cliente</div>
                    <div style="flex: 2;">Carteira</div>
                    <div style="flex: 1;">Custo</div>
                    <div style="flex: 2;">Usuário Vinculado</div>
                    <div style="flex: 1; text-align: center;">Ações</div>
                </div>""", unsafe_allow_html=True)

                for _, r in df_cart_l.iterrows():
                    cc1, cc2, cc3, cc4, cc5 = st.columns([2, 2, 1, 2, 1])
                    
                    cc1.write(f"{r['nome_cliente']}")
                    cc2.write(f"{r['nome_carteira']}")
                    cc3.write(f"R$ {float(r['custo_carteira'] or 0):.2f}")
                    
                    # Exibe usuário vinculado se existir
                    user_info = "-"
                    if r['nome_usuario']:
                        user_info = f"{r['nome_usuario']}"
                    cc4.write(user_info)
                    
                    with cc5:
                        b_ed, b_del = st.columns(2)
                        if b_ed.button("✏️", key=f"ed_cl_{r['id']}"): dialog_editar_cart_lista(r)
                        if b_del.button("🗑️", key=f"del_cl_{r['id']}"): excluir_cliente_carteira_lista(r['id']); st.rerun()
                    
                    st.markdown("<hr style='margin: 2px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

        # 6. CONFIGURAÇÃO DE CARTEIRAS (NOVO BLOCO)
        with st.expander("⚙️ Configurações de Carteiras", expanded=False):
            st.info("Aqui você pode visualizar e editar as configurações de carteiras (tabela: cliente.carteiras_config).")
            
            df_configs = listar_carteiras_config()
            
            if not df_configs.empty:
                # Cabeçalho da tabela
                st.markdown("""
                <div style="display: flex; font-weight: bold; background: #f0f2f6; padding: 8px; font-size: 0.9em;">
                    <div style="flex: 2;">Produto</div>
                    <div style="flex: 2;">Carteira</div>
                    <div style="flex: 2;">Tabela SQL</div>
                    <div style="flex: 2;">Origem Custo</div>
                    <div style="flex: 1;">Status</div>
                    <div style="flex: 1; text-align: center;">Ações</div>
                </div>""", unsafe_allow_html=True)
                
                for _, row in df_configs.iterrows():
                    c1, c2, c3, c4, c5, c6 = st.columns([2, 2, 2, 2, 1, 1])
                    c1.write(row['nome_produto'])
                    c2.write(row['nome_carteira'])
                    c3.code(row['nome_tabela_transacoes'])
                    # Exibe a origem custo, se existir
                    c4.write(row.get('origem_custo', '-'))
                    c5.write(row['status'])
                    
                    with c6:
                        b_ed, b_del = st.columns(2)
                        if b_ed.button("✏️", key=f"ed_cc_{row['id']}", help="Editar"):
                            dialog_editar_carteira_config(row)
                        if b_del.button("🗑️", key=f"del_cc_{row['id']}", help="Excluir"):
                            if excluir_carteira_config(row['id'], row['nome_tabela_transacoes']):
                                st.warning("Configuração removida.")
                                st.rerun()
                    st.markdown("<hr style='margin: 2px 0'>", unsafe_allow_html=True)
            else:
                st.info("Nenhuma carteira configurada no sistema.")

    # --- ABA CARTEIRA (NOVO: EM DESENVOLVIMENTO) ---
    with tab_carteira:
        st.markdown("### 💼 Gestão de Carteira")
        
        # 6. CONFIGURAÇÃO DE CARTEIRAS (PRODUTOS)
        with st.expander("📂 Nova Carteira (Produtos)", expanded=False):
            st.info("Cria carteiras e suas tabelas de transação automaticamente.")
            
            df_prods = listar_produtos_para_selecao()
            
            if not df_prods.empty:
                with st.container(border=True):
                    c_conf1, c_conf2, c_conf3, c_conf4 = st.columns([3, 3, 2, 2])
                    
                    # Select de Produtos
                    opts_p = df_prods.apply(lambda x: f"{x['nome']}", axis=1)
                    idx_p = c_conf1.selectbox("Produto", range(len(df_prods)), format_func=lambda x: opts_p[x], label_visibility="visible")
                    
                    # Nome Carteira
                    nome_cart = c_conf2.text_input("Nome da Carteira", placeholder="Ex: Vip 2024", label_visibility="visible")
                    
                    # Status
                    status_cart = c_conf3.selectbox("Status", ["ATIVO", "INATIVO"], label_visibility="visible")
                    
                    # Botão Salvar
                    c_conf4.write(""); c_conf4.write("")
                    if c_conf4.button("💾 Criar Carteira", type="primary", use_container_width=True):
                        if nome_cart:
                            prod_sel = df_prods.iloc[idx_p]
                            ok, msg = salvar_nova_carteira_sistema(int(prod_sel['id']), prod_sel['nome'], nome_cart, status_cart)
                            if ok: st.success(msg); time.sleep(1.5); st.rerun()
                            else: st.error(f"Erro: {msg}")
                        else:
                            st.warning("Preencha o nome da carteira.")
            else:
                st.warning("Nenhum produto cadastrado no sistema (Módulo Comercial).")

    # --- ABA RELATÓRIOS ---
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

if __name__ == "__main__":
    app_clientes()