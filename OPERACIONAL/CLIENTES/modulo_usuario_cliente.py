import streamlit as st
import pandas as pd
import psycopg2
import bcrypt
import time

# Tenta importar conexao
try:
    import conexao
except ImportError:
    st.error("Erro: conexao.py não encontrado na raiz.")

# --- FUNÇÕES AUXILIARES ---

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        print(f"Erro conexão: {e}")
        return None

def hash_senha(senha):
    if senha.startswith('$2b$'): return senha
    return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# --- FUNÇÕES DE BANCO DE DADOS (USUÁRIO) ---

def listar_permissoes_nivel():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, nivel FROM permissão.permissão_grupo_nivel ORDER BY id", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def buscar_usuarios_disponiveis():
    """Lista usuários que ainda não estão vinculados a nenhum cliente (para uso geral)."""
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        query = "SELECT id, nome, email, cpf FROM clientes_usuarios WHERE id NOT IN (SELECT id_usuario_vinculo FROM admin.clientes WHERE id_usuario_vinculo IS NOT NULL) ORDER BY nome"
        df = pd.read_sql(query, conn); conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def salvar_usuario_novo(nome, email, cpf, tel, senha, nivel, ativo):
    conn = get_conn()
    if not conn: return None
    try:
        cur = conn.cursor(); senha_f = hash_senha(senha)
        if not nivel: nivel = 'Cliente sem permissão'
        
        cur.execute("""
            INSERT INTO clientes_usuarios (nome, email, cpf, telefone, senha, nivel, ativo) 
            VALUES (%s, %s, %s, %s, %s, %s, %s) 
            RETURNING id
        """, (nome, email, cpf, tel, senha_f, nivel, ativo))
        
        nid = cur.fetchone()[0]
        conn.commit(); conn.close(); return nid
    except Exception as e:
        print(e)
        if conn: conn.close()
        return None

# --- DIALOGS ---

@st.dialog("✨ Criar Novo Usuário")
def dialog_criar_usuario():
    df_niveis = listar_permissoes_nivel()
    lista_niveis = df_niveis['nivel'].tolist() if not df_niveis.empty else ["Cliente sem permissão"]
    
    with st.form("form_novo_user"):
        nome = st.text_input("Nome Completo")
        email = st.text_input("Login (Email)")
        cpf = st.text_input("CPF")
        tel = st.text_input("Telefone")
        senha = st.text_input("Senha Inicial", type="password")
        nivel = st.selectbox("Nível de Acesso", lista_niveis)
        ativo = st.checkbox("Ativo?", value=True)
        
        # Adicionado key única para evitar conflito
        if st.form_submit_button("Salvar Usuário", key="btn_save_new_user"):
            if nome and email and senha:
                novo_id = salvar_usuario_novo(nome, email, cpf, tel, senha, nivel, ativo)
                if novo_id:
                    st.success("Usuário criado com sucesso!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Erro ao criar usuário. Verifique se o email já existe.")
            else:
                st.warning("Preencha Nome, Email e Senha.")

# --- FUNÇÃO PRINCIPAL DO MÓDULO ---

def app_usuario():
    st.markdown("### Gestão de Acesso")
    
    c1, c2 = st.columns([6, 1])
    # Key única para o campo de busca principal
    busca_user = c1.text_input("Buscar Usuário", placeholder="Nome ou Email", key="input_busca_user_main")
    
    # Key única para o botão Novo
    if c2.button("➕ Novo", type="primary", key="btn_novo_user_main"):
        dialog_criar_usuario()
    
    conn = get_conn()
    if not conn:
        st.error("Erro de conexão.")
        return

    sql_u = "SELECT id, nome, email, nivel, ativo, telefone, id_grupo_whats FROM clientes_usuarios WHERE 1=1"
    if busca_user: 
        sql_u += f" AND (nome ILIKE '%%{busca_user}%%' OR email ILIKE '%%{busca_user}%%')"
    sql_u += " ORDER BY id DESC"
    
    try:
        df_users = pd.read_sql(sql_u, conn)
    except Exception as e:
        st.error(f"Erro na consulta: {e}")
        df_users = pd.DataFrame()
    finally:
        conn.close()

    df_niveis_disponiveis = listar_permissoes_nivel()
    lista_niveis = df_niveis_disponiveis['nivel'].tolist() if not df_niveis_disponiveis.empty else ["Cliente"]

    if not df_users.empty:
        for _, u in df_users.iterrows():
            # Container estilizado ou Expander
            label_status = "✅" if u['ativo'] else "❌"
            with st.expander(f"{label_status} {u['nome']} ({u['nivel']})"):
                with st.form(f"form_user_{u['id']}"):
                    c_n, c_e = st.columns(2)
                    
                    # ADICIONADO KEYS ÚNICAS PARA TODOS OS WIDGETS DENTRO DO LOOP
                    n_nome = c_n.text_input("Nome", value=u['nome'], key=f"nome_{u['id']}")
                    n_mail = c_e.text_input("Email", value=u['email'], key=f"email_{u['id']}")
                    
                    c_h, c_s, c_a = st.columns(3) 
                    
                    idx_n = 0
                    if u['nivel'] in lista_niveis:
                        idx_n = lista_niveis.index(u['nivel'])
                    
                    n_nivel = c_h.selectbox("Nível", lista_niveis, index=idx_n, key=f"nivel_{u['id']}")
                    n_senha = c_s.text_input("Nova Senha (deixe em branco para manter)", type="password", key=f"senha_{u['id']}")
                    n_ativo = c_a.checkbox("Ativo", value=u['ativo'], key=f"ativo_{u['id']}")
                    
                    # Botão de submit com key única
                    if st.form_submit_button("Atualizar Dados", key=f"btn_upd_{u['id']}"):
                        conn = get_conn()
                        if conn:
                            cur = conn.cursor()
                            try:
                                if n_senha: 
                                    cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, nivel=%s, senha=%s, ativo=%s WHERE id=%s", (n_nome, n_mail, n_nivel, hash_senha(n_senha), n_ativo, u['id']))
                                else: 
                                    cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, nivel=%s, ativo=%s WHERE id=%s", (n_nome, n_mail, n_nivel, n_ativo, u['id']))
                                conn.commit()
                                st.success("Atualizado!")
                                time.sleep(0.5)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Erro ao atualizar: {e}")
                            finally:
                                conn.close()
    else:
        st.info("Nenhum usuário encontrado.")