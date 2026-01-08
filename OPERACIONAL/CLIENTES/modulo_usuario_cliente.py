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

# --- FUNÇÕES DE CONEXÃO E AUXILIARES ---

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

def limpar_formatacao_texto(texto):
    if not texto: return ""
    return str(texto).replace('*', '').strip()

# --- FUNÇÕES DE BANCO DE DADOS ---

def listar_permissoes_nivel():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, nivel FROM permissão.permissão_grupo_nivel ORDER BY id", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def buscar_usuario_por_id(id_user):
    conn = get_conn()
    if not conn: return None
    try:
        df = pd.read_sql(f"SELECT * FROM clientes_usuarios WHERE id = {id_user}", conn)
        conn.close()
        if not df.empty: return df.iloc[0]
        return None
    except:
        if conn: conn.close()
        return None

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

def atualizar_usuario_existente(id_user, nome, email, nivel, senha, ativo):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        if senha:
            senha_f = hash_senha(senha)
            cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, nivel=%s, senha=%s, ativo=%s WHERE id=%s", 
                        (nome, email, nivel, senha_f, ativo, id_user))
        else:
            cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, nivel=%s, ativo=%s WHERE id=%s", 
                        (nome, email, nivel, ativo, id_user))
        conn.commit(); conn.close(); return True
    except Exception as e:
        print(e)
        if conn: conn.close()
        return False

# --- FUNÇÃO PRINCIPAL DO MÓDULO ---

def app_usuario():
    # --- Lógica de Navegação (State Machine) ---
    if 'view_usuario' not in st.session_state:
        st.session_state['view_usuario'] = 'lista'

    # --- Header e Botão Novo ---
    if st.session_state['view_usuario'] == 'lista':
        c1, c2 = st.columns([6, 1])
        # KEY ADICIONADA: garante unicidade do input de busca
        busca_user = c1.text_input("🔍 Buscar Usuário", placeholder="Nome ou Email", key="input_busca_usuario_main")
        
        # KEY ADICIONADA: garante unicidade do botão novo
        if c2.button("➕ Novo", type="primary", key="btn_novo_usuario_main"):
            st.session_state['view_usuario'] = 'novo'
            st.rerun()

        conn = get_conn()
        if not conn:
            st.error("Erro de conexão.")
            return

        sql_u = "SELECT id, nome, email, nivel, ativo FROM clientes_usuarios WHERE 1=1"
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

        # --- Tabela Visual ---
        if not df_users.empty:
            st.markdown("""
            <div style="display:flex; font-weight:bold; color:#555; padding:8px; border-bottom:2px solid #ddd; margin-bottom:10px; background-color:#f8f9fa;">
                <div style="flex:3;">Nome</div>
                <div style="flex:3;">Email</div>
                <div style="flex:2;">Nível</div>
                <div style="flex:1;">Status</div>
                <div style="flex:1; text-align:center;">Ações</div>
            </div>
            """, unsafe_allow_html=True)

            # Iterrows com enumerate para garantir índice único
            for idx, row in df_users.iterrows():
                with st.container():
                    c1, c2, c3, c4, c5 = st.columns([3, 3, 2, 1, 1])
                    c1.write(f"**{limpar_formatacao_texto(row['nome'])}**")
                    c2.write(row['email'])
                    c3.write(row['nivel'])
                    
                    cor_st = 'green' if row['ativo'] else 'red'
                    status_txt = "ATIVO" if row['ativo'] else "INATIVO"
                    c4.markdown(f":{cor_st}[{status_txt}]")
                    
                    with c5:
                        # KEY COMPOSTA ADICIONADA: id + índice do loop para evitar duplicação absoluta
                        if st.button("✏️", key=f"btn_edit_user_{row['id']}_{idx}", help="Editar Usuário"):
                            st.session_state['view_usuario'] = 'editar'
                            st.session_state['user_id'] = row['id']
                            st.rerun()
                    
                    st.markdown("<hr style='margin: 5px 0; border-color: #eee;'>", unsafe_allow_html=True)
        else:
            st.info("Nenhum usuário encontrado.")

    # --- Formulário de Criação / Edição ---
    elif st.session_state['view_usuario'] in ['novo', 'editar']:
        st.markdown(f"### {'📝 Novo Usuário' if st.session_state['view_usuario']=='novo' else '✏️ Editar Usuário'}")
        
        dados = {}
        if st.session_state['view_usuario'] == 'editar':
            dados = buscar_usuario_por_id(st.session_state['user_id'])
            if dados is None:
                st.error("Usuário não encontrado.")
                st.session_state['view_usuario'] = 'lista'
                st.rerun()

        df_niveis = listar_permissoes_nivel()
        lista_niveis = df_niveis['nivel'].tolist() if not df_niveis.empty else ["Cliente sem permissão"]

        with st.form("form_usuario_main"):
            c1, c2 = st.columns(2)
            nome = c1.text_input("Nome Completo *", value=dados.get('nome', ''))
            email = c2.text_input("Login (Email) *", value=dados.get('email', ''))

            c3, c4 = st.columns(2)
            cpf = c3.text_input("CPF", value=dados.get('cpf', '')) 
            tel = c4.text_input("Telefone", value=dados.get('telefone', ''))

            c5, c6, c7 = st.columns([2, 2, 1])
            
            idx_nivel = 0
            val_nivel_atual = dados.get('nivel', '')
            if val_nivel_atual in lista_niveis: idx_nivel = lista_niveis.index(val_nivel_atual)
            
            nivel = c5.selectbox("Nível de Acesso", options=lista_niveis, index=idx_nivel)
            senha = c6.text_input("Senha" + (" (Deixe vazio para manter)" if st.session_state['view_usuario']=='editar' else " *"), type="password")
            
            ativo_val = bool(dados.get('ativo', True))
            ativo = c7.checkbox("Usuário Ativo", value=ativo_val)

            st.markdown("<br>", unsafe_allow_html=True)
            b_col1, b_col2, _ = st.columns([1, 1, 4])
            
            # KEYS ADICIONADAS para os botões do formulário
            submitted = b_col1.form_submit_button("💾 Salvar")
            cancelled = b_col2.form_submit_button("Cancelar")

            if submitted:
                if not nome or not email:
                    st.warning("Preencha Nome e Email.")
                else:
                    if st.session_state['view_usuario'] == 'novo':
                        if not senha:
                            st.warning("Senha é obrigatória para novos usuários.")
                        else:
                            res = salvar_usuario_novo(nome, email, cpf, tel, senha, nivel, ativo)
                            if res:
                                st.success("Usuário criado!")
                                time.sleep(1)
                                st.session_state['view_usuario'] = 'lista'
                                st.rerun()
                            else:
                                st.error("Erro ao criar (verifique se email já existe).")
                    else:
                        res = atualizar_usuario_existente(st.session_state['user_id'], nome, email, nivel, senha, ativo)
                        if res:
                            st.success("Usuário atualizado!")
                            time.sleep(1)
                            st.session_state['view_usuario'] = 'lista'
                            st.rerun()
                        else:
                            st.error("Erro ao atualizar.")

            if cancelled:
                st.session_state['view_usuario'] = 'lista'
                st.rerun()