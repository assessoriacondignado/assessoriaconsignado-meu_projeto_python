import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import pool
import bcrypt
import time
import contextlib
import sys
import os
import re

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

try:
    import modulo_validadores as v
except ImportError:
    st.error("Erro: modulo_validadores.py não encontrado.")
    v = None

# ==============================================================================
# 1. CONEXÃO BLINDADA (Connection Pool + Auto-Recovery)
# ==============================================================================

@st.cache_resource
def get_pool():
    if not conexao: return None
    try:
        # [ATUALIZADO] ThreadedConnectionPool para maior capacidade e segurança em threads
        return psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=30, 
            host=conexao.host, port=conexao.port,
            database=conexao.database, user=conexao.user, password=conexao.password,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5
        )
    except Exception as e:
        st.error(f"Erro fatal no Pool de Conexão: {e}")
        return None

@contextlib.contextmanager
def get_conn():
    """
    Gerenciador de contexto robusto com AUTO-RECOVERY.
    Se o pool estiver cheio ou quebrado, ele reinicia automaticamente.
    """
    pool_obj = get_pool()
    if not pool_obj:
        yield None
        return
    
    conn = None
    try:
        # Tenta pegar conexão
        try:
            conn = pool_obj.getconn()
        except (psycopg2.pool.PoolError, IndexError):
            # Se der erro de pool cheio, limpa e recria
            st.warning("⚠️ Pool de conexões reiniciado (Auto-Recovery)...")
            get_pool.clear()
            pool_obj = get_pool()
            conn = pool_obj.getconn()

        # Health Check (Verifica se a conexão está viva)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        except (psycopg2.InterfaceError, psycopg2.OperationalError, psycopg2.DatabaseError):
            # Conexão morta, descarta e tenta outra
            if conn:
                try: pool_obj.putconn(conn, close=True)
                except: pass
            conn = pool_obj.getconn()

        yield conn

    except Exception as e:
        st.error(f"Erro de Conexão: {e}")
        if conn:
            try: pool_obj.putconn(conn, close=True)
            except: pass
        yield None
    finally:
        if conn:
            try: pool_obj.putconn(conn)
            except: pass

def ler_dados_seguro(query, params=None):
    """
    Executa leituras no banco com retentativa automática (Retry Logic)
    """
    max_tentativas = 3
    for i in range(max_tentativas):
        try:
            with get_conn() as conn:
                if not conn: return pd.DataFrame()
                if params:
                    return pd.read_sql(query, conn, params=tuple(params))
                else:
                    return pd.read_sql(query, conn)
        except Exception as e:
            time.sleep(0.5)
            if i == max_tentativas - 1:
                st.error(f"Erro ao ler dados: {e}")
                return pd.DataFrame()
            continue
    return pd.DataFrame()

# --- FUNÇÕES AUXILIARES ---

def hash_senha(senha):
    if senha.startswith('$2b$'): return senha
    return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def limpar_formatacao_texto(texto):
    if not texto: return ""
    return str(texto).replace('*', '').strip()

# --- FUNÇÕES DE BANCO DE DADOS (CRUD ATUALIZADO) ---

def listar_permissoes_nivel():
    return ler_dados_seguro("SELECT id, nivel FROM permissão.permissão_grupo_nivel ORDER BY id")

def buscar_usuario_por_id(id_user):
    df = ler_dados_seguro(f"SELECT * FROM clientes_usuarios WHERE id = {id_user}")
    if not df.empty: return df.iloc[0]
    return None

def salvar_usuario_novo(nome, email, cpf, tel, senha, nivel, ativo):
    # Trata CPF para BigInt
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(cpf) if v else 0
    if not cpf_val: cpf_val = 0

    with get_conn() as conn:
        if not conn: return None
        try:
            with conn.cursor() as cur:
                senha_f = hash_senha(senha)
                if not nivel: nivel = 'Cliente sem permissão'
                
                cur.execute("""
                    INSERT INTO clientes_usuarios (nome, email, cpf, telefone, senha, nivel, ativo) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s) 
                    RETURNING id
                """, (nome, email, cpf_val, tel, senha_f, nivel, ativo))
                
                nid = cur.fetchone()[0]
            conn.commit()
            return nid
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")
            return None

def atualizar_usuario_existente(id_user, nome, email, nivel, senha, ativo, cpf=None, tel=None):
    with get_conn() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                # Prepara valor do CPF
                if cpf is not None:
                    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(cpf) if v else 0
                    if not cpf_val: cpf_val = 0
                    
                    if senha:
                        senha_f = hash_senha(senha)
                        cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, nivel=%s, senha=%s, ativo=%s, cpf=%s, telefone=%s WHERE id=%s", 
                                    (nome, email, nivel, senha_f, ativo, cpf_val, tel, id_user))
                    else:
                        cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, nivel=%s, ativo=%s, cpf=%s, telefone=%s WHERE id=%s", 
                                    (nome, email, nivel, ativo, cpf_val, tel, id_user))
                else:
                    # Modo legado
                    if senha:
                        senha_f = hash_senha(senha)
                        cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, nivel=%s, senha=%s, ativo=%s WHERE id=%s", 
                                    (nome, email, nivel, senha_f, ativo, id_user))
                    else:
                        cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, nivel=%s, ativo=%s WHERE id=%s", 
                                    (nome, email, nivel, ativo, id_user))
            conn.commit()
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}")
            return False

# --- FUNÇÃO PRINCIPAL DO MÓDULO ---

def app_usuario():
    # --- Lógica de Navegação (State Machine) ---
    if 'view_usuario' not in st.session_state:
        st.session_state['view_usuario'] = 'lista'

    # --- Header e Botão Novo ---
    if st.session_state['view_usuario'] == 'lista':
        c1, c2 = st.columns([6, 1])
        busca_user = c1.text_input("🔍 Buscar Usuário", placeholder="Nome ou Email", key="input_busca_usuario_main")
        
        if c2.button("➕ Novo", type="primary", key="btn_novo_usuario_main"):
            st.session_state['view_usuario'] = 'novo'
            st.rerun()

        # Montagem da Query de Listagem
        sql_u = "SELECT id, nome, email, nivel, ativo FROM clientes_usuarios WHERE 1=1"
        params = []
        if busca_user: 
            sql_u += " AND (nome ILIKE %s OR email ILIKE %s)"
            params = [f"%{busca_user}%", f"%{busca_user}%"]
        sql_u += " ORDER BY id DESC"
        
        # [ALTERAÇÃO] Uso da leitura segura
        df_users = ler_dados_seguro(sql_u, params)

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
            
            # Campo CPF - Formatação para tela
            val_cpf_ini = v.ValidadorDocumentos.cpf_para_tela(dados.get('cpf')) if v else str(dados.get('cpf', ''))
            cpf = c3.text_input("CPF", value=val_cpf_ini)
            
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
                        # Atualiza com CPF e Telefone
                        res = atualizar_usuario_existente(st.session_state['user_id'], nome, email, nivel, senha, ativo, cpf, tel)
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

if __name__ == "__main__":
    app_usuario()