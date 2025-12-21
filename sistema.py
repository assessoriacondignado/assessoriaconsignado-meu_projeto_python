import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
import os
import sys
import psycopg2
import uuid
import random
import string
import bcrypt 

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Assessoria Consignado", layout="wide")

# --- 2. FUNÇÕES DE SEGURANÇA (CRIPTOGRAFIA) ---
def hash_senha(senha):
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(senha.encode('utf-8'), salt).decode('utf-8')

def verificar_senha(senha_plana, senha_hash):
    try:
        if senha_hash == senha_plana:
            return True
        return bcrypt.checkpw(senha_plana.encode('utf-8'), senha_hash.encode('utf-8'))
    except:
        return False

# --- 3. ESTILOS VISUAIS GERAIS ---
st.markdown("""
<style>
    #MainMenu {visibility: hidden !important;}
    footer {display: none !important; visibility: hidden !important;}
    /* Ajuste Situação 1: Removida a ocultação total do header para permitir acesso à sidebar */
    .viewerBadge_container__1S137 {display: none !important;}
    .stAppDeployButton {display: none !important;}
    [data-testid="stFooter"], [data-testid="stDecoration"] {display: none !important;}
    .stApp { background-color: #f8f9fa; }
    .titulo-empresa { font-size: 22px !important; font-weight: 800; color: #333333; line-height: 1.1; }
    .subtitulo-empresa { font-size: 11px !important; color: #888888; }
    .block-container { padding-top: 1rem !important; }
</style>
""", unsafe_allow_html=True)

# --- 4. IMPORTAÇÃO DOS MÓDULOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "OPERACIONAL/CLIENTES E USUARIOS"))
sys.path.append(os.path.join(BASE_DIR, "OPERACIONAL/MODULO_W-API")) 
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL/PRODUTOS E SERVICOS"))
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL/PEDIDOS"))
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL/TAREFAS")) 

try:
    import modulo_cliente, modulo_usuario, modulo_wapi, conexao 
    try: import modulo_produtos
    except ImportError: modulo_produtos = None
    try: import modulo_pedidos
    except ImportError: modulo_pedidos = None
    try: import modulo_tarefas
    except ImportError: modulo_tarefas = None
except ImportError as e:
    st.error(f"Erro crítico ao carregar módulos: {e}")

# --- 5. GERENCIAMENTO DE CONEXÃO E SESSÃO ---
def get_conn():
    return psycopg2.connect(host=conexao.host, port=conexao.port, database=conexao.database, user=conexao.user, password=conexao.password)

def init_session_db():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessoes_ativas (
                token VARCHAR(50) PRIMARY KEY, id_usuario INTEGER,
                nome_usuario VARCHAR(100), data_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ultimo_clique TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit(); conn.close()
    except: pass

def validar_login_db(usuario_input, senha_input):
    try:
        conn = get_conn(); cursor = conn.cursor()
        sql = "SELECT id, nome, hierarquia, senha FROM clientes_usuarios WHERE (email = %s OR cpf = %s) AND ativo = TRUE"
        cursor.execute(sql, (usuario_input, usuario_input))
        resultado = cursor.fetchone(); conn.close()
        if resultado and verificar_senha(senha_input, resultado[3]):
            return {"id": resultado[0], "nome": resultado[1], "cargo": resultado[2]}
        return None
    except: return None

# --- 6. TELA DE LOGIN ---
def tela_login():
    st.markdown('<div style="text-align:center; padding:40px;"><h2>Assessoria Consignado</h2><p>Portal Integrado</p></div>', unsafe_allow_html=True)
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            usuario = st.text_input("E-mail ou CPF", key="user_in")
            senha = st.text_input("Senha", type="password", key="pass_in")
            if st.button("ENTRAR", use_container_width=True, type="primary"):
                user_data = validar_login_db(usuario, senha)
                if user_data:
                    st.session_state['logado'] = True
                    st.session_state['usuario_id'] = user_data['id']
                    st.session_state['usuario_nome'] = user_data['nome']
                    st.session_state['usuario_cargo'] = user_data['cargo']
                    st.rerun()
                else: st.error("Dados incorretos.")

# --- 7. BARRA SUPERIOR (SINCROZINADA COM CARGO) ---
def barra_superior():
    caminho_logo = os.path.join(BASE_DIR, "OPERACIONAL/MODULO_TELA_PRINCIPAL/logo.png")
    c_marca, c_menu, c_perfil = st.columns([2.5, 6.5, 3])
    with c_marca:
        if os.path.exists(caminho_logo): st.image(caminho_logo, width=45)
        st.markdown('<div class="titulo-empresa">ASSESSORIA CONSIGNADO</div>', unsafe_allow_html=True)

    with c_menu:
        # Ajuste Situação 2: Liberação baseada no cargo salvo na sessão
        cargo = st.session_state.get('usuario_cargo', 'Cliente')
        opcoes = ["COMERCIAL", "FINANCEIRO", "OPERACIONAL"] if cargo in ["Admin", "Gerente"] else ["OPERACIONAL"]
        selected = option_menu(menu_title=None, options=opcoes, icons=["cart", "folder", "gear"], orientation="horizontal")
    
    with c_perfil:
        st.write(f"👤 {st.session_state['usuario_nome']}")
        if st.button("Sair"):
            st.session_state.clear(); st.rerun()
    return selected

# --- 8. FUNÇÃO PRINCIPAL ---
def main():
    init_session_db()
    if not st.session_state.get('logado', False):
        tela_login()
    else:
        modulo = barra_superior()
        st.divider()
        
        # Ajuste Situação 4: Padronização da Sidebar com botões Home e Atualizar
        with st.sidebar:
            if st.button("🏠 Home", use_container_width=True):
                st.rerun()
            if st.button("🔄 Atualizar Página", use_container_width=True):
                st.rerun()
            st.divider()

        if modulo == "COMERCIAL":
            with st.sidebar:
                st.markdown("### 🛒 Comercial")
                menu = option_menu(None, ["Produtos e Serviços", "Gestão de Pedidos", "Controle de Tarefas"], icons=["box", "list-check", "calendar-event"])
            if menu == "Produtos e Serviços" and modulo_produtos: modulo_produtos.app_produtos()
            elif menu == "Gestão de Pedidos" and modulo_pedidos: modulo_pedidos.app_pedidos()
            elif menu == "Controle de Tarefas" and modulo_tarefas: modulo_tarefas.app_tarefas()
            
        elif modulo == "OPERACIONAL":
            with st.sidebar:
                st.markdown("### ⚙️ Operacional")
                menu = option_menu(None, ["Gestão de Clientes", "Usuários e Permissões", "W-API (WhatsApp)"], icons=["people", "person-vcard", "whatsapp"])
            if menu == "Gestão de Clientes" and modulo_cliente: modulo_cliente.app_clientes()
            elif menu == "Usuários e Permissões" and modulo_usuario: modulo_usuario.app_usuarios()
            elif menu == "W-API (WhatsApp)" and modulo_wapi: modulo_wapi.app_wapi()

if __name__ == "__main__":
    main()