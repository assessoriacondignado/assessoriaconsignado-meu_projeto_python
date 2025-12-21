import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
import os
import sys
import psycopg2
import bcrypt 

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Assessoria Consignado", layout="wide")

# --- 2. FUNÇÕES DE SEGURANÇA ---
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
    /* REMOVE A BARRA SUPERIOR (GITHUB, SHARE, EDIT) */
    [data-testid="stHeader"] {
        display: none !important;
    }

    #MainMenu {visibility: hidden !important;}
    footer {display: none !important; visibility: hidden !important;}
    .stAppDeployButton {display: none !important;}
    [data-testid="stFooter"], [data-testid="stDecoration"] {display: none !important;}
    .stApp { background-color: #f8f9fa; }
    .titulo-empresa { font-size: 16px !important; font-weight: 800; color: #333333; margin-top: 5px; }
    .block-container { padding-top: 1rem !important; }
    
    /* Botões compactos para apenas ícones */
    [data-testid="stSidebar"] .stButton button { 
        width: 100%; 
        padding: 0px; 
        height: 35px; 
        font-size: 18px;
    }

    /* Aproximação de itens na Sidebar e redução de gaps */
    [data-testid="stSidebarContent"] div.stVerticalBlock {
        gap: 0.2rem !important;
    }
    hr { margin: 0.5rem 0px !important; }

    /* Estilo para títulos dos menus */
    .menu-title { font-size: 12px !important; font-weight: bold; color: #666; text-transform: uppercase; }
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

# --- 5. GERENCIAMENTO DE CONEXÃO E LOGIN ---
def get_conn():
    return psycopg2.connect(host=conexao.host, port=conexao.port, database=conexao.database, user=conexao.user, password=conexao.password)

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
                    st.session_state['usuario_nome'] = user_data['nome']
                    st.session_state['usuario_cargo'] = user_data['cargo']
                    st.rerun()
                else: st.error("Dados incorretos.")

# --- 7. FUNÇÃO PRINCIPAL ---
def main():
    if not st.session_state.get('logado', False):
        tela_login()
    else:
        with st.sidebar:
            # Logo e Identificação
            caminho_logo = os.path.join(BASE_DIR, "OPERACIONAL/MODULO_TELA_PRINCIPAL/logo.png")
            if os.path.exists(caminho_logo): 
                st.image(caminho_logo, width=80)
            st.markdown('<div class="titulo-empresa">ASSESSORIA CONSIGNADO</div>', unsafe_allow_html=True)
            st.caption(f"👤 {st.session_state['usuario_nome']}")
            
            # Botões apenas com ícones
            c1, c2 = st.columns(2)
            with c1:
                if st.button("🏠"): st.rerun()
            with c2:
                if st.button("🔄"): st.rerun()
            
            st.divider()

            # Menu Principal (Tamanho reduzido)
            cargo = st.session_state.get('usuario_cargo', 'Cliente')
            opcoes_modulos = ["COMERCIAL", "FINANCEIRO", "OPERACIONAL"] if cargo in ["Admin", "Gerente"] else ["OPERACIONAL"]
            
            modulo_atual = option_menu(
                menu_title="MENU",
                options=opcoes_modulos,
                icons=["cart", "folder", "gear"],
                menu_icon=None,
                default_index=0,
                styles={
                    "container": {"padding": "0px !important", "background-color": "transparent"},
                    "menu-title": {"font-size": "12px", "text-transform": "uppercase", "font-weight": "bold"},
                    "nav-link": {"font-size": "13px", "text-align": "left", "margin": "0px"},
                    "nav-link-selected": {"background-color": "#FF4B4B"},
                }
            )

            # Submenus aproximados e sem título de módulo
            menu_sub = None
            if modulo_atual == "COMERCIAL":
                menu_sub = option_menu(
                    menu_title=None,
                    options=["Produtos e Serviços", "Gestão de Pedidos", "Controle de Tarefas"],
                    icons=["box", "list-check", "calendar-event"],
                    styles={
                        "container": {"padding": "0px !important"},
                        "nav-link": {"font-size": "12px", "margin": "0px"}
                    }
                )
            elif modulo_atual == "OPERACIONAL":
                menu_sub = option_menu(
                    menu_title=None,
                    options=["Gestão de Clientes", "Usuários e Permissões", "W-API (WhatsApp)"],
                    icons=["people", "person-vcard", "whatsapp"],
                    styles={
                        "container": {"padding": "0px !important"},
                        "nav-link": {"font-size": "12px", "margin": "0px"}
                    }
                )
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Sair do Sistema", type="secondary"):
                st.session_state.clear(); st.rerun()

        # ÁREA DE CONTEÚDO PRINCIPAL
        if modulo_atual == "COMERCIAL":
            if menu_sub == "Produtos e Serviços" and modulo_produtos: modulo_produtos.app_produtos()
            elif menu_sub == "Gestão de Pedidos" and modulo_pedidos: modulo_pedidos.app_pedidos()
            elif menu_sub == "Controle de Tarefas" and modulo_tarefas: modulo_tarefas.app_tarefas()
            
        elif modulo_atual == "OPERACIONAL":
            if menu_sub == "Gestão de Clientes" and modulo_cliente: modulo_cliente.app_clientes()
            elif menu_sub == "Usuários e Permissões" and modulo_usuario: modulo_usuario.app_usuarios()
            elif menu_sub == "W-API (WhatsApp)" and modulo_wapi: modulo_wapi.app_wapi()
            
        elif modulo_atual == "FINANCEIRO":
            st.info("Módulo Financeiro em desenvolvimento.")

if __name__ == "__main__":
    main()