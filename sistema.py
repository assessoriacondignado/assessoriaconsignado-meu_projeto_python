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
    #MainMenu {visibility: hidden !important;}
    footer {display: none !important; visibility: hidden !important;}
    .stAppDeployButton {display: none !important;}
    [data-testid="stFooter"], [data-testid="stDecoration"] {display: none !important;}
    .stApp { background-color: #f8f9fa; }
    .titulo-empresa { font-size: 18px !important; font-weight: 800; color: #333333; margin-top: 10px; }
    .block-container { padding-top: 1rem !important; }
    
    /* Aproximação e ajuste dos botões na sidebar */
    [data-testid="stSidebar"] .stButton button { 
        width: 100%; 
        padding: 5px; 
        height: 38px; 
        font-size: 14px;
    }
    /* Estilo para o menu lateral */
    .nav-link { margin: 2px 0px !important; }
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

# --- 7. FUNÇÃO PRINCIPAL (LAYOUT LATERAL) ---
def main():
    if not st.session_state.get('logado', False):
        tela_login()
    else:
        # --- CONFIGURAÇÃO DA SIDEBAR ---
        with st.sidebar:
            # Logo e Identificação da Empresa
            caminho_logo = os.path.join(BASE_DIR, "OPERACIONAL/MODULO_TELA_PRINCIPAL/logo.png")
            if os.path.exists(caminho_logo): 
                st.image(caminho_logo, width=100)
            st.markdown('<div class="titulo-empresa">ASSESSORIA CONSIGNADO</div>', unsafe_allow_html=True)
            
            # Dados do Usuário
            st.markdown(f"**👤 Usuário:** {st.session_state['usuario_nome']}")
            
            # Botões de Ação Rápida (Aproximados em colunas)
            c1, c2 = st.columns(2)
            with c1:
                if st.button("🏠 Home"): st.rerun()
            with c2:
                if st.button("🔄 Atualizar"): st.rerun()
            
            st.divider()

            # Menu Principal (Módulos)
            cargo = st.session_state.get('usuario_cargo', 'Cliente')
            opcoes_modulos = ["COMERCIAL", "FINANCEIRO", "OPERACIONAL"] if cargo in ["Admin", "Gerente"] else ["OPERACIONAL"]
            
            modulo_atual = option_menu(
                menu_title="MÓDULOS",
                options=opcoes_modulos,
                icons=["cart", "folder", "gear"],
                menu_icon="app-indicator",
                default_index=0,
                styles={
                    "container": {"padding": "5px !important", "background-color": "#ffffff"},
                    "nav-link": {"font-size": "14px", "text-align": "left", "margin": "0px"},
                    "nav-link-selected": {"background-color": "#FF4B4B"}, # Cor primaryColor
                }
            )

            st.divider()

            # Submenus Dinâmicos
            menu_sub = None
            if modulo_atual == "COMERCIAL":
                menu_sub = option_menu(
                    menu_title="COMERCIAL",
                    options=["Produtos e Serviços", "Gestão de Pedidos", "Controle de Tarefas"],
                    icons=["box", "list-check", "calendar-event"],
                    styles={"nav-link": {"font-size": "13px"}}
                )
            elif modulo_atual == "OPERACIONAL":
                menu_sub = option_menu(
                    menu_title="OPERACIONAL",
                    options=["Gestão de Clientes", "Usuários e Permissões", "W-API (WhatsApp)"],
                    icons=["people", "person-vcard", "whatsapp"],
                    styles={"nav-link": {"font-size": "13px"}}
                )
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🚪 Sair do Sistema"):
                st.session_state.clear(); st.rerun()

        # --- ÁREA DE CONTEÚDO ---
        if modulo_atual == "COMERCIAL":
            if menu_sub == "Produtos e Serviços" and modulo_produtos: modulo_produtos.app_produtos()
            elif menu_sub == "Gestão de Pedidos" and modulo_pedidos: modulo_pedidos.app_pedidos()
            elif menu_sub == "Controle de Tarefas" and modulo_tarefas: modulo_tarefas.app_tarefas()
            
        elif modulo_atual == "OPERACIONAL":
            if menu_sub == "Gestão de Clientes" and modulo_cliente: modulo_cliente.app_clientes()
            elif menu_sub == "Usuários e Permissões" and modulo_usuario: modulo_usuario.app_usuarios()
            elif menu_sub == "W-API (WhatsApp)" and modulo_wapi: modulo_wapi.app_wapi()
            
        elif modulo_atual == "FINANCEIRO":
            st.info("O módulo Financeiro está agendado para futuras implementações.")

if __name__ == "__main__":
    main()