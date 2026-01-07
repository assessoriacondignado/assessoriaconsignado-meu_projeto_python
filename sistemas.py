import streamlit as st
import os
import sys
import psycopg2
import bcrypt
import pandas as pd
from datetime import datetime, timedelta
import time

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Assessoria Consignado", layout="wide", page_icon="📈")

# --- CSS PERSONALIZADO (AQUI ESTÁ A MÁGICA DO DESIGN) ---
def aplicar_estilos_visuais():
    st.markdown("""
        <style>
        /* --- GERAL --- */
        .stApp {
            background-color: #f5f5f5; /* Fundo cinza claro igual ao login */
        }
        
        /* --- TELA DE LOGIN (CARD) --- */
        /* Estiliza o container que envolve o login para parecer um cartão */
        div[data-testid="stVerticalBlock"] > div:has(div.login-header) {
            background-color: white;
            padding: 40px;
            border-radius: 8px;
            border-top: 5px solid #ff5722; /* Borda Laranja */
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            max-width: 450px;
            margin: auto;
        }

        /* Botão de Login (Azul Vibrante) */
        .btn-login button {
            background-color: #0066ff !important;
            color: white !important;
            border: none !important;
            font-weight: bold !important;
            padding: 10px !important;
            transition: background-color 0.3s !important;
        }
        .btn-login button:hover {
            background-color: #0052cc !important;
        }

        /* --- SIDEBAR (MENU LATERAL) --- */
        section[data-testid="stSidebar"] {
            background-color: white !important;
            border-right: 1px solid #ddd;
        }
        
        /* Botões do Menu */
        div.stButton > button {
            width: 100%;
            border: none !important;
            background-color: transparent !important;
            color: #555 !important;
            text-align: left !important;
            padding-left: 15px !important;
            font-size: 16px !important;
            font-weight: 500 !important;
        }
        div.stButton > button:hover {
            background-color: #ffebe6 !important; /* Laranja bem claro */
            color: #ff5722 !important; /* Laranja texto */
            font-weight: bold !important;
        }
        
        /* Títulos do Menu */
        .menu-title {
            color: #ff5722;
            font-size: 18px;
            font-weight: 600;
            margin-top: 20px;
            margin-bottom: 10px;
            padding-left: 10px;
            text-transform: uppercase;
        }

        /* --- BARRA SUPERIOR (HEADER) --- */
        .top-bar {
            background-color: white;
            padding: 15px 30px;
            border-bottom: 1px solid #ddd;
            margin: -6rem -4rem 20px -4rem; /* Ajuste para colar no topo */
            color: #ff5722;
            font-size: 18px;
            font-weight: 600;
            text-transform: uppercase;
            display: flex;
            align-items: center;
        }
        </style>
    """, unsafe_allow_html=True)

# Aplica os estilos imediatamente
aplicar_estilos_visuais()

# --- 2. CONFIGURAÇÃO DE CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Lista de pastas onde o sistema deve procurar os módulos
pastas_modulos = [
    "OPERACIONAL/CLIENTES",
    "OPERACIONAL/BANCO DE PLANILHAS",
    "OPERACIONAL/MODULO_W-API",
    "OPERACIONAL/MODULO_CHAT",
    "COMERCIAL/PRODUTOS E SERVICOS",
    "COMERCIAL/PEDIDOS",
    "COMERCIAL/TAREFAS",
    "COMERCIAL/RENOVACAO E FEEDBACK",
    "CONEXÕES",
    "" 
]

for pasta in pastas_modulos:
    caminho = os.path.join(BASE_DIR, pasta)
    if caminho not in sys.path:
        sys.path.append(caminho)

# --- 3. IMPORTAÇÕES DE MÓDULOS ---
try:
    import conexao
    import modulo_wapi
    import modulo_whats_controlador
    
    # Importações seguras
    try:
        import modulo_tela_cliente
    except ImportError:
        try: from OPERACIONAL.CLIENTES import modulo_tela_cliente
        except: modulo_tela_cliente = None
        
    try:
        import modulo_permissoes
    except ImportError:
        try: from OPERACIONAL.CLIENTES import modulo_permissoes
        except: modulo_permissoes = None

    # Módulos opcionais
    def safe_import(name, path_part):
        full_path = os.path.join(BASE_DIR, path_part)
        return __import__(name) if os.path.exists(full_path) else None

    modulo_chat = safe_import('modulo_chat', "OPERACIONAL/MODULO_CHAT/modulo_chat.py")
    modulo_pf = safe_import('modulo_pessoa_fisica', "OPERACIONAL/BANCO DE PLANILHAS/modulo_pessoa_fisica.py")
    modulo_produtos = safe_import('modulo_produtos', "COMERCIAL/PRODUTOS E SERVICOS/modulo_produtos.py")
    modulo_pedidos = safe_import('modulo_pedidos', "COMERCIAL/PEDIDOS/modulo_pedidos.py")
    modulo_tarefas = safe_import('modulo_tarefas', "COMERCIAL/TAREFAS/modulo_tarefas.py")
    modulo_rf = safe_import('modulo_renovacao_feedback', "COMERCIAL/RENOVACAO E FEEDBACK/modulo_renovacao_feedback.py")
    modulo_pf_campanhas = safe_import('modulo_pf_campanhas', "OPERACIONAL/BANCO DE PLANILHAS/modulo_pf_campanhas.py")
    modulo_conexoes = safe_import('modulo_conexoes', "CONEXÕES/modulo_conexoes.py")

except Exception as e:
    st.error(f"Aviso do Sistema: Alguns módulos não foram carregados corretamente ({e}).")

# --- 4. FUNÇÕES DE ESTADO E UTILITÁRIOS ---

def iniciar_estado():
    if 'ultima_atividade' not in st.session_state:
        st.session_state['ultima_atividade'] = datetime.now()
    if 'hora_login' not in st.session_state:
        st.session_state['hora_login'] = datetime.now()
    if 'menu_aberto' not in st.session_state:
        st.session_state['menu_aberto'] = None
    if 'pagina_atual' not in st.session_state:
        st.session_state['pagina_atual'] = "Início"
    if 'logado' not in st.session_state:
        st.session_state['logado'] = False

def resetar_atividade():
    st.session_state['ultima_atividade'] = datetime.now()

def gerenciar_sessao():
    TEMPO_LIMITE_MINUTOS = 60
    agora = datetime.now()
    tempo_inativo = agora - st.session_state['ultima_atividade']
    if tempo_inativo.total_seconds() > (TEMPO_LIMITE_MINUTOS * 60):
        st.session_state.clear()
        st.error("Sessão expirada por inatividade. Por favor, faça login novamente.")
        st.stop()
    return ""

# --- 5. BANCO DE DADOS E AUTH ---
@st.cache_resource(ttl=600)
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password, connect_timeout=5
        )
    except: return None

def verificar_senha(senha_plana, senha_hash):
    try:
        if senha_hash == senha_plana: return True 
        return bcrypt.checkpw(senha_plana.encode('utf-8'), senha_hash.encode('utf-8'))
    except: return False

def validar_login_db(usuario_input, senha_input):
    conn = get_conn()
    if not conn: return None
    try:
        usuario_limpo = str(usuario_input).strip().lower()
        cursor = conn.cursor()
        sql = """SELECT id, nome, nivel, senha, email, COALESCE(tentativas_falhas, 0) 
                 FROM clientes_usuarios 
                 WHERE (LOWER(TRIM(email)) = %s OR TRIM(cpf) = %s OR TRIM(telefone) = %s) AND ativo = TRUE"""
        cursor.execute(sql, (usuario_limpo, usuario_limpo, usuario_limpo))
        res = cursor.fetchone()
        
        if res:
            id_user, nome, cargo, senha_hash, email_user, falhas = res
            if falhas >= 5: return {"status": "bloqueado"}
            
            if verificar_senha(senha_input, senha_hash):
                cursor.execute("UPDATE clientes_usuarios SET tentativas_falhas = 0 WHERE id = %s", (id_user,))
                conn.commit()
                return {"id": id_user, "nome": nome, "cargo": cargo, "email": email_user, "status": "sucesso"}
            else:
                cursor.execute("UPDATE clientes_usuarios SET tentativas_falhas = tentativas_falhas + 1 WHERE id = %s", (id_user,))
                conn.commit()
                return {"status": "erro_senha", "restantes": 4 - falhas}
    except: return None
    return None

# --- 7. RENDERIZAÇÃO DO MENU (SIDEBAR ESTILIZADO) ---
def renderizar_menu_lateral():
    with st.sidebar:
        # Título / Logo
        st.markdown("""
            <div style="margin-bottom: 30px;">
                <h1 style="color:#ff5722; margin:0; font-size: 32px;">logo</h1>
                <p style="color:#777; font-size: 12px; margin:0;">NOME EMPRESA</p>
            </div>
        """, unsafe_allow_html=True)
        
        # Estrutura do Menu
        nome_completo = st.session_state.get('usuario_nome', 'Usuário')
        email_user = st.session_state.get('usuario_email', 'email@exemplo.com')
        cargo_banco = st.session_state.get('usuario_cargo', 'Cargo')
        
        # Definição dos Menus
        cargo_normalizado = str(cargo_banco).strip().upper()
        estrutura_menu = {}
        
        if cargo_normalizado in ["ADMIN", "GERENTE", "ADMINISTRADOR"]:
            estrutura_menu["Operacional"] = ["CLIENTES ASSESSORIA", "Banco PF", "Campanhas", "WhatsApp"]
            estrutura_menu["Comercial"] = ["Produtos", "Pedidos", "Tarefas", "Renovação"]
            estrutura_menu["Conexões"] = [] 
        else:
            estrutura_menu["Operacional"] = ["CLIENTES ASSESSORIA", "WhatsApp"]

        # Renderização dos Botões
        st.markdown('<div class="menu-title">MENU</div>', unsafe_allow_html=True)
        
        if st.button("🏠 INÍCIO", key="btn_home", on_click=resetar_atividade):
            st.session_state['pagina_atual'] = "Início"
            st.session_state['menu_aberto'] = None

        for menu_pai, subitens in estrutura_menu.items():
            # Se não tem subitens, é um link direto
            if not subitens:
                if st.button(f"{menu_pai.upper()}", key=f"pai_{menu_pai}", on_click=resetar_atividade):
                    st.session_state['pagina_atual'] = menu_pai
                    st.session_state['menu_aberto'] = None
                continue

            # Se tem subitens, funciona como accordion
            seta = "▼" if st.session_state['menu_aberto'] == menu_pai else "►"
            if st.button(f"{menu_pai.upper()}", key=f"pai_{menu_pai}", on_click=resetar_atividade):
                novo_estado = None if st.session_state['menu_aberto'] == menu_pai else menu_pai
                st.session_state['menu_aberto'] = novo_estado
            
            # Submenus (Identados)
            if st.session_state['menu_aberto'] == menu_pai:
                for item in subitens:
                    # Usando colunas para dar indentação
                    c_espaco, c_btn = st.columns([0.15, 0.85])
                    with c_btn:
                        if st.button(f"{item}", key=f"sub_{menu_pai}_{item}", on_click=resetar_atividade):
                            st.session_state['pagina_atual'] = f"{menu_pai} > {item}"

        # Rodapé do Menu (Usuário)
        st.markdown("---")
        st.markdown(f"""
            <div style="font-size: 13px; color: #555;">
                <strong>{nome_completo}</strong><br>
                <span style="color:#888;">{email_user}</span>
            </div>
        """, unsafe_allow_html=True)
        
        if st.button("Sair", key="btn_sair"):
            st.session_state.clear()
            st.rerun()

# --- 8. FUNÇÃO PRINCIPAL ---
def main():
    iniciar_estado()
    
    # --- TELA DE LOGIN ---
    if not st.session_state.get('logado'):
        # Container centralizado
        col1, col2, col3 = st.columns([1, 1.5, 1])
        with col2:
            # Marcador para aplicar o estilo do CARD
            st.markdown('<div class="login-header"></div>', unsafe_allow_html=True) 
            
            st.markdown("""
                <h2 style='text-align: center; color: #555; margin-bottom: 5px;'>Assessoria Consignado</h2>
                <p style='text-align: center; color: #999; font-size: 14px; margin-bottom: 30px;'>Portal Integrado</p>
            """, unsafe_allow_html=True)
            
            u = st.text_input("E-mail", placeholder="Digite seu e-mail")
            s = st.text_input("Senha", type="password", placeholder="Digite sua senha")
            
            # Botão com classe CSS personalizada
            st.markdown('<div class="btn-login">', unsafe_allow_html=True)
            if st.button("ENTRAR", use_container_width=True):
                res = validar_login_db(u, s)
                if res:
                    if res.get('status') == "sucesso":
                        st.session_state.update({'logado': True, 'usuario_id': res['id'], 'usuario_nome': res['nome'], 'usuario_cargo': res['cargo'], 'usuario_email': res.get('email', '')})
                        st.rerun()
                    elif res.get('status') == "bloqueado": st.error("🚨 USUÁRIO BLOQUEADO.")
                    else: st.error(f"Senha incorreta. Tentativas: {res.get('restantes')}")
                else: st.error("Usuário não encontrado.")
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown("<p style='text-align: center; margin-top: 15px; font-size: 13px;'><a href='#' style='color: #777; text-decoration: none;'>Esqueceu a senha?</a></p>", unsafe_allow_html=True)

    # --- SISTEMA LOGADO ---
    else:
        renderizar_menu_lateral()
        
        pag = st.session_state['pagina_atual']
        
        # Barra Superior Fixa (Header Visual)
        titulo_display = pag.replace(">", "-").upper()
        st.markdown(f"""
            <div class="top-bar">
                MENU DOS MODULOS - {titulo_display}
            </div>
        """, unsafe_allow_html=True)

        # Roteamento de Módulos
        if pag == "Início":
            if modulo_chat: modulo_chat.app_chat_screen()
            else: st.info("Bem-vindo! Selecione um módulo no menu lateral.")
            
        elif "Operacional > CLIENTES ASSESSORIA" in pag and modulo_tela_cliente: 
            modulo_tela_cliente.app_clientes()
            
        elif "Operacional > Banco PF" in pag and modulo_pf: modulo_pf.app_pessoa_fisica()
        elif "Operacional > Campanhas" in pag and modulo_pf_campanhas: modulo_pf_campanhas.app_campanhas()
        elif "Operacional > WhatsApp" in pag: modulo_whats_controlador.app_wapi()
        elif "Comercial > Produtos" in pag and modulo_produtos: modulo_produtos.app_produtos()
        elif "Comercial > Pedidos" in pag and modulo_pedidos: modulo_pedidos.app_pedidos()
        elif "Comercial > Tarefas" in pag and modulo_tarefas: modulo_tarefas.app_tarefas()
        elif "Comercial > Renovação" in pag and modulo_rf: modulo_rf.app_renovacao_feedback()
        elif pag == "Conexões" and modulo_conexoes: modulo_conexoes.app_conexoes()
        else:
            if " > " in pag:
                st.warning(f"O módulo '{pag}' ainda está em desenvolvimento ou não foi carregado.")
            else:
                st.write("Utilize o menu lateral para navegar.")

if __name__ == "__main__":
    main()