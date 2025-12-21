import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
import os
import sys
import psycopg2
import socket
import uuid
import random
import string

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Assessoria Consignado", layout="wide")

# --- ESTILOS VISUAIS GERAIS (OCULTAÇÃO TOTAL E AGRESSIVA) ---
st.markdown("""
<style>
    /* Ocultar Menu, Rodapé e Cabeçalho padrão */
    #MainMenu {visibility: hidden !important;}
    footer {display: none !important; visibility: hidden !important;}
    header {display: none !important; visibility: hidden !important;}
    
    /* Ocultar especificamente o ícone vermelho e o texto do Streamlit Cloud (Viewer Badge) */
    .viewerBadge_container__1S137 {display: none !important;}
    .viewerBadge_link__1S137 {display: none !important;}
    div[class^="viewerBadge"] {display: none !important;}
    #tabs-bop-container {display: none !important;}
    
    /* Ocultar botões de Deploy e decorações de sistema */
    .stAppDeployButton {display: none !important;}
    [data-testid="stHeader"] {display: none !important;}
    [data-testid="stFooter"] {display: none !important;}
    [data-testid="stDecoration"] {display: none !important;}
    
    /* Ajustes de fundo e containers */
    .stApp { background-color: #f8f9fa; }
    .titulo-empresa {
        font-size: 22px !important;
        font-weight: 800;
        color: #333333;
        font-family: 'Arial', sans-serif;
        margin-bottom: -5px;
        line-height: 1.1;
    }
    .subtitulo-empresa {
        font-size: 11px !important;
        color: #888888;
        font-family: 'Arial', sans-serif;
        font-weight: 400;
    }
    .block-container { padding-top: 1rem !important; }
</style>
""", unsafe_allow_html=True)

# --- IMPORTAÇÃO DOS MÓDULOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "OPERACIONAL/CLIENTES E USUARIOS"))
sys.path.append(os.path.join(BASE_DIR, "OPERACIONAL/MODULO_W-API")) 
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL/PRODUTOS E SERVICOS"))
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL/PEDIDOS"))
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL/TAREFAS")) 

try:
    import modulo_cliente
    import modulo_usuario
    import modulo_wapi 
    import conexao 
    
    try:
        import modulo_produtos
    except ImportError:
        modulo_produtos = None
        
    try:
        import modulo_pedidos
    except ImportError:
        modulo_pedidos = None

    try:
        import modulo_tarefas
    except ImportError:
        modulo_tarefas = None
        
except ImportError as e:
    modulo_cliente = None
    modulo_usuario = None
    modulo_wapi = None
    modulo_produtos = None
    modulo_pedidos = None
    modulo_tarefas = None
    st.error(f"Erro crítico ao importar módulos: {e}")

# --- GERENCIAMENTO DE SESSÃO E SEGURANÇA (DB) ---
def get_conn():
    return psycopg2.connect(
        host=conexao.host, 
        port=conexao.port, 
        database=conexao.database, 
        user=conexao.user, 
        password=conexao.password
    )

def init_session_db():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessoes_ativas (
                token VARCHAR(50) PRIMARY KEY,
                id_usuario INTEGER,
                nome_usuario VARCHAR(100),
                data_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ultimo_clique TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS logs_reset_senha (
                id SERIAL PRIMARY KEY,
                id_usuario INTEGER,
                data_solicitacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Erro DB Sessão: {e}")

# --- FUNÇÕES DE RESET DE SENHA ---
def gerar_senha_aleatoria():
    letras = "".join(random.choices(string.ascii_letters, k=3))
    caractere = random.choice("!@#$%&*")
    numeros = "".join(random.choices(string.digits, k=3))
    return f"{letras}{caractere}{numeros}"

def processar_reset_senha(email_input):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, nome, telefone FROM clientes_usuarios WHERE email = %s", (email_input,))
        usuario = cur.fetchone()
        
        if not usuario:
            conn.close()
            return "erro_email"
        
        user_id, nome_user, tel_user = usuario
        cur.execute("""
            SELECT count(*) FROM logs_reset_senha 
            WHERE id_usuario = %s AND data_solicitacao > NOW() - INTERVAL '12 hours'
        """, (user_id,))
        tentativas = cur.fetchone()[0]
        
        if tentativas >= 5:
            cur.execute("UPDATE clientes_usuarios SET ativo = FALSE WHERE id = %s", (user_id,))
            conn.commit()
            conn.close()
            return "usuario_bloqueado"
        
        cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
        instancia = cur.fetchone()
        
        if not instancia:
            conn.close()
            return "erro_configuracao"

        inst_id, inst_token = instancia
        nova_senha = gerar_senha_aleatoria()
        cur.execute("UPDATE clientes_usuarios SET senha = %s WHERE id = %s", (nova_senha, user_id))
        cur.execute("INSERT INTO logs_reset_senha (id_usuario) VALUES (%s)", (user_id,))
        
        if modulo_wapi:
            msg = f"Olá {nome_user}, sua nova senha de acesso é: {nova_senha}"
            res = modulo_wapi.enviar_msg_api(inst_id, inst_token, tel_user, msg)
            if res.get('messageId') or res.get('success') is True:
                conn.commit()
                conn.close()
                return "sucesso"
        
        conn.commit()
        conn.close()
        return "erro_envio"
    except Exception as e:
        st.error(f"Erro Reset: {e}")
        return "erro_geral"

@st.dialog("Solicitação de Redefinição de Senha")
def popup_reset_senha():
    st.write("Insira seu e-mail cadastrado para receber uma nova senha via WhatsApp.")
    email_reset = st.text_input("E-mail cadastrado", placeholder="exemplo@email.com")
    
    if st.button("Nova Senha", type="primary", use_container_width=True):
        if email_reset:
            resultado = processar_reset_senha(email_reset)
            if resultado == "sucesso":
                st.success("RESET FINALIZADO, NOVA SENHA ENVIADA PARA SEU WHATSAPP")
            elif resultado == "erro_email":
                st.error("RESET ERRO: E-MAIL NÃO LOCALIZADO")
            elif resultado == "usuario_bloqueado":
                st.warning("Usuário inativado por excesso de tentativas.")
            elif resultado == "erro_configuracao":
                st.error("Nenhuma instância de WhatsApp encontrada.")
            else:
                st.error("Ocorreu um erro ao processar sua solicitação.")
        else:
            st.warning("Por favor, digite o e-mail.")

# --- LOGIN E SESSÃO ---
def criar_sessao_db(id_user, nome_user):
    token = str(uuid.uuid4())
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM sessoes_ativas WHERE id_usuario = %s", (id_user,))
        cur.execute("INSERT INTO sessoes_ativas (token, id_usuario, nome_usuario, ultimo_clique) VALUES (%s, %s, %s, NOW())", (token, id_user, nome_user))
        conn.commit()
        conn.close()
        return token
    except Exception: return None

def validar_e_atualizar_sessao(token):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id_usuario, nome_usuario, ultimo_clique FROM sessoes_ativas WHERE token = %s", (token,))
        res = cur.fetchone()
        if res:
            id_user, nome_user, ultimo_clique = res
            if (datetime.now() - ultimo_clique) < timedelta(minutes=30):
                cur.execute("UPDATE sessoes_ativas SET ultimo_clique = NOW() WHERE token = %s", (token,))
                conn.commit()
                conn.close()
                return {"id": id_user, "nome": nome_user, "valido": True}
            else:
                cur.execute("DELETE FROM sessoes_ativas WHERE token = %s", (token,))
                conn.commit()
        conn.close()
    except: pass
    return None

def logout_sessao(token):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM sessoes_ativas WHERE token = %s", (token,))
        conn.commit()
        conn.close()
    except: pass

def validar_login_db(usuario_input, senha_input):
    try:
        conn = get_conn()
        cursor = conn.cursor()
        sql = "SELECT id, nome, hierarquia FROM clientes_usuarios WHERE (email = %s OR cpf = %s) AND senha = %s AND ativo = TRUE"
        cursor.execute(sql, (usuario_input, usuario_input, senha_input))
        resultado = cursor.fetchone()
        conn.close()
        return {"id": resultado[0], "nome": resultado[1], "cargo": resultado[2]} if resultado else None
    except: return None

def buscar_permissoes(id_usuario):
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT modulo FROM permissoes WHERE id_usuario = %s AND acesso = TRUE", (id_usuario,))
        mods = [row[0] for row in cursor.fetchall()]
        conn.close()
        return mods
    except: return []

# --- INICIALIZAÇÃO ---
init_session_db()
token_atual = st.query_params.get("session_id", None)
dados_sessao = None

if token_atual:
    dados_sessao = validar_e_atualizar_sessao(token_atual)

if dados_sessao and dados_sessao["valido"]:
    st.session_state['logado'] = True
    st.session_state['usuario_id'] = dados_sessao["id"]
    st.session_state['usuario_nome'] = dados_sessao["nome"]
    if 'permissoes' not in st.session_state:
        st.session_state['permissoes'] = buscar_permissoes(dados_sessao["id"])
else:
    st.session_state['logado'] = False
    if token_atual: st.query_params.clear() 

# --- TELA DE LOGIN ---
def tela_login():
    st.markdown("""
    <style>
        header, footer, [data-testid="stHeader"], [data-testid="stFooter"], [data-testid="stSidebar"], .viewerBadge_container__1S137 {display: none !important; visibility: hidden !important;}
        .stApp { display: flex; justify-content: center; align-items: center; }
        .login-card {
            background-color: white; padding: 40px; border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1); width: 400px;
            text-align: center; border-top: 6px solid #FF4B2B;
            position: fixed; top: 50%; left: 50%;
            transform: translate(-50%, -50%); z-index: 999;
        }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
        <div class="login-card">
            <div style="font-size: 28px; font-weight: bold; color: #444;">Assessoria Consignado</div>
            <div style="font-size: 14px; color: #888; margin-bottom: 20px;">Portal Integrado</div>
    """, unsafe_allow_html=True)

    col_login_central = st.container()
    with col_login_central:
        usuario = st.text_input("E-mail", placeholder="Seu e-mail ou CPF", key="user_in")
        senha = st.text_input("Senha", type="password", placeholder="Sua senha", key="pass_in")
        
        if st.button("ENTRAR", use_container_width=True, type="primary"):
            user_data = validar_login_db(usuario, senha)
            if user_data:
                token = criar_sessao_db(user_data['id'], user_data['nome'])
                if token:
                    st.query_params["session_id"] = token
                    st.session_state['logado'] = True
                    st.session_state['usuario_id'] = user_data['id']
                    st.session_state['usuario_nome'] = user_data['nome']
                    st.session_state['permissoes'] = buscar_permissoes(user_data['id'])
                    st.rerun()
            else: st.error("Dados incorretos.")
        
        if st.button("Esqueceu a senha?", key="reset_link"):
            popup_reset_senha()

    st.markdown("</div>", unsafe_allow_html=True)

# --- BARRA SUPERIOR ---
def barra_superior():
    caminho_logo = os.path.join(BASE_DIR, "OPERACIONAL/MODULO_TELA_PRINCIPAL/logo.png")
    c_marca, c_menu, c_perfil = st.columns([2.5, 6.5, 3])
    
    with c_marca:
        col_img, col_txt = st.columns([1, 3])
        with col_img:
            if os.path.exists(caminho_logo): st.image(caminho_logo, width=45)
            else: st.markdown("📷") 
        with col_txt:
            st.markdown('<div class="titulo-empresa">ASSESSORIA CONSIGNADO</div>', unsafe_allow_html=True)
            st.markdown('<div class="subtitulo-empresa">Sistema de Gestão</div>', unsafe_allow_html=True)

    with c_menu:
        opcoes = ["OPERACIONAL"]
        perms = st.session_state.get('permissoes', [])
        nome = st.session_state.get('usuario_nome', '')
        if "Administrador" in nome or "Admin" in nome:
            opcoes = ["COMERCIAL", "FINANCEIRO", "OPERACIONAL"]
        else:
            if "COMERCIAL" in perms: opcoes.insert(0, "COMERCIAL")
            if "FINANCEIRO" in perms: 
                idx = 1 if "COMERCIAL" in opcoes else 0
                opcoes.insert(idx, "FINANCEIRO")
        
        selected = option_menu(
            menu_title=None, 
            options=opcoes, 
            icons=["cart", "folder", "gear"], 
            orientation="horizontal",
            styles={
                "container": {"padding": "0!important", "background-color": "transparent"}, 
                "nav-link": {"font-size": "14px", "color": "#444"}, 
                "nav-link-selected": {"background-color": "transparent", "color": "#FF4B4B", "border-bottom": "2px solid #FF4B4B", "border-radius": "0px"}
            }
        )
    
    with c_perfil:
        cp1, cp2 = st.columns([3, 1])
        with cp1: st.markdown(f"<div style='color:#333; text-align:right; font-size:13px; margin-top:5px;'>👤 <b>{st.session_state['usuario_nome']}</b></div>", unsafe_allow_html=True)
        with cp2:
            if st.button("Sair", key="btn_sair_header"):
                token = st.query_params.get("session_id")
                if token: logout_sessao(token)
                st.query_params.clear(); st.session_state.clear(); st.rerun()
    return selected

def main():
    if not st.session_state.get('logado', False):
        tela_login()
    else:
        modulo = barra_superior()
        st.markdown("<hr style='margin-top:-10px; margin-bottom:20px; opacity:0.1;'>", unsafe_allow_html=True) 
        
        if modulo == "COMERCIAL":
            with st.sidebar:
                menu_com = option_menu("Comercial", ["Produtos e Serviços", "Gestão de Pedidos", "Controle de Tarefas"], icons=['box-seam', 'cart-check', 'list-check'], default_index=0)
            if menu_com == "Produtos e Serviços" and modulo_produtos: modulo_produtos.app_produtos()
            elif menu_com == "Gestão de Pedidos" and modulo_pedidos: modulo_pedidos.app_pedidos()
            elif menu_com == "Controle de Tarefas" and modulo_tarefas: modulo_tarefas.app_tarefas()
            
        elif modulo == "FINANCEIRO":
            st.subheader("💰 Módulo Financeiro"); st.info("Em desenvolvimento")
            
        elif modulo == "OPERACIONAL":
            with st.sidebar:
                menu_ops = option_menu("Operacional", ["Gestão de Clientes", "Usuários e Permissões", "W-API (WhatsApp)", "Configurações"], icons=['people', 'shield-lock', 'whatsapp', 'gear'], default_index=0)
            if menu_ops == "Gestão de Clientes" and modulo_cliente: modulo_cliente.app_clientes()
            elif menu_ops == "Usuários e Permissões" and modulo_usuario: modulo_usuario.app_usuarios()
            elif menu_ops == "W-API (WhatsApp)" and modulo_wapi: modulo_wapi.app_wapi()
            elif menu_ops == "Configurações": st.info("⚙️ Configurações Gerais")

if __name__ == "__main__":
    main()