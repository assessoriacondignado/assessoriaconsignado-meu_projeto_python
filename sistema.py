import streamlit as st
from streamlit_option_menu import option_menu
import os
import sys
import psycopg2
import bcrypt
import pandas as pd
from datetime import datetime

# --- 1. CONFIGURAÇÃO DA PÁGINA (Sempre a primeira linha) ---
st.set_page_config(page_title="Assessoria Consignado", layout="wide", page_icon="📈")

# --- 2. EVITAR O RISCO DO FLASK (WEBHOOK) ---
# IMPORTANTE: Nunca importe 'webhook_wapi' aqui. 
# O Webhook deve rodar apenas no seu servidor Ubuntu como um serviço independente.

# --- 3. CONFIGURAÇÃO DE CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
pastas_modulos = [
    "OPERACIONAL/CLIENTES E USUARIOS",
    "OPERACIONAL/MODULO_W-API",
    "COMERCIAL/PRODUTOS E SERVICOS",
    "COMERCIAL/PEDIDOS",
    "COMERCIAL/TAREFAS"
]
for pasta in pastas_modulos:
    caminho = os.path.join(BASE_DIR, pasta)
    if caminho not in sys.path:
        sys.path.append(caminho)

# --- 4. IMPORTAÇÕES DE MÓDULOS (Com tratamento de erro para não travar o site) ---
try:
    import conexao
    import modulo_cliente
    import modulo_usuario
    import modulo_wapi
    # Módulos comerciais carregados sob demanda
    modulo_produtos = __import__('modulo_produtos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PRODUTOS E SERVICOS/modulo_produtos.py")) else None
    modulo_pedidos = __import__('modulo_pedidos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PEDIDOS/modulo_pedidos.py")) else None
    modulo_tarefas = __import__('modulo_tarefas') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/TAREFAS/modulo_tarefas.py")) else None
except Exception as e:
    st.error(f"Aviso: Alguns módulos não puderam ser carregados. Verifique os logs. Erro: {e}")

# --- 5. OTMIZAÇÃO DE BANCO DE DADOS (CACHE) ---
# Isso evita que o sistema trave nas consultas SQL que vimos nos logs
@st.cache_resource(ttl=600)
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, 
            port=conexao.port, 
            database=conexao.database, 
            user=conexao.user, 
            password=conexao.password,
            connect_timeout=5 # Timeout para não travar a tela se o banco demorar
        )
    except Exception as e:
        st.error(f"Erro de conexão com o Banco Absam: {e}")
        return None

def verificar_senha(senha_plana, senha_hash):
    try:
        if senha_hash == senha_plana: return True # Fallback para senhas simples
        return bcrypt.checkpw(senha_plana.encode('utf-8'), senha_hash.encode('utf-8'))
    except: return False

def validar_login_db(usuario_input, senha_input):
    conn = get_conn()
    if not conn: return None
    try:
        # ATUALIZAÇÃO: Limpeza de espaços e padronização para minúsculas
        usuario_limpo = str(usuario_input).strip().lower()
        
        cursor = conn.cursor()
        # ATUALIZAÇÃO: Uso de LOWER() no SQL para busca case-insensitive
        sql = """SELECT id, nome, hierarquia, senha 
                 FROM clientes_usuarios 
                 WHERE (LOWER(email) = %s OR cpf = %s) AND ativo = TRUE"""
        cursor.execute(sql, (usuario_limpo, usuario_limpo))
        resultado = cursor.fetchone()
        conn.close()
        
        if resultado and verificar_senha(senha_input, resultado[3]):
            return {"id": resultado[0], "nome": resultado[1], "cargo": resultado[2]}
    except Exception as e: 
        print(f"Erro no login: {e}")
        return None
    return None

# --- 6. ESTILOS VISUAIS ---
st.markdown("""
<style>
    [data-testid="stHeader"] { display: none !important; }
    .stApp { background-color: #f8f9fa; }
    .titulo-empresa { font-size: 16px !important; font-weight: 800; color: #333333; margin-top: 5px; }
</style>
""", unsafe_allow_html=True)

# --- 7. INTERFACE ---
def main():
    if 'logado' not in st.session_state: st.session_state['logado'] = False

    if not st.session_state['logado']:
        # TELA DE LOGIN
        st.markdown('<div style="text-align:center; padding:40px;"><h2>Assessoria Consignado</h2><p>Portal Integrado</p></div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            usuario = st.text_input("E-mail ou CPF")
            senha = st.text_input("Senha", type="password")
            if st.button("ENTRAR", use_container_width=True, type="primary"):
                user_data = validar_login_db(usuario, senha)
                if user_data:
                    st.session_state['logado'] = True
                    st.session_state['usuario_nome'] = user_data['nome']
                    st.session_state['usuario_cargo'] = user_data['cargo']
                    st.rerun()
                else: st.error("Acesso negado. Verifique os dados.")
    else:
        # SISTEMA APÓS LOGIN
        with st.sidebar:
            st.markdown('<div class="titulo-empresa">ASSESSORIA CONSIGNADO</div>', unsafe_allow_html=True)
            st.caption(f"👤 {st.session_state['usuario_nome']} ({st.session_state['usuario_cargo']})")
            
            if st.button("🏠 Home"): st.rerun()
            st.divider()

            cargo = st.session_state.get('usuario_cargo', 'Cliente')
            opcoes_menu = ["OPERACIONAL"]
            if cargo in ["Admin", "Gerente"]:
                opcoes_menu = ["COMERCIAL", "FINANCEIRO", "OPERACIONAL"]

            modulo_atual = option_menu("MENU PRINCIPAL", opcoes_menu, icons=["cart", "cash", "gear"], menu_icon="cast", default_index=0)

            if modulo_atual == "COMERCIAL":
                sub = option_menu(None, ["Produtos", "Pedidos", "Tarefas"], icons=["box", "cart-check", "check2-all"])
            elif modulo_atual == "OPERACIONAL":
                sub = option_menu(None, ["Clientes", "Usuários", "WhatsApp"], icons=["people", "lock", "whatsapp"])
            else: sub = None

            if st.sidebar.button("Sair"):
                st.session_state.clear()
                st.rerun()

        # RENDERIZAÇÃO DOS MÓDULOS
        if modulo_atual == "COMERCIAL":
            if sub == "Produtos" and modulo_produtos: modulo_produtos.app_produtos()
            elif sub == "Pedidos" and modulo_pedidos: modulo_pedidos.app_pedidos()
            elif sub == "Tarefas" and modulo_tarefas: modulo_tarefas.app_tarefas()
        elif modulo_atual == "OPERACIONAL":
            if sub == "Clientes": modulo_cliente.app_clientes()
            elif sub == "Usuários": modulo_usuario.app_usuarios()
            elif sub == "WhatsApp": modulo_wapi.app_wapi()

if __name__ == "__main__":
    main()