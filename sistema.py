import streamlit as st
from streamlit_option_menu import option_menu
import os
import sys
import psycopg2
import bcrypt
import pandas as pd
from datetime import datetime
import random
import string
import time

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Assessoria Consignado", layout="wide", page_icon="📈")

# --- 2. CONFIGURAÇÃO DE CAMINHOS ---
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

# --- 3. IMPORTAÇÕES DE MÓDULOS ---
try:
    import conexao
    import modulo_cliente
    import modulo_usuario
    import modulo_wapi
    modulo_produtos = __import__('modulo_produtos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PRODUTOS E SERVICOS/modulo_produtos.py")) else None
    modulo_pedidos = __import__('modulo_pedidos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PEDIDOS/modulo_pedidos.py")) else None
    modulo_tarefas = __import__('modulo_tarefas') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/TAREFAS/modulo_tarefas.py")) else None
except Exception as e:
    st.error(f"Erro ao carregar módulos: {e}")

# --- 4. FUNÇÕES DE BANCO E SEGURANÇA ---
@st.cache_resource(ttl=600)
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password, connect_timeout=5
        )
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

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
        sql = """SELECT id, nome, hierarquia, senha FROM clientes_usuarios 
                 WHERE (LOWER(email) = %s OR cpf = %s OR telefone = %s) AND ativo = TRUE"""
        cursor.execute(sql, (usuario_limpo, usuario_limpo, usuario_limpo))
        resultado = cursor.fetchone()
        conn.close()
        if resultado and verificar_senha(senha_input, resultado[3]):
            return {"id": resultado[0], "nome": resultado[1], "cargo": resultado[2]}
    except: return None
    return None

# --- 5. FUNÇÕES DE RESET DE SENHA ---
def gerar_nova_senha(tamanho=8):
    caracteres = string.ascii_letters + string.digits
    return ''.join(random.choice(caracteres) for i in range(tamanho))

def processar_reset_senha(identificador):
    identificador = str(identificador).strip().lower()
    conn = get_conn()
    if not conn: return False, "Falha na conexão com o banco."
    
    try:
        cur = conn.cursor()
        # 1. Localizar Usuário
        cur.execute("""SELECT id, nome, telefone FROM clientes_usuarios 
                       WHERE (LOWER(email) = %s OR cpf = %s OR telefone = %s) AND ativo = TRUE""", 
                    (identificador, identificador, identificador))
        user = cur.fetchone()
        
        if not user:
            conn.close()
            return False, "Usuário não localizado. Verifique os dados informados."
        
        id_user, nome_user, tel_user = user
        if not tel_user:
            conn.close()
            return False, "Usuário não possui telefone cadastrado para envio."

        # 2. Gerar e Criptografar Senha
        nova_senha_plana = gerar_nova_senha()
        senha_hash = bcrypt.hashpw(nova_senha_plana.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # 3. Buscar Instância W-API Ativa
        cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
        instancia = cur.fetchone()
        if not instancia:
            conn.close()
            return False, "Serviço de WhatsApp temporariamente indisponível."
        
        # 4. Enviar Mensagem via W-API
        msg = f"Olá {nome_user.split()[0]}! 🛡️\nSua senha de acesso ao sistema foi resetada.\n\nNova Senha: *{nova_senha_plana}*\n\nRecomendamos alterar sua senha após o login."
        res = modulo_wapi.enviar_msg_api(instancia[0], instancia[1], tel_user, msg)
        
        if res.get('messageId') or res.get('success'):
            # 5. Atualizar no Banco apenas se o envio der certo
            cur.execute("UPDATE clientes_usuarios SET senha = %s WHERE id = %s", (senha_hash, id_user))
            conn.commit()
            conn.close()
            return True, f"Tudo certo, {nome_user.split()[0]}! Uma nova senha foi enviada para o seu WhatsApp."
        else:
            conn.close()
            return False, "Erro ao enviar mensagem de WhatsApp. Tente novamente."
            
    except Exception as e:
        if conn: conn.close()
        return False, f"Erro interno: {e}"

@st.dialog("Recuperar Acesso")
def dialog_reset_senha():
    st.write("Informe seus dados para receber uma nova senha via WhatsApp.")
    identificador = st.text_input("E-mail, CPF ou Telefone")
    if st.button("Enviar Nova Senha", use_container_width=True, type="primary"):
        if identificador:
            com_sucesso, mensagem = processar_reset_senha(identificador)
            if com_sucesso:
                st.success(mensagem)
                time.sleep(3)
                st.rerun()
            else:
                st.error(mensagem)
        else:
            st.warning("Por favor, preencha o campo de identificação.")

# --- 6. INTERFACE PRINCIPAL ---
def main():
    if 'logado' not in st.session_state: st.session_state['logado'] = False

    if not st.session_state['logado']:
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
            
            # ATUALIZAÇÃO: Botão de Reset de Senha
            if st.button("Esqueci minha senha", use_container_width=True):
                dialog_reset_senha()
                
    else:
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