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
pastas_modulos = ["OPERACIONAL/CLIENTES E USUARIOS", "OPERACIONAL/MODULO_W-API", "COMERCIAL/PRODUTOS E SERVICOS", "COMERCIAL/PEDIDOS", "COMERCIAL/TAREFAS"]
for pasta in pastas_modulos:
    caminho = os.path.join(BASE_DIR, pasta)
    if caminho not in sys.path: sys.path.append(caminho)

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
    st.error(f"Aviso: Erro ao carregar módulos: {e}")

# --- 4. FUNÇÕES DE BANCO ---
@st.cache_resource(ttl=600)
def get_conn():
    try:
        return psycopg2.connect(host=conexao.host, port=conexao.port, database=conexao.database, user=conexao.user, password=conexao.password, connect_timeout=5)
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
        sql = "SELECT id, nome, hierarquia, senha FROM clientes_usuarios WHERE (LOWER(TRIM(email)) = %s OR TRIM(cpf) = %s) AND ativo = TRUE"
        cursor.execute(sql, (usuario_limpo, usuario_limpo))
        res = cursor.fetchone()
        conn.close()
        if res and verificar_senha(senha_input, res[3]):
            return {"id": res[0], "nome": res[1], "cargo": res[2]}
    except: return None
    return None

# --- 5. FUNÇÃO RESET DE SENHA (SISTEMA) ---
@st.dialog("Recuperar Acesso")
def dialog_reset_senha():
    st.write("Informe seu E-mail ou CPF para receber uma nova senha via WhatsApp.")
    identificador = st.text_input("Identificador")
    if st.button("Enviar Nova Senha", use_container_width=True, type="primary"):
        if identificador:
            id_limpo = str(identificador).strip().lower()
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT id, nome, telefone FROM clientes_usuarios WHERE (LOWER(TRIM(email)) = %s OR TRIM(cpf) = %s) AND ativo = TRUE", (id_limpo, id_limpo))
            user = cur.fetchone()
            
            if user and user[2]:
                nova_s = ''.join(random.choice(string.ascii_letters + string.digits) for i in range(8))
                hash_s = bcrypt.hashpw(nova_s.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                
                cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
                inst = cur.fetchone()
                if inst:
                    msg = f"Olá {user[1].split()[0]}! 🛡️\nSua nova senha de acesso é: *{nova_s}*"
                    res = modulo_wapi.enviar_msg_api(inst[0], inst[1], user[2], msg)
                    if res.get('success') or res.get('messageId'):
                        cur.execute("UPDATE clientes_usuarios SET senha = %s WHERE id = %s", (hash_s, user[0]))
                        conn.commit()
                        st.success("Senha enviada com sucesso!")
                        time.sleep(2); st.rerun()
                else: st.error("Serviço de WhatsApp indisponível.")
            else: st.error("Usuário não encontrado ou sem telefone.")
            conn.close()

# --- 6. INTERFACE ---
def main():
    if 'logado' not in st.session_state: st.session_state['logado'] = False

    if not st.session_state['logado']:
        st.markdown('<div style="text-align:center; padding:40px;"><h2>Assessoria Consignado</h2><p>Portal Integrado</p></div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            u = st.text_input("E-mail ou CPF")
            s = st.text_input("Senha", type="password")
            if st.button("ENTRAR", use_container_width=True, type="primary"):
                data = validar_login_db(u, s)
                if data:
                    st.session_state.update({'logado': True, 'usuario_nome': data['nome'], 'usuario_cargo': data['cargo']})
                    st.rerun()
                else: st.error("Acesso negado.")
            if st.button("Esqueci minha senha", use_container_width=True): dialog_reset_senha()
    else:
        with st.sidebar:
            st.markdown('<div style="font-size:16px; font-weight:800;">ASSESSORIA CONSIGNADO</div>', unsafe_allow_html=True)
            st.caption(f"👤 {st.session_state['usuario_nome']} ({st.session_state['usuario_cargo']})")
            if st.button("🏠 Home"): st.rerun()
            st.divider()
            cargo = st.session_state.get('usuario_cargo', 'Cliente')
            opcoes = ["OPERACIONAL"]
            if cargo in ["Admin", "Gerente"]: opcoes = ["COMERCIAL", "FINANCEIRO", "OPERACIONAL"]
            mod = option_menu("MENU", opcoes, icons=["cart", "cash", "gear"], default_index=0)
            
            if mod == "COMERCIAL": sub = option_menu(None, ["Produtos", "Pedidos", "Tarefas"], icons=["box", "cart-check", "check2-all"])
            elif mod == "OPERACIONAL": sub = option_menu(None, ["Clientes", "Usuários", "WhatsApp"], icons=["people", "lock", "whatsapp"])
            else: sub = None
            
            if st.sidebar.button("Sair"): st.session_state.clear(); st.rerun()

        if mod == "COMERCIAL":
            if sub == "Produtos" and modulo_produtos: modulo_produtos.app_produtos()
            elif sub == "Pedidos" and modulo_pedidos: modulo_pedidos.app_pedidos()
            elif sub == "Tarefas" and modulo_tarefas: modulo_tarefas.app_tarefas()
        elif mod == "OPERACIONAL":
            if sub == "Clientes": modulo_cliente.app_clientes()
            elif sub == "Usuários": modulo_usuario.app_usuarios()
            elif sub == "WhatsApp": modulo_wapi.app_wapi()

if __name__ == "__main__": main()