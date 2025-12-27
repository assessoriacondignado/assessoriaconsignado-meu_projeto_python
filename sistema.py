import streamlit as st
from streamlit_option_menu import option_menu
import os
import sys
import psycopg2
import bcrypt
import pandas as pd
from datetime import datetime, timedelta
import random
import string
import time

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Assessoria Consignado", layout="wide", page_icon="📈")

# --- 2. CONFIGURAÇÃO DE CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
pastas_modulos = [
    "OPERACIONAL/CLIENTES E USUARIOS",
    "OPERACIONAL/BANCO DE PLANILHAS",  # <--- NOVA PASTA ADICIONADA AQUI
    "OPERACIONAL/MODULO_W-API",
    "COMERCIAL/PRODUTOS E SERVICOS",
    "COMERCIAL/PEDIDOS",
    "COMERCIAL/TAREFAS",
    "COMERCIAL/RENOVACAO E FEEDBACK"
]
for pasta in pastas_modulos:
    caminho = os.path.join(BASE_DIR, pasta)
    if caminho not in sys.path:
        sys.path.append(caminho)

# --- 3. IMPORTAÇÕES DE MÓDULOS (Com tratamento de erro) ---
try:
    import conexao
    import modulo_cliente
    import modulo_usuario
    import modulo_wapi
    import modulo_whats_controlador  # <--- NOVO CONTROLADOR ADICIONADO
    
    # Importação do novo módulo com tratamento de erro
    modulo_pf = __import__('modulo_pessoa_fisica') if os.path.exists(os.path.join(BASE_DIR, "OPERACIONAL/BANCO DE PLANILHAS/modulo_pessoa_fisica.py")) else None
    
    modulo_produtos = __import__('modulo_produtos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PRODUTOS E SERVICOS/modulo_produtos.py")) else None
    modulo_pedidos = __import__('modulo_pedidos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PEDIDOS/modulo_pedidos.py")) else None
    modulo_tarefas = __import__('modulo_tarefas') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/TAREFAS/modulo_tarefas.py")) else None
    modulo_rf = __import__('modulo_renovacao_feedback') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/RENOVACAO E FEEDBACK/modulo_renovacao_feedback.py")) else None
except Exception as e:
    st.error(f"Erro ao carregar módulos: {e}")

# ... (restante das funções de banco e login permanecem iguais) ...
# ... (funções verificar_senha, validar_login_db, dialog_mensagem_rapida, dialog_reset_senha) ...

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
        sql = """SELECT id, nome, hierarquia, senha, COALESCE(tentativas_falhas, 0) 
                 FROM clientes_usuarios 
                 WHERE (LOWER(TRIM(email)) = %s OR TRIM(cpf) = %s OR TRIM(telefone) = %s) AND ativo = TRUE"""
        cursor.execute(sql, (usuario_limpo, usuario_limpo, usuario_limpo))
        res = cursor.fetchone()
        
        if res:
            id_user, nome, cargo, senha_hash, falhas = res
            if falhas >= 5: return {"status": "bloqueado"}
            
            if verificar_senha(senha_input, senha_hash):
                cursor.execute("UPDATE clientes_usuarios SET tentativas_falhas = 0 WHERE id = %s", (id_user,))
                conn.commit()
                return {"id": id_user, "nome": nome, "cargo": cargo, "status": "sucesso"}
            else:
                cursor.execute("UPDATE clientes_usuarios SET tentativas_falhas = tentativas_falhas + 1 WHERE id = %s", (id_user,))
                conn.commit()
                return {"status": "erro_senha", "restantes": 4 - falhas}
    except: return None
    return None

@st.dialog("🚀 Mensagem Rápida")
def dialog_mensagem_rapida():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
        inst = cur.fetchone()
        if not inst:
            st.error("Sem instância de WhatsApp ativa.")
            return

        opcao = st.selectbox("Destinatário", ["Selecionar Cliente", "Número Manual", "ID Grupo Manual"])
        destino = ""
        
        if opcao == "Selecionar Cliente":
            cur.execute("SELECT nome, telefone, id_grupo_whats FROM admin.clientes ORDER BY nome")
            clis = cur.fetchall()
            if clis:
                sel_n = st.selectbox("Buscar Cliente", [c[0] for c in clis])
                c_info = next(i for i in clis if i[0] == sel_n)
                tel, grp = c_info[1], c_info[2]
                opts = []
                if tel: opts.append(f"Telefone: {tel}")
                if grp: opts.append(f"Grupo: {grp}")
                if opts:
                    contato = st.radio("Enviar para:", opts)
                    destino = contato.split(": ")[1]
                else: st.warning("Cliente sem dados de contato.")
            else: st.warning("Nenhum cliente na base.")
        elif opcao == "Número Manual": destino = st.text_input("DDI+DDD+Número")
        elif opcao == "ID Grupo Manual": destino = st.text_input("ID (@g.us)")

        msg = st.text_area("Mensagem")
        if st.button("Enviar Agora", type="primary") and destino and msg:
            res = modulo_wapi.enviar_msg_api(inst[0], inst[1], destino, msg)
            if res.get('success') or res.get('messageId'):
                st.success("Enviado!"); time.sleep(1); st.rerun()
            else: st.error("Erro no envio.")
    finally:
        if 'cur' in locals(): cur.close()

@st.dialog("Recuperar Acesso")
def dialog_reset_senha():
    st.write("Receba uma nova senha via WhatsApp.")
    identificador = st.text_input("E-mail ou CPF")
    if st.button("Enviar Nova Senha", use_container_width=True, type="primary") and identificador:
        id_limpo = str(identificador).strip().lower()
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT id, nome, telefone FROM clientes_usuarios WHERE (LOWER(TRIM(email)) = %s OR TRIM(cpf) = %s) AND ativo = TRUE", (id_limpo, id_limpo))
        user = cur.fetchone()
        if user and user[2]:
            nova_s = ''.join(random.choice(string.ascii_letters + string.digits) for i in range(8))
            hash_s = bcrypt.hashpw(nova_s.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
            inst = cur.fetchone()
            if inst:
                msg = f"Olá {user[1].split()[0]}! Sua nova senha é: *{nova_s}*"
                res = modulo_wapi.enviar_msg_api(inst[0], inst[1], user[2], msg)
                if res.get('success') or res.get('messageId'):
                    cur.execute("UPDATE clientes_usuarios SET senha = %s, tentativas_falhas = 0 WHERE id = %s", (hash_s, user[0]))
                    conn.commit(); st.success("Senha enviada!"); time.sleep(2); st.rerun()
            else: st.error("WhatsApp indisponível.")
        else: st.error("Usuário não localizado ou sem telefone.")

# --- 7. INTERFACE PRINCIPAL ---
def main():
    if 'last_action' not in st.session_state: st.session_state['last_action'] = datetime.now()
    if st.session_state.get('logado') and datetime.now() - st.session_state['last_action'] > timedelta(minutes=30):
        st.session_state.clear(); st.warning("Sessão encerrada por inatividade."); st.rerun()
    st.session_state['last_action'] = datetime.now()

    if not st.session_state.get('logado'):
        st.markdown('<div style="text-align:center; padding:40px;"><h2>Assessoria Consignado</h2><p>Portal Integrado</p></div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            u = st.text_input("E-mail ou CPF")
            s = st.text_input("Senha", type="password")
            if st.button("ENTRAR", use_container_width=True, type="primary"):
                res = validar_login_db(u, s)
                if res:
                    if res.get('status') == "sucesso":
                        st.session_state.update({'logado': True, 'usuario_id': res['id'], 'usuario_nome': res['nome'], 'usuario_cargo': res['cargo']})
                        st.rerun()
                    elif res.get('status') == "bloqueado": st.error("🚨 USUÁRIO BLOQUEADO por múltiplas falhas.")
                    else: st.error(f"Senha incorreta. Tentativas restantes: {res.get('restantes')}")
                else: st.error("Acesso negado.")
            if st.button("Esqueci minha senha", use_container_width=True): dialog_reset_senha()
    else:
        col_m1, col_m2 = st.columns([10, 2])
        with col_m2:
            if st.button("🟢 Mensagem Rápida", use_container_width=True): dialog_mensagem_rapida()

        with st.sidebar:
            st.markdown('<div style="font-size:16px; font-weight:800; color:#333;">ASSESSORIA CONSIGNADO</div>', unsafe_allow_html=True)
            st.caption(f"👤 {st.session_state['usuario_nome']} ({st.session_state['usuario_cargo']})")
            if st.button("🏠 Home"): st.rerun()
            st.divider()
            cargo = st.session_state.get('usuario_cargo', 'Cliente')
            opcoes = ["OPERACIONAL"]
            if cargo in ["Admin", "Gerente"]: opcoes = ["COMERCIAL", "FINANCEIRO", "OPERACIONAL"]
            mod = option_menu("MENU PRINCIPAL", opcoes, icons=["cart", "cash", "gear"], default_index=0)
            
            if mod == "COMERCIAL":
                sub = option_menu(None, ["Produtos", "Pedidos", "Tarefas", "Renovação"], 
                                  icons=["box", "cart-check", "check2-all", "arrow-repeat"])
            elif mod == "OPERACIONAL":
                # MENU ATUALIZADO AQUI
                sub = option_menu(None, ["Clientes", "Usuários", "Banco PF", "WhatsApp"], 
                                  icons=["people", "lock", "person-vcard", "whatsapp"])
            else: sub = None
            if st.sidebar.button("Sair"): st.session_state.clear(); st.rerun()

        if mod == "COMERCIAL":
            if sub == "Produtos" and modulo_produtos: modulo_produtos.app_produtos()
            elif sub == "Pedidos" and modulo_pedidos: modulo_pedidos.app_pedidos()
            elif sub == "Tarefas" and modulo_tarefas: modulo_tarefas.app_tarefas()
            elif sub == "Renovação" and modulo_rf: modulo_rf.app_renovacao_feedback()
        elif mod == "OPERACIONAL":
            if sub == "Clientes": modulo_cliente.app_clientes()
            elif sub == "Usuários": modulo_usuario.app_usuarios()
            elif sub == "Banco PF" and modulo_pf: modulo_pf.app_pessoa_fisica() 
            elif sub == "WhatsApp": modulo_whats_controlador.app_wapi() # <--- CHAMADA CORRIGIDA AQUI

if __name__ == "__main__": main()