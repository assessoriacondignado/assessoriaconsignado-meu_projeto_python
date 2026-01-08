import streamlit as st
import os
import sys
import psycopg2
import bcrypt
# import pandas as pd  <-- REMOVIDO (Não era usado)
from datetime import datetime
import time

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Assessoria Consignado", layout="wide", page_icon="📈")

# --- 2. CONFIGURAÇÃO DE CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Pastas dos módulos
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

# Adiciona ao path apenas se não existir
for pasta in pastas_modulos:
    caminho = os.path.join(BASE_DIR, pasta)
    if os.path.exists(caminho) and caminho not in sys.path:
        sys.path.append(caminho)

# --- 3. IMPORTAÇÕES DE MÓDULOS ---
try:
    import conexao
    import modulo_wapi
    import modulo_whats_controlador
    
    try: import modulo_tela_cliente
    except ImportError: modulo_tela_cliente = None
        
    try: import modulo_permissoes
    except ImportError: modulo_permissoes = None

    def carregar_modulo(caminho_relativo, nome_modulo):
        if os.path.exists(os.path.join(BASE_DIR, caminho_relativo)):
            return __import__(nome_modulo)
        return None

    modulo_chat = carregar_modulo("OPERACIONAL/MODULO_CHAT/modulo_chat.py", "modulo_chat")
    modulo_pf = carregar_modulo("OPERACIONAL/BANCO DE PLANILHAS/modulo_pessoa_fisica.py", "modulo_pessoa_fisica")
    modulo_pf_campanhas = carregar_modulo("OPERACIONAL/BANCO DE PLANILHAS/modulo_pf_campanhas.py", "modulo_pf_campanhas")
    modulo_produtos = carregar_modulo("COMERCIAL/PRODUTOS E SERVICOS/modulo_produtos.py", "modulo_produtos")
    modulo_pedidos = carregar_modulo("COMERCIAL/PEDIDOS/modulo_pedidos.py", "modulo_pedidos")
    modulo_tarefas = carregar_modulo("COMERCIAL/TAREFAS/modulo_tarefas.py", "modulo_tarefas")
    modulo_rf = carregar_modulo("COMERCIAL/RENOVACAO E FEEDBACK/modulo_renovacao_feedback.py", "modulo_renovacao_feedback")
    modulo_conexoes = carregar_modulo("CONEXÕES/modulo_conexoes.py", "modulo_conexoes")

except Exception as e:
    st.error(f"Erro Crítico ao carregar módulos: {e}")

# --- 4. FUNÇÕES DE ESTADO ---
def iniciar_estado():
    if 'ultima_atividade' not in st.session_state:
        st.session_state['ultima_atividade'] = datetime.now()
    if 'hora_login' not in st.session_state:
        st.session_state['hora_login'] = datetime.now()
    if 'pagina_central' not in st.session_state:
        st.session_state['pagina_central'] = "Início"
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
        st.error("Sessão expirada. Faça login novamente.")
        st.stop()

    tempo_total = agora - st.session_state['hora_login']
    mm, ss = divmod(tempo_total.seconds, 60)
    hh, mm = divmod(mm, 60)
    return f"{hh:02d}:{mm:02d}" if hh > 0 else f"{mm:02d}:{ss:02d}"

# --- 5. BANCO DE DADOS ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password, connect_timeout=5
        )
    except Exception as e: 
        print(f"Erro DB: {e}")
        return None

def verificar_senha(senha_input, senha_hash):
    try:
        return bcrypt.checkpw(senha_input.encode('utf-8'), senha_hash.encode('utf-8'))
    except: return False

def validar_login_db(usuario, senha):
    conn = get_conn()
    if not conn: return {"status": "erro_conexao"}
    
    try:
        cur = conn.cursor()
        usuario = str(usuario).strip().lower()
        sql = """SELECT id, nome, nivel, senha, email, COALESCE(tentativas_falhas, 0) 
                 FROM clientes_usuarios 
                 WHERE (LOWER(TRIM(email)) = %s OR TRIM(cpf) = %s OR TRIM(telefone) = %s) 
                 AND ativo = TRUE"""
        cur.execute(sql, (usuario, usuario, usuario))
        res = cur.fetchone()
        
        if res:
            uid, nome, cargo, hash_db, email, falhas = res
            if falhas >= 5: return {"status": "bloqueado"}
            
            if verificar_senha(senha, hash_db):
                cur.execute("UPDATE clientes_usuarios SET tentativas_falhas = 0 WHERE id = %s", (uid,))
                conn.commit()
                return {"status": "sucesso", "id": uid, "nome": nome, "cargo": cargo, "email": email}
            else:
                cur.execute("UPDATE clientes_usuarios SET tentativas_falhas = tentativas_falhas + 1 WHERE id = %s", (uid,))
                conn.commit()
                return {"status": "erro_senha", "restantes": 4 - falhas}
        return {"status": "nao_encontrado"}
    except Exception as e:
        return {"status": "erro_generico", "msg": str(e)}
    finally:
        conn.close()

# --- 6. INTERFACE (MENSAGEM RÁPIDA - MANTIDA A FUNÇÃO, MAS O BOTÃO FOI REMOVIDO DA MAIN) ---
@st.dialog("🚀 Mensagem Rápida")
def dialog_mensagem_rapida():
    conn = get_conn()
    if not conn: st.error("Erro Conexão DB"); return

    try:
        cur = conn.cursor()
        cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
        inst = cur.fetchone()
        if not inst: st.warning("Configure a API do WhatsApp primeiro."); return

        opcao = st.radio("Destino", ["Cliente Cadastrado", "Número Avulso"], horizontal=True)
        destino = ""
        
        if opcao == "Cliente Cadastrado":
            cur.execute("SELECT nome, telefone FROM admin.clientes ORDER BY nome LIMIT 50")
            clis = cur.fetchall()
            if clis:
                sel = st.selectbox("Selecione", [f"{c[0]} | {c[1]}" for c in clis])
                destino = sel.split("|")[1].strip() if sel else ""
        else:
            destino = st.text_input("Número (ex: 5511999999999)")

        msg = st.text_area("Mensagem")
        if st.button("Enviar", type="primary"):
            if destino and msg:
                res = modulo_wapi.enviar_msg_api(inst[0], inst[1], destino, msg)
                if res.get('success'): st.success("Enviado!"); time.sleep(1); st.rerun()
                else: st.error("Erro no envio.")
            else: st.warning("Preencha todos os campos.")
    finally:
        conn.close()

# --- 7. MENU LATERAL E ESTILIZAÇÃO ---
def renderizar_menu_lateral():
    # CSS para Layout e Menu
    st.markdown("""
        <style>
        /* 1. Remove Espaço do Topo (Sobe o conteúdo) */
        .block-container {
            padding-top: 1rem !important;
            padding-bottom: 0rem !important;
            margin-top: 0px !important;
        }
        
        /* 2. Cor do Menu Lateral (Laranja Claro) */
        section[data-testid="stSidebar"] {
            background-color: #FFF3E0; /* Laranja bem claro */
        }
        
        /* 3. Estilo dos botões do menu */
        div.stButton > button {
            width: 100%; border: none; text-align: left; padding-left: 15px;
            background: transparent; color: #444;
        }
        div.stButton > button:hover { background: #ffe0b2; color: #d84315; font-weight: bold; }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### 🚀 Assessoria")
        st.markdown(f"Olá, **{st.session_state.get('usuario_nome', '').split()[0]}**")
        st.markdown("---")
        
        # Mapa de navegação
        botoes = {
            "🏠 Início": "Início",
            "👥 Clientes": "Clientes",
            "💼 Comercial": "Comercial",
            "🏦 Banco de Dados": "BancoDados",
            "💬 WhatsApp": "WhatsApp",
            "🔌 Conexões": "Conexoes"
        }
        
        for rotulo, chave in botoes.items():
            if st.button(rotulo):
                st.session_state['pagina_central'] = chave
                resetar_atividade()
                st.rerun()

        st.markdown("---")
        if st.button("🚪 Sair"):
            st.session_state.clear()
            st.rerun()

# --- 8. FUNÇÃO PRINCIPAL ---
def main():
    iniciar_estado()
    
    # 8.1 TELA DE LOGIN
    if not st.session_state['logado']:
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.title("🔐 Acesso Restrito")
            u = st.text_input("Usuário (E-mail/CPF)")
            s = st.text_input("Senha", type="password")
            if st.button("Entrar", type="primary", use_container_width=True):
                res = validar_login_db(u, s)
                if res['status'] == 'sucesso':
                    st.session_state.update({'logado': True, 'usuario_nome': res['nome'], 'usuario_cargo': res['cargo']})
                    st.rerun()
                elif res['status'] == 'bloqueado': st.error("Usuário bloqueado por excesso de tentativas.")
                elif res['status'] == 'erro_senha': st.error(f"Senha incorreta. Restam {res.get('restantes')} tentativas.")
                else: st.error("Erro no login ou usuário inexistente.")
    
    # 8.2 SISTEMA LOGADO
    else:
        renderizar_menu_lateral()
        
        # --- REMOVIDO O CABEÇALHO COM O BOTÃO 'Msg' ---
        # O conteúdo agora flui diretamente, ficando mais ao topo devido ao CSS inserido
        
        pagina = st.session_state['pagina_central']
        
        # Roteamento
        if pagina == "Início":
            if modulo_chat: modulo_chat.app_chat_screen()
            else: st.info("Painel Inicial (Módulo Chat não detectado)")
            
        elif pagina == "Clientes":
            if modulo_permissoes and modulo_permissoes.verificar_bloqueio_de_acesso("bloqueio_menu_cliente", "Clientes", False):
                st.error("🚫 Acesso Negado ao Módulo Clientes"); st.stop()
            
            if modulo_tela_cliente: modulo_tela_cliente.app_clientes()
            
        elif pagina == "Comercial":
            t1, t2, t3, t4 = st.tabs(["Produtos", "Pedidos", "Tarefas", "Renovação"])
            with t1: modulo_produtos.app_produtos() if modulo_produtos else st.warning("N/A")
            with t2: modulo_pedidos.app_pedidos() if modulo_pedidos else st.warning("N/A")
            with t3: modulo_tarefas.app_tarefas() if modulo_tarefas else st.warning("N/A")
            with t4: modulo_rf.app_renovacao_feedback() if modulo_rf else st.warning("N/A")

        elif pagina == "BancoDados":
            t1, t2 = st.tabs(["Pessoa Física", "Campanhas"])
            with t1: modulo_pf.app_pessoa_fisica() if modulo_pf else st.warning("N/A")
            with t2: modulo_pf_campanhas.app_campanhas() if modulo_pf_campanhas else st.warning("N/A")

        elif pagina == "WhatsApp":
            modulo_whats_controlador.app_wapi() if modulo_whats_controlador else st.warning("Módulo WhatsApp Off")
            
        elif pagina == "Conexoes":
            modulo_conexoes.app_conexoes() if modulo_conexoes else st.warning("Módulo Conexões Off")

if __name__ == "__main__":
    main()