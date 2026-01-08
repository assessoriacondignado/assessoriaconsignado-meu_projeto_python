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

# --- 2. CONFIGURAÇÃO DE CAMINHOS (AJUSTE ITEM 2) ---
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

# Adiciona as pastas ao sys.path de forma segura (evita duplicação)
for pasta in pastas_modulos:
    caminho = os.path.join(BASE_DIR, pasta)
    if caminho not in sys.path:
        sys.path.append(caminho)

# --- 3. IMPORTAÇÕES DE MÓDULOS ---
try:
    import conexao
    import modulo_wapi
    import modulo_whats_controlador
    
    # Módulo Clientes
    try:
        import modulo_tela_cliente
    except ImportError:
        modulo_tela_cliente = None
        
    # Módulo Permissões
    try:
        import modulo_permissoes
    except ImportError:
        modulo_permissoes = None

    # Demais Módulos (Carregamento Condicional)
    # Verifica se o arquivo existe antes de tentar importar para evitar erros
    modulo_chat = __import__('modulo_chat') if os.path.exists(os.path.join(BASE_DIR, "OPERACIONAL/MODULO_CHAT/modulo_chat.py")) else None
    modulo_pf = __import__('modulo_pessoa_fisica') if os.path.exists(os.path.join(BASE_DIR, "OPERACIONAL/BANCO DE PLANILHAS/modulo_pessoa_fisica.py")) else None
    modulo_pf_campanhas = __import__('modulo_pf_campanhas') if os.path.exists(os.path.join(BASE_DIR, "OPERACIONAL/BANCO DE PLANILHAS/modulo_pf_campanhas.py")) else None
    modulo_produtos = __import__('modulo_produtos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PRODUTOS E SERVICOS/modulo_produtos.py")) else None
    modulo_pedidos = __import__('modulo_pedidos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PEDIDOS/modulo_pedidos.py")) else None
    modulo_tarefas = __import__('modulo_tarefas') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/TAREFAS/modulo_tarefas.py")) else None
    modulo_rf = __import__('modulo_renovacao_feedback') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/RENOVACAO E FEEDBACK/modulo_renovacao_feedback.py")) else None
    modulo_conexoes = __import__('modulo_conexoes') if os.path.exists(os.path.join(BASE_DIR, "CONEXÕES/modulo_conexoes.py")) else None

except Exception as e:
    st.error(f"Aviso do Sistema: Alguns módulos não foram carregados corretamente ({e}).")

# --- 4. FUNÇÕES DE ESTADO E UTILITÁRIOS ---

def iniciar_estado():
    if 'ultima_atividade' not in st.session_state:
        st.session_state['ultima_atividade'] = datetime.now()
    if 'hora_login' not in st.session_state:
        st.session_state['hora_login'] = datetime.now()
    # Define a página inicial padrão
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
        st.error("Sessão expirada por inatividade. Por favor, faça login novamente.")
        st.stop()

    tempo_total = agora - st.session_state['hora_login']
    mm, ss = divmod(tempo_total.seconds, 60)
    hh, mm = divmod(mm, 60)
    if hh > 0: return f"{hh:02d}:{mm:02d}"
    return f"{mm:02d}:{ss:02d}"

# --- 5. BANCO DE DADOS E AUTH (CORREÇÃO ITEM 2 - ARQUITETURA) ---
# REMOVIDO @st.cache_resource para evitar uso de conexão velha/fechada
def get_conn():
    try:
        # Cria uma conexão nova a cada chamada para garantir estabilidade
        # Em produção de alta escala, usaríamos um Connection Pool aqui.
        conn = psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password, connect_timeout=5
        )
        return conn
    except Exception as e: 
        # Log de erro silencioso ou print para debug
        print(f"Erro de conexão DB: {e}")
        return None

def verificar_senha(senha_plana, senha_hash):
    try:
        # Nota: Idealmente remover a comparação direta no futuro para segurança total
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
            if falhas >= 5: 
                conn.close()
                return {"status": "bloqueado"}
            
            if verificar_senha(senha_input, senha_hash):
                cursor.execute("UPDATE clientes_usuarios SET tentativas_falhas = 0 WHERE id = %s", (id_user,))
                conn.commit()
                conn.close()
                return {"id": id_user, "nome": nome, "cargo": cargo, "email": email_user, "status": "sucesso"}
            else:
                cursor.execute("UPDATE clientes_usuarios SET tentativas_falhas = tentativas_falhas + 1 WHERE id = %s", (id_user,))
                conn.commit()
                conn.close()
                return {"status": "erro_senha", "restantes": 4 - falhas}
        conn.close()
    except Exception as e: 
        if conn: conn.close()
        return None
    return None

# --- 6. DIALOGS (MENSAGEM RÁPIDA) ---
@st.dialog("🚀 Mensagem Rápida")
def dialog_mensagem_rapida():
    conn = get_conn()
    if not conn:
        st.error("Erro de conexão com banco de dados.")
        return

    try:
        cur = conn.cursor()
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
        if conn: conn.close()

# --- 7. MENU LATERAL (SIMPLIFICADO) ---
def renderizar_menu_lateral():
    # Estilização dos botões da sidebar
    st.markdown("""
        <style>
        div.stButton > button {
            width: 100%; 
            border: none !important; 
            text-align: left !important;
            padding-left: 15px !important;
            background-color: transparent;
            color: #333;
            font-size: 16px;
        }
        div.stButton > button:hover {
            background-color: #f0f2f6;
            color: #FF4B4B;
            font-weight: bold;
        }
        div.stButton > button:focus {
            background-color: #e0e0e0;
            color: #FF4B4B;
        }
        section[data-testid="stSidebar"] { background-color: #f8f9fa; }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        try: st.image("logo_assessoria.png", use_container_width=True)
        except: st.markdown("### Assessoria Consignado")
        
        # Dados do Usuário
        nome_completo = st.session_state.get('usuario_nome', 'Visitante')
        primeiro_nome = nome_completo.split()[0].title()
        cargo_banco = st.session_state.get('usuario_cargo', '-')
        
        st.markdown(f"""
            <div style="margin-bottom: 20px; padding: 10px; background-color: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <small style="color: gray;">Bem-vindo,</small><br>
                <strong>{primeiro_nome}</strong><br>
                <span style="font-size: 0.8em; color: #555;">{cargo_banco}</span>
            </div>
        """, unsafe_allow_html=True)
        
        # --- BOTÕES DE NAVEGAÇÃO ---
        st.markdown("---")
        
        if st.button("🏠 Início", use_container_width=True):
            st.session_state['pagina_central'] = "Início"
            resetar_atividade()
            st.rerun()

        st.caption("Módulos Principais")
        
        if st.button("👥 Clientes & Assessoria", use_container_width=True):
            st.session_state['pagina_central'] = "Clientes"
            resetar_atividade()
            st.rerun()
            
        if st.button("💼 Comercial", use_container_width=True):
            st.session_state['pagina_central'] = "Comercial"
            resetar_atividade()
            st.rerun()
            
        if st.button("🏦 Banco de Dados & MKT", use_container_width=True):
            st.session_state['pagina_central'] = "BancoDados"
            resetar_atividade()
            st.rerun()

        if st.button("💬 WhatsApp", use_container_width=True):
            st.session_state['pagina_central'] = "WhatsApp"
            resetar_atividade()
            st.rerun()
            
        if st.button("🔌 Conexões", use_container_width=True):
            st.session_state['pagina_central'] = "Conexoes"
            resetar_atividade()
            st.rerun()

        # Rodapé
        st.markdown("<br>" * 5, unsafe_allow_html=True)
        if st.button("🚪 Sair", key="btn_sair"):
            st.session_state.clear()
            st.rerun()
        
        st.markdown(f"<div style='text-align:center; margin-top:10px; font-size:0.8em; color:#888;'>Sessão: {gerenciar_sessao()}</div>", unsafe_allow_html=True)

# --- 8. FUNÇÃO PRINCIPAL (ROTEADOR DE MÓDULOS) ---
def main():
    iniciar_estado()
    
    # TELA DE LOGIN
    if not st.session_state.get('logado'):
        st.markdown("""<style>div.stButton > button {width: 100%;}</style>""", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 1.5, 1])
        with col2:
            st.markdown('<div style="text-align:center; padding-top:50px; padding-bottom:20px;"><h2>Assessoria Consignado</h2><p>Acesso ao Sistema</p></div>', unsafe_allow_html=True)
            u = st.text_input("E-mail ou CPF")
            s = st.text_input("Senha", type="password")
            if st.button("ENTRAR", type="primary"):
                res = validar_login_db(u, s)
                if res:
                    if res.get('status') == "sucesso":
                        st.session_state.update({
                            'logado': True, 
                            'usuario_id': res['id'], 
                            'usuario_nome': res['nome'], 
                            'usuario_cargo': res['cargo'], 
                            'usuario_email': res.get('email', '')
                        })
                        st.rerun()
                    elif res.get('status') == "bloqueado": st.error("🚨 USUÁRIO BLOQUEADO.")
                    else: st.error(f"Senha incorreta. Tentativas restantes: {res.get('restantes')}")
                else: st.error("Usuário não encontrado.")
    
    # SISTEMA LOGADO
    else:
        renderizar_menu_lateral()
        
        # Botão de ação rápida no topo direito
        c_topo1, c_topo2 = st.columns([8, 2])
        with c_topo2:
             if st.button("💬 Msg Rápida", use_container_width=True): 
                 dialog_mensagem_rapida()
        
        pagina = st.session_state['pagina_central']

        # --- ROTEAMENTO DO CONTEÚDO ---
        
        # 1. INÍCIO
        if pagina == "Início":
            if modulo_chat: 
                modulo_chat.app_chat_screen()
            else: 
                st.info("Bem-vindo ao Sistema. (Módulo Chat não carregado)")

        # 2. CLIENTES
        elif pagina == "Clientes":
            # --- CORREÇÃO ITEM 3 (LÓGICA DE BLOQUEIO) ---
            bloqueado = False
            if modulo_permissoes:
                 # Captura se está bloqueado ou não
                 bloqueado = modulo_permissoes.verificar_bloqueio_de_acesso(
                    chave="bloqueio_menu_cliente", 
                    caminho_atual="Clientes", 
                    parar_se_bloqueado=False # Mudamos para False para controlar aqui
                )
            
            if bloqueado:
                st.error("🔒 Você não tem permissão para acessar este módulo.")
                st.stop() # Força a parada do script aqui
            
            if modulo_tela_cliente:
                modulo_tela_cliente.app_clientes()
            else:
                st.error("Módulo de Clientes não encontrado.")

        # 3. COMERCIAL
        elif pagina == "Comercial":
            st.title("💼 Comercial")
            tab_prod, tab_ped, tab_tar, tab_ren = st.tabs(["📦 Produtos", "🛒 Pedidos", "📝 Tarefas", "🔄 Renovação"])
            
            with tab_prod:
                if modulo_produtos: modulo_produtos.app_produtos()
                else: st.warning("Módulo Produtos indisponível.")
            
            with tab_ped:
                if modulo_pedidos: modulo_pedidos.app_pedidos()
                else: st.warning("Módulo Pedidos indisponível.")
                
            with tab_tar:
                if modulo_tarefas: modulo_tarefas.app_tarefas()
                else: st.warning("Módulo Tarefas indisponível.")
                
            with tab_ren:
                if modulo_rf: modulo_rf.app_renovacao_feedback()
                else: st.warning("Módulo Renovação indisponível.")

        # 4. BANCO DE DADOS & MKT
        elif pagina == "BancoDados":
            st.title("🏦 Banco de Dados & Marketing")
            tab_pf, tab_camp = st.tabs(["👥 Banco Pessoa Física", "📢 Campanhas MKT"])
            
            with tab_pf:
                if modulo_pf: modulo_pf.app_pessoa_fisica()
                else: st.warning("Módulo Banco PF indisponível.")
            
            with tab_camp:
                if modulo_pf_campanhas: modulo_pf_campanhas.app_campanhas()
                else: st.warning("Módulo Campanhas indisponível.")

        # 5. WHATSAPP
        elif pagina == "WhatsApp":
            if modulo_whats_controlador:
                modulo_whats_controlador.app_wapi()
            else:
                st.error("Módulo WhatsApp indisponível.")

        # 6. CONEXÕES
        elif pagina == "Conexoes":
            if modulo_conexoes:
                modulo_conexoes.app_conexoes()
            else:
                st.error("Módulo Conexões indisponível.")

if __name__ == "__main__":
    main()