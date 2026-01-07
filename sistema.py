import streamlit as st
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

# [CORREÇÃO 1] Caminho ajustado para a pasta real 'OPERACIONAL/CLIENTES'
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
    
    # --- [CORREÇÃO 2] Importação Ajustada para modulo_tela_cliente ---
    try:
        # Tenta importar direto (pois adicionamos 'OPERACIONAL/CLIENTES' ao sys.path)
        import modulo_tela_cliente
    except ImportError:
        try:
            # Se falhar, tenta o caminho completo com o nome correto da pasta (CLIENTES plural)
            from OPERACIONAL.CLIENTES import modulo_tela_cliente
        except ImportError:
            modulo_tela_cliente = None
        
    try:
        # [CORREÇÃO 3] Caminho de permissões ajustado para CLIENTES (plural)
        from OPERACIONAL.CLIENTES.PERMISSÕES import modulo_permissoes
    except ImportError:
        modulo_permissoes = None

    # Importações condicionais (Módulos Legados/Outros)
    # Ajuste opcional: verifica se o módulo legado existe no caminho novo ou antigo
    caminho_usuario_novo = os.path.join(BASE_DIR, "OPERACIONAL/CLIENTES/USUÁRIOS/modulo_usuario.py")
    modulo_usuario = None
    if os.path.exists(caminho_usuario_novo):
         # Se precisar importar manualmente
         pass 

    modulo_chat = __import__('modulo_chat') if os.path.exists(os.path.join(BASE_DIR, "OPERACIONAL/MODULO_CHAT/modulo_chat.py")) else None
    modulo_pf = __import__('modulo_pessoa_fisica') if os.path.exists(os.path.join(BASE_DIR, "OPERACIONAL/BANCO DE PLANILHAS/modulo_pessoa_fisica.py")) else None
    modulo_produtos = __import__('modulo_produtos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PRODUTOS E SERVICOS/modulo_produtos.py")) else None
    modulo_pedidos = __import__('modulo_pedidos') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/PEDIDOS/modulo_pedidos.py")) else None
    modulo_tarefas = __import__('modulo_tarefas') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/TAREFAS/modulo_tarefas.py")) else None
    modulo_rf = __import__('modulo_renovacao_feedback') if os.path.exists(os.path.join(BASE_DIR, "COMERCIAL/RENOVACAO E FEEDBACK/modulo_renovacao_feedback.py")) else None
    modulo_pf_campanhas = __import__('modulo_pf_campanhas') if os.path.exists(os.path.join(BASE_DIR, "OPERACIONAL/BANCO DE PLANILHAS/modulo_pf_campanhas.py")) else None
    modulo_conexoes = __import__('modulo_conexoes') if os.path.exists(os.path.join(BASE_DIR, "CONEXÕES/modulo_conexoes.py")) else None

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

    tempo_total = agora - st.session_state['hora_login']
    mm, ss = divmod(tempo_total.seconds, 60)
    hh, mm = divmod(mm, 60)
    if hh > 0: return f"{hh:02d}:{mm:02d}"
    return f"{mm:02d}:{ss:02d}"

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

# --- 6. DIALOGS ---
@st.dialog("🚀 Mensagem Rápida")
def dialog_mensagem_rapida():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
        inst = cur.fetchone()
        if not inst:
            st.error("Sem instância de WhatsApp ativa."); return

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
        if 'cur' in locals(): cur.close()

@st.dialog("Recuperar Acesso")
def dialog_reset_senha():
    st.write("Receba uma nova senha via WhatsApp.")
    identificador = st.text_input("E-mail ou CPF")
    if st.button("Enviar Nova Senha", use_container_width=True, type="primary") and identificador:
        st.info("Funcionalidade em manutenção.")

# --- 7. RENDERIZAÇÃO DO MENU (LAYOUT + EMOJIS) ---
def renderizar_menu_lateral():
    st.markdown("""
        <style>
        div.stButton > button {
            width: 100%; border: 1px solid #000000 !important; border-radius: 0px !important;
            color: black; background-color: #ffffff; font-weight: 500; margin-bottom: 5px;
            justify-content: flex-start; padding-left: 15px;
        }
        div.stButton > button:hover {
            border-color: #FF4B4B !important; color: #FF4B4B; background-color: #f0f0f0;
        }
        section[data-testid="stSidebar"] { background-color: rgba(255, 224, 178, 0.3); }
        #MainMenu {visibility: hidden;} footer {visibility: hidden;}
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        try: st.image("logo_assessoria.png", use_container_width=True)
        except: st.warning("Logo não encontrada")
        
        nome_completo = st.session_state.get('usuario_nome', 'Visitante')
        primeiro_nome = nome_completo.split()[0].title() if nome_completo else "Visitante"
        email_user = st.session_state.get('usuario_email', 'sem_email')
        cargo_banco = st.session_state.get('usuario_cargo', '-')
        
        st.markdown(f"""
            <div style="text-align: center; margin-bottom: 20px; line-height: 1.4;">
                <strong style="font-size: 1.1em;">{primeiro_nome}</strong><br>
                <span style="font-size: 0.85em; color: #333;">{email_user}</span><br>
                <span style="font-size: 0.85em; color: gray;">{cargo_banco}</span>
            </div>
            <hr style="margin-top: 5px; margin-bottom: 15px;">
        """, unsafe_allow_html=True)

        # --- MENU PRINCIPAL ---
        icones = {
            "Operacional": "⚙️", "Comercial": "💼", "Conexões": "🔌",
            "CLIENTES ASSESSORIA": "👥", "Banco PF": "🏦",
            "Campanhas": "📣", "WhatsApp": "💬", "Produtos": "📦",
            "Pedidos": "🛒", "Tarefas": "📝", "Renovação": "🔄"
        }

        cargo_normalizado = str(cargo_banco).strip().upper()
        estrutura_menu = {}
        
        if st.button("🏠 Início", key="btn_home", on_click=resetar_atividade):
            st.session_state['pagina_atual'] = "Início"
            st.session_state['menu_aberto'] = None
            
        if cargo_normalizado in ["ADMIN", "GERENTE", "ADMINISTRADOR"]:
            estrutura_menu["Operacional"] = ["CLIENTES ASSESSORIA", "Banco PF", "Campanhas", "WhatsApp"]
            estrutura_menu["Comercial"] = ["Produtos", "Pedidos", "Tarefas", "Renovação"]
            estrutura_menu["Conexões"] = [] 
        else:
            estrutura_menu["Operacional"] = ["CLIENTES ASSESSORIA", "WhatsApp"]

        for menu_pai, subitens in estrutura_menu.items():
            icon_pai = icones.get(menu_pai, "📂")
            if not subitens:
                if st.button(f"{icon_pai} {menu_pai}", key=f"pai_{menu_pai}", on_click=resetar_atividade):
                    st.session_state['pagina_atual'] = menu_pai
                    st.session_state['menu_aberto'] = None
                continue

            seta = "▼" if st.session_state['menu_aberto'] == menu_pai else "►"
            if st.button(f"{icon_pai} {menu_pai} {seta}", key=f"pai_{menu_pai}", on_click=resetar_atividade):
                st.session_state['menu_aberto'] = None if st.session_state['menu_aberto'] == menu_pai else menu_pai
            
            if st.session_state['menu_aberto'] == menu_pai:
                for item in subitens:
                    _, col_btn = st.columns([0.1, 0.9])
                    with col_btn:
                        icon_filho = icones.get(item, "↳")
                        if st.button(f"{icon_filho} {item}", key=f"sub_{menu_pai}_{item}", on_click=resetar_atividade):
                            st.session_state['pagina_atual'] = f"{menu_pai} > {item}"

        st.markdown("<br>" * 10, unsafe_allow_html=True)
        if st.button("🚪 Sair", key="btn_sair"):
            st.session_state.clear()
            st.rerun()

        st.markdown(f"<div style='text-align:center; margin-top:10px; font-size:0.9em; color:#444;'>sessão ativa: {gerenciar_sessao()}</div>", unsafe_allow_html=True)

# --- 8. FUNÇÃO PRINCIPAL ---
def main():
    iniciar_estado()
    
    if not st.session_state.get('logado'):
        st.markdown("""<style>div.stButton > button {border: 1px solid black; border-radius: 0px;}</style>""", unsafe_allow_html=True)
        st.markdown('<div style="text-align:center; padding:40px;"><h2>Assessoria Consignado</h2><p>Portal Integrado</p></div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            u = st.text_input("E-mail ou CPF")
            s = st.text_input("Senha", type="password")
            if st.button("ENTRAR", use_container_width=True, type="primary"):
                res = validar_login_db(u, s)
                if res:
                    if res.get('status') == "sucesso":
                        st.session_state.update({'logado': True, 'usuario_id': res['id'], 'usuario_nome': res['nome'], 'usuario_cargo': res['cargo'], 'usuario_email': res.get('email', '')})
                        st.rerun()
                    elif res.get('status') == "bloqueado": st.error("🚨 USUÁRIO BLOQUEADO.")
                    else: st.error(f"Senha incorreta. Tentativas restantes: {res.get('restantes')}")
                else: st.error("Acesso negado.")
            if st.button("Esqueci minha senha", use_container_width=True): dialog_reset_senha()
    else:
        renderizar_menu_lateral()
        c1, c2 = st.columns([10, 2])
        with c2:
            if st.button("💬 Msg Rápida"): dialog_mensagem_rapida()

        pag = st.session_state['pagina_atual']
        
        if pag == "Início":
            if modulo_chat: modulo_chat.app_chat_screen()
            else: st.info("Módulo Chat não carregado.")
            
        # --- ROTA ATUALIZADA: CLIENTES ASSESSORIA ---
        elif "Operacional > CLIENTES ASSESSORIA" in pag: 
            # 1. Verificação de Permissão usando o NOVO módulo de permissões
            if modulo_permissoes:
                 modulo_permissoes.verificar_bloqueio_de_acesso(
                    chave="bloqueio_menu_cliente", 
                    caminho_atual="Operacional > Clientes Assessoria", 
                    parar_se_bloqueado=True
                )
            
            # 2. Carregamento do NOVO Hub de Clientes
            if modulo_tela_cliente:
                modulo_tela_cliente.app_clientes()
            else:
                st.error("Erro: Módulo Refatorado 'OPERACIONAL.CLIENTES' não carregado.")
                st.info("Verifique se as pastas e arquivos __init__.py foram criados na pasta OPERACIONAL/CLIENTES.")
            
        elif "Operacional > Banco PF" in pag and modulo_pf: modulo_pf.app_pessoa_fisica()
        elif "Operacional > Campanhas" in pag and modulo_pf_campanhas: modulo_pf_campanhas.app_campanhas()
        elif "Operacional > WhatsApp" in pag: modulo_whats_controlador.app_wapi()
        elif "Comercial > Produtos" in pag and modulo_produtos: modulo_produtos.app_produtos()
        elif "Comercial > Pedidos" in pag and modulo_pedidos: modulo_pedidos.app_pedidos()
        elif "Comercial > Tarefas" in pag and modulo_tarefas: modulo_tarefas.app_tarefas()
        elif "Comercial > Renovação" in pag and modulo_rf: modulo_rf.app_renovacao_feedback()
        elif pag == "Conexões" and modulo_conexoes: modulo_conexoes.app_conexoes()
        else:
            st.warning(f"Página '{pag}' não encontrada ou módulo indisponível.")

if __name__ == "__main__":
    main()