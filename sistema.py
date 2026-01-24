import streamlit as st
import os
import sys
import psycopg2
from datetime import datetime, timedelta
import time
import importlib
import secrets
import string

# Tenta importar bcrypt para segurança de senhas
try:
    import bcrypt
except ImportError:
    st.error("⚠️ Biblioteca 'bcrypt' não instalada. Execute: pip install bcrypt")
    st.stop()

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Assessoria Consignado - SISTEMA", layout="wide", page_icon="📈")

# --- 2. CONFIGURAÇÃO DE CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

pastas_modulos = [
    "OPERACIONAL/CLIENTES",
    "OPERACIONAL/BANCO DE PLANILHAS",
    "OPERACIONAL/MODULO_W-API",
    "OPERACIONAL/MODULO_CHAT",
    "COMERCIAL",
    "COMERCIAL/PRODUTOS_E_SERVICOS",
    "COMERCIAL/PEDIDOS",
    "COMERCIAL/TAREFAS",
    "COMERCIAL/RENOVACAO_E_FEEDBACK",
    "CONEXÕES",
    "SISTEMA_CONSULTA", 
    "" 
]

for pasta in pastas_modulos:
    caminho = os.path.join(BASE_DIR, pasta)
    if os.path.exists(caminho) and caminho not in sys.path:
        sys.path.append(caminho)

# --- 3. IMPORTAÇÕES DE MÓDULOS ---
try:
    import conexao
    
    try:
        import modulo_validadores
    except ImportError:
        st.error("❌ ERRO FATAL: O arquivo 'modulo_validadores.py' não foi encontrado na pasta raiz!")
        st.stop()

    import modulo_wapi
    import modulo_whats_controlador
    
    def importar_seguro(nome_modulo):
        try:
            if nome_modulo in sys.modules:
                return importlib.reload(sys.modules[nome_modulo])
            else:
                return __import__(nome_modulo)
        except ImportError:
            return None
        except Exception as e:
            st.error(f"⚠️ Erro grave ao carregar módulo '{nome_modulo}': {e}")
            return None

    modulo_tela_cliente = importar_seguro("modulo_tela_cliente")
    modulo_permissoes = importar_seguro("modulo_permissoes")

    def carregar_modulo_por_caminho(caminho_relativo, nome_modulo):
        caminho_completo = os.path.join(BASE_DIR, caminho_relativo)
        if os.path.exists(caminho_completo):
            try:
                if nome_modulo in sys.modules:
                    return importlib.reload(sys.modules[nome_modulo])
                else:
                    return __import__(nome_modulo)
            except Exception as e:
                st.error(f"⚠️ Erro no arquivo '{caminho_relativo}': {e}")
                return None
        return None

    modulo_chat = carregar_modulo_por_caminho("OPERACIONAL/MODULO_CHAT/modulo_chat.py", "modulo_chat")
    modulo_pf_cadastro = carregar_modulo_por_caminho("OPERACIONAL/BANCO DE PLANILHAS/modulo_pf_cadastro.py", "modulo_pf_cadastro")
    modulo_pf_pesquisa = carregar_modulo_por_caminho("OPERACIONAL/BANCO DE PLANILHAS/modulo_pf_pesquisa.py", "modulo_pf_pesquisa")
    modulo_pf_importacao = carregar_modulo_por_caminho("OPERACIONAL/BANCO DE PLANILHAS/modulo_pf_importacao.py", "modulo_pf_importacao")
    modulo_pf = carregar_modulo_por_caminho("OPERACIONAL/BANCO DE PLANILHAS/modulo_pessoa_fisica.py", "modulo_pessoa_fisica")
    modulo_pf_campanhas = carregar_modulo_por_caminho("OPERACIONAL/BANCO DE PLANILHAS/modulo_pf_campanhas.py", "modulo_pf_campanhas")
    modulo_produtos = carregar_modulo_por_caminho("COMERCIAL/PRODUTOS_E_SERVICOS/modulo_produtos.py", "modulo_produtos")
    modulo_pedidos = carregar_modulo_por_caminho("COMERCIAL/PEDIDOS/modulo_pedidos.py", "modulo_pedidos")
    modulo_tarefas = carregar_modulo_por_caminho("COMERCIAL/TAREFAS/modulo_tarefas.py", "modulo_tarefas")
    modulo_rf = carregar_modulo_por_caminho("COMERCIAL/RENOVACAO_E_FEEDBACK/modulo_renovacao_feedback.py", "modulo_renovacao_feedback")
    modulo_comercial_geral = carregar_modulo_por_caminho("COMERCIAL/modulo_comercial_geral.py", "modulo_comercial_geral")
    modulo_conexoes = carregar_modulo_por_caminho("CONEXÕES/modulo_conexoes.py", "modulo_conexoes")
    modulo_sistema_consulta_menu = carregar_modulo_por_caminho("SISTEMA_CONSULTA/modulo_sistema_consulta_menu.py", "modulo_sistema_consulta_menu")

except Exception as e:
    st.error(f"🔥 Erro Crítico Geral nas Importações: {e}")
    st.stop()

# --- 4. FUNÇÕES DE BANCO DE DADOS ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password, connect_timeout=5
        )
    except Exception as e: 
        st.error(f"Erro Conexão DB: {e}")
        return None

# --- 5. FUNÇÕES DE SEGURANÇA E LOGIN ---

def verificar_sessao_unica_db(id_usuario, token_atual):
    """Verifica se o token da sessão atual ainda é o válido no banco."""
    conn = get_conn()
    if not conn: return True # Em caso de erro de DB, permite (fail-open) ou bloqueia, dependendo da politica. Aqui fail-open pra nao travar.
    try:
        cur = conn.cursor()
        cur.execute("SELECT token FROM admin.sessoes_ativas WHERE id_usuario = %s", (id_usuario,))
        res = cur.fetchone()
        if res and res[0] != token_atual:
            return False # Token mudou, derruba sessão
        return True
    except:
        return True
    finally:
        conn.close()

def registrar_sessao_db(id_usuario, nome_usuario):
    """Cria um token novo, derruba sessões anteriores e salva no banco."""
    conn = get_conn()
    if not conn: return None
    try:
        token = secrets.token_urlsafe(32)
        cur = conn.cursor()
        # Remove sessão anterior
        cur.execute("DELETE FROM admin.sessoes_ativas WHERE id_usuario = %s", (id_usuario,))
        # Cria nova sessão
        cur.execute("""
            INSERT INTO admin.sessoes_ativas (token, id_usuario, nome_usuario, data_inicio, ultimo_clique)
            VALUES (%s, %s, %s, NOW(), NOW())
        """, (token, id_usuario, nome_usuario))
        conn.commit()
        return token
    except Exception as e:
        print(f"Erro ao registrar sessão: {e}")
        return None
    finally:
        conn.close()

def validar_login_db(usuario, senha):
    conn = get_conn()
    if not conn: return {"status": "erro_conexao"}
    
    try:
        cur = conn.cursor()
        email_login = str(usuario).strip()
        senha_login = str(senha).strip()

        # Busca dados, incluindo colunas de segurança
        # Assume-se que as colunas 'tentativas_falhas' e 'bloqueado_ate' existam ou sejam tratadas
        sql = """
            SELECT id, email, senha, nome, tentativas_falhas, bloqueado_ate
            FROM admin.clientes_usuarios 
            WHERE email = %s
        """
        cur.execute(sql, (email_login,))
        res = cur.fetchone()
        
        if res:
            uid, email_banco, senha_banco, nome_banco, tentativas, bloqueado_ate = res
            
            # 1. Verifica bloqueio
            if bloqueado_ate and bloqueado_ate > datetime.now():
                tempo_restante = (bloqueado_ate - datetime.now()).seconds // 60
                return {"status": "bloqueado", "msg": f"Conta bloqueada. Tente em {tempo_restante} min."}

            senha_correta = False
            precisa_atualizar_hash = False

            # 2. Verifica Senha (Hash ou Texto Puro)
            senha_banco_str = str(senha_banco).strip() if senha_banco else ""
            
            # Tenta verificar como Hash BCrypt
            try:
                if senha_banco_str.startswith('$2b$') or senha_banco_str.startswith('$2a$'):
                    if bcrypt.checkpw(senha_login.encode('utf-8'), senha_banco_str.encode('utf-8')):
                        senha_correta = True
                else:
                    # Fallback: Texto Puro (Legacy)
                    if senha_banco_str == senha_login:
                        senha_correta = True
                        precisa_atualizar_hash = True
            except:
                # Se der erro no bcrypt, tenta texto puro por garantia
                if senha_banco_str == senha_login:
                    senha_correta = True
                    precisa_atualizar_hash = True

            if senha_correta:
                # Sucesso: Zera tentativas
                sql_update = "UPDATE admin.clientes_usuarios SET tentativas_falhas = 0 WHERE id = %s"
                params = [uid]
                
                # Se era texto puro, migra para Hash
                if precisa_atualizar_hash:
                    novo_hash = bcrypt.hashpw(senha_login.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                    sql_update = "UPDATE admin.clientes_usuarios SET tentativas_falhas = 0, senha = %s WHERE id = %s"
                    params = [novo_hash, uid]
                
                cur.execute(sql_update, tuple(params))
                conn.commit()
                
                return {"status": "sucesso", "id": uid, "email": email_banco, "nome": nome_banco}
            
            else:
                # Senha Errada: Incrementa falhas
                novas_tentativas = (tentativas or 0) + 1
                if novas_tentativas >= 5:
                    # Bloqueia por 15 minutos
                    bloqueio = datetime.now() + timedelta(minutes=15)
                    cur.execute("UPDATE admin.clientes_usuarios SET tentativas_falhas = %s, bloqueado_ate = %s WHERE id = %s", 
                                (novas_tentativas, bloqueio, uid))
                    conn.commit()
                    return {"status": "bloqueado", "msg": "Muitas tentativas. Bloqueado por 15 min."}
                else:
                    cur.execute("UPDATE admin.clientes_usuarios SET tentativas_falhas = %s WHERE id = %s", 
                                (novas_tentativas, uid))
                    conn.commit()
                    return {"status": "erro_senha", "tentativas": novas_tentativas}
        
        return {"status": "nao_encontrado"}
    except Exception as e:
        return {"status": "erro_generico", "msg": str(e)}
    finally:
        conn.close()

def enviar_nova_senha_whatsapp(email_destino):
    """Gera senha, atualiza banco e envia via WhatsApp"""
    conn = get_conn()
    if not conn: return "Erro conexão DB"
    
    try:
        cur = conn.cursor()
        # Verifica usuario
        cur.execute("SELECT id, nome, telefone FROM admin.clientes_usuarios WHERE email = %s", (email_destino,))
        user = cur.fetchone()
        
        if not user: return "E-mail não encontrado."
        uid, nome, telefone = user
        
        if not telefone or len(telefone) < 10: return "Usuário sem telefone válido cadastrado."

        # Busca Instancia WAPI Ativa
        cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
        inst = cur.fetchone()
        if not inst: return "Nenhuma instância de WhatsApp configurada no sistema."
        
        # Gera nova senha aleatoria
        alfabeto = string.ascii_letters + string.digits
        nova_senha = ''.join(secrets.choice(alfabeto) for i in range(8))
        senha_hash = bcrypt.hashpw(nova_senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Atualiza Banco
        cur.execute("UPDATE admin.clientes_usuarios SET senha = %s, tentativas_falhas = 0, bloqueado_ate = NULL WHERE id = %s", (senha_hash, uid))
        conn.commit()
        
        # Envia WhatsApp
        msg = f"🔐 *Solicitação de Reset de Senha*\n\nOlá {nome},\nSua nova senha temporária é: *{nova_senha}*\n\nAcesse o sistema e altere sua senha se desejar."
        
        res = modulo_wapi.enviar_msg_api(inst[0], inst[1], telefone, msg)
        
        if res.get('success') or res.get('messageId'):
            return "Sucesso! Senha enviada para o WhatsApp cadastrado."
        else:
            return f"Erro no envio do WhatsApp: {res}"

    except Exception as e:
        return f"Erro ao resetar: {e}"
    finally:
        conn.close()

# --- 6. FUNÇÕES DE ESTADO E SESSÃO ---
def iniciar_estado():
    if 'ultima_atividade' not in st.session_state:
        st.session_state['ultima_atividade'] = datetime.now()
    if 'hora_login' not in st.session_state:
        st.session_state['hora_login'] = datetime.now()
    if 'pagina_central' not in st.session_state:
        st.session_state['pagina_central'] = "Início"
    if 'logado' not in st.session_state:
        st.session_state['logado'] = False
    if 'token_sessao' not in st.session_state:
        st.session_state['token_sessao'] = None
    if 'tempo_limite_minutos' not in st.session_state:
        st.session_state['tempo_limite_minutos'] = 60 # Padrão

def resetar_atividade():
    st.session_state['ultima_atividade'] = datetime.now()
    # Atualiza DB para indicar atividade (opcional, para controle fino)
    # Poderia dar update em sessoes_ativas set ultimo_clique = now()

def gerenciar_sessao():
    # 1. Verifica tempo inativo
    limite = st.session_state.get('tempo_limite_minutos', 60)
    agora = datetime.now()
    tempo_inativo = agora - st.session_state['ultima_atividade']
    
    if tempo_inativo.total_seconds() > (limite * 60):
        st.session_state.clear()
        st.error("Sessão expirada por inatividade.")
        st.stop()

    # 2. Verifica Sessão Única (Banco de Dados)
    if st.session_state.get('logado') and st.session_state.get('token_sessao'):
        uid = st.session_state.get('usuario_id')
        token = st.session_state.get('token_sessao')
        
        if not verificar_sessao_unica_db(uid, token):
            st.session_state.clear()
            st.warning("🔒 Sua conta foi conectada em outro dispositivo/navegador. Esta sessão foi encerrada.")
            st.stop()

    tempo_total = agora - st.session_state['hora_login']
    mm, ss = divmod(tempo_total.seconds, 60)
    hh, mm = divmod(mm, 60)
    return f"{hh:02d}:{mm:02d}"

# --- 7. INTERFACE (MENSAGEM RÁPIDA) ---
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

# --- 8. MENU LATERAL ---
def renderizar_menu_lateral():
    st.markdown("""
        <style>
        div.stButton > button {
            width: 100%; border: none; text-align: left; padding-left: 15px;
            background: transparent; color: #444;
        }
        div.stButton > button:hover { background: #f0f2f6; color: #FF4B4B; font-weight: bold; }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### 🚀 Assessoria")
        nome_display = st.session_state.get('usuario_nome', 'Usuário')
        st.markdown(f"Olá, **{nome_display}**")
        
        # Mostra tempo da sessão (Debug/Info)
        tempo_online = gerenciar_sessao()
        st.caption(f"Online há: {tempo_online}")

        st.markdown("---")
        
        botoes = {
            "🏠 Início": "Início",
            "👥 Clientes": "Clientes",
            "🔍 CRM Consulta": "CRM_Consulta", 
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

# --- 9. TELA DE RECUPERAÇÃO DE SENHA ---
@st.dialog("🔑 Recuperar Senha")
def dialog_recuperar_senha():
    st.markdown("Informe seu e-mail cadastrado. Uma nova senha será enviada para o seu **WhatsApp**.")
    email_rec = st.text_input("E-mail de Cadastro")
    if st.button("Enviar Nova Senha", type="primary"):
        with st.spinner("Processando..."):
            msg = enviar_nova_senha_whatsapp(email_rec)
            if "Sucesso" in msg:
                st.success(msg)
                time.sleep(3)
                st.rerun()
            else:
                st.error(msg)

# --- 10. FUNÇÃO PRINCIPAL ---
def main():
    iniciar_estado()
    
    # 10.1 TELA DE LOGIN
    if not st.session_state['logado']:
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.title("🔐 Login Seguro")
            
            u = st.text_input("E-mail")
            s = st.text_input("Senha", type="password")
            
            # Opções de Sessão
            col_sessao, col_check = st.columns([2, 1])
            with col_sessao:
                # O usuário escolhe o tempo de sessão
                opcoes_tempo = {
                    "60 minutos": 60,
                    "4 horas": 240,
                    "8 horas": 480,
                    "12 horas": 720
                }
                tempo_escolhido = st.selectbox("Tempo de Sessão", list(opcoes_tempo.keys()))
                
            with col_check:
                st.write("") # Espaçamento
                st.write("") 
                manter_conectado = st.checkbox("Salvar Login")

            c_btn, c_esq = st.columns([1,1])
            with c_btn:
                btn_entrar = st.button("Entrar", type="primary", use_container_width=True)
            with c_esq:
                if st.button("Esqueci a Senha", use_container_width=True):
                    dialog_recuperar_senha()
            
            if btn_entrar:
                res = validar_login_db(u, s)
                
                if res['status'] == 'sucesso':
                    # Lógica de Tempo de Sessão
                    tempo_minutos = opcoes_tempo[tempo_escolhido]
                    if manter_conectado:
                        tempo_minutos = 43200 # 30 dias (30 * 24 * 60)
                    
                    # Gera Token e Registra no DB (Sessão Única)
                    token = registrar_sessao_db(res['id'], res['nome'])
                    
                    if token:
                        st.success(f"Bem-vindo, {res['nome']}!")
                        time.sleep(0.5)
                        
                        st.session_state.update({
                            'logado': True, 
                            'usuario_id': res['id'],
                            'usuario_nome': res['nome'],
                            'token_sessao': token,
                            'tempo_limite_minutos': tempo_minutos,
                            'hora_login': datetime.now(),
                            'ultima_atividade': datetime.now()
                        })
                        st.rerun()
                    else:
                        st.error("Erro ao criar sessão segura. Tente novamente.")

                elif res['status'] == 'bloqueado':
                    st.error(res['msg'])
                elif res['status'] == 'erro_senha':
                    msg = "Senha incorreta."
                    if 'tentativas' in res:
                        restantes = 5 - res['tentativas']
                        msg += f" Restam {restantes} tentativas."
                    st.error(msg)
                else:
                    st.error("E-mail não encontrado.")
    
    # 10.2 SISTEMA LOGADO
    else:
        renderizar_menu_lateral()
        
        c1, c2 = st.columns([6, 1])
        with c2: 
            if st.button("💬 Msg"): dialog_mensagem_rapida()

        pagina = st.session_state['pagina_central']
        
        # Roteamento de Módulos (Mantido Original)
        if pagina == "Início":
            if modulo_chat: modulo_chat.app_chat_screen()
            else: st.info("Painel Inicial (Módulo Chat não detectado)")
            
        elif pagina == "Clientes":
            if modulo_permissoes and modulo_permissoes.verificar_bloqueio_de_acesso("bloqueio_menu_cliente", "Clientes", False):
                st.error("🚫 Acesso Negado ao Módulo Clientes"); st.stop()
            if modulo_tela_cliente: modulo_tela_cliente.app_clientes()
            
        elif pagina == "Comercial":
            if modulo_comercial_geral: modulo_comercial_geral.app_comercial_geral()
            else: st.warning("⚠️ Módulo Comercial Geral não encontrado.")

        elif pagina == "BancoDados":
            if modulo_pf: modulo_pf.app_pessoa_fisica()
            else: st.warning("Módulo Pessoa Física não carregado.")

        elif pagina == "WhatsApp":
            modulo_whats_controlador.app_wapi() if modulo_whats_controlador else st.warning("Módulo WhatsApp Off")
            
        elif pagina == "Conexoes":
            modulo_conexoes.app_conexoes() if modulo_conexoes else st.warning("Módulo Conexões Off")

        elif pagina == "CRM_Consulta":
            if modulo_sistema_consulta_menu:
                modulo_sistema_consulta_menu.app_sistema_consulta()
            else:
                st.warning("Módulo CRM Consulta não carregado.")

if __name__ == "__main__":
    main()