import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
import os
import sys

# --- IMPORTAÇÃO DE MÓDULOS ESPECÍFICOS ---
# Ajuste de importação para o novo Hub de Clientes
try:
    # Tenta importar da estrutura de pastas correta (OPERACIONAL/CLIENTES)
    from OPERACIONAL.CLIENTES import modulo_tela_cliente
except ImportError:
    modulo_tela_cliente = None

# Ajuste de importação para o novo Módulo de Permissões (Substitui a segurança do antigo modulo_cliente)
try:
    from OPERACIONAL.CLIENTES.PERMISSÕES import modulo_permissoes
except ImportError:
    modulo_permissoes = None

# --- DEMAIS IMPORTAÇÕES NECESSÁRIAS (Mantendo compatibilidade) ---
# Tenta importar módulos auxiliares se existirem no path
try:
    import modulo_wapi
    import modulo_whats_controlador
    import modulo_usuario # Mantido caso precise de funções internas, mas removido do menu
    import modulo_chat
    import modulo_pessoa_fisica as modulo_pf
    import modulo_produtos
    import modulo_pedidos
    import modulo_tarefas
    import modulo_renovacao_feedback as modulo_rf
    import modulo_pf_campanhas
    import modulo_conexoes
except ImportError:
    pass

# --- 1. CONFIGURAÇÃO DA PÁGINA E ESTADO ---
st.set_page_config(page_title="Sistema de Gestão", layout="wide")

def iniciar_estado():
    if 'ultima_atividade' not in st.session_state:
        st.session_state['ultima_atividade'] = datetime.now()
    if 'hora_login' not in st.session_state:
        st.session_state['hora_login'] = datetime.now()
    if 'menu_aberto' not in st.session_state:
        st.session_state['menu_aberto'] = None
    if 'pagina_atual' not in st.session_state:
        st.session_state['pagina_atual'] = "Home"

def resetar_atividade():
    st.session_state['ultima_atividade'] = datetime.now()

# --- 2. CSS (ESTILOS E LAYOUT) ---
def carregar_css():
    st.markdown("""
        <style>
        div.stButton > button {
            width: 100%;
            border: 1px solid #000000 !important;
            border-radius: 0px !important;
            color: black;
            background-color: #ffffff;
            font-weight: 500;
            margin-bottom: 5px;
        }
        div.stButton > button:hover {
            border-color: #FF4B4B !important;
            color: #FF4B4B;
        }
        section[data-testid="stSidebar"] {
            background-color: #f0f2f6;
        }
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        </style>
    """, unsafe_allow_html=True)

# --- 3. REGRAS DE SESSÃO ---
def gerenciar_sessao():
    TEMPO_LIMITE_MINUTOS = 60
    agora = datetime.now()
    tempo_inativo = agora - st.session_state['ultima_atividade']
    
    if tempo_inativo.total_seconds() > (TEMPO_LIMITE_MINUTOS * 60):
        st.session_state.clear()
        st.error("Sessão expirada. Recarregue a página.")
        st.stop()

    tempo_total = agora - st.session_state['hora_login']
    mm, ss = divmod(tempo_total.seconds, 60)
    hh, mm = divmod(mm, 60)
    return f"{hh:02d}:{mm:02d}:{ss:02d}" if hh > 0 else f"{mm:02d}:{ss:02d}"

# --- 4. TELAS AUXILIARES ---
def tela_fluxo_caixa():
    st.title("💰 Financeiro > Fluxo de Caixa")
    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    with c1: st.date_input("Data Início")
    with c2: st.date_input("Data Fim")
    with c3: st.selectbox("Conta", ["Banco A", "Banco B"])
    
    m1, m2, m3 = st.columns(3)
    m1.metric("Entradas", "R$ 45.200,00", "+5%")
    m2.metric("Saídas", "R$ 32.100,00", "-2%")
    m3.metric("Saldo", "R$ 13.100,00", "OK")
    
    st.line_chart(pd.DataFrame(np.random.randn(20, 3), columns=['Entradas', 'Saídas', 'Saldo']))

def tela_generica(titulo):
    st.title(f"📂 {titulo}")
    st.info("Funcionalidade em desenvolvimento ou módulo não carregado.")

# --- 5. MENU LATERAL ---
def renderizar_menu():
    with st.sidebar:
        st.markdown("**Usuário:** Admin System")
        try: st.image("logo_assessoria.png", use_container_width=True)
        except: pass 
        
        # --- CONFIGURAÇÃO DOS ÍCONES ---
        icones = {
            "Operacional": "⚙️", "Comercial": "💼", "Conexões": "🔌",
            "CLIENTES ASSESSORIA": "👥", "Banco PF": "🏦",
            "Campanhas": "📣", "WhatsApp": "💬", "Produtos": "📦",
            "Pedidos": "🛒", "Tarefas": "📝", "Renovação": "🔄"
        }

        # Pega o cargo do usuário na sessão (se existir)
        cargo_banco = st.session_state.get('usuario_cargo', 'VISITANTE')
        cargo_normalizado = str(cargo_banco).strip().upper()

        # --- DEFINIÇÃO DA ESTRUTURA DO MENU ---
        estrutura_menu = {}

        # Botão Home
        if st.button("🏠 Início", key="btn_home", on_click=resetar_atividade):
            st.session_state['pagina_atual'] = "Início"
            st.session_state['menu_aberto'] = None

        # Lógica de Permissão de Menu
        if cargo_normalizado in ["ADMIN", "GERENTE", "ADMINISTRADOR"]:
            # MENU ADMIN: Removemos "Usuários" e adicionamos "CLIENTES ASSESSORIA"
            estrutura_menu["Operacional"] = ["CLIENTES ASSESSORIA", "Banco PF", "Campanhas", "WhatsApp"]
            estrutura_menu["Comercial"] = ["Produtos", "Pedidos", "Tarefas", "Renovação"]
            estrutura_menu["Conexões"] = [] 
        else:
            # MENU PADRÃO: Removemos "Usuários" e adicionamos "CLIENTES ASSESSORIA"
            estrutura_menu["Operacional"] = ["CLIENTES ASSESSORIA", "WhatsApp"]

        # Renderização Dinâmica dos Botões
        for menu_pai, subitens in estrutura_menu.items():
            icon_pai = icones.get(menu_pai, "📂")
            
            # Se não tiver subitens, é um botão direto
            if not subitens:
                if st.button(f"{icon_pai} {menu_pai}", key=f"pai_{menu_pai}", on_click=resetar_atividade):
                    st.session_state['pagina_atual'] = menu_pai
                    st.session_state['menu_aberto'] = None
                continue

            # Se tiver subitens, é um acordeão
            icone_seta = "▼" if st.session_state['menu_aberto'] == menu_pai else "►"
            if st.button(f"{icon_pai} {menu_pai} {icone_seta}", key=f"pai_{menu_pai}", on_click=resetar_atividade):
                if st.session_state['menu_aberto'] == menu_pai:
                    st.session_state['menu_aberto'] = None
                else:
                    st.session_state['menu_aberto'] = menu_pai

            # Renderiza os filhos se estiver aberto
            if st.session_state['menu_aberto'] == menu_pai:
                for item in subitens:
                    _, col_btn = st.columns([0.1, 0.9])
                    with col_btn:
                        icon_filho = icones.get(item, "↳")
                        if st.button(f"{icon_filho} {item}", key=f"sub_{item}", on_click=resetar_atividade):
                            st.session_state['pagina_atual'] = f"{menu_pai} > {item}"

        st.markdown("<br>"*5 + "---", unsafe_allow_html=True)
        st.markdown(f"<div style='text-align:center'><strong>{gerenciar_sessao()}</strong></div>", unsafe_allow_html=True)

# --- 6. MAIN (ROTEAMENTO) ---
def main():
    iniciar_estado()
    carregar_css()
    
    # Auto-refresh para manter o relógio da sessão
    st_autorefresh(interval=1000, key="sistema_relogio")
    
    renderizar_menu()
    
    pagina = st.session_state['pagina_atual']
    
    # --- ROTEAMENTO DE PÁGINAS ---
    
    if pagina == "Home" or pagina == "Início": 
        st.title("Bem-vindo ao Sistema de Gestão")
        st.info("Utilize o menu lateral para navegar.")
        if 'modulo_chat' in globals() and modulo_chat:
             modulo_chat.app_chat_screen()

    # --- NOVO HUB DE CLIENTES ---
    elif "Operacional > CLIENTES ASSESSORIA" in pagina:
        # 1. Verifica Permissão (Usando o novo módulo, se disponível)
        if modulo_permissoes:
            modulo_permissoes.verificar_bloqueio_de_acesso(
                chave="bloqueio_menu_cliente", 
                caminho_atual="Operacional > Clientes Assessoria", 
                parar_se_bloqueado=True
            )
        
        # 2. Carrega o Hub Visual
        if modulo_tela_cliente:
            modulo_tela_cliente.app_clientes()
        else:
            st.error("Erro: Módulo 'modulo_tela_cliente' não carregado.")
            st.info("Verifique se o arquivo está na pasta OPERACIONAL/CLIENTES e se existe o __init__.py")

    # --- DEMAIS ROTAS ---
    elif "Operacional > Banco PF" in pagina and 'modulo_pf' in globals(): modulo_pf.app_pessoa_fisica()
    elif "Operacional > Campanhas" in pagina and 'modulo_pf_campanhas' in globals(): modulo_pf_campanhas.app_campanhas()
    elif "Operacional > WhatsApp" in pagina and 'modulo_whats_controlador' in globals(): modulo_whats_controlador.app_wapi()
    
    elif "Comercial > Produtos" in pagina and 'modulo_produtos' in globals(): modulo_produtos.app_produtos()
    elif "Comercial > Pedidos" in pagina and 'modulo_pedidos' in globals(): modulo_pedidos.app_pedidos()
    elif "Comercial > Tarefas" in pagina and 'modulo_tarefas' in globals(): modulo_tarefas.app_tarefas()
    elif "Comercial > Renovação" in pagina and 'modulo_rf' in globals(): modulo_rf.app_renovacao_feedback()
    
    elif "Financeiro > Fluxo de Caixa" in pagina: 
        tela_fluxo_caixa()

    elif pagina == "Conexões" and 'modulo_conexoes' in globals():
        modulo_conexoes.app_conexoes()
        
    else: 
        tela_generica(pagina)

if __name__ == "__main__":
    main()