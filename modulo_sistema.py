import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

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

# --- 4. TELAS ---
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
    st.info("Funcionalidade em desenvolvimento.")

# --- 5. MENU ---
def renderizar_menu():
    with st.sidebar:
        st.markdown("**Usuário:** Admin System")
        try: st.image("logo_assessoria.png", use_container_width=True)
        except: st.warning("Sem logo")
        
        opcoes = {
            "Cadastros": ["Clientes", "Fornecedores", "Produtos"],
            "Financeiro": ["Contas a Pagar", "Contas a Receber", "Fluxo de Caixa"],
            "Relatórios": ["Geral", "Vendas", "Auditoria"]
        }

        for menu_pai, subitens in opcoes.items():
            icone = "▼" if st.session_state['menu_aberto'] == menu_pai else "►"
            if st.button(f"{menu_pai} {icone}", key=f"pai_{menu_pai}", on_click=resetar_atividade):
                st.session_state['menu_aberto'] = None if st.session_state['menu_aberto'] == menu_pai else menu_pai

            if st.session_state['menu_aberto'] == menu_pai:
                for item in subitens:
                    _, col_btn = st.columns([0.1, 0.9])
                    with col_btn:
                        if st.button(f"{item}", key=f"sub_{item}", on_click=resetar_atividade):
                            st.session_state['pagina_atual'] = f"{menu_pai} > {item}"

        st.markdown("<br>"*5 + "---", unsafe_allow_html=True)
        st.markdown(f"<div style='text-align:center'><strong>{gerenciar_sessao()}</strong></div>", unsafe_allow_html=True)

# --- MAIN ---
def main():
    iniciar_estado()
    carregar_css()
    st_autorefresh(interval=1000, key="sistema_relogio")
    renderizar_menu()
    
    pagina = st.session_state['pagina_atual']
    if "Fluxo de Caixa" in pagina: tela_fluxo_caixa()
    elif pagina == "Home": st.title("Bem-vindo")
    else: tela_generica(pagina)

if __name__ == "__main__":
    main()
