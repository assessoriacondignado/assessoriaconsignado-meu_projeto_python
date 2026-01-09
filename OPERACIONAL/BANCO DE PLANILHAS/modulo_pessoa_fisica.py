import streamlit as st
import importlib
import modulo_pf_cadastro as pf_core
import modulo_pf_pesquisa as pf_pesquisa
import modulo_pf_importacao as pf_importacao

# Importações Opcionais
try:
    import modulo_pf_campanhas as pf_campanhas
except ImportError: pf_campanhas = None
try:
    import modulo_pf_exportacao as pf_export
except ImportError: pf_export = None
try:
    import modulo_pf_config_exportacao as pf_config_exp
except ImportError: pf_config_exp = None
try:
    import modulo_pf_planilhas
except ImportError: modulo_pf_planilhas = None

def app_pessoa_fisica():
    # Inicializa BD
    pf_core.init_db_structures()
    
    st.markdown("""
        <style>
            .stButton button { height: 28px; padding-top: 0px; padding-bottom: 0px; }
            div[data-testid="stExpander"] details summary p { font-weight: bold; font-size: 1.1em; }
            div[role="radiogroup"] > label { padding-right: 20px; font-weight: bold; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("## 👤 Banco de Dados Pessoa Física")

    # =========================================================================
    # MENU SUPERIOR
    # =========================================================================
    
    # Opções do menu
    MENU_OPTIONS = [
        "🔍 Gestão & Pesquisa", 
        "➕ Novo Cadastro", 
        "📥 Importação", 
        "📢 Campanhas", 
        "📊 Planilhas", 
        "⚙️ Configurações"
    ]

    # Estado do Menu
    if 'pf_menu_ativo' not in st.session_state:
        st.session_state['pf_menu_ativo'] = "🔍 Gestão & Pesquisa"

    # Widget de Menu
    selected = st.radio(
        "Menu", 
        options=MENU_OPTIONS, 
        index=MENU_OPTIONS.index(st.session_state['pf_menu_ativo']),
        horizontal=True, 
        label_visibility="collapsed",
        key="pf_nav_radio"
    )

    # Se mudar o menu, atualiza o estado
    if selected != st.session_state['pf_menu_ativo']:
        st.session_state['pf_menu_ativo'] = selected
        # Reseta estados internos ao mudar de aba
        if selected == "➕ Novo Cadastro":
            st.session_state['pf_view'] = 'novo'
            st.session_state['form_loaded'] = False
        elif selected == "🔍 Gestão & Pesquisa":
            st.session_state['pf_view'] = 'lista'
        st.rerun()

    st.divider()

    # =========================================================================
    # ROTEAMENTO
    # =========================================================================
    
    if selected == "🔍 Gestão & Pesquisa":
        # Chama o NOVO GERENTE de pesquisa
        pf_pesquisa.app_gestao_pesquisa()

    elif selected == "➕ Novo Cadastro":
        # Chama cadastro direto
        pf_core.interface_cadastro_pf()

    elif selected == "📥 Importação":
        pf_importacao.interface_importacao()

    elif selected == "📢 Campanhas":
        if pf_campanhas: pf_campanhas.app_campanhas(key_sufix="interno_pf")
        else: st.warning("Módulo Campanhas não instalado.")

    elif selected == "📊 Planilhas":
        if modulo_pf_planilhas: modulo_pf_planilhas.app_gestao_planilhas()
        else: st.warning("Módulo Planilhas não instalado.")
    
    elif selected == "⚙️ Configurações":
        if pf_config_exp: pf_config_exp.app_config_exportacao()
        else: st.warning("Módulo Configurações não instalado.")

if __name__ == "__main__":
    app_pessoa_fisica()