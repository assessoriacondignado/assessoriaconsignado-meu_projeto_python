import streamlit as st
import modulo_pf_cadastro as pf_core
import modulo_pf_pesquisa as pf_pesquisa
import modulo_pf_importacao as pf_importacao

# Importações Opcionais
try:
    import modulo_pf_campanhas as pf_campanhas
except ImportError:
    pf_campanhas = None

try:
    import modulo_pf_exportacao as pf_export
except ImportError:
    pf_export = None

try:
    import modulo_pf_config_exportacao as pf_config_exp
except ImportError:
    pf_config_exp = None

try:
    import modulo_pf_planilhas
except ImportError:
    modulo_pf_planilhas = None

def app_pessoa_fisica():
    pf_core.init_db_structures()
    
    st.markdown("""
        <style>
            .stButton button { height: 28px; padding-top: 0px; padding-bottom: 0px; }
            div[data-testid="stExpander"] details summary p { font-weight: bold; font-size: 1.1em; }
            div[role="radiogroup"] > label { padding-right: 20px; font-weight: bold; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    # Estados Iniciais
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'regras_pesquisa' not in st.session_state: st.session_state['regras_pesquisa'] = []
    if 'pf_pagina_atual' not in st.session_state: st.session_state['pf_pagina_atual'] = 1

    # =========================================================================
    # MENU SUPERIOR
    # =========================================================================
    
    MENU_MAP = {
        "🔍 Gestão & Pesquisa": "lista",
        "🔎 Pesquisa Avançada": "pesquisa_ampla",
        "➕ Novo Cadastro": "novo",
        "📥 Importação": "importacao",
        "📢 Campanhas": "campanhas",
        "📊 Planilhas": "planilhas",
        "⚙️ Configurações": "config_exportacao"
    }
    
    VIEW_TO_MENU = {v: k for k, v in MENU_MAP.items()}
    current_view = st.session_state.get('pf_view', 'lista')
    
    # Mantém aba "Gestão" visualmente ativa se estiver nas sub-telas
    if current_view in ['editar', 'visualizar']:
        active_menu_label = "🔍 Gestão & Pesquisa"
    else:
        active_menu_label = VIEW_TO_MENU.get(current_view, "🔍 Gestão & Pesquisa")
    
    # Sincronia do Widget
    if 'pf_top_menu_radio' not in st.session_state:
        st.session_state['pf_top_menu_radio'] = active_menu_label
    
    if st.session_state['pf_top_menu_radio'] != active_menu_label:
         st.session_state['pf_top_menu_radio'] = active_menu_label

    selected_menu_label = st.radio(
        "Submenu Superior", 
        options=list(MENU_MAP.keys()), 
        index=list(MENU_MAP.keys()).index(active_menu_label), 
        horizontal=True, 
        label_visibility="collapsed",
        key="pf_top_menu_radio_widget"
    )
    
    if selected_menu_label != active_menu_label:
        target_view = MENU_MAP[selected_menu_label]
        if not (current_view in ['editar', 'visualizar'] and target_view == 'lista'):
            st.session_state['pf_view'] = target_view
            if target_view == 'novo': st.session_state['form_loaded'] = False
            if target_view == 'importacao': st.session_state['import_step'] = 1
            st.session_state['pf_top_menu_radio'] = selected_menu_label
            st.rerun()

    st.divider()

    # =========================================================================
    # ROTEADOR DE CONTEÚDO (SIMPLIFICADO)
    # =========================================================================
    
    # 1. GESTÃO E PESQUISA (Agora engloba lista, visualizar e editar)
    if st.session_state['pf_view'] in ['lista', 'visualizar', 'editar']:
        pf_pesquisa.app_gestao_pesquisa()

    # 2. OUTROS MÓDULOS
    elif st.session_state['pf_view'] == 'pesquisa_ampla':
        pf_pesquisa.interface_pesquisa_ampla()

    elif st.session_state['pf_view'] == 'campanhas':
        if pf_campanhas: pf_campanhas.app_campanhas(key_sufix="interno_pf")

    elif st.session_state['pf_view'] == 'modelos_exportacao':
        if st.button("⬅️ Voltar"): st.session_state['pf_view'] = 'lista'; st.rerun()
        if pf_export: pf_export.app_gestao_modelos()

    elif st.session_state['pf_view'] == 'config_exportacao':
        if pf_config_exp: pf_config_exp.app_config_exportacao()

    elif st.session_state['pf_view'] == 'planilhas':
        if modulo_pf_planilhas:
            modulo_pf_planilhas.app_gestao_planilhas()
        else:
            st.error("Módulo 'modulo_pf_planilhas.py' não encontrado.")

    elif st.session_state['pf_view'] == 'importacao':
        pf_importacao.interface_importacao()

    # Novo cadastro é tratado separadamente, fora do gerenciador de pesquisa
    elif st.session_state['pf_view'] == 'novo':
        pf_core.interface_cadastro_pf()

if __name__ == "__main__":
    app_pessoa_fisica()