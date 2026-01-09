import streamlit as st
import importlib
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

# --- CALLBACKS DE NAVEGAÇÃO (GARANTIA DE FUNCIONAMENTO) ---
def navegar_visualizar(cpf):
    st.session_state['pf_view'] = 'visualizar'
    st.session_state['pf_cpf_selecionado'] = str(cpf)

def navegar_editar(cpf):
    st.session_state['pf_view'] = 'editar'
    st.session_state['pf_cpf_selecionado'] = str(cpf)
    st.session_state['form_loaded'] = False

def navegar_novo():
    st.session_state['pf_view'] = 'novo'
    st.session_state['form_loaded'] = False

def navegar_importacao():
    st.session_state['pf_view'] = 'importacao'
    st.session_state['import_step'] = 1

def app_pessoa_fisica():
    # Recarrega módulos para garantir atualizações (opcional, útil em dev)
    try:
        importlib.reload(pf_core)
        importlib.reload(pf_pesquisa)
    except: pass

    pf_core.init_db_structures()
    
    st.markdown("""
        <style>
            .stButton button { height: 28px; padding-top: 0px; padding-bottom: 0px; }
            div[data-testid="stExpander"] details summary p { font-weight: bold; font-size: 1.1em; }
            div[role="radiogroup"] > label { padding-right: 20px; font-weight: bold; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    # Inicializa estados
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'regras_pesquisa' not in st.session_state: st.session_state['regras_pesquisa'] = []
    if 'pf_pagina_atual' not in st.session_state: st.session_state['pf_pagina_atual'] = 1

    # =========================================================================
    # MENU SUPERIOR (NAVEGAÇÃO)
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
    
    # Mantém a aba "Gestão" ativa visualmente se estiver em sub-telas
    if current_view in ['editar', 'visualizar']:
        active_menu_label = "🔍 Gestão & Pesquisa"
    else:
        active_menu_label = VIEW_TO_MENU.get(current_view, "🔍 Gestão & Pesquisa")
    
    # Sincroniza widget
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
    
    # Detecta mudança manual no menu
    if selected_menu_label != active_menu_label:
        target_view = MENU_MAP[selected_menu_label]
        # Só navega se não for um "falso positivo" (clicar na aba que já "contém" a tela atual)
        if not (current_view in ['editar', 'visualizar'] and target_view == 'lista'):
            st.session_state['pf_view'] = target_view
            if target_view == 'novo': st.session_state['form_loaded'] = False
            if target_view == 'importacao': st.session_state['import_step'] = 1
            st.session_state['pf_top_menu_radio'] = selected_menu_label
            st.rerun()

    st.divider()

    # =========================================================================
    # ROTEAMENTO DE CONTEÚDO
    # =========================================================================
    
    if st.session_state['pf_view'] == 'pesquisa_ampla':
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
    
    # --- VISUALIZAR (Direciona para o cadastro) ---
    elif st.session_state['pf_view'] == 'visualizar':
        if hasattr(pf_core, 'interface_visualizar_cliente'):
            pf_core.interface_visualizar_cliente()
        else:
            st.error("Erro: Função 'interface_visualizar_cliente' não encontrada.")

    elif st.session_state['pf_view'] == 'importacao':
        pf_importacao.interface_importacao()

    # --- NOVO / EDITAR ---
    elif st.session_state['pf_view'] in ['novo', 'editar']:
        pf_core.interface_cadastro_pf()

    # --- LISTA RÁPIDA (PADRÃO) ---
    elif st.session_state['pf_view'] == 'lista':
        c1, c2 = st.columns([2, 2])
        busca = c2.text_input("🔎 Pesquisa Rápida (Nome/CPF)", key="pf_busca")
        
        if busca:
            df_lista, total = pf_pesquisa.buscar_pf_simples(busca, pagina=st.session_state.get('pf_pagina_atual', 1))
            
            if not df_lista.empty:
                st.markdown(f"**Encontrados: {total}**")
                st.markdown("""
                <div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex;">
                    <div style="flex: 2;">Ações</div>
                    <div style="flex: 1;">ID</div>
                    <div style="flex: 2;">CPF</div>
                    <div style="flex: 4;">Nome</div>
                </div>""", unsafe_allow_html=True)

                for _, row in df_lista.iterrows():
                    c_act, c_id, c_cpf, c_nome = st.columns([2, 1, 2, 4])
                    with c_act:
                        b1, b2, b3 = st.columns(3)
                        
                        # --- USO DE CALLBACKS PARA NAVEGAÇÃO SEGURA ---
                        b1.button("👁️", key=f"vq_{row['id']}", on_click=navegar_visualizar, args=(row['cpf'],))
                        b2.button("✏️", key=f"eq_{row['id']}", on_click=navegar_editar, args=(row['cpf'],))
                        
                        if b3.button("🗑️", key=f"dq_{row['id']}"): 
                            pf_core.dialog_excluir_pf(str(row['cpf']), row['nome'])
                            
                    c_id.write(str(row['id']))
                    c_cpf.write(pf_core.formatar_cpf_visual(row['cpf']))
                    c_nome.write(row['nome'])
                    st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
            else: 
                st.warning("Nenhum registro encontrado.")
        else:
            st.info("Utilize a busca acima para localizar clientes.")

if __name__ == "__main__":
    app_pessoa_fisica()