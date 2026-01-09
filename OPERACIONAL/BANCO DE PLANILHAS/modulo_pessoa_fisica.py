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
    
    # Inicializa estados
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'regras_pesquisa' not in st.session_state: st.session_state['regras_pesquisa'] = []
    if 'pf_pagina_atual' not in st.session_state: st.session_state['pf_pagina_atual'] = 1

    # =========================================================================
    # MENU SUPERIOR (NAVEGAÇÃO)
    # =========================================================================
    
    # Mapeamento: "Nome no Menu" -> "Valor da pf_view"
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
    
    # Lógica Inteligente de Aba Ativa
    # Se estiver em sub-telas (editar/visualizar), a aba ativa deve ser "Gestão & Pesquisa"
    if current_view in ['editar', 'visualizar']:
        active_menu_label = "🔍 Gestão & Pesquisa"
    else:
        active_menu_label = VIEW_TO_MENU.get(current_view, "🔍 Gestão & Pesquisa")
    
    # --- CORREÇÃO CRÍTICA: Sincronizar estado do widget ---
    # Força o widget st.radio a refletir a aba correta, evitando que ele resete a navegação
    # Ex: Se vim de "Pesquisa Avançada" para "Visualizar", forço o radio a ir para "Gestão"
    if 'pf_top_menu_radio' not in st.session_state or st.session_state['pf_top_menu_radio'] != active_menu_label:
        st.session_state['pf_top_menu_radio'] = active_menu_label

    # Renderiza o Menu
    selected_menu_label = st.radio(
        "Submenu Superior", 
        options=list(MENU_MAP.keys()), 
        # index não é estritamente necessário se usamos session_state, mas mantemos por segurança
        index=list(MENU_MAP.keys()).index(active_menu_label), 
        horizontal=True, 
        label_visibility="collapsed",
        key="pf_top_menu_radio"
    )
    
    # Lógica de Troca de Tela via Menu
    target_view = MENU_MAP[selected_menu_label]
    
    # Verifica se houve mudança real de aba
    if target_view != current_view:
        # Permite ficar em 'editar'/'visualizar' se a aba selecionada for 'lista' (que é a pai delas)
        if current_view in ['editar', 'visualizar'] and target_view == 'lista':
            pass # Não faz nada, mantém a tela de edição/visualização aberta
        else:
            # Se clicou em outra aba (ex: Importação), muda a tela
            st.session_state['pf_view'] = target_view
            
            # Reseta flags auxiliares ao mudar de módulo
            if target_view == 'novo': st.session_state['form_loaded'] = False
            if target_view == 'importacao': st.session_state['import_step'] = 1
            st.rerun()

    st.divider()

    # =========================================================================
    # ROTEAMENTO DE CONTEÚDO
    # =========================================================================
    
    # 1. PESQUISA AVANÇADA / AMPLA
    if st.session_state['pf_view'] == 'pesquisa_ampla':
        pf_pesquisa.interface_pesquisa_ampla()

    # 2. CAMPANHAS
    elif st.session_state['pf_view'] == 'campanhas':
        if pf_campanhas: pf_campanhas.app_campanhas(key_sufix="interno_pf")

    # 3. EXPORTAÇÃO
    elif st.session_state['pf_view'] == 'modelos_exportacao':
        if st.button("⬅️ Voltar"): st.session_state['pf_view'] = 'lista'; st.rerun()
        if pf_export: pf_export.app_gestao_modelos()

    # 4. CONFIG EXPORTAÇÃO
    elif st.session_state['pf_view'] == 'config_exportacao':
        if pf_config_exp: pf_config_exp.app_config_exportacao()

    # 5. PLANILHAS
    elif st.session_state['pf_view'] == 'planilhas':
        if modulo_pf_planilhas:
            modulo_pf_planilhas.app_gestao_planilhas()
        else:
            st.error("Módulo 'modulo_pf_planilhas.py' não encontrado.")
    
    # 6. VISUALIZAR CLIENTE (TELA)
    elif st.session_state['pf_view'] == 'visualizar':
        pf_core.interface_visualizar_cliente()

    # 7. IMPORTAÇÃO
    elif st.session_state['pf_view'] == 'importacao':
        pf_importacao.interface_importacao()

    # 8. NOVO CADASTRO / EDIÇÃO (Formulário)
    elif st.session_state['pf_view'] in ['novo', 'editar']:
        pf_core.interface_cadastro_pf()

    # 9. GESTÃO & PESQUISA (LISTA PADRÃO)
    elif st.session_state['pf_view'] == 'lista':
        c1, c2 = st.columns([2, 2])
        busca = c2.text_input("🔎 Pesquisa Rápida (Nome/CPF)", key="pf_busca")
        
        # RESULTADO DA BUSCA RÁPIDA
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
                        
                        if b1.button("👁️", key=f"vq_{row['id']}"): 
                            st.session_state.update({'pf_view': 'visualizar', 'pf_cpf_selecionado': str(row['cpf'])})
                            st.rerun()
                            
                        if b2.button("✏️", key=f"eq_{row['id']}"): 
                            st.session_state.update({'pf_view': 'editar', 'pf_cpf_selecionado': str(row['cpf']), 'form_loaded': False})
                            st.rerun()
                        if b3.button("🗑️", key=f"dq_{row['id']}"): pf_core.dialog_excluir_pf(str(row['cpf']), row['nome'])
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