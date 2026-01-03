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

# --- NOVO IMPORT ---
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
        </style>
    """, unsafe_allow_html=True)

    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    # Inicializa estados
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'regras_pesquisa' not in st.session_state: st.session_state['regras_pesquisa'] = []
    if 'pagina_atual' not in st.session_state: st.session_state['pagina_atual'] = 1

    # =========================================================================
    # ROTEAMENTO DE TELAS
    # =========================================================================
    
    # 1. PESQUISA AMPLA
    if st.session_state['pf_view'] == 'pesquisa_ampla':
        pf_pesquisa.interface_pesquisa_ampla()

    # 2. CAMPANHAS
    elif st.session_state['pf_view'] == 'campanhas':
        if st.button("⬅️ Voltar para Lista"): st.session_state['pf_view'] = 'lista'; st.rerun()
        if pf_campanhas: pf_campanhas.app_campanhas()

    # 3. EXPORTAÇÃO (LEGADO)
    elif st.session_state['pf_view'] == 'modelos_exportacao':
        if st.button("⬅️ Voltar para Lista"): st.session_state['pf_view'] = 'lista'; st.rerun()
        if pf_export: pf_export.app_gestao_modelos()

    # 4. CONFIG EXPORTAÇÃO
    elif st.session_state['pf_view'] == 'config_exportacao':
        if st.button("⬅️ Voltar para Lista"): st.session_state['pf_view'] = 'lista'; st.rerun()
        if pf_config_exp: pf_config_exp.app_config_exportacao()

    # --- 5. NOVO: PLANILHAS (SOMENTE BANCO_PF) ---
    elif st.session_state['pf_view'] == 'planilhas':
        if st.button("⬅️ Voltar para Lista"): st.session_state['pf_view'] = 'lista'; st.rerun()
        
        if modulo_pf_planilhas:
            modulo_pf_planilhas.app_gestao_planilhas()
        else:
            st.error("Módulo 'modulo_pf_planilhas.py' não encontrado.")

    # 6. TELA INICIAL (LISTA + MENU)
    elif st.session_state['pf_view'] == 'lista':
        c1, c2 = st.columns([2, 2])
        busca = c2.text_input("🔎 Pesquisa Rápida (Nome/CPF)", key="pf_busca")
        
        # --- MENU ATUALIZADO ---
        # Adicionado col_b6 para o novo botão
        col_b1, col_b2, col_b3, col_b4, col_b5, col_b6 = st.columns([1, 1, 1, 1, 1, 1])
        
        if col_b1.button("➕ Novo", use_container_width=True): 
            st.session_state.update({'pf_view': 'novo', 'form_loaded': False}); st.rerun()
            
        if col_b2.button("🔍 Pesq.", help="Pesquisa Ampla", use_container_width=True): 
            st.session_state.update({'pf_view': 'pesquisa_ampla'}); st.rerun()
            
        if col_b3.button("📥 Importar", use_container_width=True): 
            st.session_state.update({'pf_view': 'importacao', 'import_step': 1}); st.rerun()
            
        if col_b4.button("📢 Campanhas", use_container_width=True): 
            st.session_state.update({'pf_view': 'campanhas'}); st.rerun()

        if col_b5.button("⚙️ Config", help="Configurar Exportação", use_container_width=True):
            st.session_state.update({'pf_view': 'config_exportacao'}); st.rerun()

        # BOTÃO NOVO
        if col_b6.button("📊 Planilhas", help="Ver/Editar Tabelas (banco_pf)", use_container_width=True):
            st.session_state.update({'pf_view': 'planilhas'}); st.rerun()
        
        # RESULTADO DA BUSCA RÁPIDA (Código mantido igual)
        if busca:
            df_lista, total = pf_pesquisa.buscar_pf_simples(busca, pagina=st.session_state.get('pagina_atual', 1))
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
                        if b1.button("👁️", key=f"vq_{row['id']}"): pf_core.dialog_visualizar_cliente(str(row['cpf']))
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

    # 7. TELAS AUXILIARES
    elif st.session_state['pf_view'] == 'importacao': pf_importacao.interface_importacao()
    elif st.session_state['pf_view'] in ['novo', 'editar']: pf_core.interface_cadastro_pf()

if __name__ == "__main__":
    app_pessoa_fisica()