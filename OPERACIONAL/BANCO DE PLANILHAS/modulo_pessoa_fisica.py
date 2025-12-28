import streamlit as st
import modulo_pf_cadastro as pf_core
import modulo_pf_pesquisa as pf_pesquisa
import modulo_pf_importacao as pf_importacao

# Tenta importar o novo módulo de campanhas (garante que não quebre se o arquivo não existir)
try:
    import modulo_pf_campanhas as pf_campanhas
except ImportError:
    pf_campanhas = None

def app_pessoa_fisica():
    pf_core.init_db_structures()
    
    st.markdown("""
        <style>
            .stButton button { height: 28px; padding-top: 0px; padding-bottom: 0px; }
            div[data-testid="stExpander"] details summary p { font-weight: bold; font-size: 1.1em; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'regras_pesquisa' not in st.session_state: st.session_state['regras_pesquisa'] = []
    if 'pagina_atual' not in st.session_state: st.session_state['pagina_atual'] = 1

    # --- ROTEAMENTO DE TELAS ---
    
    if st.session_state['pf_view'] == 'pesquisa_ampla':
        pf_pesquisa.interface_pesquisa_ampla()

    # --- NOVA TELA: GESTÃO DE CAMPANHAS ---
    elif st.session_state['pf_view'] == 'campanhas':
        if st.button("⬅️ Voltar para Lista"):
            st.session_state['pf_view'] = 'lista'
            st.rerun()
        
        if pf_campanhas:
            pf_campanhas.app_campanhas()
        else:
            st.error("O módulo 'modulo_pf_campanhas.py' não foi encontrado na pasta.")

    elif st.session_state['pf_view'] == 'lista':
        # Lista simples (Tela Inicial)
        c1, c2 = st.columns([2, 2])
        busca = c2.text_input("🔎 Pesquisa Rápida (Nome/CPF)", key="pf_busca")
        
        # ATUALIZAÇÃO: Adicionado o botão 'Campanhas' no menu superior
        col_b1, col_b2, col_b3, col_b4 = st.columns([1, 1, 1, 1])
        
        if col_b1.button("➕ Novo"): 
            st.session_state.update({'pf_view': 'novo', 'form_loaded': False})
            st.rerun()
            
        if col_b2.button("🔍 Pesquisa Ampla"): 
            st.session_state.update({'pf_view': 'pesquisa_ampla'})
            st.rerun()
            
        if col_b3.button("📥 Importar"): 
            st.session_state.update({'pf_view': 'importacao', 'import_step': 1})
            st.rerun()
            
        if col_b4.button("📢 Campanhas"): 
            st.session_state.update({'pf_view': 'campanhas'})
            st.rerun()
        
        if busca:
            df_lista, total = pf_pesquisa.buscar_pf_simples(busca, pagina=st.session_state['pagina_atual'])
            if not df_lista.empty:
                df_lista['cpf'] = df_lista['cpf'].apply(pf_core.formatar_cpf_visual)
                st.dataframe(df_lista[['id', 'nome', 'cpf']], use_container_width=True)
            else: st.warning("Nada encontrado.")
        else:
            st.info("Utilize a busca para listar clientes.")

    elif st.session_state['pf_view'] == 'importacao':
        pf_importacao.interface_importacao()

    elif st.session_state['pf_view'] in ['novo', 'editar']:
        pf_core.interface_cadastro_pf()

if __name__ == "__main__":
    app_pessoa_fisica()