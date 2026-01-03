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
    import modulo_pf_config_exportacao as pf_config_exp
except ImportError:
    pf_config_exp = None

def app_pessoa_fisica():
    pf_core.init_db_structures()
    
    # CSS para ajustes finos (mantido)
    st.markdown("""
        <style>
            .stButton button { height: 28px; padding-top: 0px; padding-bottom: 0px; }
            div[data-testid="stExpander"] details summary p { font-weight: bold; font-size: 1.1em; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    # --- NOVO LAYOUT COM ABAS (IGUAL AO MODULO CLIENTE) ---
    tabs = st.tabs([
        "📋 Lista / Pesquisa", 
        "🔍 Pesquisa Avançada", 
        "📥 Importação", 
        "➕ Novo Cadastro", 
        "📢 Campanhas", 
        "⚙️ Config. Exportação"
    ])

    # =========================================================================
    # ABA 1: LISTA E PESQUISA RÁPIDA
    # =========================================================================
    with tabs[0]:
        st.markdown("#### Pesquisa Rápida")
        
        # Seletor de busca simples
        c1, c2 = st.columns([1, 4])
        busca = c2.text_input("Buscar por Nome ou CPF", key="pf_busca_rapida_aba", placeholder="Digite para buscar...")
        
        if busca:
            df_lista, total = pf_pesquisa.buscar_pf_simples(busca, pagina=1) # Paginação pode ser melhorada depois
            
            if not df_lista.empty:
                st.info(f"**Registros Encontrados:** {total}")
                
                # Cabeçalho da Lista
                st.markdown("""
                <div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex; margin-bottom: 5px; border-radius: 4px;">
                    <div style="flex: 4;">Nome</div>
                    <div style="flex: 2;">CPF</div>
                    <div style="flex: 1; text-align: center;">ID</div>
                    <div style="flex: 2; text-align: center;">Ações</div>
                </div>""", unsafe_allow_html=True)

                for _, row in df_lista.iterrows():
                    with st.container():
                        c_nome, c_cpf, c_id, c_act = st.columns([4, 2, 1, 2])
                        c_nome.write(row['nome'])
                        c_cpf.write(pf_core.formatar_cpf_visual(row['cpf']))
                        c_id.markdown(f"<div style='text-align: center;'>{row['id']}</div>", unsafe_allow_html=True)
                        
                        with c_act:
                            b1, b2, b3 = st.columns(3)
                            # Ações: Visualizar e Excluir funcionam direto. Editar requer roteamento (ou modal futuramente)
                            if b1.button("👁️", key=f"vq_{row['id']}", help="Ver Detalhes"): 
                                pf_core.dialog_visualizar_cliente(str(row['cpf']))
                            
                            if b2.button("✏️", key=f"eq_{row['id']}", help="Editar"): 
                                # Para editar, jogamos para o estado de edição, mas precisamos avisar o usuário
                                st.session_state.update({'pf_cpf_selecionado': str(row['cpf']), 'form_loaded': False})
                                # O ideal aqui seria abrir um st.dialog de edição, mas como o módulo cadastro é complexo,
                                # podemos exibir um aviso ou redirecionar.
                                # Como estamos em abas, vamos abrir um Dialog simplificado ou redirecionar a aba de cadastro.
                                st.toast(f"Editando {row['nome']} na aba 'Novo Cadastro'...")
                                # Nota: O controle de abas ativo programaticamente no Streamlit é limitado sem hacks.
                                # Uma solução é renderizar o form de edição aqui mesmo num expander ou dialog.
                                pf_core.dialog_editar_pf_rapido(str(row['cpf'])) # Supondo que essa func exista ou adaptamos

                            if b3.button("🗑️", key=f"dq_{row['id']}", help="Excluir"): 
                                pf_core.dialog_excluir_pf(str(row['cpf']), row['nome'])
                        
                        st.markdown("<hr style='margin: 2px 0; border-color: #eee;'>", unsafe_allow_html=True)
            else: 
                st.warning("Nenhum registro encontrado para o termo pesquisado.")
        else:
            st.info("Utilize o campo acima para localizar registros no banco de dados.")

    # =========================================================================
    # ABA 2: PESQUISA AVANÇADA (AMPLA)
    # =========================================================================
    with tabs[1]:
        pf_pesquisa.interface_pesquisa_ampla()

    # =========================================================================
    # ABA 3: IMPORTAÇÃO
    # =========================================================================
    with tabs[2]:
        pf_importacao.interface_importacao()

    # =========================================================================
    # ABA 4: NOVO CADASTRO (OU EDIÇÃO)
    # =========================================================================
    with tabs[3]:
        # Verifica se há um CPF selecionado para edição vindo de outra aba
        cpf_edit = st.session_state.get('pf_cpf_selecionado')
        
        if cpf_edit:
            st.markdown(f"#### ✏️ Editando CPF: {cpf_edit}")
            if st.button("Cancelar Edição / Limpar"):
                st.session_state['pf_cpf_selecionado'] = None
                st.rerun()
        else:
            st.markdown("#### ➕ Novo Cadastro")

        # Chama a interface de cadastro (que deve estar preparada para lidar com st.session_state['pf_cpf_selecionado'])
        pf_core.interface_cadastro_pf()

    # =========================================================================
    # ABA 5: GESTÃO DE CAMPANHAS
    # =========================================================================
    with tabs[4]:
        if pf_campanhas:
            pf_campanhas.app_campanhas()
        else:
            st.error("Módulo de Campanhas não carregado.")

    # =========================================================================
    # ABA 6: CONFIGURAÇÃO DE EXPORTAÇÃO
    # =========================================================================
    with tabs[5]:
        if pf_config_exp:
            pf_config_exp.app_config_exportacao()
        else:
            st.warning("Módulo de Configuração de Exportação não encontrado.")

if __name__ == "__main__":
    app_pessoa_fisica()