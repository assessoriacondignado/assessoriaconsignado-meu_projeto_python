import streamlit as st
import modulo_pf_cadastro as pf_core
import modulo_pf_importacao as pf_imp
import modulo_pf_config_exportacao as pf_conf_exp
import modulo_pf_exportacao as pf_exp
import modulo_pf_campanhas as pf_mkt
import modulo_pf_planilhas as pf_sheets

def app_pessoa_fisica():
    # --- CABEÇALHO ---
    st.title("👥 Módulo Pessoa Física")
    
    # --- MENU SUPERIOR (HORIZONTAL) ---
    # Definição das opções do menu
    menu_options = {
        "Gestão de Clientes": "gestao",
        "Importação": "importacao",
        "Campanhas (CRM)": "campanhas",
        "Exportar Dados": "exportar",
        "Config. Exportação": "config_exp",
        "Config. Planilhas": "config_sheets"
    }
    
    # Criação das colunas para simular uma barra de navegação ou usar radio horizontal
    # Opção A (Radio Horizontal - Mais limpo e funcional para navegação de abas):
    escolha_label = st.radio(
        "Navegação:",
        options=list(menu_options.keys()),
        horizontal=True,
        label_visibility="collapsed" # Esconde o label "Navegação:" para ficar clean
    )
    
    st.divider() # Linha separadora entre menu e conteúdo
    
    # Recupera a chave interna baseada na escolha
    choice_pf = menu_options[escolha_label]

    # --- ROTEAMENTO DE TELAS ---
    if choice_pf == "gestao":
        # Chama a função única do módulo unificado (Pesquisa/Cadastro/Visualização)
        pf_core.app_cadastro_unificado()

    elif choice_pf == "importacao":
        pf_imp.interface_importacao()

    elif choice_pf == "campanhas":
        pf_mkt.app_campanhas()

    elif choice_pf == "exportar":
        pf_exp.app_exportacao_dados()

    elif choice_pf == "config_exp":
        pf_conf_exp.app_config_exportacao()
    
    elif choice_pf == "config_sheets":
        pf_sheets.app_config_planilhas()

# Bloco para teste isolado (opcional)
if __name__ == "__main__":
    app_pessoa_fisica()