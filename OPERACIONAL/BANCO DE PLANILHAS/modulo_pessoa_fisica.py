import streamlit as st
import modulo_pf_cadastro as pf_core # Agora é o módulo unificado
import modulo_pf_importacao as pf_imp
import modulo_pf_config_exportacao as pf_conf_exp
import modulo_pf_exportacao as pf_exp
import modulo_pf_campanhas as pf_mkt
import modulo_pf_planilhas as pf_sheets

def app_pessoa_fisica():
    st.title("👥 Módulo Pessoa Física")

    # Menu Superior de Navegação do Módulo
    menu_pf = [
        "Gestão de Clientes",  # Unificado (Pesquisa + Cadastro + Visualização)
        "Importação de Dados",
        "Configurar Exportação",
        "Exportar Dados",
        "Campanhas (CRM)",
        "Configurar Planilhas"
    ]
    
    choice_pf = st.sidebar.selectbox("Navegação PF", menu_pf)

    if choice_pf == "Gestão de Clientes":
        # Chama a função única do módulo unificado
        pf_core.app_cadastro_unificado()

    elif choice_pf == "Importação de Dados":
        pf_imp.interface_importacao()

    elif choice_pf == "Configurar Exportação":
        pf_conf_exp.app_config_exportacao()

    elif choice_pf == "Exportar Dados":
        pf_exp.app_exportacao_dados()

    elif choice_pf == "Campanhas (CRM)":
        pf_mkt.app_campanhas()
    
    elif choice_pf == "Configurar Planilhas":
        pf_sheets.app_config_planilhas()