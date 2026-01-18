import streamlit as st
import os
import sys
import importlib

# --- CONFIGURAÇÃO DE CAMINHOS PARA SUB-MÓDULOS ---
# Garante que a pasta atual esteja no path para importar os sub-módulos desta pasta
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# --- IMPORTAÇÃO SEGURA DOS SUB-MÓDULOS ---
def importar_modulo_interno(nome_modulo):
    try:
        if nome_modulo in sys.modules:
            return importlib.reload(sys.modules[nome_modulo])
        else:
            return __import__(nome_modulo)
    except ImportError:
        return None
    except Exception as e:
        st.error(f"Erro ao carregar {nome_modulo}: {e}")
        return None

# Tenta importar os módulos funcionais (que criaremos nos próximos passos)
modulo_cadastro = importar_modulo_interno("modulo_sistema_consulta_cadastro")
modulo_planilhas = importar_modulo_interno("modulo_sistema_consulta_planilhas")
modulo_crm = importar_modulo_interno("modulo_sistema_consulta_crm")

def app_sistema_consulta():
    st.markdown("## 👥 CRM CONSULTA")

    # --- MENU SUPERIOR (Conforme DOC ) ---
    # Opções do menu baseadas no e layout 
    menu_opcoes = ["Cadastro / Pesquisa", "Planilhas (Tabelas)", "CRM / Gestão"]
    
    # Armazena a escolha no session_state para persistência durante a navegação
    if 'menu_consulta_selecionado' not in st.session_state:
        st.session_state['menu_consulta_selecionado'] = menu_opcoes[0]

    # Renderiza o Menu Superior (estilo abas ou radio horizontal)
    escolha = st.radio(
        "", 
        menu_opcoes, 
        horizontal=True, 
        label_visibility="collapsed",
        key="nav_sistema_consulta"
    )

    st.divider()

    # --- ROTEAMENTO DE TELAS ---
    
    if escolha == "Cadastro / Pesquisa":
        if modulo_cadastro:
            # O módulo de cadastro terá suas próprias sub-abas (Novo, Pesquisa Simples, Completa)
            modulo_cadastro.app_cadastro()
        else:
            st.warning("⚠️ Módulo 'Cadastro' (modulo_sistema_consulta_cadastro.py) não encontrado ou em construção.")
            st.info("O próximo passo é criar este arquivo.")

    elif escolha == "Planilhas (Tabelas)":
        if modulo_planilhas:
            modulo_planilhas.app_planilhas()
        else:
            st.warning("⚠️ Módulo 'Planilhas' (modulo_sistema_consulta_planilhas.py) não encontrado.")
            
    elif escolha == "CRM / Gestão":
        if modulo_crm:
            modulo_crm.app_crm()
        else:
            st.warning("⚠️ Módulo 'CRM' (modulo_sistema_consulta_crm.py) não encontrado.")

# Bloco para teste individual do módulo
if __name__ == "__main__":
    app_sistema_consulta()