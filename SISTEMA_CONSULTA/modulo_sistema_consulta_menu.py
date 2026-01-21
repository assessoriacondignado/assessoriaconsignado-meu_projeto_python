import streamlit as st
import os
import sys
import importlib

# --- CONFIGURAÇÃO DE CAMINHOS PARA SUB-MÓDULOS ---
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
            
    except ImportError as e:
        # CORREÇÃO: Mostra o motivo real da falha de importação
        st.error(f"🔴 Erro ao importar '{nome_modulo}': {e}")
        # Dica pro usuário
        if "modulo_validadores" in str(e):
            st.warning("DICA: Verifique se o arquivo 'modulo_validadores.py' está na mesma pasta.")
        if "conexao" in str(e):
            st.warning("DICA: Verifique se o arquivo 'conexao.py' está na mesma pasta.")
        return None
        
    except Exception as e:
        st.error(f"Erro crítico ao carregar {nome_modulo}: {e}")
        return None

# Tenta importar os módulos funcionais
modulo_cadastro = importar_modulo_interno("modulo_sistema_consulta_cadastro")
modulo_planilhas = importar_modulo_interno("modulo_sistema_consulta_planilhas")
modulo_crm = importar_modulo_interno("modulo_sistema_consulta_crm")
modulo_importacao = importar_modulo_interno("modulo_sistema_consulta_importacao")

def app_sistema_consulta():
    st.markdown("## 👥 CRM CONSULTA")

    # --- MENU SUPERIOR ---
    menu_opcoes = ["Cadastro / Pesquisa", "Planilhas (Tabelas)", "CRM / Gestão", "Importação"]
    
    if 'menu_consulta_selecionado' not in st.session_state:
        st.session_state['menu_consulta_selecionado'] = menu_opcoes[0]

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
            try:
                modulo_cadastro.app_cadastro()
            except Exception as e:
                st.error(f"Erro ao executar o módulo Cadastro: {e}")
        else:
            st.warning("⚠️ Módulo 'Cadastro' não carregado. Verifique os erros acima.")

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

    elif escolha == "Importação":
        if modulo_importacao:
            modulo_importacao.tela_importacao()
        else:
            st.warning("⚠️ Módulo 'Importação' (modulo_sistema_consulta_importacao.py) não encontrado.")

# Bloco para teste individual
if __name__ == "__main__":
    app_sistema_consulta()