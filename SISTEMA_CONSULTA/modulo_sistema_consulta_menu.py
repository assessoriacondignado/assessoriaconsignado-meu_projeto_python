import streamlit as st
import os
import sys
import importlib

# --- CONFIGURAÇÃO DE CAMINHOS ---
# Garante que o Python encontre os arquivos na pasta atual
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

def carregar_modulo(nome_modulo):
    """
    Função auxiliar para importar módulos apenas quando necessário (Lazy Import).
    Isso evita o erro de 'Circular Import'.
    """
    try:
        if nome_modulo in sys.modules:
            # Se já foi importado, recarrega para pegar alterações recentes
            return importlib.reload(sys.modules[nome_modulo])
        else:
            # Se não, importa pela primeira vez
            return importlib.import_module(nome_modulo)
    except ImportError as e:
        st.error(f"🔴 Erro ao carregar '{nome_modulo}': {e}")
        return None
    except Exception as e:
        st.error(f"🔴 Erro crítico em '{nome_modulo}': {e}")
        return None

def app_sistema_consulta():
    st.markdown("## 👥 CRM CONSULTA")

    # --- MENU SUPERIOR ---
    menu_opcoes = ["Cadastro / Pesquisa", "Planilhas (Tabelas)", "CRM / Gestão", "Importação"]
    
    # Gerencia o estado da navegação
    if 'nav_sistema_consulta' not in st.session_state:
        st.session_state['nav_sistema_consulta'] = menu_opcoes[0]

    escolha = st.radio(
        "Navegação", 
        menu_opcoes, 
        horizontal=True, 
        label_visibility="collapsed",
        key="nav_sistema_consulta_radio"
    )

    st.divider()

    # --- ROTEAMENTO DE TELAS (Com Importação Tardia) ---
    
    if escolha == "Cadastro / Pesquisa":
        # Só importa agora, evitando o ciclo no início do programa
        mod = carregar_modulo("modulo_sistema_consulta_cadastro")
        if mod and hasattr(mod, 'app_cadastro'):
            mod.app_cadastro()
        else:
            st.warning("⚠️ Módulo 'Cadastro' não disponível ou função 'app_cadastro' não encontrada.")

    elif escolha == "Planilhas (Tabelas)":
        mod = carregar_modulo("modulo_sistema_consulta_planilhas")
        if mod and hasattr(mod, 'app_planilhas'):
            mod.app_planilhas()
        else:
            st.warning("⚠️ Módulo 'Planilhas' não disponível.")
            
    elif escolha == "CRM / Gestão":
        mod = carregar_modulo("modulo_sistema_consulta_crm")
        if mod and hasattr(mod, 'app_crm'):
            mod.app_crm()
        else:
            st.warning("⚠️ Módulo 'CRM' não disponível.")

    elif escolha == "Importação":
        mod = carregar_modulo("modulo_sistema_consulta_importacao")
        if mod and hasattr(mod, 'tela_importacao'):
            mod.tela_importacao()
        else:
            st.warning("⚠️ Módulo 'Importação' não disponível.")

# Bloco para teste isolado deste arquivo
if __name__ == "__main__":
    app_sistema_consulta()