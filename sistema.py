import streamlit as st
from streamlit_option_menu import option_menu
import os
import sys
import importlib.util

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Sistema Assessoria",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- DEFINIÇÃO DE DIRETÓRIOS E PATHS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Lista de diretórios que contêm módulos (incluindo aqueles com espaços ou hífens)
# Adicionar ao sys.path permite importar os arquivos diretamente, ignorando nomes de pastas inválidos
paths_to_add = [
    BASE_DIR,
    os.path.join(BASE_DIR, "OPERACIONAL"),
    os.path.join(BASE_DIR, "OPERACIONAL", "CLIENTES"),
    os.path.join(BASE_DIR, "OPERACIONAL", "MODULO_W-API"), # Resolve erro do hífen
    os.path.join(BASE_DIR, "CONEXÕES"),                     # Resolve erro do acento
    os.path.join(BASE_DIR, "COMERCIAL"),
    os.path.join(BASE_DIR, "COMERCIAL", "PEDIDOS"),
    os.path.join(BASE_DIR, "COMERCIAL", "PRODUTOS E SERVICOS"),
    os.path.join(BASE_DIR, "COMERCIAL", "TAREFAS"),
    os.path.join(BASE_DIR, "COMERCIAL", "RENOVACAO E FEEDBACK"), # Resolve erro do espaço
    os.path.join(BASE_DIR, "OPERACIONAL", "BANCO DE PLANILHAS")
]

for path in paths_to_add:
    if path not in sys.path:
        sys.path.append(path)

# --- IMPORTAÇÃO ROBUSTA DOS MÓDULOS ---

# 1. HOME (site.py) - Carregamento via Spec para evitar conflito com módulo 'site' do Python
try:
    spec_home = importlib.util.spec_from_file_location("modulo_home", os.path.join(BASE_DIR, "site.py"))
    modulo_home = importlib.util.module_from_spec(spec_home)
    spec_home.loader.exec_module(modulo_home)
except Exception as e:
    modulo_home = None
    print(f"Erro ao carregar Home: {e}")

# 2. CLIENTES
try:
    import modulo_tela_cliente as modulo_clientes
except ImportError:
    modulo_clientes = None

# 3. MÓDULOS COMERCIAIS
try:
    import modulo_produtos
except ImportError:
    modulo_produtos = None

try:
    import modulo_pedidos
except ImportError:
    modulo_pedidos = None

try:
    import modulo_tarefas
except ImportError:
    modulo_tarefas = None

try:
    # Importa direto pois a pasta "RENOVACAO E FEEDBACK" já está no path
    import modulo_renovacao_feedback
except ImportError:
    modulo_renovacao_feedback = None

# 4. WHATSAPP (W-API)
try:
    # Importa direto pois "MODULO_W-API" já está no path
    import modulo_wapi
except ImportError:
    modulo_wapi = None

# 5. CONEXÕES
try:
    # Importa direto pois "CONEXÕES" já está no path
    import modulo_conexoes
except ImportError:
    modulo_conexoes = None

# 6. BANCO DE DADOS (Planilhas)
try:
    import modulo_planilhas
except ImportError:
    modulo_planilhas = None


# --- FUNÇÃO PRINCIPAL ---
def main():
    # --- CSS PERSONALIZADO ---
    st.markdown("""
        <style>
            [data-testid="stSidebarNav"] {display: none;}
            .main .block-container {padding-top: 2rem;}
        </style>
    """, unsafe_allow_html=True)

    # --- MENU LATERAL ---
    with st.sidebar:
        logo_path = os.path.join(BASE_DIR, "OPERACIONAL", "MODULO_TELA_PRINCIPAL", "logo_assessoria.png")
        if os.path.exists(logo_path):
            st.image(logo_path, use_column_width=True)
        else:
            st.markdown("### 📊 Assessoria")

        selected = option_menu(
            menu_title="Menu Principal",
            options=[
                "Home", 
                "Clientes", 
                "Produtos", 
                "Pedidos", 
                "Tarefas", 
                "Renovação", 
                "Banco de Dados", 
                "WhatsApp", 
                "Conexões"
            ],
            icons=[
                "house",           
                "people",          
                "box-seam",        
                "cart",            
                "list-task",       
                "arrow-repeat",    
                "database",        
                "whatsapp",        
                "hdd-network"      
            ],
            menu_icon="cast",
            default_index=0,
            styles={
                "container": {"padding": "0!important", "background-color": "#f0f2f6"},
                "icon": {"color": "orange", "font-size": "18px"}, 
                "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
                "nav-link-selected": {"background-color": "#ff4b4b"},
            }
        )

    # --- ROTEAMENTO DAS PÁGINAS ---
    
    if selected == "Home":
        if modulo_home:
            try:
                modulo_home.app_home() 
            except AttributeError:
                # Tenta nome alternativo caso a função tenha mudado
                if hasattr(modulo_home, 'app'): modulo_home.app()
                else: st.error("Função principal não encontrada no módulo Home (site.py).")
            except Exception as e:
                st.error(f"Erro na execução da Home: {e}")
        else:
            st.error("Erro fatal: Módulo 'site.py' não pôde ser carregado.")

    elif selected == "Clientes":
        if modulo_clientes:
            try:
                modulo_clientes.app_tela_cliente()
            except Exception as e:
                st.error(f"Erro ao abrir Clientes: {e}")
        else:
            st.error("Módulo Clientes não encontrado em 'OPERACIONAL/CLIENTES'.")

    elif selected == "Produtos":
        if modulo_produtos:
            try:
                modulo_produtos.app_produtos()
            except Exception as e:
                st.error(f"Erro em Produtos: {e}")
        else:
            st.warning("Módulo Produtos não carregado.")

    elif selected == "Pedidos":
        if modulo_pedidos:
            try:
                modulo_pedidos.app_pedidos()
            except Exception as e:
                st.error(f"Erro em Pedidos: {e}")
        else:
            st.warning("Módulo Pedidos não carregado.")

    elif selected == "Tarefas":
        if modulo_tarefas:
            try:
                modulo_tarefas.app_tarefas()
            except Exception as e:
                st.error(f"Erro em Tarefas: {e}")
        else:
            st.warning("Módulo Tarefas não carregado.")

    elif selected == "Renovação":
        if modulo_renovacao_feedback:
            try:
                # Verifica nomes comuns de função principal
                if hasattr(modulo_renovacao_feedback, 'app_renovacao'):
                    modulo_renovacao_feedback.app_renovacao()
                elif hasattr(modulo_renovacao_feedback, 'app_main'):
                    modulo_renovacao_feedback.app_main()
                elif hasattr(modulo_renovacao_feedback, 'app'):
                    modulo_renovacao_feedback.app()
                else:
                    st.info("Módulo carregado, mas função principal 'app_renovacao' não encontrada.")
            except Exception as e:
                st.error(f"Erro em Renovação: {e}")
        else:
            st.error("Módulo Renovação não encontrado (verifique a pasta 'RENOVACAO E FEEDBACK').")

    elif selected == "Banco de Dados":
        st.title("🗄️ Banco de Dados")
        if modulo_planilhas:
            try:
                modulo_planilhas.app_banco_planilhas()
            except Exception as e:
                st.error(f"Erro interno no módulo de planilhas: {e}")
        else:
            st.warning("Módulo de Banco de Planilhas não localizado.")

    elif selected == "WhatsApp":
        st.title("💬 WhatsApp (W-API)")
        if modulo_wapi:
            # Verifica se existe uma interface visual, senão mostra status
            if hasattr(modulo_wapi, 'app_wapi'):
                modulo_wapi.app_wapi()
            elif hasattr(modulo_wapi, 'dashboard'):
                modulo_wapi.dashboard()
            else:
                st.success("✅ Conexão com Módulo W-API estabelecida.")
                st.info("Este módulo parece ser apenas de backend (API).")
        else:
            st.error("Falha ao carregar módulo W-API. Verifique a pasta 'OPERACIONAL/MODULO_W-API'.")

    elif selected == "Conexões":
        if modulo_conexoes:
            try:
                modulo_conexoes.app_conexoes()
            except Exception as e:
                st.error(f"Erro ao abrir Conexões: {e}")
        else:
            st.error("Módulo Conexões não encontrado (verifique a pasta 'CONEXÕES').")

if __name__ == "__main__":
    main()