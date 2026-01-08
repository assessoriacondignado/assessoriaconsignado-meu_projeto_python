import streamlit as st
from streamlit_option_menu import option_menu
import os
import sys

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Sistema Assessoria",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- ADICIONAR CAMINHOS AO SYSTEM PATH ---
# Isso garante que o Python encontre os módulos dentro das subpastas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "OPERACIONAL"))
sys.path.append(os.path.join(BASE_DIR, "OPERACIONAL", "CLIENTES"))
sys.path.append(os.path.join(BASE_DIR, "OPERACIONAL", "MODULO_W-API"))
sys.path.append(os.path.join(BASE_DIR, "CONEXÕES"))
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL"))
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL", "PEDIDOS"))
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL", "PRODUTOS E SERVICOS"))
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL", "TAREFAS"))
sys.path.append(os.path.join(BASE_DIR, "COMERCIAL", "RENOVACAO E FEEDBACK"))

# --- IMPORTAÇÃO DOS MÓDULOS ---
try:
    # Módulos Operacionais
    from OPERACIONAL.MODULO_TELA_PRINCIPAL import site as modulo_home
    from OPERACIONAL.CLIENTES import modulo_tela_cliente as modulo_clientes
    from OPERACIONAL.MODULO_CHAT import modulo_chat
    from CONEXÕES import modulo_conexoes
    from OPERACIONAL.BANCO_DE_PLANILHAS import modulo_planilhas  # Ajuste conforme nome real da pasta se necessário
except ImportError as e:
    # Fallback para imports diretos ou tratamento de erro silencioso para carregamento parcial
    pass

# Módulos Comerciais (Novos)
try:
    import modulo_produtos
except ImportError:
    try: from COMERCIAL.PRODUTOS_E_SERVICOS import modulo_produtos 
    except: modulo_produtos = None

try:
    import modulo_pedidos
except ImportError:
    try: from COMERCIAL.PEDIDOS import modulo_pedidos
    except: modulo_pedidos = None

try:
    import modulo_tarefas
except ImportError:
    try: from COMERCIAL.TAREFAS import modulo_tarefas
    except: modulo_tarefas = None

try:
    import modulo_renovacao_feedback
except ImportError:
    try: from COMERCIAL.RENOVACAO_E_FEEDBACK import modulo_renovacao_feedback
    except: modulo_renovacao_feedback = None

# Módulos de Infraestrutura
try:
    from OPERACIONAL.MODULO_W_API import modulo_wapi
except:
    modulo_wapi = None

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
        # Logo (opcional, se existir)
        logo_path = os.path.join(BASE_DIR, "OPERACIONAL", "MODULO_TELA_PRINCIPAL", "logo_assessoria.png")
        if os.path.exists(logo_path):
            st.image(logo_path, use_column_width=True)
        else:
            st.markdown("### 📊 Assessoria")

        # Definição do Menu
        # 1. Home
        # 2. Clientes
        # 3. Produtos (Novo)
        # 4. Pedidos (Novo)
        # 5. Tarefas (Novo)
        # 6. Renovação (Novo)
        # 7. Banco de Dados
        # 8. WhatsApp
        # 9. Conexões
        
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
                "house",           # Home
                "people",          # Clientes
                "box-seam",        # Produtos
                "cart",            # Pedidos
                "list-task",       # Tarefas
                "arrow-repeat",    # Renovação
                "database",        # Banco de Dados
                "whatsapp",        # WhatsApp
                "hdd-network"      # Conexões
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
        try:
            modulo_home.app_home() 
        except Exception as e:
            st.error(f"Erro ao carregar Home: {e}")
            st.info("Verifique se o módulo 'site.py' ou 'modulo_home' está correto.")

    elif selected == "Clientes":
        try:
            modulo_clientes.app_tela_cliente()
        except Exception as e:
            st.error(f"Erro ao carregar Clientes: {e}")

    elif selected == "Produtos":
        if modulo_produtos:
            try:
                modulo_produtos.app_produtos()
            except AttributeError:
                st.error("A função 'app_produtos' não foi encontrada no módulo.")
            except Exception as e:
                st.error(f"Erro no módulo Produtos: {e}")
        else:
            st.warning("Módulo de Produtos não encontrado.")

    elif selected == "Pedidos":
        if modulo_pedidos:
            try:
                modulo_pedidos.app_pedidos()
            except AttributeError:
                st.error("A função 'app_pedidos' não foi encontrada no módulo.")
            except Exception as e:
                st.error(f"Erro no módulo Pedidos: {e}")
        else:
            st.warning("Módulo de Pedidos não encontrado.")

    elif selected == "Tarefas":
        if modulo_tarefas:
            try:
                modulo_tarefas.app_tarefas()
            except AttributeError:
                st.error("A função 'app_tarefas' não foi encontrada no módulo.")
            except Exception as e:
                st.error(f"Erro no módulo Tarefas: {e}")
        else:
            st.warning("Módulo de Tarefas não encontrado.")

    elif selected == "Renovação":
        if modulo_renovacao_feedback:
            try:
                # Tenta chamar a função principal. Ajuste o nome se for diferente no arquivo.
                if hasattr(modulo_renovacao_feedback, 'app_renovacao'):
                    modulo_renovacao_feedback.app_renovacao()
                elif hasattr(modulo_renovacao_feedback, 'app_main'):
                    modulo_renovacao_feedback.app_main()
                else:
                    # Fallback genérico ou aviso
                    st.info("Módulo carregado, mas a função principal 'app_renovacao' não foi localizada.")
            except Exception as e:
                st.error(f"Erro no módulo Renovação: {e}")
        else:
            st.warning("Módulo de Renovação não encontrado.")

    elif selected == "Banco de Dados":
        # Assumindo que este módulo existia ou era uma view direta
        # Se não houver módulo específico importado acima, mantemos um placeholder ou a lógica anterior
        st.title("🗄️ Banco de Dados")
        st.info("Gestão de Planilhas e Importações (Módulo Operacional)")
        try:
            # Tenta importar dinamicamente se não estiver no topo
            from OPERACIONAL.BANCO_DE_PLANILHAS import modulo_planilhas
            modulo_planilhas.app_banco_planilhas()
        except:
            st.warning("Módulo de Banco de Dados/Planilhas em manutenção ou não localizado.")

    elif selected == "WhatsApp":
        st.title("💬 Gestão WhatsApp (W-API)")
        if modulo_wapi:
            # Se o módulo W-API tiver uma interface visual, chame-a aqui.
            # Caso contrário, exibe status.
            st.success("Módulo W-API carregado.")
            # Exemplo de chamada se existir uma função visual:
            # modulo_wapi.dashboard_wapi()
        else:
            st.error("Módulo WhatsApp não carregado.")

    elif selected == "Conexões":
        try:
            modulo_conexoes.app_conexoes()
        except Exception as e:
            st.error(f"Erro ao carregar Conexões: {e}")

if __name__ == "__main__":
    main()