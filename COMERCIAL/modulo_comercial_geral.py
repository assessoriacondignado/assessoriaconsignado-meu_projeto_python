import streamlit as st
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO ---
# Garante que a raiz do projeto esteja no path para importar conexao e outros utils
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
diretorio_pai = os.path.dirname(diretorio_atual) # Sobe para OPERACIONAL
raiz_projeto = os.path.dirname(diretorio_pai) # Sobe para Raiz do Projeto

if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)

erros_importacao = []

# --- 2. IMPORTAÇÃO DOS MÓDULOS ---

# 1. Produtos e Serviços
try:
    from COMERCIAL.PRODUTOS_E_SERVICOS import modulo_produtos
except ImportError as e:
    modulo_produtos = None
    erros_importacao.append(f"Produtos: {e}")

# 2. Pedidos
try:
    from COMERCIAL.PEDIDOS import modulo_pedidos
except ImportError as e:
    modulo_pedidos = None
    erros_importacao.append(f"Pedidos: {e}")

# 3. Tarefas
try:
    from COMERCIAL.TAREFAS import modulo_tarefas
except ImportError as e:
    modulo_tarefas = None
    erros_importacao.append(f"Tarefas: {e}")

# 4. Renovação e Feedback
try:
    from COMERCIAL.RENOVACAO_E_FEEDBACK import modulo_renovacao_feedback
except ImportError as e:
    modulo_renovacao_feedback = None
    erros_importacao.append(f"Renovação: {e}")


# --- 3. FUNÇÃO PRINCIPAL DA TELA ---
def app_comercial_geral():
    st.markdown("## 🏢 Gestão Comercial Integrada - TESTE")

    # Diagnóstico técnico (aparece apenas se houver erro crítico de importação)
    if erros_importacao:
        with st.expander("⚠️ Diagnóstico de Sistema", expanded=False):
            st.warning("Alguns módulos não foram carregados corretamente:")
            for erro in erros_importacao:
                st.error(erro)

    # --- DEFINIÇÃO DAS ABAS ---
    # Cria as abas na ordem solicitada
    tab_prod, tab_ped, tab_tar, tab_renov = st.tabs([
        "📦 Produtos", 
        "🛒 Pedidos", 
        "✅ Tarefas", 
        "🔄 Renovação"
    ])

    # --- ABA 1: PRODUTOS ---
    with tab_prod:
        if modulo_produtos and hasattr(modulo_produtos, 'app_produtos'):
            try:
                modulo_produtos.app_produtos()
            except Exception as e:
                st.error(f"Erro ao executar Produtos: {e}")
        else:
            st.info("Módulo de Produtos indisponível no momento.")

    # --- ABA 2: PEDIDOS ---
    with tab_ped:
        if modulo_pedidos and hasattr(modulo_pedidos, 'app_pedidos'):
            try:
                modulo_pedidos.app_pedidos()
            except Exception as e:
                st.error(f"Erro ao executar Pedidos: {e}")
        else:
            st.info("Módulo de Pedidos indisponível no momento.")

    # --- ABA 3: TAREFAS ---
    with tab_tar:
        if modulo_tarefas and hasattr(modulo_tarefas, 'app_tarefas'):
            try:
                modulo_tarefas.app_tarefas()
            except Exception as e:
                st.error(f"Erro ao executar Tarefas: {e}")
        else:
            st.info("Módulo de Tarefas indisponível no momento.")

    # --- ABA 4: RENOVAÇÃO ---
    with tab_renov:
        if modulo_renovacao_feedback and hasattr(modulo_renovacao_feedback, 'app_renovacao_feedback'):
            try:
                modulo_renovacao_feedback.app_renovacao_feedback()
            except Exception as e:
                st.error(f"Erro ao executar Renovação: {e}")
        else:
            st.info("Módulo de Renovação indisponível no momento.")

if __name__ == "__main__":
    app_comercial_geral()