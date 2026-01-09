import streamlit as st
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO ---
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
diretorio_pai = os.path.dirname(diretorio_atual) 
raiz_projeto = os.path.dirname(diretorio_pai) 

if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)

erros_importacao = []

# --- 2. IMPORTAÇÃO DOS MÓDULOS ---

# 1. Produtos
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

# 4. Renovação
try:
    from COMERCIAL.RENOVACAO_E_FEEDBACK import modulo_renovacao_feedback
except ImportError as e:
    modulo_renovacao_feedback = None
    erros_importacao.append(f"Renovação: {e}")

# 5. Configurações (NOVO)
try:
    from COMERCIAL import modulo_comercial_configuracoes
except ImportError as e:
    modulo_comercial_configuracoes = None
    erros_importacao.append(f"Configurações: {e}")


# --- 3. FUNÇÃO PRINCIPAL DA TELA ---
def app_comercial_geral():
    st.markdown("## 🏢 Gestão Comercial Integrada")

    if erros_importacao:
        with st.expander("⚠️ Diagnóstico de Sistema", expanded=False):
            st.warning("Alguns módulos não foram carregados corretamente:")
            for erro in erros_importacao:
                st.error(erro)

    # --- DEFINIÇÃO DAS ABAS ---
    # Adicionada a aba Configurações no final
    tab_prod, tab_ped, tab_tar, tab_renov, tab_conf = st.tabs([
        "📦 Produtos", 
        "🛒 Pedidos", 
        "✅ Tarefas", 
        "🔄 Renovação",
        "⚙️ Configurações"
    ])

    with tab_prod:
        if modulo_produtos and hasattr(modulo_produtos, 'app_produtos'):
            modulo_produtos.app_produtos()
        else: st.info("Módulo de Produtos indisponível.")

    with tab_ped:
        if modulo_pedidos and hasattr(modulo_pedidos, 'app_pedidos'):
            modulo_pedidos.app_pedidos()
        else: st.info("Módulo de Pedidos indisponível.")

    with tab_tar:
        if modulo_tarefas and hasattr(modulo_tarefas, 'app_tarefas'):
            modulo_tarefas.app_tarefas()
        else: st.info("Módulo de Tarefas indisponível.")

    with tab_renov:
        if modulo_renovacao_feedback and hasattr(modulo_renovacao_feedback, 'app_renovacao_feedback'):
            modulo_renovacao_feedback.app_renovacao_feedback()
        else: st.info("Módulo de Renovação indisponível.")

    # --- ABA 5: CONFIGURAÇÕES ---
    with tab_conf:
        if modulo_comercial_configuracoes and hasattr(modulo_comercial_configuracoes, 'app_configuracoes'):
            modulo_comercial_configuracoes.app_configuracoes()
        else: st.info("Módulo de Configurações indisponível.")

if __name__ == "__main__":
    app_comercial_geral()