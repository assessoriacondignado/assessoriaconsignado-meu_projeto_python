import streamlit as st

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA (Deve ser a primeira linha executável do Streamlit)
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Sistema Assessoria",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# IMPORTAÇÃO DOS MÓDULOS REFATORADOS
# -----------------------------------------------------------------------------
# Tenta importar o módulo principal (HUB) da estrutura OPERACIONAL/CLIENTE
try:
    from OPERACIONAL.CLIENTES import modulo_tela_cliente
except ImportError as e:
    st.error(f"Erro Crítico de Importação: {e}")
    st.info("Dica: Verifique se existem arquivos vazios chamados '__init__.py' dentro das pastas 'OPERACIONAL' e 'OPERACIONAL/CLIENTES'.")

# -----------------------------------------------------------------------------
# MENU LATERAL E NAVEGAÇÃO
# -----------------------------------------------------------------------------
def main():
    st.sidebar.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=100)
    st.sidebar.title("Navegação")
    
    # Opções do Menu
    opcoes_menu = ["🏠 Dashboard", "👥 Gestão Clientes", "⚙️ Configurações", "🚪 Sair"]
    escolha = st.sidebar.radio("Ir para:", opcoes_menu)

    st.sidebar.markdown("---")
    st.sidebar.caption("v2.0 - Refatorado")

    # 1. TELA INICIAL
    if escolha == "🏠 Dashboard":
        st.title("Bem-vindo ao Sistema")
        st.write("Utilize o menu lateral para acessar os módulos.")
        
        c1, c2, c3 = st.columns(3)
        with c1:
            st.info("Status do Banco: **Conectado**")
        with c2:
            st.info("Módulo Cliente: **Ativo**")

    # 2. MÓDULO CLIENTES (AQUI CHAMA A NOVA ESTRUTURA)
    elif escolha == "👥 Gestão Clientes":
        if 'modulo_tela_cliente' in locals():
            # Chama a função principal do módulo HUB que gerencia as Tabs (Cadastro, Financeiro, etc)
            modulo_tela_cliente.app_clientes()
        else:
            st.warning("O módulo de clientes não foi carregado corretamente.")

    # 3. CONFIGURAÇÕES (Placeholder)
    elif escolha == "⚙️ Configurações":
        st.header("Configurações do Sistema")
        st.write("Em desenvolvimento...")

    # 4. SAIR
    elif escolha == "🚪 Sair":
        st.session_state.clear()
        st.success("Sessão encerrada com segurança.")
        if st.button("Recarregar Página"):
            st.rerun()

if __name__ == "__main__":
    main()