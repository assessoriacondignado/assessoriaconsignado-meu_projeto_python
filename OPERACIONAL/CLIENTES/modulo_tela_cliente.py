import streamlit as st
import time

try:
    # AJUSTE AQUI: Adicionado "S" em CLIENTES para bater com o nome da pasta real
    from OPERACIONAL.CLIENTES.CLIENTES import modulo_cadastro_cliente
    from OPERACIONAL.CLIENTES.USUÁRIOS import modulo_usuario
    from OPERACIONAL.CLIENTES.PARAMETROS import modulo_parametros
    from OPERACIONAL.CLIENTES.PERMISSÕES import modulo_permissoes
    from OPERACIONAL.CLIENTES.FINANCEIRO import modulo_financeiro
    from OPERACIONAL.CLIENTES.GESTAOTABELAS import modulo_gestao_tabelas
except ImportError as e:
    # Se der erro, ele avisa aqui
    print(f"Alerta de Importação: {e}")

def app_clientes():
    st.markdown("## 👥 Central de Clientes e Usuários")
    
    # Definição das Abas
    tab_cli, tab_user, tab_param, tab_regras, tab_financeiro, tab_plan = st.tabs([
        "🏢 Clientes", 
        "👤 Usuários", 
        "⚙️ Parâmetros", 
        "🛡️ Regras (Vis)", 
        "💰 Financeiro", 
        "📅 Gestão Tabelas"
    ])

    # --- ABA 1: CLIENTES ---
    with tab_cli:
        try:
            if 'modulo_cadastro_cliente' in locals():
                modulo_cadastro_cliente.app_cadastro_cliente()
            else:
                st.warning("Módulo 'Cadastro Cliente' não foi importado corretamente.")
        except Exception as e:
            st.error(f"Erro no módulo Clientes: {e}")

    # --- ABA 2: USUÁRIOS ---
    with tab_user:
        try:
            if 'modulo_usuario' in locals():
                modulo_usuario.app_usuario()
            else:
                st.warning("Módulo 'Usuário' não foi importado corretamente.")
        except Exception as e:
            st.error(f"Erro no módulo Usuários: {e}")

    # --- ABA 3: PARÂMETROS ---
    with tab_param:
        try:
            if 'modulo_parametros' in locals():
                modulo_parametros.app_parametros()
            else:
                st.warning("Módulo 'Parâmetros' não foi importado corretamente.")
        except Exception as e:
            st.error(f"Erro no módulo Parâmetros: {e}")

    # --- ABA 4: REGRAS / PERMISSÕES ---
    with tab_regras:
        try:
            if 'modulo_permissoes' in locals():
                modulo_permissoes.app_permissoes()
            else:
                st.warning("Módulo 'Permissões' não foi importado corretamente.")
        except Exception as e:
            st.error(f"Erro no módulo Permissões: {e}")

    # --- ABA 5: FINANCEIRO ---
    with tab_financeiro:
        try:
            if 'modulo_financeiro' in locals():
                modulo_financeiro.app_financeiro()
            else:
                st.warning("Módulo 'Financeiro' não foi importado corretamente.")
        except Exception as e:
            st.error(f"Erro no módulo Financeiro: {e}")

    # --- ABA 6: GESTÃO DE TABELAS ---
    with tab_plan:
        try:
            if 'modulo_gestao_tabelas' in locals():
                modulo_gestao_tabelas.app_gestao_tabelas()
            else:
                st.warning("Módulo 'Gestão Tabelas' não foi importado corretamente.")
        except Exception as e:
            st.error(f"Erro no módulo Gestão Tabelas: {e}")

if __name__ == "__main__":
    app_clientes()