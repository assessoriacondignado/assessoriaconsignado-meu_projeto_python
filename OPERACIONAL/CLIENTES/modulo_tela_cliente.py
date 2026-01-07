import streamlit as st
import time

# Tenta importar os submódulos. 
# Nota: Você precisará ajustar os caminhos de importação dependendo de como 
# o Python reconhece a pasta raiz do seu projeto.
# Se todos estiverem acessíveis como pacotes, os imports abaixo funcionarão.
# Caso contrário, pode ser necessário ajustar sys.path ou usar imports relativos.

try:
    # Ajuste os imports conforme a estrutura de pastas exata do seu projeto
    # Exemplo: from assessoriacondignado.OPERACIONAL.CLIENTE.CLIENTE import modulo_cadastro_cliente
    # Para facilitar, estou usando imports assumindo que a pasta raiz está no path.
    
    from OPERACIONAL.CLIENTE.CLIENTES import modulo_cadastro_cliente
    from OPERACIONAL.CLIENTE.USUÁRIOS import modulo_usuario
    from OPERACIONAL.CLIENTE.PARAMETROS import modulo_parametros
    from OPERACIONAL.CLIENTE.PERMISSÕES import modulo_permissoes
    from OPERACIONAL.CLIENTE.FINANCEIRO import modulo_financeiro
    from OPERACIONAL.CLIENTE.GESTAOTABELAS import modulo_gestao_tabelas
except ImportError as e:
    # Isso serve apenas para não quebrar o código enquanto você ainda não criou os outros arquivos
    print(f"Alerta de Importação (normal durante a refatoração): {e}")

def app_clientes():
    st.markdown("## 👥 Central de Clientes e Usuários")
    
    # Definição das Abas Principais
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
            modulo_cadastro_cliente.app_cadastro_cliente()
        except NameError:
            st.warning("Módulo 'modulo_cadastro_cliente' ainda não carregado ou não encontrado.")
        except Exception as e:
            st.error(f"Erro no módulo Clientes: {e}")

    # --- ABA 2: USUÁRIOS ---
    with tab_user:
        try:
            modulo_usuario.app_usuario()
        except NameError:
            st.warning("Módulo 'modulo_usuario' ainda não carregado.")
        except Exception as e:
            st.error(f"Erro no módulo Usuários: {e}")

    # --- ABA 3: PARÂMETROS ---
    with tab_param:
        try:
            modulo_parametros.app_parametros()
        except NameError:
            st.warning("Módulo 'modulo_parametros' ainda não carregado.")
        except Exception as e:
            st.error(f"Erro no módulo Parâmetros: {e}")

    # --- ABA 4: REGRAS / PERMISSÕES ---
    with tab_regras:
        try:
            modulo_permissoes.app_permissoes()
        except NameError:
            st.warning("Módulo 'modulo_permissoes' ainda não carregado.")
        except Exception as e:
            st.error(f"Erro no módulo Permissões: {e}")

    # --- ABA 5: FINANCEIRO (Carteira + Relatórios) ---
    with tab_financeiro:
        try:
            modulo_financeiro.app_financeiro()
        except NameError:
            st.warning("Módulo 'modulo_financeiro' ainda não carregado.")
        except Exception as e:
            st.error(f"Erro no módulo Financeiro: {e}")

    # --- ABA 6: GESTÃO DE TABELAS (PLANILHAS) ---
    with tab_plan:
        try:
            modulo_gestao_tabelas.app_gestao_tabelas()
        except NameError:
            st.warning("Módulo 'modulo_gestao_tabelas' ainda não carregado.")
        except Exception as e:
            st.error(f"Erro no módulo Gestão Tabelas: {e}")

if __name__ == "__main__":
    app_clientes()