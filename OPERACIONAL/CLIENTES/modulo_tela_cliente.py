import streamlit as st
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO ROBUSTA ---
# Garante que o Python enxergue as subpastas no diretório atual
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
if diretorio_atual not in sys.path:
    sys.path.append(diretorio_atual)

erros_importacao = []

# --- 2. IMPORTAÇÃO DOS MÓDULOS (APENAS OS VÁLIDOS) ---

# 1. Clientes
try:
    from OPERACIONAL.CLIENTES import modulo_cadastro_cliente
except ImportError:
    try:
        from OPERACIONAL.CLIENTES import modulo_cadastro_cliente
    except ImportError as e:
        modulo_cadastro_cliente = None
        erros_importacao.append(f"Cadastro Clientes: {e}")

# 2. Usuários
try:
    from OPERACIONAL.CLIENTES.USUÁRIOS import modulo_usuario
except ImportError:
    try:
        from USUÁRIOS import modulo_usuario
    except ImportError as e:
        modulo_usuario = None
        erros_importacao.append(f"Usuários: {e}")

# 3. Parâmetros
try:
    from OPERACIONAL.CLIENTES import modulo_parametros
except ImportError:
    try:
        from OPERACIONAL.CLIENTES import modulo_parametros
    except ImportError as e:
        modulo_parametros = None

# 4. Permissões (Regras)
try:
    from OPERACIONAL.CLIENTES import modulo_permissoes
except ImportError:
    try:
        from OPERACIONAL.CLIENTES import modulo_permissoes
    except ImportError as e:
        modulo_permissoes = None

# 5. Financeiro
try:
    from OPERACIONAL.CLIENTES import modulo_financeiro
except ImportError:
    try:
        from FINANCEIRO import modulo_financeiro
    except ImportError as e:
        modulo_financeiro = None

# NOTA: O módulo "Gestão Tabelas" foi removido pois era o código antigo.

# --- 3. FUNÇÃO PRINCIPAL DA TELA ---
def app_clientes():
    st.markdown("## 👥 Central de Clientes e Usuários")

    # Exibe erros técnicos apenas se houver falha crítica
    if erros_importacao:
        with st.expander("⚠️ Detalhes de Erros de Carregamento", expanded=False):
            for erro in erros_importacao:
                st.error(erro)

    # --- DEFINIÇÃO DAS 5 ABAS (SEM O MÓDULO ANTIGO) ---
    tab_cli, tab_user, tab_param, tab_regras, tab_financeiro = st.tabs([
        "🏢 Clientes ", 
        "👤 Usuários", 
        "⚙️ Parâmetros", 
        "🛡️ Regras (Vis)", 
        "💰 Financeiro"
    ])

    # --- ABA 1: CLIENTES ---
    with tab_cli:
        if modulo_cadastro_cliente:
            try:
                modulo_cadastro_cliente.app_cadastro_cliente()
            except Exception as e:
                st.error(f"Erro ao executar app_cadastro_cliente: {e}")
        else:
            st.warning("Módulo 'Cadastro de Clientes' não carregado.")

    # --- ABA 2: USUÁRIOS ---
    with tab_user:
        if modulo_usuario:
            try:
                modulo_usuario.app_usuario()
            except Exception as e:
                st.error(f"Erro ao executar app_usuario: {e}")
        else:
            st.warning("Módulo 'Usuários' não disponível.")

    # --- ABA 3: PARÂMETROS ---
    with tab_param:
        if modulo_parametros:
            try:
                modulo_parametros.app_parametros()
            except Exception as e:
                st.error(f"Erro em Parâmetros: {e}")
        else:
            st.info("Módulo de Parâmetros não carregado.")

    # --- ABA 4: REGRAS / PERMISSÕES ---
    with tab_regras:
        if modulo_permissoes:
            try:
                modulo_permissoes.app_permissoes()
            except Exception as e:
                st.error(f"Erro em Permissões: {e}")
        else:
            st.info("Módulo de Permissões não carregado.")

    # --- ABA 5: FINANCEIRO ---
    with tab_financeiro:
        if modulo_financeiro:
            try:
                modulo_financeiro.app_financeiro()
            except Exception as e:
                st.error(f"Erro em Financeiro: {e}")
        else:
            st.info("Módulo Financeiro não carregado.")

if __name__ == "__main__":
    app_clientes()