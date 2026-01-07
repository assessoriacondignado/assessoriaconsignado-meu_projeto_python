import streamlit as st
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO ROBUSTA ---
# Isso garante que o Python encontre as pastas que estão junto com este arquivo
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
if diretorio_atual not in sys.path:
    sys.path.append(diretorio_atual)

# Tenta importar os módulos. Se falhar, mostra o erro na tela para facilitar o diagnóstico.
erros_importacao = []

try:
    # Tenta importar via caminho completo (Recomendado)
    from OPERACIONAL.CLIENTES.CLIENTES import modulo_cadastro_cliente
except ImportError:
    try:
        # Tenta importar diretamente da subpasta (Fallback)
        from CLIENTES import modulo_cadastro_cliente
    except ImportError as e:
        modulo_cadastro_cliente = None
        erros_importacao.append(f"Cadastro Clientes: {e}")

try:
    from OPERACIONAL.CLIENTES.USUÁRIOS import modulo_usuario
except ImportError:
    try:
        from USUÁRIOS import modulo_usuario
    except ImportError as e:
        modulo_usuario = None
        erros_importacao.append(f"Usuários: {e}")

try:
    from OPERACIONAL.CLIENTES.PARAMETROS import modulo_parametros
except ImportError:
    try:
        from PARAMETROS import modulo_parametros
    except ImportError as e:
        modulo_parametros = None
        # Não adiciona erro crítico se for apenas parâmetros, mas avisa
        print(f"Aviso Parametros: {e}")

try:
    from OPERACIONAL.CLIENTES.PERMISSÕES import modulo_permissoes
except ImportError:
    try:
        from PERMISSÕES import modulo_permissoes
    except ImportError as e:
        modulo_permissoes = None
        print(f"Aviso Permissoes: {e}")

try:
    from OPERACIONAL.CLIENTES.FINANCEIRO import modulo_financeiro
except ImportError:
    try:
        from FINANCEIRO import modulo_financeiro
    except ImportError as e:
        modulo_financeiro = None
        print(f"Aviso Financeiro: {e}")

try:
    from OPERACIONAL.CLIENTES.GESTAOTABELAS import modulo_gestao_tabelas
except ImportError:
    try:
        from GESTAOTABELAS import modulo_gestao_tabelas
    except ImportError as e:
        modulo_gestao_tabelas = None
        print(f"Aviso Gestao Tabelas: {e}")


# --- 2. FUNÇÃO PRINCIPAL DA TELA ---
def app_clientes():
    st.markdown("## 👥 Central de Clientes e Usuários")

    # Se houver erros graves de importação, mostra no topo
    if erros_importacao:
        with st.expander("⚠️ Detalhes de Erros de Carregamento (Técnico)", expanded=False):
            for erro in erros_importacao:
                st.error(erro)
            st.info("Verifique se os arquivos __init__.py existem dentro de cada subpasta (CLIENTES, USUÁRIOS, etc).")

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
        if modulo_cadastro_cliente:
            try:
                modulo_cadastro_cliente.app_cadastro_cliente()
            except Exception as e:
                st.error(f"Erro ao executar app_cadastro_cliente: {e}")
        else:
            st.warning("Módulo 'Cadastro de Clientes' não foi carregado. Verifique os logs.")

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

    # --- ABA 6: GESTÃO DE TABELAS ---
    with tab_plan:
        if modulo_gestao_tabelas:
            try:
                modulo_gestao_tabelas.app_gestao_tabelas()
            except Exception as e:
                st.error(f"Erro em Gestão Tabelas: {e}")
        else:
            st.info("Módulo Gestão de Tabelas não carregado.")

if __name__ == "__main__":
    app_clientes()