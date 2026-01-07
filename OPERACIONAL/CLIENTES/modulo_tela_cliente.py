import streamlit as st
import sys
import os

# --- AJUSTE DE IMPORTAÇÃO ROBUSTO ---
# Tenta importar os sub-módulos considerando variações de nomes de pasta (Singular/Plural/Acentos)
try:
    # 1. Tenta Caminho Padrão (Plural/Com Acentos conforme sua estrutura)
    from OPERACIONAL.CLIENTES.CLIENTES import modulo_cadastro_cliente
    from OPERACIONAL.CLIENTES.USUÁRIOS import modulo_usuario
    from OPERACIONAL.CLIENTES.FINANCEIRO import modulo_financeiro
    from OPERACIONAL.CLIENTES.PERMISSÕES import modulo_permissoes
    from OPERACIONAL.CLIENTES.GESTAOTABELAS import modulo_gestao_tabelas
    from OPERACIONAL.CLIENTES.PARAMETROS import modulo_parametros
    
except ImportError as e_original:
    # 2. Fallback: Tenta Caminho Singular/Sem Acentos (Caso tenha renomeado)
    try:
        # Adicione outros paths se necessário
        from OPERACIONAL.CLIENTE.CLIENTE import modulo_cadastro_cliente
        from OPERACIONAL.CLIENTE.USUARIO import modulo_usuario
        from OPERACIONAL.CLIENTE.FINANCEIRO import modulo_financeiro
        from OPERACIONAL.CLIENTE.PERMISSOES import modulo_permissoes
        from OPERACIONAL.CLIENTE.GESTAOTABELAS import modulo_gestao_tabelas
        from OPERACIONAL.CLIENTE.PARAMETROS import modulo_parametros
    except ImportError as e_secundario:
        # Se falhar tudo, define como None para não quebrar a tela inteira
        print(f"Erro de Importação nos Sub-módulos: {e_original} | {e_secundario}")
        modulo_cadastro_cliente = None
        modulo_usuario = None
        modulo_financeiro = None
        modulo_permissoes = None
        modulo_gestao_tabelas = None
        modulo_parametros = None

def app_clientes():
    st.markdown("## 👥 Central de Clientes e Usuários")
    
    # --- CRIAÇÃO DAS ABAS (MENU SUPERIOR) ---
    tabs = st.tabs([
        "🏢 Clientes", 
        "👤 Usuários", 
        "⚙️ Parâmetros", 
        "🛡️ Permissões", 
        "💰 Financeiro", 
        "📅 Tabelas/SQL"
    ])
    
    tab_cli, tab_user, tab_param, tab_regras, tab_fin, tab_sql = tabs

    # --- ABA 1: CLIENTES ---
    with tab_cli:
        if modulo_cadastro_cliente:
            modulo_cadastro_cliente.app_cadastro_cliente()
        else:
            st.error("Erro: Módulo 'modulo_cadastro_cliente' não encontrado.")
            st.info("Verifique se a pasta 'CLIENTES' existe dentro de 'OPERACIONAL/CLIENTES' e possui o arquivo '__init__.py'.")

    # --- ABA 2: USUÁRIOS ---
    with tab_user:
        if modulo_usuario:
            # Tenta chamar a função principal (pode ser app_usuario ou app_usuarios dependendo da versão)
            if hasattr(modulo_usuario, 'app_usuario'):
                modulo_usuario.app_usuario()
            elif hasattr(modulo_usuario, 'app_usuarios'):
                modulo_usuario.app_usuarios()
            else:
                st.warning("Função principal não encontrada no módulo usuário.")
        else:
            st.warning("Módulo Usuários não carregado. Verifique a pasta 'USUÁRIOS' ou 'USUARIO'.")

    # --- ABA 3: PARÂMETROS ---
    with tab_param:
        if modulo_parametros:
            modulo_parametros.app_parametros()
        else:
            st.warning("Módulo Parâmetros não carregado.")

    # --- ABA 4: PERMISSÕES ---
    with tab_regras:
        if modulo_permissoes:
            modulo_permissoes.app_permissoes()
        else:
            st.warning("Módulo Permissões não carregado. Verifique a pasta 'PERMISSÕES'.")

    # --- ABA 5: FINANCEIRO ---
    with tab_fin:
        if modulo_financeiro:
            modulo_financeiro.app_financeiro()
        else:
            st.warning("Módulo Financeiro não carregado.")

    # --- ABA 6: GESTÃO TABELAS ---
    with tab_sql:
        if modulo_gestao_tabelas:
            modulo_gestao_tabelas.app_gestao_tabelas()
        else:
            st.warning("Módulo Gestão Tabelas não carregado.")

if __name__ == "__main__":
    app_clientes()