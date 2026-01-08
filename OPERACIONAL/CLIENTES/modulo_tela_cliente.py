import streamlit as st
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO ---
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
if diretorio_atual not in sys.path:
    sys.path.append(diretorio_atual)

erros_importacao = []

# --- 2. IMPORTAÇÃO DOS MÓDULOS ---

# 1. Cadastro
try:
    import modulo_cadastro_cliente
except ImportError as e:
    modulo_cadastro_cliente = None
    erros_importacao.append(f"Cadastro: {e}")

# 2. Tabelas (Antigo Edição de Transações)
try:
    import modulo_gestao_tabelas_cliente
except ImportError as e:
    modulo_gestao_tabelas_cliente = None
    # Não é crítico, apenas loga se necessário
    # erros_importacao.append(f"Tabelas: {e}")

# 3. Financeiro
try:
    import modulo_financeiro_cliente
except ImportError:
    modulo_financeiro_cliente = None

# 4. Config. Carteiras (Parâmetros)
try:
    import modulo_parametros_cliente
except ImportError:
    modulo_parametros_cliente = None

# 5. Usuários
try:
    import modulo_usuario_cliente
except ImportError:
    try:
        from USUÁRIOS import modulo_usuario
        modulo_usuario_cliente = modulo_usuario
    except ImportError:
        modulo_usuario_cliente = None

# 6. Regras / Permissões
try:
    import modulo_permissoes_cliente
except ImportError:
    modulo_permissoes_cliente = None


# --- 3. FUNÇÃO PRINCIPAL DA TELA ---
def app_clientes():
    st.markdown("## 👥 Central de Clientes")

    # Mostra erros técnicos apenas se houver falha crítica
    if erros_importacao:
        with st.expander("⚠️ Diagnóstico de Sistema", expanded=True):
            st.warning("Alguns módulos não foram carregados:")
            for erro in erros_importacao:
                st.error(erro)

    # --- DEFINIÇÃO DINÂMICA DAS ABAS ---
    # Dicionário: "Nome da Aba" -> Objeto do Módulo (ou None para locais)
    mapa_abas = {}
    
    # Ordem de exibição solicitada:
    if modulo_cadastro_cliente:         mapa_abas["📝 Cadastro"] = modulo_cadastro_cliente
    if modulo_gestao_tabelas_cliente:   mapa_abas["📊 Tabelas"] = modulo_gestao_tabelas_cliente
    if modulo_financeiro_cliente:       mapa_abas["💰 Financeiro"] = modulo_financeiro_cliente
    
    # Aba Relatórios (Local - sempre visível)
    mapa_abas["📈 Relatórios"] = "local_relatorios"
    
    # Configurações e Admin
    if modulo_parametros_cliente:       mapa_abas["⚙️ Config. Carteiras"] = modulo_parametros_cliente
    if modulo_usuario_cliente:          mapa_abas["👤 Usuários"] = modulo_usuario_cliente
    if modulo_permissoes_cliente:       mapa_abas["🛡️ Regras"] = modulo_permissoes_cliente

    if not mapa_abas:
        st.error("❌ Nenhum módulo operacional encontrado.")
        return

    # Cria as abas
    nomes_abas = list(mapa_abas.keys())
    tabs = st.tabs(nomes_abas)

    # Renderiza o conteúdo
    for i, nome_aba in enumerate(nomes_abas):
        modulo = mapa_abas[nome_aba]
        
        with tabs[i]:
            try:
                # 1. CADASTRO
                if nome_aba == "📝 Cadastro":
                    if hasattr(modulo, 'app_cadastro_cliente'):
                        modulo.app_cadastro_cliente()
                    elif hasattr(modulo, 'main'):
                        modulo.main()

                # 2. TABELAS (Edição de Transações)
                elif nome_aba == "📊 Tabelas":
                    if hasattr(modulo, 'app_tabelas'):
                        modulo.app_tabelas()
                    elif hasattr(modulo, 'main'):
                        modulo.main()
                    else:
                        st.info("Módulo de Tabelas carregado (função principal não identificada).")

                # 3. FINANCEIRO
                elif nome_aba == "💰 Financeiro":
                    if hasattr(modulo, 'app_financeiro'):
                        modulo.app_financeiro()

                # 4. RELATÓRIOS (Implementação Local)
                elif nome_aba == "📈 Relatórios":
                    st.subheader("Relatórios Gerenciais")
                    st.info("Área destinada à emissão de relatórios.")
                    # Exemplo de placeholder para futura implementação
                    c1, c2 = st.columns(2)
                    with c1:
                        st.selectbox("Tipo de Relatório", ["Geral", "Inadimplência", "Novos Clientes"])
                    with c2:
                        st.button("Gerar PDF")

                # 5. CONFIG. CARTEIRAS (Parâmetros)
                elif nome_aba == "⚙️ Config. Carteiras":
                    if hasattr(modulo, 'app_parametros'):
                        modulo.app_parametros()

                # 6. USUÁRIOS
                elif nome_aba == "👤 Usuários":
                    if hasattr(modulo, 'app_usuario'): modulo.app_usuario()
                    elif hasattr(modulo, 'app_usuarios'): modulo.app_usuarios()

                # 7. REGRAS
                elif nome_aba == "🛡️ Regras":
                    if hasattr(modulo, 'app_permissoes'):
                        modulo.app_permissoes()

            except Exception as e:
                st.error(f"Erro ao executar a aba '{nome_aba}': {e}")

if __name__ == "__main__":
    app_clientes()