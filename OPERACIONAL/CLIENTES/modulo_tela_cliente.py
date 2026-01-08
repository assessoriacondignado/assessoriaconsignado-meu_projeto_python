import streamlit as st
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO (CORREÇÃO) ---
# Adiciona o diretório atual ao sistema para permitir que o Python
# encontre os arquivos "vizinhos" (cadastro, financeiro, etc.)
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
if diretorio_atual not in sys.path:
    sys.path.append(diretorio_atual)

erros_importacao = []

# --- 2. IMPORTAÇÃO DOS MÓDULOS (ESTILO DIRETO) ---
# Agora usamos "import nome_do_arquivo" diretamente, sem o caminho longo.

# 1. Cadastro de Clientes
try:
    import modulo_cadastro_cliente
except ImportError as e:
    modulo_cadastro_cliente = None
    erros_importacao.append(f"Cadastro Clientes: {e}")

# 2. Usuários
try:
    # Tenta importar o módulo de usuários local
    import modulo_usuario_cliente
except ImportError:
    try:
        # Fallback: tenta importar de uma pasta antiga se existir
        from USUÁRIOS import modulo_usuario
        modulo_usuario_cliente = modulo_usuario
    except ImportError as e:
        modulo_usuario_cliente = None
        # Usuários pode ser opcional, não geramos erro crítico aqui

# 3. Parâmetros
try:
    import modulo_parametros_cliente
except ImportError:
    modulo_parametros_cliente = None

# 4. Permissões / Regras
try:
    import modulo_permissoes_cliente
except ImportError:
    modulo_permissoes_cliente = None

# 5. Financeiro
try:
    import modulo_financeiro_cliente
except ImportError:
    modulo_financeiro_cliente = None


# --- 3. FUNÇÃO PRINCIPAL DA TELA ---
def app_clientes():
    st.markdown("## 👥 Central de Clientes")

    # Mostra erros técnicos apenas se houver falha no módulo principal (Cadastro)
    if erros_importacao:
        with st.expander("⚠️ Diagnóstico de Sistema", expanded=True):
            st.warning("Alguns módulos não foram carregados corretamente:")
            for erro in erros_importacao:
                st.error(erro)
            st.info(f"Pasta verificada: {diretorio_atual}")

    # --- DEFINIÇÃO DINÂMICA DAS ABAS ---
    # Só cria a aba se o módulo existir
    mapa_abas = {}
    
    # Ordem de exibição:
    if modulo_cadastro_cliente: mapa_abas["🏢 Clientes"] = modulo_cadastro_cliente
    if modulo_usuario_cliente:  mapa_abas["👤 Usuários"] = modulo_usuario_cliente
    if modulo_parametros_cliente: mapa_abas["⚙️ Parâmetros"] = modulo_parametros_cliente
    if modulo_permissoes_cliente: mapa_abas["🛡️ Regras"] = modulo_permissoes_cliente
    if modulo_financeiro_cliente: mapa_abas["💰 Financeiro"] = modulo_financeiro_cliente

    if not mapa_abas:
        st.error("❌ Nenhum módulo operacional encontrado nesta pasta.")
        return

    # Cria as abas visualmente
    nomes_abas = list(mapa_abas.keys())
    tabs = st.tabs(nomes_abas)

    # Preenche o conteúdo de cada aba
    for i, nome_aba in enumerate(nomes_abas):
        modulo = mapa_abas[nome_aba]
        with tabs[i]:
            try:
                # Cada módulo deve ter sua função principal de inicialização
                if nome_aba == "🏢 Clientes":
                    modulo.app_cadastro_cliente()
                elif nome_aba == "👤 Usuários":
                    # Verifica qual nome de função o módulo usa (app_usuario ou app_usuarios)
                    if hasattr(modulo, 'app_usuario'): modulo.app_usuario()
                    elif hasattr(modulo, 'app_usuarios'): modulo.app_usuarios()
                elif nome_aba == "⚙️ Parâmetros":
                    modulo.app_parametros()
                elif nome_aba == "🛡️ Regras":
                    modulo.app_permissoes()
                elif nome_aba == "💰 Financeiro":
                    modulo.app_financeiro()
            except Exception as e:
                st.error(f"Erro ao executar o módulo '{nome_aba}': {e}")

if __name__ == "__main__":
    app_clientes()