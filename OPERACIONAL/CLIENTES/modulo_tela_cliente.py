import streamlit as st
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO ---
# Adiciona o diretório atual ao path para permitir imports diretos
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
if diretorio_atual not in sys.path:
    sys.path.append(diretorio_atual)

erros_importacao = []

# --- 2. IMPORTAÇÃO DOS MÓDULOS (CORRIGIDA) ---
# Como os arquivos estão na mesma pasta e ela está no sys.path,
# usamos importação direta em vez de "from OPERACIONAL.CLIENTES..."

# 1. Clientes
try:
    import modulo_cadastro_cliente
except ImportError as e:
    modulo_cadastro_cliente = None
    erros_importacao.append(f"Cadastro Clientes: {e}")

# 2. Usuários
try:
    # Tenta importar da mesma pasta primeiro
    import modulo_usuario_cliente as modulo_usuario
except ImportError:
    try:
        # Tenta nome antigo ou outra pasta se necessário
        from USUÁRIOS import modulo_usuario
    except ImportError as e:
        modulo_usuario = None
        # Não adicionamos erro crítico aqui pois pode ser opcional

# 3. Parâmetros
try:
    import modulo_parametros_cliente
except ImportError:
    modulo_parametros_cliente = None

# 4. Permissões (Regras)
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
    st.markdown("## 👥 Central de Clientes e Usuários")

    # Exibe erros técnicos apenas se houver falha crítica
    if erros_importacao:
        with st.expander("⚠️ Detalhes de Erros de Carregamento", expanded=True):
            for erro in erros_importacao:
                st.error(erro)
            st.info("Dica: Verifique se os arquivos .py estão na mesma pasta 'OPERACIONAL/CLIENTES'.")

    # --- DEFINIÇÃO DAS ABAS ---
    # Verifica quais módulos carregaram para montar as abas
    abas = ["🏢 Clientes"]
    if modulo_usuario: abas.append("👤 Usuários")
    if modulo_parametros_cliente: abas.append("⚙️ Parâmetros")
    if modulo_permissoes_cliente: abas.append("🛡️ Regras")
    if modulo_financeiro_cliente: abas.append("💰 Financeiro")
    
    # Cria as abas dinamicamente
    tabs = st.tabs(abas)

    # --- ABA 1: CLIENTES ---
    with tabs[0]:
        if modulo_cadastro_cliente:
            try:
                modulo_cadastro_cliente.app_cadastro_cliente()
            except Exception as e:
                st.error(f"Erro ao executar app_cadastro_cliente: {e}")
        else:
            st.warning("O arquivo 'modulo_cadastro_cliente.py' não foi encontrado na pasta.")

    # --- DEMAIS ABAS (Lógica Dinâmica) ---
    idx = 1
    
    if modulo_usuario:
        with tabs[idx]:
            try: modulo_usuario.app_usuario()
            except Exception as e: st.error(f"Erro Usuários: {e}")
        idx += 1

    if modulo_parametros_cliente:
        with tabs[idx]:
            try: modulo_parametros_cliente.app_parametros()
            except Exception as e: st.error(f"Erro Parâmetros: {e}")
        idx += 1

    if modulo_permissoes_cliente:
        with tabs[idx]:
            try: modulo_permissoes_cliente.app_permissoes()
            except Exception as e: st.error(f"Erro Permissões: {e}")
        idx += 1

    if modulo_financeiro_cliente:
        with tabs[idx]:
            try: modulo_financeiro_cliente.app_financeiro()
            except Exception as e: st.error(f"Erro Financeiro: {e}")
        idx += 1

if __name__ == "__main__":
    app_clientes()