import streamlit as st
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO (PATH FIX) ---
# Garante que o Python encontre os módulos na mesma pasta ou na raiz
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

# Adiciona o diretório atual e o pai ao path para encontrar conexao.py e outros módulos
if current_dir not in sys.path:
    sys.path.append(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

erros_importacao = []

# --- 2. IMPORTAÇÃO DOS MÓDULOS CONECTADOS ---

# 1. Cadastro (modulo_cadastro_cliente.py)
try:
    import modulo_cadastro_cliente
except ImportError as e:
    modulo_cadastro_cliente = None
    erros_importacao.append(f"Cadastro: {e}")

# 2. Tabelas / Gestão (modulo_sistema_consulta_planilhas.py)
try:
    # Usamos um alias para manter a lógica, mas apontamos para o arquivo correto
    import modulo_sistema_consulta_planilhas as modulo_gestao_tabelas_cliente
except ImportError as e:
    modulo_gestao_tabelas_cliente = None
    # erros_importacao.append(f"Tabelas: {e}")

# 3. Financeiro (modulo_financeiro.py)
try:
    import modulo_financeiro as modulo_financeiro_cliente
except ImportError:
    modulo_financeiro_cliente = None

# 4. Config. Carteiras (modulo_admin_parametros.py)
try:
    import modulo_admin_parametros as modulo_parametros_cliente
except ImportError:
    modulo_parametros_cliente = None

# 5. Importação em Massa (modulo_sistema_consulta_importacao.py)
try:
    import modulo_sistema_consulta_importacao
except ImportError:
    modulo_sistema_consulta_importacao = None

# 6. Usuários (Placeholder ou Pasta USUÁRIOS)
try:
    import modulo_usuario_cliente
except ImportError:
    try:
        from USUÁRIOS import modulo_usuario
        modulo_usuario_cliente = modulo_usuario
    except ImportError:
        modulo_usuario_cliente = None

# 7. Regras / Permissões (Placeholder)
try:
    import modulo_permissoes_cliente
except ImportError:
    modulo_permissoes_cliente = None

# [NOVO] 8. Relatórios (modulo_relatorios.py)
try:
    import modulo_relatorios
except ImportError as e:
    modulo_relatorios = None
    erros_importacao.append(f"Relatórios: {e}")


# --- 3. FUNÇÃO PRINCIPAL DA TELA ---
def app_clientes():
    st.markdown("## 👥 Central de Clientes")

    # Diagnóstico de Erros (Apenas se houver falhas críticas de importação)
    if erros_importacao:
        with st.expander("⚠️ Diagnóstico de Sistema", expanded=True):
            st.warning("Alguns módulos não foram carregados:")
            for erro in erros_importacao:
                st.error(erro)

    # --- DEFINIÇÃO DINÂMICA DAS ABAS ---
    mapa_abas = {}
    
    # Monta as abas baseadas nos módulos que foram encontrados com sucesso
    if modulo_cadastro_cliente:         mapa_abas["📝 Cadastro"] = modulo_cadastro_cliente
    if modulo_sistema_consulta_importacao: mapa_abas["📥 Importação (Enterprise)"] = modulo_sistema_consulta_importacao
    if modulo_gestao_tabelas_cliente:   mapa_abas["📊 Tabelas (Admin)"] = modulo_gestao_tabelas_cliente
    if modulo_financeiro_cliente:       mapa_abas["💰 Financeiro"] = modulo_financeiro_cliente
    
    # [ALTERAÇÃO] Aba Relatórios Conectada ao Módulo Real
    if modulo_relatorios:               mapa_abas["📈 Relatórios"] = modulo_relatorios
    
    # Configurações e Admin
    if modulo_parametros_cliente:       mapa_abas["⚙️ Config. Carteiras"] = modulo_parametros_cliente
    if modulo_usuario_cliente:          mapa_abas["👤 Usuários"] = modulo_usuario_cliente
    if modulo_permissoes_cliente:       mapa_abas["🛡️ Regras"] = modulo_permissoes_cliente

    if not mapa_abas:
        st.error("❌ Nenhum módulo operacional encontrado. Verifique se os arquivos estão na mesma pasta.")
        return

    # Renderização das Abas
    nomes_abas = list(mapa_abas.keys())
    tabs = st.tabs(nomes_abas)

    for i, nome_aba in enumerate(nomes_abas):
        modulo = mapa_abas[nome_aba]
        
        with tabs[i]:
            try:
                # 1. CADASTRO
                if nome_aba == "📝 Cadastro":
                    if hasattr(modulo, 'app_cadastro_cliente'): modulo.app_cadastro_cliente()
                    elif hasattr(modulo, 'main'): modulo.main()

                # 2. IMPORTAÇÃO (Novo)
                elif nome_aba == "📥 Importação (Enterprise)":
                    if hasattr(modulo, 'tela_importacao'): modulo.tela_importacao()

                # 3. TABELAS (Admin DB)
                elif nome_aba == "📊 Tabelas (Admin)":
                    if hasattr(modulo, 'app_planilhas'): modulo.app_planilhas()
                    elif hasattr(modulo, 'app_tabelas'): modulo.app_tabelas()

                # 4. FINANCEIRO
                elif nome_aba == "💰 Financeiro":
                    if hasattr(modulo, 'app_financeiro'): modulo.app_financeiro()

                # 5. RELATÓRIOS (Atualizado)
                elif nome_aba == "📈 Relatórios":
                    # [ALTERAÇÃO] Chamada da função real do módulo
                    if hasattr(modulo, 'app_relatorios'): modulo.app_relatorios()
                    else: st.warning("Função 'app_relatorios' não encontrada no módulo.")

                # 6. CONFIG. CARTEIRAS
                elif nome_aba == "⚙️ Config. Carteiras":
                    if hasattr(modulo, 'app_parametros'): modulo.app_parametros()

                # 7. USUÁRIOS
                elif nome_aba == "👤 Usuários":
                    if hasattr(modulo, 'app_usuario'): modulo.app_usuario()
                    elif hasattr(modulo, 'app_usuarios'): modulo.app_usuarios()

                # 8. REGRAS
                elif nome_aba == "🛡️ Regras":
                    if hasattr(modulo, 'app_permissoes'): modulo.app_permissoes()

            except Exception as e:
                st.error(f"Erro ao executar a aba '{nome_aba}': {e}")

if __name__ == "__main__":
    app_clientes()