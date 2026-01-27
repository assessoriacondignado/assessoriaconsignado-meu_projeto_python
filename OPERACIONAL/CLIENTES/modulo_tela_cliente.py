import streamlit as st
import os
import sys

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO (PATH FIX) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if current_dir not in sys.path:
    sys.path.append(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# --- 2. IMPORTAÇÃO DOS MÓDULOS CONECTADOS ---
# Tentamos importar todos, mesmo que não sejam usados nas abas atuais,
# para manter a integridade caso precise reativar algo futuro.

# 1. Cadastro
try:
    import modulo_cadastro_cliente
except ImportError:
    modulo_cadastro_cliente = None

# 2. Tabelas / Gestão (Arquivo Físico: modulo_gestao_tabelas_clientes.py)
try:
    import modulo_gestao_tabelas_clientes as modulo_gestao_tabelas_cliente
except ImportError:
    modulo_gestao_tabelas_cliente = None

# 3. Importação em Massa
try:
    import modulo_sistema_consulta_importacao
except ImportError:
    modulo_sistema_consulta_importacao = None

# 4. Relatórios
try:
    import modulo_relatorio_cliente
except ImportError:
    modulo_relatorio_cliente = None

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

# (Módulos não listados nas abas, mas mantidos no import por segurança)
try: import modulo_financeiro as modulo_financeiro_cliente
except: modulo_financeiro_cliente = None
try: import modulo_admin_parametros as modulo_parametros_cliente
except: modulo_parametros_cliente = None


# --- 3. FUNÇÃO PRINCIPAL DA TELA ---
def app_clientes():
    st.markdown("## 👥 Central de Clientes")

    # --- DEFINIÇÃO DE ABAS FIXAS ---
    # Ordem solicitada: Cadastro -> Importação -> Relatórios -> Tabelas -> Usuários -> Regras
    abas = [
        "📝 Cadastro", 
        "📥 Importação (Empresa)", 
        "📈 Relatórios", 
        "📊 Tabelas", 
        "👤 Usuários", 
        "🛡️ Regras"
    ]
    
    t_cadastro, t_importacao, t_relatorios, t_tabelas, t_usuarios, t_regras = st.tabs(abas)

    # --- RENDERIZAÇÃO DAS ABAS ---

    # 1. CADASTRO
    with t_cadastro:
        if modulo_cadastro_cliente:
            if hasattr(modulo_cadastro_cliente, 'app_cadastro_cliente'):
                modulo_cadastro_cliente.app_cadastro_cliente()
            elif hasattr(modulo_cadastro_cliente, 'main'):
                modulo_cadastro_cliente.main()
        else:
            st.error("⚠️ Módulo 'Cadastro' não encontrado ou com erro de importação.")

    # 2. IMPORTAÇÃO (EMPRESA)
    with t_importacao:
        if modulo_sistema_consulta_importacao:
            if hasattr(modulo_sistema_consulta_importacao, 'tela_importacao'):
                modulo_sistema_consulta_importacao.tela_importacao()
            else:
                st.warning("Função 'tela_importacao' não encontrada.")
        else:
            st.info("⚠️ Módulo de Importação não carregado.")

    # 3. RELATÓRIOS
    with t_relatorios:
        if modulo_relatorio_cliente:
            if hasattr(modulo_relatorio_cliente, 'app_relatorios'):
                modulo_relatorio_cliente.app_relatorios()
            else:
                st.warning("Função 'app_relatorios' não encontrada.")
        else:
            st.info("⚠️ Módulo de Relatórios não carregado.")

    # 4. TABELAS
    with t_tabelas:
        if modulo_gestao_tabelas_cliente:
            if hasattr(modulo_gestao_tabelas_cliente, 'app_tabelas'):
                modulo_gestao_tabelas_cliente.app_tabelas()
            elif hasattr(modulo_gestao_tabelas_cliente, 'app_planilhas'):
                modulo_gestao_tabelas_cliente.app_planilhas()
        else:
            st.error("⚠️ Módulo 'Tabelas' (modulo_gestao_tabelas_clientes.py) não encontrado.")

    # 5. USUÁRIOS
    with t_usuarios:
        if modulo_usuario_cliente:
            if hasattr(modulo_usuario_cliente, 'app_usuario'):
                modulo_usuario_cliente.app_usuario()
            elif hasattr(modulo_usuario_cliente, 'app_usuarios'):
                modulo_usuario_cliente.app_usuarios()
        else:
            st.info("⚠️ Módulo de Usuários não carregado.")

    # 6. REGRAS
    with t_regras:
        if modulo_permissoes_cliente:
            if hasattr(modulo_permissoes_cliente, 'app_permissoes'):
                modulo_permissoes_cliente.app_permissoes()
        else:
            st.info("⚠️ Módulo de Regras não carregado.")

if __name__ == "__main__":
    app_clientes()