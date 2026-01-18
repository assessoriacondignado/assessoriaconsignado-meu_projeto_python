import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import sys
import os

# Tenta importar configurações de conexão
try:
    import conexao
except ImportError:
    # Ajuste de Path: Adiciona o diretório raiz ao path (caso este arquivo esteja em subpasta)
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    try:
        import conexao
    except ImportError:
        st.error("Arquivo 'conexao.py' não encontrado. Verifique a configuração.")
        conexao = None

# --- CONFIGURAÇÕES ---
# Define o schema exclusivo deste módulo conforme solicitado no DOC
SCHEMAS_PERMITIDOS = ['sistema_consulta']

def get_db_url():
    """Gera a URL de conexão para o SQLAlchemy"""
    if not conexao: return None
    return f"postgresql+psycopg2://{conexao.user}:{conexao.password}@{conexao.host}:{conexao.port}/{conexao.database}"

def obter_lista_tabelas_consulta():
    """
    Busca todas as tabelas do schema 'sistema_consulta'.
    """
    if not conexao: return []
    
    try:
        conn = psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
        cursor = conn.cursor()
        
        # Query específica para o schema do sistema
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'sistema_consulta'
            ORDER BY table_name;
        """
        
        cursor.execute(query)
        resultados = [r[0] for r in cursor.fetchall()] # Retorna lista simples de nomes
        conn.close()
        
        return resultados
    except Exception as e:
        st.error(f"Erro ao buscar lista de tabelas: {e}")
        return []

def carregar_dados(tabela):
    """Lê os dados da tabela para um DataFrame"""
    try:
        engine = create_engine(get_db_url())
        # Lê sempre do schema 'sistema_consulta'
        query = f'SELECT * FROM "sistema_consulta"."{tabela}" ORDER BY id ASC'
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao ler tabela: {e}")
        return None

def salvar_alteracoes(df, tabela):
    """
    Substitui a tabela inteira pelos dados novos (Truncate + Insert).
    CUIDADO: Isso apaga e recria os dados.
    """
    try:
        engine = create_engine(get_db_url())
        with engine.begin() as conn:
            # 1. Truncate (Limpa a tabela mantendo a estrutura)
            conn.exec_driver_sql(f'TRUNCATE TABLE "sistema_consulta"."{tabela}" RESTART IDENTITY CASCADE')
            
            # 2. Insert (Insere os dados do DataFrame)
            df.to_sql(tabela, conn, schema='sistema_consulta', if_exists='append', index=False)
            
        return True, "Dados salvos com sucesso!"
    except SQLAlchemyError as e:
        return False, f"Erro de banco de dados: {str(e)}"
    except Exception as e:
        return False, f"Erro genérico: {str(e)}"

# --- FUNÇÃO PRINCIPAL DO MÓDULO ---
def app_planilhas():
    st.markdown("### 📊 Gestão de Planilhas (Tabelas do Sistema)")
    
    if not conexao:
        st.warning("Sem conexão configurada.")
        return

    # 1. Busca tabelas do schema 'sistema_consulta'
    tabelas_disponiveis = obter_lista_tabelas_consulta()

    if not tabelas_disponiveis:
        st.info("Nenhuma tabela encontrada no schema 'sistema_consulta'. Execute o script SQL de criação primeiro.")
        return

    # 2. Seletor de Tabela
    col_filtro, col_vazia = st.columns([1, 1])
    with col_filtro:
        tabela_selecionada = st.selectbox("Selecione a Tabela para Editar:", tabelas_disponiveis)

    # 3. Lógica de Edição
    if tabela_selecionada:
        st.caption(f"Editando: **sistema_consulta**.**{tabela_selecionada}**")

        # Gerenciamento de estado para carregar dados apenas quando muda a tabela
        if 'df_editor_consulta' not in st.session_state or st.session_state.get('tabela_atual_consulta') != tabela_selecionada:
            st.session_state['df_base_consulta'] = carregar_dados(tabela_selecionada)
            st.session_state['tabela_atual_consulta'] = tabela_selecionada
        
        df_original = st.session_state.get('df_base_consulta')

        if df_original is not None:
            # Editor de Dados
            df_editado = st.data_editor(
                df_original, 
                use_container_width=True, 
                num_rows="dynamic",
                key="editor_planilhas_consulta"
            )

            # Botão de Salvar
            col_save, col_info = st.columns([1, 4])
            with col_save:
                if st.button("💾 Salvar Alterações", type="primary", key="btn_salvar_planilha"):
                    if df_editado.equals(df_original):
                        st.warning("Nenhuma alteração detectada.")
                    else:
                        with st.spinner("Salvando no Banco de Dados..."):
                            sucesso, msg = salvar_alteracoes(df_editado, tabela_selecionada)
                            if sucesso:
                                st.success(msg)
                                st.session_state['df_base_consulta'] = df_editado
                            else:
                                st.error(f"Falha ao salvar: {msg}")
            
            with col_info:
                st.info("⚠️ Atenção: As alterações são aplicadas diretamente no banco de dados.")

if __name__ == "__main__":
    app_planilhas()