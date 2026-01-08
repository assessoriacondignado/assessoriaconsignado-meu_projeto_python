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
    # Fallback para permitir testes isolados se necessário
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    try:
        import conexao
    except ImportError:
        st.error("Arquivo 'conexao.py' não encontrado. Verifique a configuração.")
        conexao = None

# --- CONFIGURAÇÕES ---
# Lista de schemas permitidos para visualização/edição
SCHEMAS_PERMITIDOS = ['cliente', 'admin', 'permissoes', 'permissao', 'public']

def get_db_url():
    """Gera a URL de conexão para o SQLAlchemy"""
    if not conexao: return None
    return f"postgresql+psycopg2://{conexao.user}:{conexao.password}@{conexao.host}:{conexao.port}/{conexao.database}"

def listar_tabelas(schemas):
    """Busca tabelas apenas dos schemas selecionados"""
    conn = psycopg2.connect(
        host=conexao.host, port=conexao.port, database=conexao.database,
        user=conexao.user, password=conexao.password
    )
    cursor = conn.cursor()
    
    # Formata a lista para o SQL (ex: 'admin', 'cliente')
    schemas_str = "', '".join(schemas)
    query = f"""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('{schemas_str}')
        ORDER BY table_schema, table_name;
    """
    
    cursor.execute(query)
    resultados = cursor.fetchall()
    conn.close()
    
    # Retorna lista de strings "schema.tabela"
    return [f"{res[0]}.{res[1]}" for res in resultados]

def carregar_dados(schema, tabela):
    """Lê os dados da tabela para um DataFrame"""
    try:
        engine = create_engine(get_db_url())
        # Usa aspas duplas para garantir que o case sensitive do SQL seja respeitado
        query = f'SELECT * FROM "{schema}"."{tabela}"'
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao ler tabela: {e}")
        return None

def salvar_alteracoes(df, schema, tabela):
    """
    Substitui a tabela inteira pelos dados novos.
    ATENÇÃO: Método Truncate/Insert (Simples e eficaz para tabelas de apoio/cadastro).
    """
    try:
        engine = create_engine(get_db_url())
        with engine.begin() as conn:
            # 1. Truncate (Limpa a tabela mantendo a estrutura)
            conn.exec_driver_sql(f'TRUNCATE TABLE "{schema}"."{tabela}" RESTART IDENTITY CASCADE')
            
            # 2. Insert (Insere os dados do DataFrame)
            # 'if_exists="append"' porque a tabela já existe (apenas a limpamos)
            df.to_sql(tabela, conn, schema=schema, if_exists='append', index=False)
            
        return True, "Dados salvos com sucesso!"
    except SQLAlchemyError as e:
        return False, f"Erro de banco de dados: {str(e)}"
    except Exception as e:
        return False, f"Erro genérico: {str(e)}"

# --- FUNÇÃO PRINCIPAL DO MÓDULO ---
def app_tabelas():
    st.subheader("📊 Gerenciador de Tabelas (SQL)")

    if not conexao:
        st.warning("Sem conexão configurada.")
        return

    # 1. Seletor de Tabelas
    try:
        lista_tabelas = listar_tabelas(SCHEMAS_PERMITIDOS)
    except Exception as e:
        st.error(f"Erro ao listar tabelas: {e}")
        lista_tabelas = []

    if not lista_tabelas:
        st.info("Nenhuma tabela encontrada nos schemas: " + ", ".join(SCHEMAS_PERMITIDOS))
        return

    tabela_selecionada = st.selectbox("Selecione a Tabela para Editar:", lista_tabelas)

    if tabela_selecionada:
        schema_atual, nome_tabela_atual = tabela_selecionada.split('.')
        
        st.markdown(f"**Tabela:** `{nome_tabela_atual}` | **Schema:** `{schema_atual}`")
        st.info("💡 Edite os valores diretamente na planilha abaixo. Clique em 'Salvar Alterações' para confirmar.")

        # 2. Carregamento dos Dados
        # Cacheamos o carregamento para não recarregar a cada clique na tela, 
        # mas permitimos recarga forçada com botão.
        if 'df_editor' not in st.session_state or st.session_state.get('tabela_atual') != tabela_selecionada:
            st.session_state['df_base'] = carregar_dados(schema_atual, nome_tabela_atual)
            st.session_state['tabela_atual'] = tabela_selecionada
        
        df_original = st.session_state['df_base']

        if df_original is not None:
            # 3. Editor de Dados (Visualizar / Editar)
            # num_rows="dynamic" permite adicionar/remover linhas
            df_editado = st.data_editor(
                df_original, 
                use_container_width=True, 
                num_rows="dynamic",
                key="editor_tabelas_sql"
            )

            # 4. Botão de Salvar
            col_save, col_info = st.columns([1, 4])
            with col_save:
                if st.button("💾 Salvar Alterações", type="primary"):
                    if df_editado.equals(df_original):
                        st.warning("Nenhuma alteração detectada.")
                    else:
                        with st.spinner("Salvando no Banco de Dados..."):
                            sucesso, msg = salvar_alteracoes(df_editado, schema_atual, nome_tabela_atual)
                            if sucesso:
                                st.success(msg)
                                # Atualiza o estado para refletir a nova base
                                st.session_state['df_base'] = df_editado
                                # Opcional: st.rerun() para forçar refresh visual
                            else:
                                st.error(f"Falha ao salvar: {msg}")

# Permite execução direta para testes
if __name__ == "__main__":
    app_tabelas()