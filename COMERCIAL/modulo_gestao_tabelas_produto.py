import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import sys
import os

# --- 1. CONFIGURAÇÃO DE IMPORTAÇÃO (PADRONIZAÇÃO) ---
# Garante que o Python encontre os módulos na raiz do projeto
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
# Se este arquivo estiver dentro de COMERCIAL ou uma subpasta, subimos até a raiz
raiz_projeto = os.path.dirname(os.path.dirname(diretorio_atual)) # Ajuste conforme profundidade
if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)

# Tenta importar configurações de conexão
try:
    import conexao
except ImportError:
    # Tentativa de fallback subindo mais um nível se necessário
    sys.path.append(os.path.dirname(raiz_projeto))
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

def obter_lista_completa_tabelas(schemas):
    """
    Busca todas as tabelas dos schemas permitidos e retorna uma lista de tuplas (schema, tabela).
    """
    if not conexao: return []
    
    try:
        conn = psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
        cursor = conn.cursor()
        
        # Formata a lista para o SQL
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
        
        return resultados # Retorna a variável correta 'resultados'
    except Exception as e:
        st.error(f"Erro ao buscar lista de tabelas: {e}")
        return []

def carregar_dados(schema, tabela):
    """Lê os dados da tabela para um DataFrame"""
    try:
        engine = create_engine(get_db_url())
        query = f'SELECT * FROM "{schema}"."{tabela}"'
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao ler tabela: {e}")
        return None

def salvar_alteracoes(df, schema, tabela):
    """
    Substitui a tabela inteira pelos dados novos (Truncate + Insert).
    """
    try:
        engine = create_engine(get_db_url())
        with engine.begin() as conn:
            # 1. Truncate (Limpa a tabela mantendo a estrutura)
            conn.exec_driver_sql(f'TRUNCATE TABLE "{schema}"."{tabela}" RESTART IDENTITY CASCADE')
            
            # 2. Insert (Insere os dados do DataFrame)
            df.to_sql(tabela, conn, schema=schema, if_exists='append', index=False)
            
        return True, "Dados salvos com sucesso!"
    except SQLAlchemyError as e:
        return False, f"Erro de banco de dados: {str(e)}"
    except Exception as e:
        return False, f"Erro genérico: {str(e)}"

# --- FUNÇÃO PRINCIPAL DO MÓDULO ---
def app_tabelas():
    if not conexao:
        st.warning("Sem conexão configurada.")
        return

    # 1. Busca a lista bruta de todas as tabelas disponíveis
    todos_dados = obter_lista_completa_tabelas(SCHEMAS_PERMITIDOS)

    if not todos_dados:
        st.info("Nenhuma tabela encontrada ou erro na conexão.")
        return

    # 2. Área de Filtros (Duas Colunas)
    col_filtro_schema, col_filtro_nome = st.columns(2)

    with col_filtro_schema:
        # Cria lista única de schemas encontrados para o filtro
        schemas_encontrados = sorted(list(set([t[0] for t in todos_dados])))
        filtro_schema = st.selectbox("Filtrar por Schema", ["Todos"] + schemas_encontrados)

    with col_filtro_nome:
        filtro_nome = st.text_input("Filtrar por Nome da Tabela")

    # 3. Aplicação dos Filtros
    lista_opcoes = []
    for schema, tabela in todos_dados:
        # Filtra Schema
        if filtro_schema != "Todos" and schema != filtro_schema:
            continue
        
        # Filtra Nome (Case insensitive)
        if filtro_nome and filtro_nome.lower() not in tabela.lower():
            continue
            
        # Adiciona formato "schema.tabela" para o selectbox final
        lista_opcoes.append(f"{schema}.{tabela}")

    # 4. Selectbox Principal (Mostra apenas os filtrados)
    if not lista_opcoes:
        st.warning("Nenhuma tabela corresponde aos filtros selecionados.")
        tabela_selecionada = None
    else:
        tabela_selecionada = st.selectbox("Selecione a Tabela para Editar:", lista_opcoes)

    # 5. Lógica de Edição
    if tabela_selecionada:
        schema_atual, nome_tabela_atual = tabela_selecionada.split('.')
        
        st.caption(f"Editando: **{schema_atual}**.**{nome_tabela_atual}**")

        # Gerenciamento de estado para carregar dados apenas quando muda a tabela
        if 'df_editor' not in st.session_state or st.session_state.get('tabela_atual') != tabela_selecionada:
            st.session_state['df_base'] = carregar_dados(schema_atual, nome_tabela_atual)
            st.session_state['tabela_atual'] = tabela_selecionada
        
        df_original = st.session_state['df_base']

        if df_original is not None:
            # Editor de Dados
            df_editado = st.data_editor(
                df_original, 
                use_container_width=True, 
                num_rows="dynamic",
                key="editor_tabelas_sql"
            )

            # Botão de Salvar
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
                                st.session_state['df_base'] = df_editado
                            else:
                                st.error(f"Falha ao salvar: {msg}")

if __name__ == "__main__":
    app_tabelas()