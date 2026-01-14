import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import sys
import os

# Tenta importar configurações de conexão
try:
    import conexao
except ImportError:
    # Ajuste de Path: Adiciona o diretório raiz ao path (3 níveis acima: OPERACIONAL/BANCO DE PLANILHAS -> Raiz)
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    try:
        import conexao
    except ImportError:
        st.error("Arquivo 'conexao.py' não encontrado. Verifique a configuração.")
        conexao = None

# --- CONFIGURAÇÕES ---
# Lista de schemas permitidos para visualização/edição (Foco no banco_pf)
SCHEMAS_PERMITIDOS = ['banco_pf', 'public']

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
            AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name;
        """
        
        cursor.execute(query)
        resultados = cursor.fetchall() 
        conn.close()
        
        return resultados
    except Exception as e:
        st.error(f"Erro ao buscar lista de tabelas: {e}")
        return []

def carregar_dados(schema, tabela):
    """Lê os dados da tabela para um DataFrame"""
    try:
        engine = create_engine(get_db_url())
        query = f'SELECT * FROM "{schema}"."{tabela}" ORDER BY 1 ASC' # Ordena pela primeira coluna (geralmente ID)
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao ler tabela: {e}")
        return None

def salvar_alteracoes(df, schema, tabela):
    """
    Substitui a tabela inteira pelos dados novos (Truncate + Insert).
    Atenção: Isso reinicia a tabela com os dados do DataFrame.
    """
    try:
        engine = create_engine(get_db_url())
        with engine.begin() as conn:
            # 1. Truncate (Limpa a tabela mantendo a estrutura)
            # RESTART IDENTITY reinicia os contadores de ID Serial
            # CASCADE permite limpar mesmo se houver dependências (cuidado!)
            conn.execute(text(f'TRUNCATE TABLE "{schema}"."{tabela}" RESTART IDENTITY CASCADE'))
            
            # 2. Insert (Insere os dados do DataFrame)
            df.to_sql(tabela, conn, schema=schema, if_exists='append', index=False)
            
        return True, "Dados salvos com sucesso (Tabela substituída)!"
    except SQLAlchemyError as e:
        return False, f"Erro de banco de dados: {str(e)}"
    except Exception as e:
        return False, f"Erro genérico: {str(e)}"

# --- FUNÇÃO PRINCIPAL DO MÓDULO ---
def app_config_planilhas():
    st.markdown("### 📊 Gestão Avançada de Planilhas (SQLAlchemy)")

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
        # Padrão: Selecionar banco_pf se existir, senão Todos
        idx_padrao = schemas_encontrados.index('banco_pf') + 1 if 'banco_pf' in schemas_encontrados else 0
        filtro_schema = st.selectbox("Filtrar por Schema", ["Todos"] + schemas_encontrados, index=idx_padrao)

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
    
    st.divider()

    # 5. Lógica de Edição
    if tabela_selecionada:
        schema_atual, nome_tabela_atual = tabela_selecionada.split('.')
        
        # Gerenciamento de estado para carregar dados apenas quando muda a tabela
        if 'df_editor_pf' not in st.session_state or st.session_state.get('tabela_atual_pf') != tabela_selecionada:
            with st.spinner(f"Carregando {tabela_selecionada}..."):
                st.session_state['df_base_pf'] = carregar_dados(schema_atual, nome_tabela_atual)
                st.session_state['tabela_atual_pf'] = tabela_selecionada
                # Limpa chave do editor para forçar recarregamento visual
                if 'editor_tabelas_sql_pf' in st.session_state:
                    del st.session_state['editor_tabelas_sql_pf']
        
        # USA CÓPIA PARA NÃO AFETAR REFERÊNCIA EM MEMÓRIA
        df_original = st.session_state.get('df_base_pf').copy() if st.session_state.get('df_base_pf') is not None else None

        if df_original is not None:
            st.caption(f"Visualizando: **{len(df_original)}** registros.")
            
            # Editor de Dados
            df_editado = st.data_editor(
                df_original, 
                use_container_width=True, 
                num_rows="dynamic",
                key="editor_tabelas_sql_pf"
            )

            # Botão de Salvar
            st.markdown("---")
            col_save, col_info = st.columns([1, 4])
            with col_save:
                if st.button("💾 Salvar Alterações", type="primary"):
                    # LÓGICA DE COMPARAÇÃO ROBUSTA (Correção aplicada aqui)
                    # 1. Verifica se tamanhos são diferentes (Inclusão/Exclusão)
                    mudou_tamanho = df_editado.shape != df_original.shape
                    
                    # 2. Verifica conteúdo ignorando índices (reset_index)
                    # Isso resolve o problema de exclusão onde o índice 'pula'
                    mudou_conteudo = False
                    if not mudou_tamanho:
                        try:
                            # Compara valores resetando o índice para garantir alinhamento
                            mudou_conteudo = not df_editado.reset_index(drop=True).equals(df_original.reset_index(drop=True))
                        except:
                            mudou_conteudo = True # Se der erro na comparação, assume que mudou
                    
                    if not mudou_tamanho and not mudou_conteudo:
                        st.warning("Nenhuma alteração detectada.")
                    else:
                        with st.spinner("Substituindo dados da tabela..."):
                            sucesso, msg = salvar_alteracoes(df_editado, schema_atual, nome_tabela_atual)
                            if sucesso:
                                st.success(msg)
                                # Atualiza o estado base com o novo dataframe
                                st.session_state['df_base_pf'] = df_editado
                                import time
                                time.sleep(1) # Pequena pausa para visualização
                                st.rerun()
                            else:
                                st.error(f"Falha ao salvar: {msg}")

if __name__ == "__main__":
    import time # Import local para o rerun funcionar no main
    app_config_planilhas()