import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime

try:
    import conexao
except ImportError:
    st.error("Erro: conexao.py não encontrado.")

# --- CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

# --- FUNÇÕES DE BANCO ---
def listar_tabelas_banco_pf():
    conn = get_conn()
    tabelas = []
    if conn:
        try:
            cur = conn.cursor()
            # Busca todas as tabelas do schema 'banco_pf'
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'banco_pf' 
                ORDER BY table_name;
            """
            cur.execute(query)
            tabelas = [t[0] for t in cur.fetchall()]
            conn.close()
        except Exception as e:
            st.error(f"Erro ao listar tabelas: {e}")
    return tabelas

def carregar_dados_tabela(nome_tabela):
    conn = get_conn()
    df = pd.DataFrame()
    if conn:
        try:
            query = f"SELECT * FROM banco_pf.{nome_tabela} ORDER BY id ASC"
            df = pd.read_sql(query, conn)
            conn.close()
        except Exception as e:
            st.error(f"Erro ao ler tabela {nome_tabela}: {e}")
            conn.close()
    return df

def executar_sql(query, params=None):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            conn.close()
            return True, None
        except Exception as e:
            conn.close()
            return False, str(e)
    return False, "Sem conexão"

# --- LÓGICA DE SALVAMENTO (GENÉRICA) ---
def salvar_alteracoes_lote(nome_tabela, changes, df_original):
    """
    Processa as alterações retornadas pelo st.data_editor (added_rows, deleted_rows, edited_rows)
    Assume que a tabela tem uma coluna 'id' como chave primária.
    """
    conn = get_conn()
    if not conn: return
    cur = conn.cursor()
    
    erros = []
    sucesso = 0
    
    try:
        # 1. DELETAR
        for idx in changes['deleted_rows']:
            try:
                # Recupera o ID da linha original pelo índice
                id_row = df_original.iloc[idx]['id']
                cur.execute(f"DELETE FROM banco_pf.{nome_tabela} WHERE id = %s", (int(id_row),))
                sucesso += 1
            except Exception as e:
                erros.append(f"Erro ao deletar linha {idx}: {e}")

        # 2. ADICIONAR
        for new_row in changes['added_rows']:
            try:
                # Remove chaves vazias ou None
                clean_row = {k: v for k, v in new_row.items() if v is not None and str(v).strip() != ''}
                if not clean_row: continue
                
                cols = list(clean_row.keys())
                vals = list(clean_row.values())
                placeholders = ", ".join(["%s"] * len(vals))
                col_names = ", ".join(cols)
                
                query = f"INSERT INTO banco_pf.{nome_tabela} ({col_names}) VALUES ({placeholders})"
                cur.execute(query, vals)
                sucesso += 1
            except Exception as e:
                erros.append(f"Erro ao inserir: {e}")

        # 3. EDITAR
        for idx_str, edits in changes['edited_rows'].items():
            try:
                idx = int(idx_str)
                id_row = df_original.iloc[idx]['id']
                
                if not edits: continue
                
                set_parts = []
                vals = []
                for k, v in edits.items():
                    set_parts.append(f"{k} = %s")
                    vals.append(v)
                
                vals.append(id_row) # ID para o WHERE
                
                query = f"UPDATE banco_pf.{nome_tabela} SET {', '.join(set_parts)} WHERE id = %s"
                cur.execute(query, vals)
                sucesso += 1
            except Exception as e:
                erros.append(f"Erro ao editar linha {idx}: {e}")

        conn.commit()
        if sucesso > 0:
            st.toast(f"✅ {sucesso} alterações aplicadas com sucesso!")
        if erros:
            st.error(f"Ocorreram {len(erros)} erros durante o salvamento.")
            with st.expander("Ver detalhes dos erros"):
                for err in erros: st.write(err)
                
    except Exception as e:
        st.error(f"Erro crítico no processamento: {e}")
    finally:
        conn.close()

# --- INTERFACE PRINCIPAL ---
def app_gestao_planilhas():
    st.markdown("### 📊 Gestão de Planilhas (Banco PF)")
    
    # 1. Seletor de Tabela
    tabelas = listar_tabelas_banco_pf()
    if not tabelas:
        st.warning("Nenhuma tabela encontrada no schema 'banco_pf'.")
        return

    col_sel, col_btn = st.columns([3, 1])
    tabela_selecionada = col_sel.selectbox("Selecione a Tabela para Editar:", tabelas)
    
    if col_btn.button("🔄 Atualizar"):
        st.rerun()

    # 2. Carregar Dados
    if tabela_selecionada:
        # Importante: Criar uma key única para o editor baseada na tabela para limpar cache ao trocar
        if 'tabela_atual' not in st.session_state or st.session_state['tabela_atual'] != tabela_selecionada:
            st.session_state['tabela_atual'] = tabela_selecionada
            # Limpa o estado do editor se trocar de tabela
            if 'editor_changes' in st.session_state: del st.session_state['editor_changes']

        df = carregar_dados_tabela(tabela_selecionada)
        
        if df.empty:
            st.info("Tabela vazia ou erro ao carregar.")
        else:
            st.info(f"Visualizando **{len(df)}** registros de `{tabela_selecionada}`. Edite diretamente na grade abaixo.")
            
            # Configurações do Data Editor
            # Desabilitamos a edição da coluna ID para evitar quebra de integridade
            col_config = {}
            if 'id' in df.columns:
                col_config['id'] = st.column_config.NumberColumn(disabled=True)
            
            # 3. Editor de Dados
            changes = st.data_editor(
                df,
                key="editor_changes",
                num_rows="dynamic", # Permite adicionar/remover linhas
                use_container_width=True,
                column_config=col_config,
                hide_index=True
            )

            # 4. Botão de Salvar
            col_save, _ = st.columns([1, 4])
            if col_save.button("💾 Salvar Alterações", type="primary"):
                # Verifica se houve alguma mudança nos dicionários de estado do editor
                has_changes = any([changes['added_rows'], changes['deleted_rows'], changes['edited_rows']])
                
                if has_changes:
                    salvar_alteracoes_lote(tabela_selecionada, changes, df)
                    st.rerun() # Recarrega a página para atualizar os dados do banco
                else:
                    st.toast("Nenhuma alteração detectada.")