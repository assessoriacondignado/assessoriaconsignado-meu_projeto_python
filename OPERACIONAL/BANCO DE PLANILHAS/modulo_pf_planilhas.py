import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime

# Tenta importar a conexão
try:
    import conexao
except ImportError:
    st.error("Erro crítico: conexao.py não encontrado.")

# --- CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro de conexão com o banco: {e}")
        return None

# --- LISTAGEM ESTRITA DO SCHEMA BANCO_PF ---
def listar_tabelas_pf():
    conn = get_conn()
    tabelas = []
    if conn:
        try:
            cur = conn.cursor()
            # FILTRO OBRIGATÓRIO: table_schema = 'banco_pf'
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'banco_pf' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """
            cur.execute(query)
            # Retorna lista simples de nomes
            tabelas = [t[0] for t in cur.fetchall()]
            conn.close()
        except Exception as e:
            st.error(f"Erro ao listar tabelas: {e}")
            conn.close()
    return tabelas

# --- CARREGAR DADOS ---
def carregar_dados_tabela(nome_tabela):
    conn = get_conn()
    if conn:
        try:
            # Força o uso do schema banco_pf na query
            query = f"SELECT * FROM banco_pf.{nome_tabela} ORDER BY id ASC"
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Erro ao ler banco_pf.{nome_tabela}: {e}")
            conn.close()
    return pd.DataFrame()

# --- SALVAR ALTERAÇÕES (CRUD) ---
def salvar_lote(nome_tabela, changes, df_original):
    conn = get_conn()
    if not conn: return
    
    cur = conn.cursor()
    sucesso_count = 0
    erros = []

    try:
        # 1. DELETAR (deleted_rows retorna lista de índices)
        for idx in changes['deleted_rows']:
            try:
                # Pega o ID da linha original usando o índice
                row_id = df_original.iloc[idx]['id']
                cur.execute(f"DELETE FROM banco_pf.{nome_tabela} WHERE id = %s", (int(row_id),))
                sucesso_count += 1
            except Exception as e:
                erros.append(f"Erro ao excluir linha {idx}: {e}")

        # 2. INSERIR (added_rows retorna lista de dicts)
        for row in changes['added_rows']:
            try:
                # Remove chaves vazias ou nulas para evitar erro de sintaxe
                clean_row = {k: v for k, v in row.items() if v is not None and str(v).strip() != ''}
                if not clean_row: continue

                cols = list(clean_row.keys())
                vals = list(clean_row.values())
                placeholders = ", ".join(["%s"] * len(vals))
                col_names = ", ".join(cols)

                query = f"INSERT INTO banco_pf.{nome_tabela} ({col_names}) VALUES ({placeholders})"
                cur.execute(query, vals)
                sucesso_count += 1
            except Exception as e:
                erros.append(f"Erro ao inserir linha: {e}")

        # 3. EDITAR (edited_rows retorna dict {indice: {coluna: valor}})
        for idx_str, edits in changes['edited_rows'].items():
            try:
                idx = int(idx_str)
                row_id = df_original.iloc[idx]['id']
                
                if not edits: continue

                set_clauses = []
                vals = []
                for col, val in edits.items():
                    set_clauses.append(f"{col} = %s")
                    vals.append(val)
                
                # Adiciona ID no final para o WHERE
                vals.append(row_id)
                
                query = f"UPDATE banco_pf.{nome_tabela} SET {', '.join(set_clauses)} WHERE id = %s"
                cur.execute(query, vals)
                sucesso_count += 1
            except Exception as e:
                erros.append(f"Erro ao editar linha {idx}: {e}")

        conn.commit()
        
        if sucesso_count > 0:
            st.toast(f"✅ {sucesso_count} alterações salvas em 'banco_pf.{nome_tabela}'!")
        
        if erros:
            st.error("Erros ocorreram durante o salvamento:")
            for err in erros: st.write(err)

    except Exception as e:
        st.error(f"Erro geral ao salvar: {e}")
    finally:
        conn.close()

# --- INTERFACE PRINCIPAL DO MÓDULO ---
def app_gestao_planilhas():
    st.markdown("### 📊 Gestão de Planilhas (Schema: banco_pf)")
    
    lista_tabelas = listar_tabelas_pf()
    
    if not lista_tabelas:
        st.warning("Nenhuma tabela encontrada no schema 'banco_pf'.")
        return

    # Seletor de tabelas
    col1, col2 = st.columns([3, 1])
    tb_selecionada = col1.selectbox("Selecione a Tabela:", lista_tabelas)
    
    if col2.button("🔄 Atualizar"):
        st.rerun()

    if tb_selecionada:
        # Garante limpeza de cache ao trocar de tabela
        if 'tabela_atual_pf' not in st.session_state or st.session_state['tabela_atual_pf'] != tb_selecionada:
            st.session_state['tabela_atual_pf'] = tb_selecionada
            if 'editor_key' in st.session_state: del st.session_state['editor_key']

        df = carregar_dados_tabela(tb_selecionada)
        
        if df.empty:
            st.info(f"A tabela 'banco_pf.{tb_selecionada}' está vazia ou não pôde ser carregada.")
            # Permite tentar adicionar dados mesmo em tabela vazia se tiver colunas (precisaria ler schema, mas aqui simplificamos)
        else:
            # Configuração das colunas (Bloqueia edição de ID e datas automáticas se existirem)
            cfg_colunas = {}
            if 'id' in df.columns:
                cfg_colunas['id'] = st.column_config.NumberColumn(disabled=True, help="ID Automático")
            if 'data_criacao' in df.columns:
                cfg_colunas['data_criacao'] = st.column_config.DatetimeColumn(disabled=True)

            st.write(f"Editando: **banco_pf.{tb_selecionada}**")
            
            # EDITOR
            alteracoes = st.data_editor(
                df,
                key="editor_pf_changes",
                num_rows="dynamic",
                use_container_width=True,
                column_config=cfg_colunas,
                hide_index=True
            )

            # BOTÃO DE SALVAR
            if st.button("💾 Salvar Alterações no Banco", type="primary"):
                if any([alteracoes['added_rows'], alteracoes['deleted_rows'], alteracoes['edited_rows']]):
                    salvar_lote(tb_selecionada, alteracoes, df)
                    st.rerun()
                else:
                    st.info("Nenhuma alteração detectada.")