import streamlit as st
import pandas as pd
import psycopg2
import time

# Tenta importar conexao
try:
    import conexao
except ImportError:
    st.error("Erro: conexao.py não encontrado na raiz.")

# --- CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        print(f"Erro conexão: {e}")
        return None

# --- FUNÇÕES DE BANCO DE DADOS (GESTÃO TABELAS) ---

def listar_tabelas_planilhas():
    conn = get_conn()
    if not conn: return []
    try:
        cur = conn.cursor()
        # Filtra apenas schemas seguros para edição direta
        query = """
            SELECT table_schema || '.' || table_name 
            FROM information_schema.tables 
            WHERE 
                table_schema IN ('cliente', 'admin', 'permissão')
            ORDER BY table_schema, table_name;
        """
        cur.execute(query)
        res = [row[0] for row in cur.fetchall()]
        conn.close()
        return res
    except Exception as e:
        if conn: conn.close()
        return []

def salvar_alteracoes_planilha_generica(nome_tabela_completo, df_original, df_editado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        
        # 1. Identificar IDs originais para saber o que excluir ou atualizar
        ids_originais = set()
        if 'id' in df_original.columns:
            ids_originais = set(df_original['id'].dropna().astype(int).tolist())
        
        # 2. Identificar IDs presentes no DF editado (o que sobrou)
        ids_editados_atuais = set()
        for _, row in df_editado.iterrows():
            if 'id' in row and pd.notna(row['id']) and row['id'] != '':
                try: ids_editados_atuais.add(int(row['id']))
                except: pass

        # 3. Detectar Exclusões (Estava no original, não está no editado)
        ids_del = ids_originais - ids_editados_atuais
        if ids_del:
            ids_str = ",".join(map(str, ids_del))
            cur.execute(f"DELETE FROM {nome_tabela_completo} WHERE id IN ({ids_str})")

        # 4. Iterar sobre linhas para UPDATE ou INSERT
        for index, row in df_editado.iterrows():
            # Ignora colunas de timestamp automático se existirem no DF
            colunas_db = [c for c in row.index if c not in ['data_criacao', 'data_registro', 'data_lancamento']]
            
            row_id = row.get('id')
            eh_novo = pd.isna(row_id) or row_id == '' or row_id is None
            
            valores = [row[c] for c in colunas_db if c != 'id']
            
            if eh_novo:
                # INSERT
                cols_str = ", ".join([c for c in colunas_db if c != 'id'])
                placeholders = ", ".join(["%s"] * len(valores))
                if cols_str:
                    cur.execute(f"INSERT INTO {nome_tabela_completo} ({cols_str}) VALUES ({placeholders})", valores)
            elif int(row_id) in ids_originais:
                # UPDATE
                set_clause = ", ".join([f"{c} = %s" for c in colunas_db if c != 'id'])
                valores_update = valores + [int(row_id)]
                if set_clause:
                    cur.execute(f"UPDATE {nome_tabela_completo} SET {set_clause} WHERE id = %s", valores_update)
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar tabela {nome_tabela_completo}: {e}")
        if conn: conn.close()
        return False

# --- APP PRINCIPAL DO MÓDULO ---

def app_gestao_tabelas():
    st.markdown("### 📅 Gestão de Planilhas do Banco")
    st.caption("Visualização e edição direta de tabelas (Schemas: admin, cliente, permissão).")
    
    lista_tabelas = listar_tabelas_planilhas()
    
    if lista_tabelas:
        col_sel, col_info = st.columns([1, 2])
        tabela_selecionada = col_sel.selectbox("Selecione a Tabela", lista_tabelas)
        
        if tabela_selecionada:
            conn = get_conn()
            if conn:
                try:
                    # Limite de 1000 linhas para performance
                    st.markdown(f"**Editando:** `{tabela_selecionada}`")
                    df_tabela = pd.read_sql(f"SELECT * FROM {tabela_selecionada} ORDER BY id DESC LIMIT 1000", conn)
                    conn.close()
                    
                    # Colunas que não devem ser editadas manualmente
                    cols_travadas = ["data_criacao", "data_registro", "data_lancamento"]
                    
                    df_editado = st.data_editor(
                        df_tabela,
                        key=f"editor_planilha_{tabela_selecionada}",
                        num_rows="dynamic",
                        use_container_width=True,
                        disabled=[c for c in cols_travadas if c in df_tabela.columns]
                    )
                    
                    if st.button("💾 Salvar Alterações na Planilha", type="primary"):
                        with st.spinner("Salvando alterações..."):
                            if salvar_alteracoes_planilha_generica(tabela_selecionada, df_tabela, df_editado):
                                st.success("Tabela atualizada com sucesso!")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Erro ao salvar alterações. Verifique os logs.")
                except Exception as e:
                    st.error(f"Erro ao ler tabela: {e}")
                    if conn: conn.close()
    else:
        st.warning("Nenhuma tabela encontrada nos schemas selecionados (admin, cliente, permissão).")