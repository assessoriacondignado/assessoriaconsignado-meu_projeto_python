import streamlit as st
import pandas as pd
import psycopg2
import sys
import os
import time

# --- CONFIGURAÇÃO DE CAMINHOS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    import conexao
except ImportError:
    st.error("Arquivo 'conexao.py' não encontrado.")
    conexao = None

# Tabelas protegidas contra edição direta (Big Data)
TABELAS_READ_ONLY = [
    'sistema_consulta_dados_cadastrais_cpf', 
    'sistema_consulta_contrato', 
    'sistema_consulta_dados_cadastrais_telefone',
    'sistema_consulta_dados_cadastrais_email',
    'sistema_consulta_dados_cadastrais_endereco'
]

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port,
            database=conexao.database, user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

def listar_schemas():
    conn = get_conn()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog') ORDER BY schema_name")
            return [r[0] for r in cur.fetchall()]
    except: return []
    finally: conn.close()

def listar_tabelas(schema):
    conn = get_conn()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                ORDER BY table_name
            """, (schema,))
            return [r[0] for r in cur.fetchall()]
    except: return []
    finally: conn.close()

def carregar_dados_paginados(schema, tabela, pagina, linhas_por_pagina, filtro_col=None, filtro_val=None):
    conn = get_conn()
    if not conn: return pd.DataFrame(), 0
    
    offset = (pagina - 1) * linhas_por_pagina
    
    try:
        query_base = f"FROM {schema}.{tabela}"
        params = []
        
        if filtro_col and filtro_val:
            if filtro_val.isdigit():
                query_base += f" WHERE {filtro_col} = %s"
                params.append(int(filtro_val)) 
            else:
                query_base += f" WHERE {filtro_col}::text ILIKE %s"
                params.append(f"%{filtro_val}%")

        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) {query_base}", tuple(params))
            total_linhas = cur.fetchone()[0]
            
            sql_data = f"SELECT * {query_base} ORDER BY 1 DESC LIMIT %s OFFSET %s" # Ordena pelo primeiro campo (geralmente ID) DESC
            params.extend([linhas_por_pagina, offset])
            
            df = pd.read_sql(sql_data, conn, params=tuple(params))
            
        return df, total_linhas
    except Exception as e:
        st.error(f"Erro ao ler tabela: {e}")
        return pd.DataFrame(), 0
    finally: conn.close()

def salvar_edicao_pequena(schema, tabela, df_alterado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        # Truncate é perigoso em admin, melhor usar DELETE
        # Mas para simplificar a edição em massa de tabelas pequenas:
        cur.execute(f"DELETE FROM {schema}.{tabela}")
        
        colunas = list(df_alterado.columns)
        vals = [tuple(x) for x in df_alterado.to_numpy()]
        
        placeholders = ",".join(["%s"] * len(colunas))
        cols_str = ",".join(colunas)
        
        insert_query = f"INSERT INTO {schema}.{tabela} ({cols_str}) VALUES ({placeholders})"
        cur.executemany(insert_query, vals)
        
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False
    finally: conn.close()

# --- APP ---
def app_planilhas():
    st.markdown("### 📊 Visualizador de Tabelas (DB Admin)")
    
    c_schema, c_tabela = st.columns([1, 2])
    
    lista_sch = listar_schemas()
    idx_def = 0
    if 'admin' in lista_sch: idx_def = lista_sch.index('admin')
    
    schema_sel = c_schema.selectbox("Schema", lista_sch, index=idx_def)
    
    if schema_sel:
        tabelas = listar_tabelas(schema_sel)
        tabela_sel = c_tabela.selectbox("Tabela", tabelas)
        
        if tabela_sel:
            if 'pagina_atual' not in st.session_state: st.session_state['pagina_atual'] = 1
            
            # Filtros
            cols_filtro = []
            conn_temp = get_conn()
            try:
                cur = conn_temp.cursor()
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tabela_sel}' AND table_schema = '{schema_sel}'")
                cols_filtro = [r[0] for r in cur.fetchall()]
            except: pass
            finally: conn_temp.close()

            with st.expander("🔎 Filtros", expanded=False):
                c_filtro_col, c_filtro_val = st.columns([1, 2])
                col_f = c_filtro_col.selectbox("Coluna:", ["(Sem Filtro)"] + cols_filtro)
                val_f = c_filtro_val.text_input("Valor:")
            
            filtro_c = col_f if col_f != "(Sem Filtro)" and val_f else None
            filtro_v = val_f if filtro_c else None

            # Carrega Dados
            df, total = carregar_dados_paginados(schema_sel, tabela_sel, st.session_state['pagina_atual'], 50, filtro_c, filtro_v)
            
            st.caption(f"Total: {total} registros | Página {st.session_state['pagina_atual']}")
            
            # Bloqueio de Edição
            is_read_only = tabela_sel in TABELAS_READ_ONLY or total > 5000 # Proteção extra para tabelas médias
            
            if is_read_only:
                st.info("🔒 Modo Leitura (Tabela Grande ou Protegida)")
                st.dataframe(df, use_container_width=True)
            else:
                df_editado = st.data_editor(df, num_rows="dynamic", use_container_width=True, key=f"edit_{schema_sel}_{tabela_sel}")
                
                if st.button("💾 Salvar Alterações (Sobrescrever Tabela)"):
                    if salvar_edicao_pequena(schema_sel, tabela_sel, df_editado):
                        st.success("Salvo!")
                        time.sleep(1)
                        st.rerun()

            # Paginação
            c_prev, c_page, c_next = st.columns([1, 2, 1])
            if c_prev.button("◀ Anterior") and st.session_state['pagina_atual'] > 1:
                st.session_state['pagina_atual'] -= 1
                st.rerun()
            
            if c_next.button("Próxima ▶") and len(df) == 50:
                st.session_state['pagina_atual'] += 1
                st.rerun()

if __name__ == "__main__":
    app_planilhas()