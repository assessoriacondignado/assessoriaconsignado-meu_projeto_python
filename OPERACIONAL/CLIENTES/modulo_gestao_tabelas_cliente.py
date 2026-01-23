import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import pool
import sys
import os
import time
import contextlib

# ==============================================================================
# 0. CONFIGURAÇÃO DE CAMINHOS
# ==============================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    import conexao
except ImportError:
    st.error("Arquivo 'conexao.py' não encontrado na raiz.")
    conexao = None

# ==============================================================================
# 1. CONFIGURAÇÕES DE PROTEÇÃO E FILTROS
# ==============================================================================

# Tabelas que o sistema bloqueia edição direta (Modo Leitura)
TABELAS_READ_ONLY = [
    'admin.logs_acesso',
    'admin.wapi_logs',
    'admin.pedidos_historico',
    'admin.tarefas_historico',
    'cliente.extrato_carteira_por_produto'
]

# Schemas permitidos para visualização neste módulo
SCHEMAS_PERMITIDOS = ('admin', 'cliente')

# ==============================================================================
# 2. CONEXÃO BLINDADA
# ==============================================================================

@st.cache_resource
def get_pool():
    if not conexao: return None
    try:
        return psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=5, 
            host=conexao.host, port=conexao.port,
            database=conexao.database, user=conexao.user, password=conexao.password,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5
        )
    except Exception as e:
        st.error(f"Erro no Pool de Conexão: {e}")
        return None

@contextlib.contextmanager
def get_conn():
    pool_obj = get_pool()
    if not pool_obj:
        yield None
        return
    
    conn = pool_obj.getconn()
    try:
        conn.rollback() # Health Check
        yield conn
        pool_obj.putconn(conn)
    except Exception:
        pool_obj.putconn(conn, close=True)
        yield None

# ==============================================================================
# 3. FUNÇÕES DE METADADOS E DADOS
# ==============================================================================

def listar_schemas_filtrados():
    """Retorna apenas os schemas definidos em SCHEMAS_PERMITIDOS"""
    with get_conn() as conn:
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name IN %s
                    ORDER BY schema_name
                """, (SCHEMAS_PERMITIDOS,))
                return [r[0] for r in cur.fetchall()]
        except: return []

def listar_tabelas(schema):
    with get_conn() as conn:
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

def carregar_dados_paginados(schema, tabela, pagina, linhas_por_pagina, filtro_col=None, filtro_val=None):
    with get_conn() as conn:
        if not conn: return pd.DataFrame(), 0
        
        offset = (pagina - 1) * linhas_por_pagina
        try:
            query_base = f"FROM {schema}.{tabela}"
            params = []
            
            if filtro_col and filtro_val:
                # Tratamento básico para evitar erro de tipo na query
                if filtro_val.isdigit() and len(filtro_val) < 19: 
                    query_base += f" WHERE {filtro_col} = %s"
                    params.append(int(filtro_val)) 
                else:
                    query_base += f" WHERE {filtro_col}::text ILIKE %s"
                    params.append(f"%{filtro_val}%")

            with conn.cursor() as cur:
                # Conta total
                cur.execute(f"SELECT count(*) {query_base}", tuple(params))
                total_linhas = cur.fetchone()[0]
                
                # Busca dados (Ordena pela 1ª coluna desc, assumindo ID/Data recente)
                sql_data = f"SELECT * {query_base} ORDER BY 1 DESC LIMIT %s OFFSET %s"
                params.extend([linhas_por_pagina, offset])
                
                df = pd.read_sql(sql_data, conn, params=tuple(params))
                
            return df, total_linhas
        except Exception as e:
            st.error(f"Erro ao ler tabela: {e}")
            return pd.DataFrame(), 0

def salvar_edicao_pequena(schema, tabela, df_alterado):
    with get_conn() as conn:
        if not conn: return False
        try:
            cur = conn.cursor()
            # ⚠️ DELETE + INSERT (Seguro apenas para tabelas de configuração pequenas)
            cur.execute(f"DELETE FROM {schema}.{tabela}")
            
            colunas = list(df_alterado.columns)
            # Converte para lista de tuplas para inserção em massa
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

# ==============================================================================
# 4. APLICAÇÃO PRINCIPAL (APP)
# ==============================================================================

def app_tabelas():
    st.markdown("### 📊 Gestão de Tabelas (Admin & Cliente)")
    
    c_schema, c_tabela = st.columns([1, 2])
    
    lista_sch = listar_schemas_filtrados()
    
    if not lista_sch:
        st.warning("⚠️ Schemas 'admin' ou 'cliente' não encontrados. Verifique a conexão.")
        return

    # Tenta selecionar 'admin' por padrão
    idx_def = 0
    if 'admin' in lista_sch: idx_def = lista_sch.index('admin')
    
    schema_sel = c_schema.selectbox("Schema", lista_sch, index=idx_def)
    
    if schema_sel:
        tabelas = listar_tabelas(schema_sel)
        tabela_sel = c_tabela.selectbox("Tabela", tabelas)
        
        if tabela_sel:
            if 'pagina_atual' not in st.session_state: st.session_state['pagina_atual'] = 1
            
            # --- Filtros Dinâmicos ---
            cols_filtro = []
            with get_conn() as conn:
                if conn:
                    try:
                        cur = conn.cursor()
                        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tabela_sel}' AND table_schema = '{schema_sel}'")
                        cols_filtro = [r[0] for r in cur.fetchall()]
                    except: pass

            with st.expander("🔎 Filtros Avançados", expanded=False):
                c_filtro_col, c_filtro_val = st.columns([1, 2])
                col_f = c_filtro_col.selectbox("Filtrar por Coluna:", ["(Sem Filtro)"] + cols_filtro)
                val_f = c_filtro_val.text_input("Valor da Busca:")
            
            filtro_c = col_f if col_f != "(Sem Filtro)" and val_f else None
            filtro_v = val_f if filtro_c else None

            # --- Carregamento de Dados ---
            df, total = carregar_dados_paginados(schema_sel, tabela_sel, st.session_state['pagina_atual'], 50, filtro_c, filtro_v)
            
            st.caption(f"Total: {total} registros encontrados | Página {st.session_state['pagina_atual']}")
            
            # --- Bloqueio de Segurança ---
            nome_completo = f"{schema_sel}.{tabela_sel}"
            # Bloqueia se estiver na lista negra OU se tiver mais de 10k registros (para não quebrar o DELETE/INSERT)
            is_read_only = nome_completo in TABELAS_READ_ONLY or total > 10000 
            
            if is_read_only:
                st.info(f"🔒 Modo Leitura (Tabela Protegida ou Muito Grande para Edição Direta)")
                st.dataframe(df, use_container_width=True)
            else:
                # Editor Interativo
                df_editado = st.data_editor(df, num_rows="dynamic", use_container_width=True, key=f"edit_{schema_sel}_{tabela_sel}")
                
                if st.button("💾 Salvar Alterações (Sobrescrever Tabela)"):
                    if salvar_edicao_pequena(schema_sel, tabela_sel, df_editado):
                        st.success("Tabela atualizada com sucesso!")
                        time.sleep(1)
                        st.rerun()

            # --- Paginação ---
            c_prev, c_page, c_next = st.columns([1, 2, 1])
            if c_prev.button("◀ Anterior") and st.session_state['pagina_atual'] > 1:
                st.session_state['pagina_atual'] -= 1
                st.rerun()
            
            if c_next.button("Próxima ▶") and len(df) == 50:
                st.session_state['pagina_atual'] += 1
                st.rerun()

if __name__ == "__main__":
    app_tabelas()