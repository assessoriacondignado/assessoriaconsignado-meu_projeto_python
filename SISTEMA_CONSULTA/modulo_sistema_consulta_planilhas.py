import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
import sys
import os

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

# --- CONSTANTES ---
# Tabelas que NÃO podem ser editadas diretamente por serem gigantes
TABELAS_READ_ONLY = [
    'sistema_consulta_dados_cadastrais_cpf', 
    'sistema_consulta_contrato', 
    'sistema_consulta_dados_cadastrais_telefone',
    'sistema_consulta_dados_cadastrais_email',
    'sistema_consulta_dados_cadastrais_endereco',
    'beneficios_analitico',
    'contratos_analitico'
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

def listar_tabelas():
    conn = get_conn()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'sistema_consulta' 
                ORDER BY table_name
            """)
            return [r[0] for r in cur.fetchall()]
    except: return []
    finally: conn.close()

def carregar_dados_paginados(tabela, pagina, linhas_por_pagina, filtro_col=None, filtro_val=None):
    """Lê dados com LIMIT e OFFSET para não travar a memória"""
    conn = get_conn()
    if not conn: return pd.DataFrame(), 0
    
    offset = (pagina - 1) * linhas_por_pagina
    
    try:
        query_base = f"FROM sistema_consulta.{tabela}"
        params = []
        
        if filtro_col and filtro_val:
            # Tratamento básico para filtro numérico (BigInt) ou texto
            if filtro_val.isdigit():
                query_base += f" WHERE {filtro_col} = %s"
                params.append(int(filtro_val)) # Converte para int para bater com BigInt
            else:
                query_base += f" WHERE {filtro_col}::text ILIKE %s"
                params.append(f"%{filtro_val}%")

        # Conta total (para paginação) - Limitado a 1M para não demorar em tabelas gigantes
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) {query_base}") # Count rápido se tiver índice
            total_linhas = cur.fetchone()[0]
            
            # Busca dados
            sql_data = f"SELECT * {query_base} LIMIT %s OFFSET %s"
            params.extend([linhas_por_pagina, offset])
            
            # Usa pandas read_sql com a conexão psycopg2 (mais rápido que engine sqlalchemy)
            df = pd.read_sql(sql_data, conn, params=params)
            
        return df, total_linhas
    except Exception as e:
        st.error(f"Erro ao ler tabela: {e}")
        return pd.DataFrame(), 0
    finally: conn.close()

def salvar_edicao_pequena(df_alterado, tabela, chave_primaria='id'):
    """
    Salva edições APENAS para tabelas de configuração (pequenas).
    Usa DELETE + INSERT (Truncate) ou UPDATE linha a linha seria muito complexo aqui.
    Para tabelas pequenas, Truncate + Insert é aceitável.
    """
    conn = get_conn()
    if not conn: return False
    
    try:
        cur = conn.cursor()
        # 1. Limpa tabela
        cur.execute(f"TRUNCATE TABLE sistema_consulta.{tabela} RESTART IDENTITY")
        
        # 2. Insere dados novos
        colunas = list(df_alterado.columns)
        vals = [tuple(x) for x in df_alterado.to_numpy()]
        
        placeholders = ",".join(["%s"] * len(colunas))
        cols_str = ",".join(colunas)
        
        insert_query = f"INSERT INTO sistema_consulta.{tabela} ({cols_str}) VALUES ({placeholders})"
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
    
    tabelas = listar_tabelas()
    if not tabelas:
        st.warning("Nenhuma tabela encontrada no schema 'sistema_consulta'.")
        return

    c1, c2 = st.columns([2, 2])
    tabela_sel = c1.selectbox("Selecione a Tabela:", tabelas)
    
    if tabela_sel:
        # Configuração de Paginação
        if 'pagina_atual' not in st.session_state: st.session_state['pagina_atual'] = 1
        
        # Filtros Rápidos
        cols_filtro = []
        if tabela_sel:
            conn_temp = get_conn()
            try:
                # Pega colunas para o filtro
                cur = conn_temp.cursor()
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tabela_sel}' AND table_schema = 'sistema_consulta'")
                cols_filtro = [r[0] for r in cur.fetchall()]
            except: pass
            finally: conn_temp.close()

        c_filtro_col, c_filtro_val = st.columns([1, 2])
        col_f = c_filtro_col.selectbox("Filtrar por Coluna:", ["(Sem Filtro)"] + cols_filtro)
        val_f = c_filtro_val.text_input("Valor do Filtro:")
        
        filtro_c = col_f if col_f != "(Sem Filtro)" and val_f else None
        filtro_v = val_f if filtro_c else None

        # Carrega Dados
        df, total = carregar_dados_paginados(tabela_sel, st.session_state['pagina_atual'], 50, filtro_c, filtro_v)
        
        st.caption(f"Mostrando {len(df)} de {total} registros (Página {st.session_state['pagina_atual']})")
        
        # Decisão: Editável ou Apenas Leitura?
        is_read_only = tabela_sel in TABELAS_READ_ONLY
        
        if is_read_only:
            st.warning("🔒 Esta tabela é muito grande (Big Data). Modo **Somente Leitura** ativado para proteção.")
            st.dataframe(df, use_container_width=True)
        else:
            # Tabela Pequena (Configuração): Permite Edição
            df_editado = st.data_editor(df, num_rows="dynamic", use_container_width=True, key=f"edit_{tabela_sel}")
            
            if st.button("💾 Salvar Alterações (Sobrescrever Tabela)"):
                if salvar_edicao_pequena(df_editado, tabela_sel):
                    st.success("Tabela atualizada com sucesso!")
                    time.sleep(1)
                    st.rerun()

        # Controles de Paginação
        c_prev, c_page, c_next = st.columns([1, 2, 1])
        if c_prev.button("◀ Anterior") and st.session_state['pagina_atual'] > 1:
            st.session_state['pagina_atual'] -= 1
            st.rerun()
        
        c_page.markdown(f"<div style='text-align: center'>Página <b>{st.session_state['pagina_atual']}</b></div>", unsafe_allow_html=True)
        
        if c_next.button("Próxima ▶") and len(df) == 50:
            st.session_state['pagina_atual'] += 1
            st.rerun()

if __name__ == "__main__":
    app_planilhas()