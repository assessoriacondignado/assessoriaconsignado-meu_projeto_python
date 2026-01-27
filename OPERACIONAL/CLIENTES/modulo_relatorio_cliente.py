import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import pool
import contextlib
import os
import sys
import time
from datetime import datetime, date

# ==============================================================================
# 0. CONFIGURAÇÃO E CONEXÃO (Padrão Blindado)
# ==============================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    import conexao
except ImportError:
    conexao = None

# IMPORTAÇÃO DO MÓDULO DE VALIDADORES (PADRONIZAÇÃO)
try:
    import modulo_validadores as v
except ImportError:
    v = None

# --- 1. CONEXÃO BLINDADA (Connection Pool + Retry Logic) ---

@st.cache_resource
def get_pool():
    if not conexao: return None
    try:
        # [ATUALIZADO] ThreadedConnectionPool para maior robustez
        return psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=30,
            host=conexao.host, port=conexao.port,
            database=conexao.database, user=conexao.user, password=conexao.password,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5
        )
    except Exception as e:
        st.error(f"Erro fatal no Pool de Conexão: {e}")
        return None

@contextlib.contextmanager
def get_db_connection():
    """
    Gerenciador de contexto robusto com AUTO-RECOVERY para Pool Esgotado.
    """
    pool_obj = get_pool()
    if not pool_obj:
        yield None
        return
    
    conn = None
    try:
        # Tenta obter conexão
        try:
            conn = pool_obj.getconn()
        except (psycopg2.pool.PoolError, IndexError):
            # Se pool estiver cheio/travado, reinicia
            st.warning("⚠️ Pool de conexões esgotado. Reiniciando pool...")
            get_pool.clear()
            pool_obj = get_pool()
            conn = pool_obj.getconn()

        # Health Check
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        except (psycopg2.InterfaceError, psycopg2.OperationalError, psycopg2.DatabaseError):
            if conn:
                try: pool_obj.putconn(conn, close=True)
                except: pass
            conn = pool_obj.getconn()

        yield conn

    except Exception as e:
        st.error(f"Falha na comunicação com o banco: {e}")
        if conn:
            try: pool_obj.putconn(conn, close=True)
            except: pass
        yield None
    finally:
        if conn:
            try: pool_obj.putconn(conn)
            except: pass

def ler_dados_seguro(query, params=None):
    """
    Executa pd.read_sql com sistema de retentativa automática (Retry)
    """
    max_tentativas = 3
    for i in range(max_tentativas):
        try:
            with get_db_connection() as conn:
                if not conn: return pd.DataFrame()
                if params:
                    return pd.read_sql(query, conn, params=tuple(params))
                else:
                    return pd.read_sql(query, conn)
        except Exception as e:
            msg = str(e)
            if "SSL" in msg or "EOF" in msg or "terminating" in msg or "closed" in msg or "pool" in msg:
                time.sleep(0.5)
                if i == max_tentativas - 1:
                    st.error(f"Erro de conexão persistente: {e}")
                    return pd.DataFrame()
                continue
            else:
                st.error(f"Erro SQL: {e}")
                return pd.DataFrame()
    return pd.DataFrame()

# ==============================================================================
# 2. FUNÇÕES AUXILIARES E REGRAS
# ==============================================================================

def buscar_clientes_vinculados_grupo():
    """
    Busca clientes vinculados à mesma empresa ou grupo.
    """
    id_usuario = st.session_state.get('usuario_id')
    
    # Query Base
    base_sql = "SELECT id, nome, nome_empresa FROM admin.clientes WHERE status = 'ATIVO' ORDER BY nome"
    
    # 1. Se for admin, retorna todos via leitura segura
    nivel = st.session_state.get('usuario_nivel', '')
    if 'admin' in nivel.lower() or not id_usuario:
        return ler_dados_seguro(base_sql)
            
    # 2. Filtra por vinculo do usuário
    # Precisamos ler todos primeiro ou fazer query especifica. 
    # Para manter consistência com lógica anterior, buscamos o vinculo especifico.
    with get_db_connection() as conn:
        if not conn: return pd.DataFrame()
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, nome_empresa FROM admin.clientes WHERE id_usuario_vinculo = %s LIMIT 1", (id_usuario,))
            res = cur.fetchone()
            
            if res:
                meu_cli_id = res[0]
                minha_empresa = res[1]
                
                # Usa ler_dados_seguro com filtro SQL direto para ser mais eficiente
                if minha_empresa:
                    return ler_dados_seguro(
                        "SELECT id, nome, nome_empresa FROM admin.clientes WHERE status = 'ATIVO' AND nome_empresa = %s ORDER BY nome", 
                        [minha_empresa]
                    )
                else:
                    return ler_dados_seguro(
                        "SELECT id, nome, nome_empresa FROM admin.clientes WHERE status = 'ATIVO' AND id = %s ORDER BY nome", 
                        [meu_cli_id]
                    )
            else:
                return pd.DataFrame(columns=['id', 'nome'])
        except:
            return ler_dados_seguro(base_sql) # Fallback

# ==============================================================================
# 3. CARREGAMENTO DE DADOS (USANDO LER_DADOS_SEGURO)
# ==============================================================================

def carregar_custos(id_cliente):
    sql = """
        SELECT nome_produto as "Produto", 
               origem_custo as "Origem", 
               valor_custo as "Custo Atual"
        FROM cliente.valor_custo_carteira_cliente 
        WHERE id_cliente = %s 
        ORDER BY nome_produto
    """
    return ler_dados_seguro(sql, [str(id_cliente)])

def carregar_pedidos(id_cliente, filtros):
    base_sql = """
        SELECT codigo as "Código", 
               nome_produto as "Produto", 
               data_criacao as "Data", 
               status as "Status", 
               valor_total as "Valor" 
        FROM admin.pedidos 
        WHERE id_cliente = %s
    """
    params = [int(id_cliente)]
    
    if filtros.get('nome'):
        base_sql += " AND (codigo ILIKE %s OR nome_produto ILIKE %s)"
        params.extend([f"%{filtros['nome']}%", f"%{filtros['nome']}%"])
        
    if filtros.get('status') and filtros['status'] != 'Todos':
        base_sql += " AND status = %s"
        params.append(filtros['status'])
        
    base_sql += " ORDER BY data_criacao DESC"
    return ler_dados_seguro(base_sql, params)

def carregar_tarefas(id_cliente, filtros):
    base_sql = """
        SELECT t.id, 
               p.codigo as "Pedido",
               p.nome_produto as "Produto",
               t.data_previsao as "Previsão", 
               t.status as "Status", 
               t.observacao_tarefa as "Obs"
        FROM admin.tarefas t
        LEFT JOIN admin.pedidos p ON t.id_pedido = p.id
        WHERE t.id_cliente = %s
    """
    params = [int(id_cliente)]
    
    if filtros.get('nome'):
        base_sql += " AND (p.nome_produto ILIKE %s OR t.observacao_tarefa ILIKE %s)"
        params.extend([f"%{filtros['nome']}%", f"%{filtros['nome']}%"])
        
    if filtros.get('status') and filtros['status'] != 'Todos':
        base_sql += " AND t.status = %s"
        params.append(filtros['status'])

    base_sql += " ORDER BY t.data_previsao DESC"
    return ler_dados_seguro(base_sql, params)

def carregar_renovacao(id_cliente, filtros):
    base_sql = """
        SELECT rf.data_previsao as "Previsão", 
               p.nome_produto as "Produto",
               rf.status as "Status", 
               rf.observacao as "Obs"
        FROM admin.renovacao_feedback rf
        INNER JOIN admin.pedidos p ON rf.id_pedido = p.id
        WHERE p.id_cliente = %s
    """
    params = [int(id_cliente)]
    
    if filtros.get('status') and filtros['status'] != 'Todos':
        base_sql += " AND rf.status = %s"
        params.append(filtros['status'])

    base_sql += " ORDER BY rf.data_previsao DESC"
    return ler_dados_seguro(base_sql, params)

def carregar_extrato(id_cliente, filtros):
    base_sql = """
        SELECT data_lancamento as "Data", 
               tipo_lancamento as "Tipo", 
               produto_vinculado as "Produto/Motivo", 
               valor_lancado as "Valor", 
               saldo_novo as "Saldo Final"
        FROM cliente.extrato_carteira_por_produto 
        WHERE id_cliente = %s
    """
    params = [str(id_cliente)]
    
    if filtros.get('data_inicio'):
        base_sql += " AND data_lancamento >= %s"
        params.append(filtros['data_inicio'])
    if filtros.get('data_fim'):
        base_sql += " AND data_lancamento <= %s"
        params.append(f"{filtros['data_fim']} 23:59:59")
    if filtros.get('produto'):
        base_sql += " AND produto_vinculado ILIKE %s"
        params.append(f"%{filtros['produto']}%")
        
    base_sql += " ORDER BY data_lancamento DESC"
    return ler_dados_seguro(base_sql, params)

# ==============================================================================
# 4. INTERFACE DO USUÁRIO
# ==============================================================================

def app_relatorios():
    # CSS Global
    st.markdown("""
        <style>
        .stButton > button {
            padding: 0px 10px !important; 
            line-height: 1.2 !important;
            border-radius: 4px;
        }
        .stButton > button:hover {
            background-color: #ffe6e6 !important; 
            color: black !important;
            border-color: #ffcccc !important;
        }
        </style>
    """, unsafe_allow_html=True)

    st.title("📊 Relatórios Gerenciais")

    # --- 0. SELEÇÃO DE VÍNCULO ---
    df_clientes = buscar_clientes_vinculados_grupo()
    if df_clientes.empty:
        st.warning("Nenhum cliente vinculado encontrado para seu usuário.")
        return

    col_sel, col_btn = st.columns([3, 1])
    
    opcoes = {row['id']: f"{row['nome']} {(' - ' + row['nome_empresa']) if row['nome_empresa'] else ''}" for _, row in df_clientes.iterrows()}
    
    idx_default = 0
    
    id_cliente = col_sel.selectbox(
        "Selecione o Cliente / Empresa:", 
        options=list(opcoes.keys()), 
        format_func=lambda x: opcoes[x],
        index=idx_default
    )

    if col_btn.button("🧹 Limpar Filtros", use_container_width=True):
        for k in list(st.session_state.keys()):
            if k.startswith('filtro_'): del st.session_state[k]
        st.rerun()

    st.divider()

    # --- 1. SELETOR DE RELATÓRIO ---
    tipo_relatorio = st.radio(
        "Escolha o Tipo de Relatório:", 
        ["1. Relação de Custos", "2. Lista de Pedidos", "3. Lista de Tarefas", "4. Lista de Renovação", "5. Extrato Financeiro"],
        horizontal=True
    )

    filtros = {}
    df_resultado = pd.DataFrame()

    # --- 2 a 5. LÓGICA POR RELATÓRIO ---
    
    # RELATÓRIO 1: CUSTOS
    if "Custos" in tipo_relatorio:
        with st.expander("🛠️ Visualização", expanded=True):
            st.info("Produtos contratados e custos vinculados à carteira.")
            df_resultado = carregar_custos(id_cliente)

    # RELATÓRIO 2: PEDIDOS
    elif "Pedidos" in tipo_relatorio:
        with st.expander("🛠️ Filtros e Visualização", expanded=True):
            c1, c2 = st.columns(2)
            filtros['nome'] = c1.text_input("Nome/Código:", key='filtro_nome_ped')
            filtros['status'] = c2.selectbox("Status:", ["Todos", "Solicitado", "Pendente", "Pago", "Cancelado"], key='filtro_status_ped')
            df_resultado = carregar_pedidos(id_cliente, filtros)

    # RELATÓRIO 3: TAREFAS
    elif "Tarefas" in tipo_relatorio:
        with st.expander("🛠️ Filtros e Visualização", expanded=True):
            c1, c2 = st.columns(2)
            filtros['nome'] = c1.text_input("Título/Obs:", key='filtro_nome_tar')
            filtros['status'] = c2.selectbox("Status:", ["Todos", "Solicitado", "Em execução", "Concluído", "Cancelado"], key='filtro_status_tar')
            df_resultado = carregar_tarefas(id_cliente, filtros)

    # RELATÓRIO 4: RENOVAÇÃO
    elif "Renovação" in tipo_relatorio:
        with st.expander("🛠️ Filtros e Visualização", expanded=True):
            c1, c2 = st.columns(2)
            filtros['status'] = c1.selectbox("Status Renovação:", ["Todos", "Entrada", "Em Análise", "Concluído", "Pendente"], key='filtro_status_ren')
            df_resultado = carregar_renovacao(id_cliente, filtros)

    # RELATÓRIO 5: EXTRATO
    elif "Extrato" in tipo_relatorio:
        with st.expander("🛠️ Filtros e Visualização", expanded=True):
            c1, c2, c3 = st.columns(3)
            filtros['data_inicio'] = c1.date_input("De:", value=date(date.today().year, 1, 1), key='filtro_dt_ini', format="DD/MM/YYYY")
            filtros['data_fim'] = c2.date_input("Até:", value=date.today(), key='filtro_dt_fim', format="DD/MM/YYYY")
            filtros['produto'] = c3.text_input("Produto/Motivo:", key='filtro_prod')
            
            df_resultado = carregar_extrato(id_cliente, filtros)
            
            if not df_resultado.empty:
                st.dataframe(
                    df_resultado.style.format({
                        "Valor": "R$ {:.2f}", 
                        "Saldo Final": "R$ {:.2f}",
                        "Data": lambda x: pd.to_datetime(x).strftime('%d/%m/%Y %H:%M') if pd.notnull(x) else ""
                    }),
                    use_container_width=True
                )
            else:
                st.info("Nenhum lançamento no período.")

    # --- EXIBIÇÃO PADRÃO E FORMATAÇÃO ---
    if "Extrato" not in tipo_relatorio and not df_resultado.empty:
        df_show = df_resultado.copy()
        cols_para_formatar = df_show.columns.tolist()
        
        for col in cols_para_formatar:
            # 1. Datas
            if 'Data' in col or 'Previsão' in col:
                try:
                    df_show[col] = pd.to_datetime(df_show[col], errors='coerce')
                    df_show[col] = df_show[col].dt.strftime('%d/%m/%Y').fillna("")
                except: pass
            
            # 2. Valores Financeiros
            if 'Valor' in col or 'Custo' in col or 'Saldo' in col:
                try:
                    if v:
                        df_show[col] = df_show[col].apply(lambda x: v.ValidadorFinanceiro.para_tela(x) if x is not None else "R$ 0,00")
                    else:
                        df_show[col] = df_show[col].apply(lambda x: f"R$ {float(x):,.2f}" if x is not None else "")
                except: pass

        st.dataframe(df_show, use_container_width=True, hide_index=True)
    
    elif "Extrato" not in tipo_relatorio:
        st.warning("Nenhum registro encontrado.")

    # --- BOTÃO VOLTAR ---
    st.write("---")
    if st.button("⬅️ Voltar ao Início", use_container_width=True):
        st.session_state['modo_visualizacao'] = None
        st.rerun()

if __name__ == "__main__":
    app_relatorios()