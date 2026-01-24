import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import pool
import contextlib
import os
import sys
from datetime import datetime, date
from fpdf import FPDF
import io

# ==============================================================================
# 0. CONFIGURAÇÃO E CONEXÃO (Padrão do Projeto)
# ==============================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    import conexao
except ImportError:
    conexao = None

# Cache da Conexão (Pool)
@st.cache_resource
def get_pool():
    if not conexao: return None
    try:
        return psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=20,
            host=conexao.host, port=conexao.port,
            database=conexao.database, user=conexao.user, password=conexao.password,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5
        )
    except Exception as e:
        st.error(f"Erro de Conexão: {e}")
        return None

@contextlib.contextmanager
def get_db_connection():
    pool_obj = get_pool()
    if not pool_obj:
        yield None
        return
    conn = pool_obj.getconn()
    try:
        conn.rollback()
        yield conn
        pool_obj.putconn(conn)
    except Exception:
        pool_obj.putconn(conn, close=True)
        yield None

# ==============================================================================
# 1. FUNÇÕES AUXILIARES E PDF
# ==============================================================================

def criar_pdf(df, titulo="Relatorio"):
    """Gera um PDF simples a partir de um DataFrame"""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=10)
    
    # Título
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(0, 10, txt=titulo, ln=True, align='C')
    pdf.ln(10)
    
    if df.empty:
        pdf.cell(0, 10, "Sem dados para exibir.", ln=True)
        return pdf.output(dest='S').encode('latin-1', 'ignore')

    # Cabeçalho
    pdf.set_font("Arial", 'B', 9)
    cols = df.columns
    # Largura dinâmica: 190mm total / qtd colunas
    col_width = 190 / len(cols) if len(cols) > 0 else 190
    
    for col in cols:
        # Tenta limpar nomes técnicos
        col_name = str(col).replace("_", " ").upper()
        pdf.cell(col_width, 8, col_name[:15], border=1, align='C')
    pdf.ln()
    
    # Dados
    pdf.set_font("Arial", size=8)
    for index, row in df.iterrows():
        for col in cols:
            val = str(row[col])
            # Remove caracteres incompatíveis com latin-1 se necessário
            val_safe = val.encode('latin-1', 'replace').decode('latin-1')
            pdf.cell(col_width, 8, val_safe[:25], border=1)
        pdf.ln()
        
    return pdf.output(dest='S').encode('latin-1', 'ignore')

def buscar_clientes_vinculados_grupo():
    """
    Busca clientes vinculados à mesma empresa ou grupo.
    Regra: 
    1. Se usuário tem vínculo direto, traz o cliente e outros com mesmo 'nome_empresa'.
    2. Se não tem vínculo (Admin), traz todos.
    """
    id_usuario = st.session_state.get('usuario_id')
    
    # Query Base
    base_sql = "SELECT id, nome, nome_empresa FROM admin.clientes WHERE status = 'ATIVO'"
    
    with get_db_connection() as conn:
        if not conn: return pd.DataFrame()
        
        # 1. Tenta achar vínculo direto
        df_todos = pd.read_sql(base_sql + " ORDER BY nome", conn)
        
        # Se for admin (sem id_usuario na session ou flag admin), retorna todos
        nivel = st.session_state.get('usuario_nivel', '')
        if 'admin' in nivel.lower() or not id_usuario:
            return df_todos
            
        # 2. Filtra por vinculo do usuário
        try:
            # Busca qual cliente esse usuário representa
            cur = conn.cursor()
            cur.execute("SELECT id, nome_empresa FROM admin.clientes WHERE id_usuario_vinculo = %s LIMIT 1", (id_usuario,))
            res = cur.fetchone()
            
            if res:
                meu_cli_id = res[0]
                minha_empresa = res[1]
                
                # Se tiver empresa, traz todos da empresa. Se não, só ele mesmo.
                if minha_empresa:
                    return df_todos[df_todos['nome_empresa'] == minha_empresa]
                else:
                    return df_todos[df_todos['id'] == meu_cli_id]
            else:
                # Usuário sem cliente vinculado vê vazio ou todos? (Assumindo vazio por segurança)
                return pd.DataFrame(columns=['id', 'nome'])
        except:
            return df_todos # Fallback

# ==============================================================================
# 2. FUNÇÕES DE BUSCA DE DADOS (QUERIES REAIS)
# ==============================================================================

def carregar_custos(id_cliente):
    sql = """
        SELECT nome_produto as "Produto", 
               origem_custo as "Origem", 
               valor_custo as "Custo Atual (R$)"
        FROM cliente.valor_custo_carteira_cliente 
        WHERE id_cliente = %s -- id_cliente é text nesta tabela (segundo schema fornecido)
        ORDER BY nome_produto
    """
    with get_db_connection() as conn:
        if conn:
            # Tenta converter ID para string pois o schema indica 'text'
            return pd.read_sql(sql, conn, params=(str(id_cliente),))
    return pd.DataFrame()

def carregar_pedidos(id_cliente, filtros):
    # Schema: admin.pedidos
    base_sql = """
        SELECT codigo as "Código", 
               nome_produto as "Produto", 
               data_criacao as "Data", 
               status as "Status", 
               valor_total as "Valor (R$)" 
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

    with get_db_connection() as conn:
        if conn:
            return pd.read_sql(base_sql, conn, params=tuple(params))
    return pd.DataFrame()

def carregar_tarefas(id_cliente, filtros):
    # Schema: admin.tarefas
    # Join com Pedidos para pegar nome do produto se necessário
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

    with get_db_connection() as conn:
        if conn:
            return pd.read_sql(base_sql, conn, params=tuple(params))
    return pd.DataFrame()

def carregar_renovacao(id_cliente, filtros):
    # Schema: admin.renovacao_feedback
    # Precisa de JOIN com pedidos para filtrar por cliente, pois a tabela admin.renovacao_feedback
    # tem id_pedido mas não tem id_cliente direto no schema fornecido
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

    with get_db_connection() as conn:
        if conn:
            return pd.read_sql(base_sql, conn, params=tuple(params))
    return pd.DataFrame()

def carregar_extrato(id_cliente, filtros):
    # Schema: cliente.extrato_carteira_por_produto
    # Nota: id_cliente é TEXT nesta tabela conforme schema fornecido
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

    with get_db_connection() as conn:
        if conn:
            return pd.read_sql(base_sql, conn, params=tuple(params))
    return pd.DataFrame()

# ==============================================================================
# 3. INTERFACE DO USUÁRIO
# ==============================================================================

def app_relatorios():
    # CSS Global para Botões e Tabelas
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
    
    # Default: primeiro da lista (que pela lógica de busca é o próprio vinculado se existir)
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
    titulo_relatorio = ""

    # --- 2 a 5. LÓGICA POR RELATÓRIO ---
    
    # RELATÓRIO 1: CUSTOS
    if "Custos" in tipo_relatorio:
        titulo_relatorio = f"Custos - {opcoes[id_cliente]}"
        with st.expander("🛠️ Visualização", expanded=True):
            st.info("Produtos contratados e custos vinculados à carteira.")
            df_resultado = carregar_custos(id_cliente)

    # RELATÓRIO 2: PEDIDOS
    elif "Pedidos" in tipo_relatorio:
        titulo_relatorio = f"Pedidos - {opcoes[id_cliente]}"
        with st.expander("🛠️ Filtros e Visualização", expanded=True):
            c1, c2 = st.columns(2)
            filtros['nome'] = c1.text_input("Nome/Código:", key='filtro_nome_ped')
            filtros['status'] = c2.selectbox("Status:", ["Todos", "Solicitado", "Pendente", "Pago", "Cancelado"], key='filtro_status_ped')
            df_resultado = carregar_pedidos(id_cliente, filtros)

    # RELATÓRIO 3: TAREFAS
    elif "Tarefas" in tipo_relatorio:
        titulo_relatorio = f"Tarefas - {opcoes[id_cliente]}"
        with st.expander("🛠️ Filtros e Visualização", expanded=True):
            c1, c2 = st.columns(2)
            filtros['nome'] = c1.text_input("Título/Obs:", key='filtro_nome_tar')
            filtros['status'] = c2.selectbox("Status:", ["Todos", "Solicitado", "Em execução", "Concluído", "Cancelado"], key='filtro_status_tar')
            df_resultado = carregar_tarefas(id_cliente, filtros)

    # RELATÓRIO 4: RENOVAÇÃO
    elif "Renovação" in tipo_relatorio:
        titulo_relatorio = f"Renovações - {opcoes[id_cliente]}"
        with st.expander("🛠️ Filtros e Visualização", expanded=True):
            c1, c2 = st.columns(2)
            filtros['status'] = c1.selectbox("Status Renovação:", ["Todos", "Entrada", "Em Análise", "Concluído", "Pendente"], key='filtro_status_ren')
            df_resultado = carregar_renovacao(id_cliente, filtros)

    # RELATÓRIO 5: EXTRATO
    elif "Extrato" in tipo_relatorio:
        titulo_relatorio = f"Extrato - {opcoes[id_cliente]}"
        with st.expander("🛠️ Filtros e Visualização", expanded=True):
            c1, c2, c3 = st.columns(3)
            filtros['data_inicio'] = c1.date_input("De:", value=date(date.today().year, 1, 1), key='filtro_dt_ini')
            filtros['data_fim'] = c2.date_input("Até:", value=date.today(), key='filtro_dt_fim')
            filtros['produto'] = c3.text_input("Produto/Motivo:", key='filtro_prod')
            
            df_resultado = carregar_extrato(id_cliente, filtros)
            
            if not df_resultado.empty:
                # Tratamento visual
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

    # --- EXIBIÇÃO PADRÃO (EXCETO EXTRATO QUE JÁ MOSTROU) ---
    if "Extrato" not in tipo_relatorio and not df_resultado.empty:
        # Formatação genérica de datas
        for col in df_resultado.columns:
            if 'Data' in col or 'Previsão' in col:
                try:
                    df_resultado[col] = pd.to_datetime(df_resultado[col]).dt.strftime('%d/%m/%Y')
                except: pass
        
        st.dataframe(df_resultado, use_container_width=True, hide_index=True)
    elif "Extrato" not in tipo_relatorio:
        st.warning("Nenhum registro encontrado.")

    # --- BOTÃO PDF ---
    if not df_resultado.empty:
        st.write("")
        col_pdf, _ = st.columns([1, 4])
        try:
            pdf_bytes = criar_pdf(df_resultado, titulo_relatorio)
            col_pdf.download_button(
                label="📄 Baixar PDF",
                data=pdf_bytes,
                file_name=f"{titulo_relatorio.replace(' ', '_').lower()}.pdf",
                mime="application/pdf",
                type="primary"
            )
        except Exception as e:
            st.error(f"Erro ao gerar PDF: {e}")

    # --- BOTÃO VOLTAR ---
    st.write("---")
    if st.button("⬅️ Voltar ao Início", use_container_width=True):
        st.session_state['modo_visualizacao'] = None
        st.rerun()

if __name__ == "__main__":
    app_relatorios()