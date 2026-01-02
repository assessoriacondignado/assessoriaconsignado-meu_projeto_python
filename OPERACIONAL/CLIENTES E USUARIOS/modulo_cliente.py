import streamlit as st
import pandas as pd
import psycopg2
import bcrypt
import re
import time
from datetime import datetime, date, timedelta

try: 
    import conexao
except ImportError: 
    st.error("Erro crítico: conexao.py não encontrado.")

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password
        )
    except: return None

# =============================================================================
# 1. FUNÇÕES AUXILIARES E DB
# =============================================================================

def formatar_cnpj(v):
    v = re.sub(r'\D', '', str(v))
    return f"{v[:2]}.{v[2:5]}.{v[5:8]}/{v[8:12]}-{v[12:]}" if len(v) == 14 else v

def limpar_formatacao_texto(texto):
    if not texto: return ""
    return str(texto).replace('*', '').strip()

def sanitizar_nome_tabela(nome):
    s = str(nome).lower().strip()
    s = re.sub(r'[^a-z0-9]', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.strip('_')

def listar_origens_para_selecao():
    conn = get_conn()
    lista = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT origem FROM conexoes.fatorconferi_origem_consulta_fator ORDER BY origem ASC")
            lista = [row[0] for row in cur.fetchall()]
            conn.close()
        except:
            if conn: conn.close()
    return lista

def listar_usuarios_para_selecao():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome, cpf FROM clientes_usuarios WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_clientes_para_selecao():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome, cpf FROM admin.clientes ORDER BY nome", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

# =============================================================================
# 2. GESTÃO DE CARTEIRAS E TRANSAÇÕES
# =============================================================================

def listar_tabelas_transacao_reais():
    conn = get_conn()
    tabelas = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'cliente' AND table_name LIKE 'transacoes_%%' ORDER BY table_name")
            tabelas = [row[0] for row in cur.fetchall()]
            conn.close()
        except:
            if conn: conn.close()
    return tabelas

def carregar_dados_tabela_dinamica(nome_tabela):
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql(f"SELECT * FROM cliente.{nome_tabela} ORDER BY id DESC", conn)
            conn.close(); return df
        except:
            if conn: conn.close()
    return pd.DataFrame()

def salvar_alteracoes_tabela_dinamica(nome_tabela, df_original, df_editado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        for index, row in df_editado.iterrows():
            colunas = row.index.tolist()
            if 'id' in colunas: colunas.remove('id')
            set_clause = ", ".join([f"{col} = %s" for col in colunas])
            valores = [row[col] for col in colunas]
            valores.append(row['id'])
            cur.execute(f"UPDATE cliente.{nome_tabela} SET {set_clause} WHERE id = %s", valores)
        conn.commit(); conn.close(); return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        if conn: conn.close()
        return False

def salvar_cliente_carteira_lista(cpf, nome, carteira, custo, origem_custo):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cpf_limpo = re.sub(r'\D', '', str(cpf))
        query_vinculo = """
            SELECT u.cpf, u.nome FROM admin.clientes c
            JOIN clientes_usuarios u ON c.id_usuario_vinculo = u.id
            WHERE regexp_replace(c.cpf, '[^0-9]', '', 'g') = %s LIMIT 1
        """
        cur.execute(query_vinculo, (cpf_limpo,))
        res_v = cur.fetchone()
        cpf_u = res_v[0] if res_v else None
        nome_u = res_v[1] if res_v else None
        cur.execute("""
            INSERT INTO cliente.cliente_carteira_lista (cpf_cliente, nome_cliente, nome_carteira, custo_carteira, cpf_usuario, nome_usuario, origem_custo) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (cpf, nome, carteira, custo, cpf_u, nome_u, origem_custo))
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

def excluir_cliente_carteira_lista(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor(); cur.execute("DELETE FROM cliente.cliente_carteira_lista WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

def salvar_nova_carteira_sistema(id_prod, nome_prod, nome_carteira, status):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        sufixo = sanitizar_nome_tabela(nome_carteira)
        nome_tab = f"cliente.transacoes_{sufixo}"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {nome_tab} (id SERIAL PRIMARY KEY, cpf_cliente VARCHAR(20), nome_cliente VARCHAR(255), motivo VARCHAR(255), origem_lancamento VARCHAR(100), data_transacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP, tipo_lancamento VARCHAR(50), valor NUMERIC(10, 2), saldo_anterior NUMERIC(10, 2), saldo_novo NUMERIC(10, 2))")
        cur.execute("INSERT INTO cliente.carteiras_config (id_produto, nome_produto, nome_carteira, nome_tabela_transacoes, status) VALUES (%s, %s, %s, %s, %s)", (id_prod, nome_prod, nome_carteira, nome_tab, status))
        conn.commit(); conn.close(); return True
    except:
        if conn: conn.close()
        return False

def listar_produtos_para_selecao():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome FROM produtos_servicos WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_todas_carteiras_ativas():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, nome_carteira, nome_tabela_transacoes FROM cliente.carteiras_config WHERE status = 'ATIVO' ORDER BY nome_carteira", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_cliente_carteira_lista():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT * FROM cliente.cliente_carteira_lista ORDER BY nome_cliente", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

# =============================================================================
# 3. INTERFACE PRINCIPAL
# =============================================================================

def app_clientes():
    st.markdown("## 👥 Central de Clientes e Usuários")
    tabs = st.tabs(["🏢 Clientes", "👤 Usuários", "⚙️ Parâmetros", "💼 Carteira", "📊 Relatórios"])

    with tabs[0]:
        st.write("Conteúdo da aba Clientes")

    with tabs[1]:
        st.write("Conteúdo da aba Usuários")

    with tabs[2]:
        with st.expander("📂 Lista de Carteiras", expanded=False):
            with st.container(border=True):
                st.caption("Nova Carteira")
                c1, c2, c3, c4, c5 = st.columns([1.5, 2, 1.5, 1, 1])
                n_cpf = c1.text_input("CPF", key="n_cpf_l")
                df_clis = listar_clientes_para_selecao()
                n_nome_sel = c2.selectbox("Cliente", options=[""] + df_clis.apply(lambda x: f"{x['nome']} | CPF: {x['cpf']}", axis=1).tolist(), key="n_nome_sel")
                if n_nome_sel: n_cpf = n_nome_sel.split(" | CPF: ")[1]
                
                df_c_at = listar_todas_carteiras_ativas()
                n_cart = c3.selectbox("Carteira", options=[""] + df_c_at['nome_carteira'].tolist(), key="n_cart_l")
                n_custo = c4.number_input("Custo", key="n_custo_l")
                
                orgs = listar_origens_para_selecao()
                c_origem = st.selectbox("Origem Custo", options=[""] + orgs, key="n_orig_l")
                
                if c5.button("➕", key="add_cart_btn"):
                    if n_cpf and n_cart:
                        salvar_cliente_carteira_lista(n_cpf, n_nome_sel.split(" | ")[0], n_cart, n_custo, c_origem)
                        st.rerun()

            df_lista = listar_cliente_carteira_lista()
            if not df_lista.empty:
                st.dataframe(df_lista, use_container_width=True, hide_index=True)

    with tabs[3]:
        st.markdown("### 💼 Gestão de Carteira")
        
        with st.expander("📂 Nova Carteira (Produtos)", expanded=False):
            st.info("Cria carteiras e tabelas automaticamente.")
            df_pds = listar_produtos_para_selecao()
            if not df_pds.empty:
                with st.container(border=True):
                    cc1, cc2, cc3, cc4 = st.columns([3, 3, 2, 2])
                    idx_p = cc1.selectbox("Produto", range(len(df_pds)), format_func=lambda x: df_pds.iloc[x]['nome'])
                    nome_cart_in = cc2.text_input("Nome Carteira", key="nome_cart_new")
                    status_cart_in = cc3.selectbox("Status", ["ATIVO", "INATIVO"], key="status_cart_new")
                    if cc4.button("💾 Criar Carteira", type="primary", key="btn_criar_cart"):
                        if nome_cart_in:
                            salvar_nova_carteira_sistema(int(df_pds.iloc[idx_p]['id']), df_pds.iloc[idx_p]['nome'], nome_cart_in, status_cart_in)
                            st.rerun()

        st.divider()
        st.markdown("#### 📑 Edição de Conteúdo das Tabelas")
        st.caption("Selecione uma tabela física para editar os lançamentos diretamente.")
        
        lista_tabs = listar_tabelas_transacao_reais()
        if lista_tabs:
            tab_sel = st.selectbox("Escolha a Tabela", options=lista_tabs, key="sel_tab_edit_real")
            if tab_sel:
                df_edit = carregar_dados_tabela_dinamica(tab_sel)
                if not df_edit.empty:
                    st.info(f"Editando: `cliente.{tab_sel}`")
                    df_resultado = st.data_editor(df_edit, key=f"editor_{tab_sel}", use_container_width=True, hide_index=True, disabled=["id", "data_transacao"])
                    if st.button("💾 Salvar Planilha", type="primary", key="btn_save_planilha"):
                        if salvar_alteracoes_tabela_dinamica(tab_sel, df_edit, df_resultado):
                            st.success("Atualizado!")
                            time.sleep(1); st.rerun()
                else:
                    st.warning("Tabela sem dados.")
        else:
            st.info("Nenhuma tabela encontrada.")

    with tabs[4]:
        st.write("Relatórios")

if __name__ == "__main__":
    app_clientes()