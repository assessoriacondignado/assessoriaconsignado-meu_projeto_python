import streamlit as st
import pandas as pd
import psycopg2
import os
import re
from datetime import datetime, date
import modulo_wapi # Integração

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

# --- FUNÇÕES AUXILIARES ---
def listar_modelos_mensagens():
    """Busca os modelos de mensagem cadastrados no W-API para este módulo"""
    conn = get_conn()
    if conn:
        try:
            query = "SELECT chave_status FROM wapi_templates WHERE modulo = 'RENOVACAO' ORDER BY chave_status ASC"
            df = pd.read_sql(query, conn)
            conn.close()
            return df['chave_status'].tolist()
        except:
            conn.close()
    return []

# --- FUNÇÕES DE BANCO ---

def buscar_pedidos_disponiveis():
    conn = get_conn()
    if conn:
        query = "SELECT id, codigo, nome_cliente, nome_produto, telefone_cliente FROM pedidos ORDER BY data_criacao DESC"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def listar_rf():
    conn = get_conn()
    if conn:
        # Join com clientes_usuarios para trazer dados de contato atualizados
        query = """
            SELECT rf.id, rf.id_pedido, rf.data_criacao, rf.data_previsao, rf.status, rf.observacao,
                   p.codigo as codigo_pedido, p.nome_cliente, p.nome_produto, p.categoria_produto,
                   c.cpf as cpf_cliente, c.telefone as telefone_cliente, c.email as email_cliente
            FROM renovacao_feedback rf
            JOIN pedidos p ON rf.id_pedido = p.id
            LEFT JOIN clientes_usuarios c ON p.id_cliente = c.id
            ORDER BY rf.data_criacao DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def buscar_historico_rf(id_rf):
    conn = get_conn()
    if conn:
        query = "SELECT data_mudanca, status_novo, observacao FROM renovacao_feedback_historico WHERE id_rf = %s ORDER BY data_mudanca DESC"
        df = pd.read_sql(query, conn, params=(int(id_rf),))
        conn.close()
        return df
    return pd.DataFrame()

def criar_registro_rf(id_pedido, data_prev, obs, dados_pedido, avisar):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO renovacao_feedback (id_pedido, data_previsao, observacao, status) VALUES (%s, %s, %s, 'Entrada') RETURNING id", 
                        (id_pedido, data_prev, obs))
            id_novo = cur.fetchone()[0]
            cur.execute("INSERT INTO renovacao_feedback_historico (id_rf, status_novo, observacao) VALUES (%s, 'Entrada', 'Registro criado')", (id_novo,))
            conn.commit()
            conn.close()
            
            # Notificação opcional na criação (se desejar implementar)
            
            return True
        except Exception as e:
            st.error(f"Erro: {e}")
    return False

def atualizar_status_rf(id_rf, novo_status, obs, dados_rf, avisar, modelo_msg_escolhido="Automático (Padrão)"):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE renovacao_feedback SET status=%s, data_atualizacao=NOW() WHERE id=%s", (novo_status, id_rf))
            cur.execute("INSERT INTO renovacao_feedback_historico (id_rf, status_novo, observacao) VALUES (%s, %s, %s)", (id_rf, novo_status, obs))
            conn.commit()
            conn.close()
            
            if avisar and dados_rf.get('telefone_cliente'):
                instancia = modulo_wapi.buscar_instancia_ativa()
                if instancia:
                    # Seleção de Template
                    if modelo_msg_escolhido and modelo_msg_escolhido != "Automático (Padrão)":
                        chave = modelo_msg_escolhido
                    else:
                        chave = novo_status.lower().replace(" ", "_")
                    
                    template = modulo_wapi.buscar_template("RENOVACAO", chave)
                    
                    if template:
                        msg = template.replace("{nome}", str(dados_rf['nome_cliente']).split()[0]) \
                                      .replace("{pedido}", str(dados_rf['codigo_pedido'])) \
                                      .replace("{status}", novo_status) \
                                      .replace("{produto}", str(dados_rf['nome_produto'])) \
                                      .replace("{obs_status}", obs)
                        modulo_wapi.enviar_msg_api(instancia[0], instancia[1], dados_rf['telefone_cliente'], msg)
            return True
        except Exception as e: st.error(f"Erro: {e}")
    return False

def editar_rf_dados(id_rf, nova_data, nova_obs):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE renovacao_feedback SET data_previsao=%s, observacao=%s WHERE id=%s", (nova_data, nova_obs, id_rf))
            conn.commit()
            conn.close()
            return True
        except: return False
    return False

def excluir_rf_db(id_rf):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM renovacao_feedback WHERE id = %s", (id_rf,))
            conn.commit()
            conn.close()
            return True
        except: return False
    return False

# --- DIALOGS ---
@st.dialog("👤 Dados do Cliente")
def ver_cliente(nome, cpf, tel, email):
    st.write(f"**Nome:** {nome}")
    st.write(f"**CPF:** {cpf}")
    st.write(f"**Telefone:** {tel}")
    st.write(f"**E-mail:** {email}")

@st.dialog("👁️ Detalhes")
def dialog_visualizar(rf):
    st.markdown(f"### Registro: {rf['codigo_pedido']}")
    st.write(f"**Produto:** {rf['nome_produto']}")
    st.write(f"**Categoria:** {rf['categoria_produto']}")
    st.markdown("---")
    st.write(f"**Status:** {rf['status']}")
    st.write(f"**Previsão:** {pd.to_datetime(rf['data_previsao']).strftime('%d/%m/%Y')}")
    st.info(f"**Observação:**\n{rf['observacao']}")

@st.dialog("➕ Novo Registro")
def dialog_novo_rf():
    df_ped = buscar_pedidos_disponiveis()
    if df_ped.empty:
        st.warning("Nenhum pedido encontrado.")
        return

    opcoes = df_ped.apply(lambda x: f"{x['codigo']} | {x['nome_cliente']} - {x['nome_produto']}", axis=1)
    idx_ped = st.selectbox("Selecione o Pedido", range(len(df_ped)), format_func=lambda x: opcoes[x], index=None)
    
    with st.form("form_novo_rf"):
        d_prev = st.date_input("Data Previsão", value=date.today(), format="DD/MM/YYYY")
        obs = st.text_area("Observação inicial")
        if st.form_submit_button("Criar Registro"):
            if idx_ped is not None:
                id_p = int(df_ped.iloc[idx_ped]['id'])
                if criar_registro_rf(id_p, d_prev, obs, None, False):
                    st.success("Criado!"); st.rerun()
            else:
                st.warning("Selecione um pedido.")

@st.dialog("✏️ Editar Registro")
def dialog_editar(rf):
    st.write(f"Editando: **{rf['codigo_pedido']}**")
    with st.form("form_edit_rf"):
        n_data = st.date_input("Data Previsão", value=pd.to_datetime(rf['data_previsao']), format="DD/MM/YYYY")
        n_obs = st.text_area("Observação", value=rf['observacao'])
        if st.form_submit_button("Salvar"):
            if editar_rf_dados(rf['id'], n_data, n_obs):
                st.success("Atualizado!"); st.rerun()

@st.dialog("🔄 Atualizar Status")
def dialog_status(rf):
    status_opcoes = ["Entrada", "Em Análise", "Concluído", "Pendente", "Cancelado"]
    idx = status_opcoes.index(rf['status']) if rf['status'] in status_opcoes else 0
    
    lista_modelos = listar_modelos_mensagens()
    opcoes_msg = ["Automático (Padrão)"] + lista_modelos

    with st.form("form_st_rf"):
        novo = st.selectbox("Novo Status", status_opcoes, index=idx)
        modelo_escolhido = st.selectbox("Modelo de Mensagem", opcoes_msg, help="Selecione 'Automático' para usar a mensagem padrão do status.")
        obs = st.text_area("Observação da mudança")
        enviar_whats = st.checkbox("Enviar aviso ao cliente?", value=True)
        
        if st.form_submit_button("Atualizar"):
            if atualizar_status_rf(rf['id'], novo, obs, rf, enviar_whats, modelo_escolhido):
                st.success("Status atualizado!"); st.rerun()

@st.dialog("📜 Histórico")
def dialog_historico(id_rf):
    st.write("Histórico de Status:")
    df_h = buscar_historico_rf(id_rf)
    if not df_h.empty:
        df_h.columns = ["Data", "Status", "Obs"]
        st.dataframe(df_h, use_container_width=True, hide_index=True)
    else: st.info("Sem histórico.")

@st.dialog("⚠️ Excluir")
def dialog_excluir(id_rf):
    st.warning("Tem certeza que deseja apagar este registro?")
    if st.button("Sim, confirmar exclusão", type="primary"):
        if excluir_rf_db(id_rf):
            st.success("Apagado!"); st.rerun()

# --- INTERFACE PRINCIPAL ---
def app_renovacao_feedback():
    # Cabeçalho com Botão Novo no Topo (Estilo Pedidos)
    c_t, c_b = st.columns([5, 1])
    c_t.markdown("## 🔄 Renovação e Feedback")
    if c_b.button("➕ Novo Registro", type="primary", use_container_width=False): 
        dialog_novo_rf()

    df = listar_rf()
    
    # --- FILTROS DE PESQUISA (Estilo Pedidos/Tarefas) ---
    with st.expander("🔍 Filtros de Pesquisa", expanded=True):
        # Linha 1
        cf1, cf2, cf3 = st.columns([3, 1.5, 1.5])
        busca_geral = cf1.text_input("🔍 Buscar (Cliente, Produto, Email, Obs)", placeholder="Comece a digitar...")
        
        # Filtro de Status (Padrão: Entrada)
        opcoes_status = df['status'].unique().tolist() if not df.empty else []
        padrao_status = ["Entrada"] if "Entrada" in opcoes_status else None
        f_status = cf2.multiselect("Status", options=opcoes_status, default=padrao_status, placeholder="Filtrar Status")
        
        opcoes_cats = df['categoria_produto'].unique() if not df.empty else []
        f_cats = cf3.multiselect("Categoria", options=opcoes_cats, placeholder="Filtrar Categoria")

        # Linha 2: Data
        cd1, cd2, cd3 = st.columns([1.5, 1.5, 3])
        op_data = cd1.selectbox("Filtro de Data (Previsão)", ["Todo o período", "Igual a", "Antes de", "Depois de"])
        data_ref = cd2.date_input("Data Referência", value=date.today(), format="DD/MM/YYYY")

        # Aplicação dos Filtros
        if not df.empty:
            if busca_geral:
                mask = (
                    df['nome_cliente'].str.contains(busca_geral, case=False, na=False) |
                    df['nome_produto'].str.contains(busca_geral, case=False, na=False) |
                    df['observacao'].str.contains(busca_geral, case=False, na=False) |
                    df['email_cliente'].str.contains(busca_geral, case=False, na=False)
                )
                df = df[mask]
            
            if f_status:
                df = df[df['status'].isin(f_status)]
            
            if f_cats:
                df = df[df['categoria_produto'].isin(f_cats)]
            
            if op_data != "Todo o período":
                df_data = pd.to_datetime(df['data_previsao']).dt.date
                if op_data == "Igual a": df = df[df_data == data_ref]
                elif op_data == "Antes de": df = df[df_data < data_ref]
                elif op_data == "Depois de": df = df[df_data > data_ref]

    # --- PAGINAÇÃO ---
    st.markdown("---")
    col_res, col_pag = st.columns([4, 1])
    with col_pag:
        qtd_view = st.selectbox("Visualizar:", [10, 20, 50, 100, "Todos"], index=0)
    
    df_exibir = df.copy()
    if qtd_view != "Todos":
        df_exibir = df.head(int(qtd_view))
    
    with col_res:
        st.caption(f"Exibindo {len(df_exibir)} de {len(df)} registros.")

    # --- LISTAGEM ---
    if not df_exibir.empty:
        for _, row in df_exibir.iterrows():
            # Cores
            stt = row['status']
            cor = "🔴"
            if stt == 'Concluído': cor = "🟢"
            elif stt == 'Em Análise': cor = "🟠"
            elif stt == 'Entrada': cor = "🔵"
            
            data_fmt = pd.to_datetime(row['data_previsao']).strftime('%d/%m/%Y')
            titulo_card = f"{cor} [{stt.upper()}] | {row['codigo_pedido']} - {row['nome_cliente']} | 📅 Prev: {data_fmt}"

            with st.expander(titulo_card):
                st.write(f"**Produto:** {row['nome_produto']} ({row['categoria_produto']})")
                st.write(f"**Obs:** {row['observacao']}")
                
                # Botões (6 colunas)
                c1, c2, c3, c4, c5, c6 = st.columns(6)
                if c1.button("👤 Cliente", key=f"cli_{row['id']}"): ver_cliente(row['nome_cliente'], row['cpf_cliente'], row['telefone_cliente'], row['email_cliente'])
                if c2.button("👁️ Ver", key=f"ver_{row['id']}"): dialog_visualizar(row)
                if c3.button("🔄 Status", key=f"s_{row['id']}"): dialog_status(row)
                if c4.button("✏️ Editar", key=f"ed_{row['id']}"): dialog_editar(row)
                if c5.button("📜 Hist.", key=f"h_{row['id']}"): dialog_historico(row['id'])
                if c6.button("🗑️ Excluir", key=f"d_{row['id']}"): dialog_excluir(row['id'])
    else:
        st.info("Nenhum registro encontrado.")

if __name__ == "__main__":
    app_renovacao_feedback()