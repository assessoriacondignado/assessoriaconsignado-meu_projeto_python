import streamlit as st
import pandas as pd
import psycopg2
import modulo_wapi
from datetime import date

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
        query = """
            SELECT rf.id, rf.data_criacao, rf.data_previsao, rf.status, rf.observacao,
                   p.codigo as codigo_pedido, p.nome_cliente, p.nome_produto, p.telefone_cliente
            FROM renovacao_feedback rf
            JOIN pedidos p ON rf.id_pedido = p.id
            ORDER BY rf.data_criacao DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

@st.dialog("➕ Novo Registro")
def dialog_novo_rf():
    df_ped = buscar_pedidos_disponiveis()
    if df_ped.empty:
        st.warning("Nenhum pedido encontrado.")
        return

    opcoes = df_ped.apply(lambda x: f"{x['codigo']} | {x['nome_cliente']} - {x['nome_produto']}", axis=1)
    idx_ped = st.selectbox("Selecione o Pedido", range(len(df_ped)), format_func=lambda x: opcoes[x])
    
    with st.form("form_novo_rf"):
        d_prev = st.date_input("Data Previsão", value=date.today())
        obs = st.text_area("Observação inicial")
        if st.form_submit_button("Criar Registro"):
            id_p = int(df_ped.iloc[idx_ped]['id'])
            conn = get_conn(); cur = conn.cursor()
            cur.execute("INSERT INTO renovacao_feedback (id_pedido, data_previsao, observacao) VALUES (%s, %s, %s) RETURNING id", (id_p, d_prev, obs))
            id_novo = cur.fetchone()[0]
            cur.execute("INSERT INTO renovacao_feedback_historico (id_rf, status_novo, observacao) VALUES (%s, 'Entrada', 'Registro criado')", (id_novo,))
            conn.commit(); conn.close()
            st.success("Registro criado!"); st.rerun()

@st.dialog("👤 Visualizar Informações")
def dialog_visualizar(rf):
    st.markdown(f"### Detalhes: {rf['codigo_pedido']}")
    st.write(f"**Cliente:** {rf['nome_cliente']}")
    st.write(f"**Produto:** {rf['nome_produto']}")
    st.write(f"**Status Atual:** {rf['status']}")
    st.info(f"**Observação:** {rf['observacao']}")
    
    st.markdown("---")
    st.markdown("#### Histórico de Status")
    conn = get_conn()
    df_h = pd.read_sql(f"SELECT data_mudanca, status_novo, observacao FROM renovacao_feedback_historico WHERE id_rf = {rf['id']} ORDER BY data_mudanca DESC", conn)
    conn.close()
    if not df_h.empty:
        st.table(df_h)

@st.dialog("🔄 Atualizar Status")
def dialog_status(rf):
    status_opcoes = ["Entrada", "Em Análise", "Concluído", "Pendente", "Cancelado"]
    with st.form("form_st_rf"):
        novo = st.selectbox("Novo Status", status_opcoes)
        obs = st.text_area("Observação da mudança")
        enviar_whats = st.checkbox("Enviar aviso ao cliente?", value=True)
        if st.form_submit_button("Atualizar"):
            conn = get_conn(); cur = conn.cursor()
            cur.execute("UPDATE renovacao_feedback SET status=%s, data_atualizacao=NOW() WHERE id=%s", (novo, rf['id']))
            cur.execute("INSERT INTO renovacao_feedback_historico (id_rf, status_novo, observacao) VALUES (%s, %s, %s)", (rf['id'], novo, obs))
            conn.commit(); conn.close()
            
            if enviar_whats and rf['telefone_cliente']:
                instancia = modulo_wapi.buscar_instancia_ativa()
                if instancia:
                    chave = novo.lower().replace(" ", "_")
                    template = modulo_wapi.buscar_template("RENOVACAO", chave)
                    if template:
                        msg = template.replace("{nome}", str(rf['nome_cliente']).split()[0]) \
                                      .replace("{pedido}", str(rf['codigo_pedido'])) \
                                      .replace("{status}", novo)
                        modulo_wapi.enviar_msg_api(instancia[0], instancia[1], rf['telefone_cliente'], msg)
            
            st.success("Status atualizado!"); st.rerun()

@st.dialog("⚠️ Excluir")
def dialog_excluir(id_rf):
    st.warning("Tem certeza que deseja apagar permanentemente este registro?")
    if st.button("Sim, confirmar exclusão", type="primary"):
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM renovacao_feedback WHERE id = %s", (id_rf,))
        conn.commit(); conn.close()
        st.rerun()

# --- INTERFACE PRINCIPAL ---
def app_renovacao_feedback():
    st.markdown("## 🔄 Renovação e Feedback")
    
    c_t, c_b = st.columns([5, 1])
    with c_b:
        if st.button("➕ Novo Registro", type="primary"): dialog_novo_rf()

    df = listar_rf()
    if not df.empty:
        # Filtros
        f_txt = st.text_input("🔍 Filtrar por Cliente ou Pedido")
        if f_txt:
            df = df[df['nome_cliente'].str.contains(f_txt, case=False) | df['codigo_pedido'].str.contains(f_txt, case=False)]

        for _, row in df.iterrows():
            cor = "🔵" if row['status'] == 'Entrada' else "🟢"
            with st.expander(f"{cor} {row['codigo_pedido']} - {row['nome_cliente']} ({row['status']})"):
                st.write(f"**Previsão:** {row['data_previsao']} | **Produto:** {row['nome_produto']}")
                c1, c2, c3, c4 = st.columns(4)
                if c1.button("👁️ Ver", key=f"v_{row['id']}"): dialog_visualizar(row)
                if c2.button("🔄 Status", key=f"s_{row['id']}"): dialog_status(row)
                if c3.button("🗑️", key=f"d_{row['id']}"): dialog_excluir(row['id'])
    else:
        st.info("Nenhum registro encontrado.")

if __name__ == "__main__":
    app_renovacao_feedback()