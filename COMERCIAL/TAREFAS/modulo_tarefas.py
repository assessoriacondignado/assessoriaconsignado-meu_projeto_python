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
    except Exception as e:
        return None

def buscar_pedidos_para_tarefa():
    conn = get_conn()
    if conn:
        query = "SELECT id, codigo, nome_cliente, nome_produto, categoria_produto, observacao as obs_pedido, status as status_pedido FROM pedidos ORDER BY data_criacao DESC"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def buscar_tarefas_lista():
    conn = get_conn()
    if conn:
        query = """
            SELECT t.id, t.id_pedido, t.data_previsao, t.observacao_tarefa, t.status, t.data_criacao,
                   p.codigo as codigo_pedido, p.nome_cliente, p.cpf_cliente, p.telefone_cliente,
                   p.nome_produto, p.categoria_produto, p.observacao as obs_pedido
            FROM tarefas t
            JOIN pedidos p ON t.id_pedido = p.id
            ORDER BY t.data_criacao DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def criar_tarefa(id_pedido, data_prev, obs_tarefa, dados_pedido, avisar_cli):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO tarefas (id_pedido, data_previsao, observacao_tarefa, status) VALUES (%s, %s, %s, 'Solicitado') RETURNING id", 
                        (int(id_pedido), data_prev, obs_tarefa))
            id_tarefa = cur.fetchone()[0]
            cur.execute("INSERT INTO tarefas_historico (id_tarefa, status_novo, observacao) VALUES (%s, 'Solicitado', 'Tarefa Criada')",(id_tarefa,))
            conn.commit()
            conn.close()
            
            if avisar_cli and dados_pedido.get('telefone_cliente'):
                instancia = modulo_wapi.buscar_instancia_ativa()
                if instancia:
                    template = modulo_wapi.buscar_template("TAREFAS", "solicitado")
                    if template:
                        msg = template.replace("{nome}", str(dados_pedido['nome_cliente']).split()[0]) \
                                      .replace("{pedido}", str(dados_pedido['codigo_pedido'])) \
                                      .replace("{produto}", str(dados_pedido['nome_produto'])) \
                                      .replace("{data_previsao}", data_prev.strftime('%d/%m/%Y'))
                        modulo_wapi.enviar_msg_api(instancia[0], instancia[1], dados_pedido['telefone_cliente'], msg)
            return True
        except Exception as e: st.error(f"Erro SQL: {e}")
    return False

def atualizar_status_tarefa(id_tarefa, novo_status, obs_status, dados_completos, avisar):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE tarefas SET status=%s, data_atualizacao=NOW() WHERE id=%s", (novo_status, id_tarefa))
            cur.execute("INSERT INTO tarefas_historico (id_tarefa, status_novo, observacao) VALUES (%s, %s, %s)", (id_tarefa, novo_status, obs_status))
            conn.commit()
            conn.close()
            
            if avisar and dados_completos.get('telefone_cliente'):
                instancia = modulo_wapi.buscar_instancia_ativa()
                if instancia:
                    # Busca template centralizado
                    chave = novo_status.lower().replace(' ', '_')
                    template = modulo_wapi.buscar_template("TAREFAS", chave)
                    
                    if template:
                         msg = template.replace("{nome}", str(dados_completos['nome_cliente']).split()[0]) \
                                       .replace("{pedido}", str(dados_completos['codigo_pedido'])) \
                                       .replace("{status}", novo_status) \
                                       .replace("{obs_status}", obs_status)
                         modulo_wapi.enviar_msg_api(instancia[0], instancia[1], dados_completos['telefone_cliente'], msg)
            return True
        except: return False
    return False

def editar_tarefa_dados(id_tarefa, nova_data, nova_obs):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE tarefas SET data_previsao=%s, observacao_tarefa=%s WHERE id=%s", (nova_data, nova_obs, id_tarefa))
            conn.commit()
            conn.close()
            return True
        except: return False
    return False

def excluir_tarefa(id_tarefa):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM tarefas WHERE id=%s", (id_tarefa,))
            conn.commit()
            conn.close()
            return True
        except: return False
    return False

# --- DIALOGS ---
@st.dialog("✏️ Editar Tarefa")
def dialog_editar(tarefa):
    st.write(f"Editando Tarefa: **{tarefa['codigo_pedido']}**")
    with st.form("form_edit_tar"):
        n_data = st.date_input("Nova Previsão", value=pd.to_datetime(tarefa['data_previsao']), format="DD/MM/YYYY")
        n_obs = st.text_area("Observação da Tarefa", value=tarefa['observacao_tarefa'])
        if st.form_submit_button("Salvar"):
            if editar_tarefa_dados(tarefa['id'], n_data, n_obs):
                st.success("Editado!"); st.rerun()

@st.dialog("🔄 Atualizar Status")
def dialog_status(tarefa):
    lst_status = ["Solicitado", "Registro", "Entregue", "Em processamento", "Em execução", "Pendente", "Cancelado"]
    idx = lst_status.index(tarefa['status']) if tarefa['status'] in lst_status else 0
    with st.form("form_st_tar"):
        novo_st = st.selectbox("Novo Status", lst_status, index=idx)
        obs_st = st.text_area("Observação")
        avisar = st.checkbox("Avisar Cliente?", value=True)
        if st.form_submit_button("Atualizar"):
            if atualizar_status_tarefa(tarefa['id'], novo_st, obs_st, tarefa, avisar):
                st.success("Atualizado!"); st.rerun()

@st.dialog("⚠️ Excluir Tarefa")
def dialog_confirmar_exclusao(id_tarefa):
    st.warning("Confirmar exclusão desta tarefa?")
    if st.button("Confirmar Exclusão", type="primary"):
        if excluir_tarefa(id_tarefa): st.rerun()

@st.dialog("➕ Nova Tarefa")
def dialog_nova_tarefa():
    df_ped = buscar_pedidos_para_tarefa()
    if df_ped.empty: return st.warning("Sem pedidos.")
    opcoes = df_ped.apply(lambda x: f"{x['codigo']} | {x['nome_cliente']}", axis=1)
    idx_ped = st.selectbox("Selecione o Pedido", range(len(df_ped)), format_func=lambda x: opcoes[x], index=None)
    if idx_ped is not None:
        sel = df_ped.iloc[idx_ped]
        with st.form("form_create_task"):
            d_prev = st.date_input("Data Previsão", value=date.today(), format="DD/MM/YYYY")
            obs_tar = st.text_area("Observação")
            if st.form_submit_button("Criar Tarefa"):
                if criar_tarefa(sel['id'], d_prev, obs_tar, {'codigo_pedido': sel['codigo'], 'nome_cliente': sel['nome_cliente'], 'telefone_cliente': None, 'nome_produto': sel['nome_produto']}, True):
                    st.rerun()

# --- APP PRINCIPAL ---
def app_tarefas():
    c_title, c_btn = st.columns([4, 1])
    c_title.markdown("## ✅ CONTROLE DE TAREFAS")
    if c_btn.button("➕ Nova Tarefa", type="primary"): dialog_nova_tarefa()
    
    # Aba única de gerenciamento
    df_tar = buscar_tarefas_lista()
    if not df_tar.empty:
        df_tar['Data Prev.'] = pd.to_datetime(df_tar['data_previsao']).dt.strftime('%d/%m/%Y')
        st.dataframe(df_tar[['codigo_pedido', 'status', 'Data Prev.', 'nome_cliente']], use_container_width=True, hide_index=True)
        for i, row in df_tar.iterrows():
            with st.expander(f"📌 {row['codigo_pedido']} | {row['nome_cliente']}"):
                c1, c2, c3 = st.columns(3)
                if c1.button("Status", key=f"st_{row['id']}"): dialog_status(row)
                if c2.button("Editar", key=f"ed_{row['id']}"): dialog_editar(row)
                if c3.button("🗑️", key=f"del_{row['id']}"): dialog_confirmar_exclusao(row['id'])
    else: st.info("Sem tarefas.")

if __name__ == "__main__":
    app_tarefas()