import streamlit as st
import pandas as pd
import psycopg2
import os
import requests
import re
from datetime import datetime, date

# --- IMPORTAÇÃO ROBUSTA ---
try: 
    import conexao
except ImportError: 
    st.error("Erro crítico: conexao.py não encontrado.")

# --- CONFIGURAÇÕES DE DIRETÓRIO DINÂMICO ---
# Ajustado para funcionar tanto no servidor SSH quanto na nuvem
BASE_DIR = os.path.join(os.getcwd(), "COMERCIAL", "TAREFAS")

try:
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR, exist_ok=True)
except PermissionError:
    # Fallback para pasta temporária no Streamlit Cloud
    BASE_DIR = "/tmp"
    st.info("Operando em modo Nuvem: Registros locais salvos temporariamente.")

# --- CONEXÃO COM BANCO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host,
            port=conexao.port,
            database=conexao.database,
            user=conexao.user,
            password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")
        return None

# --- INTEGRAÇÃO W-API ---
def buscar_instancia_ativa():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
            res = cur.fetchone()
            conn.close()
            return res 
        except: return None
    return None

def buscar_configuracao_tarefa():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM config_tarefas WHERE id = 1")
            colunas = [desc[0] for desc in cur.description]
            res = cur.fetchone()
            conn.close()
            if res: return dict(zip(colunas, res))
        except: return {}
    return {}

def salvar_configuracao(grupo_id, templates):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            sql = """UPDATE config_tarefas SET 
                     grupo_aviso_id=%s, msg_solicitado=%s, msg_registro=%s, msg_entregue=%s, 
                     msg_em_processamento=%s, msg_em_execucao=%s, msg_pendente=%s, msg_cancelado=%s 
                     WHERE id=1"""
            cur.execute(sql, (grupo_id, templates['Solicitado'], templates['Registro'], templates['Entregue'],
                             templates['Em processamento'], templates['Em execução'], templates['Pendente'], templates['Cancelado']))
            conn.commit()
            conn.close()
        except Exception as e: st.error(f"Erro ao salvar config: {e}")

def enviar_whatsapp_tarefa(numero, mensagem):
    dados_instancia = buscar_instancia_ativa()
    if not dados_instancia: return False, "Sem instância."
    instance_id, token = dados_instancia
    
    BASE_URL = "https://api.w-api.app/v1"
    url = f"{BASE_URL}/message/send-text?instanceId={instance_id}"
    
    if "@g.us" in str(numero):
        numero_limpo = str(numero)
    else:
        numero_limpo = re.sub(r'\D', '', str(numero)) 
        if len(numero_limpo) < 12 and not numero_limpo.startswith("55"): numero_limpo = "55" + numero_limpo

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"phone": numero_limpo, "message": mensagem, "delayMessage": 3}
    
    try:
        requests.post(url, json=payload, headers=headers, timeout=5)
        return True, "Enviado"
    except Exception as e: return False, str(e)

# --- CRUD TAREFAS ---

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
                config = buscar_configuracao_tarefa()
                tpl = config.get('msg_solicitado', '')
                if tpl:
                    msg = tpl.replace("{nome}", str(dados_pedido['nome_cliente']).split()[0]) \
                             .replace("{pedido}", str(dados_pedido['codigo_pedido'])) \
                             .replace("{produto}", str(dados_pedido['nome_produto'])) \
                             .replace("{data_previsao}", data_prev.strftime('%d/%m/%Y'))
                    enviar_whatsapp_tarefa(dados_pedido['telefone_cliente'], msg)
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
                config = buscar_configuracao_tarefa()
                chave = f"msg_{novo_status.lower().replace(' ', '_')}"
                if "execução" in chave: chave = "msg_em_execucao"
                
                tpl = config.get(chave, '')
                if tpl:
                     msg = tpl.replace("{nome}", str(dados_completos['nome_cliente']).split()[0]) \
                              .replace("{pedido}", str(dados_completos['codigo_pedido'])) \
                              .replace("{status}", novo_status) \
                              .replace("{obs_status}", obs_status)
                     enviar_whatsapp_tarefa(dados_completos['telefone_cliente'], msg)
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

# --- POP-UPS (DIALOGS) ---

@st.dialog("👤 Dados do Cliente")
def ver_cliente(nome, cpf, tel):
    st.write(f"**Nome:** {nome}"); st.write(f"**CPF:** {cpf}"); st.write(f"**Telefone:** {tel}")

@st.dialog("📦 Dados do Produto")
def ver_produto(nome, cat):
    st.write(f"**Produto:** {nome}"); st.write(f"**Categoria:** {cat}")

@st.dialog("📄 Dados do Pedido")
def ver_pedido(codigo, obs):
    st.write(f"**Cód. Pedido:** {codigo}"); st.info(f"**Obs. do Pedido:** {obs}")

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

@st.dialog("⚙️ Configurações")
def dialog_config():
    c = buscar_configuracao_tarefa()
    with st.form("cfg_tar"):
        grp = st.text_input("ID Grupo Aviso", value=c.get('grupo_aviso_id',''))
        tpls = {
            'Solicitado': st.text_area("Solicitado", value=c.get('msg_solicitado','')),
            'Registro': st.text_area("Registro", value=c.get('msg_registro','')),
            'Entregue': st.text_area("Entregue", value=c.get('msg_entregue','')),
            'Em processamento': st.text_area("Em processamento", value=c.get('msg_em_processamento','')),
            'Em execução': st.text_area("Em execução", value=c.get('msg_em_execucao','')),
            'Pendente': st.text_area("Pendente", value=c.get('msg_pendente','')),
            'Cancelado': st.text_area("Cancelado", value=c.get('msg_cancelado',''))
        }
        if st.form_submit_button("Salvar"):
            salvar_configuracao(grp, tpls); st.rerun()

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
    
    tab1, tab2 = st.tabs(["📋 Gerenciar", "⚙️ Config"])
    with tab1:
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
    with tab2:
        if st.button("Abrir Configuração"): dialog_config()

if __name__ == "__main__":
    app_tarefas()