import streamlit as st
import pandas as pd
import psycopg2
import os
import requests
import re
from datetime import datetime, date
import conexao

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
BASE_DIR = "/root/meu_sistema/COMERCIAL/TAREFAS"
if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR, exist_ok=True)

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
        cur = conn.cursor()
        cur.execute("SELECT * FROM config_tarefas WHERE id = 1")
        colunas = [desc[0] for desc in cur.description]
        res = cur.fetchone()
        conn.close()
        if res: return dict(zip(colunas, res))
    return {}

def salvar_configuracao(grupo_id, templates):
    conn = get_conn()
    if conn:
        cur = conn.cursor()
        sql = """UPDATE config_tarefas SET 
                 grupo_aviso_id=%s, msg_solicitado=%s, msg_registro=%s, msg_entregue=%s, 
                 msg_em_processamento=%s, msg_em_execucao=%s, msg_pendente=%s, msg_cancelado=%s 
                 WHERE id=1"""
        cur.execute(sql, (grupo_id, templates['Solicitado'], templates['Registro'], templates['Entregue'],
                          templates['Em processamento'], templates['Em execução'], templates['Pendente'], templates['Cancelado']))
        conn.commit()
        conn.close()

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
        query = """
            SELECT p.id, p.codigo, p.nome_cliente, p.nome_produto, p.categoria_produto, p.observacao as obs_pedido, p.status as status_pedido
            FROM pedidos p
            ORDER BY p.data_criacao DESC
        """
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
            cur.execute("""
                INSERT INTO tarefas (id_pedido, data_previsao, observacao_tarefa, status)
                VALUES (%s, %s, %s, 'Solicitado') RETURNING id
            """, (int(id_pedido), data_prev, obs_tarefa))
            id_tarefa = cur.fetchone()[0]
            
            cur.execute("INSERT INTO tarefas_historico (id_tarefa, status_novo, observacao) VALUES (%s, 'Solicitado', 'Tarefa Criada')",(id_tarefa,))
            conn.commit()
            conn.close()
            
            config = buscar_configuracao_tarefa()
            if avisar_cli and dados_pedido.get('telefone_cliente'):
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
                # Ajuste para chaves de banco
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
        cur = conn.cursor()
        cur.execute("DELETE FROM tarefas WHERE id=%s", (id_tarefa,))
        conn.commit()
        conn.close()
        return True
    return False

# --- POP-UPS (DIALOGS) ---

@st.dialog("👤 Dados do Cliente (Via Pedido)")
def ver_cliente(nome, cpf, tel):
    st.write(f"**Nome:** {nome}")
    st.write(f"**CPF:** {cpf}")
    st.write(f"**Telefone:** {tel}")

@st.dialog("📦 Dados do Produto (Via Pedido)")
def ver_produto(nome, cat):
    st.write(f"**Produto:** {nome}")
    st.write(f"**Categoria:** {cat}")

@st.dialog("📄 Dados do Pedido Original")
def ver_pedido(codigo, obs):
    st.write(f"**Cód. Pedido:** {codigo}")
    st.info(f"**Obs. do Pedido:** {obs}")

@st.dialog("✏️ Editar Tarefa")
def dialog_editar(tarefa):
    st.write(f"Editando Tarefa do Pedido: **{tarefa['codigo_pedido']}**")
    with st.form("form_edit_tar"):
        # DATA NO FORMATO PT-BR
        n_data = st.date_input("Nova Previsão", value=pd.to_datetime(tarefa['data_previsao']), format="DD/MM/YYYY")
        n_obs = st.text_area("Observação da Tarefa", value=tarefa['observacao_tarefa'])
        if st.form_submit_button("Salvar"):
            if editar_tarefa_dados(tarefa['id'], n_data, n_obs):
                st.success("Editado!"); st.rerun()

@st.dialog("🔄 Atualizar Status da Tarefa")
def dialog_status(tarefa):
    # LISTA CORRIGIDA - Removido o status "Em" solto
    lst_status = ["Solicitado", "Registro", "Entregue", "Em processamento", "Em execução", "Pendente", "Cancelado"]
    idx = 0
    if tarefa['status'] in lst_status: idx = lst_status.index(tarefa['status'])
    
    with st.form("form_st_tar"):
        novo_st = st.selectbox("Novo Status", lst_status, index=idx)
        obs_st = st.text_area("Observação do Status", placeholder="Motivo da mudança...")
        avisar = st.checkbox("Avisar Cliente?", value=True)
        if st.form_submit_button("Atualizar"):
            if atualizar_status_tarefa(tarefa['id'], novo_st, obs_st, tarefa, avisar):
                st.success("Atualizado!"); st.rerun()

# NOVO POP-UP DE CONFIRMAÇÃO DE EXCLUSÃO
@st.dialog("⚠️ Confirmar Exclusão")
def dialog_confirmar_exclusao(id_tarefa):
    st.warning("Tem certeza que deseja excluir esta tarefa? Essa ação não pode ser desfeita.")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Sim, Excluir", type="primary"):
            if excluir_tarefa(id_tarefa):
                st.success("Tarefa excluída!")
                st.rerun()
    with col2:
        if st.button("Cancelar"):
            st.rerun()

@st.dialog("⚙️ Configuração de Tarefa")
def dialog_config():
    c = buscar_configuracao_tarefa()
    with st.form("cfg_tar"):
        st.write("Configuração W-API para TAREFAS")
        grp = st.text_input("ID Grupo Aviso", value=c.get('grupo_aviso_id',''))
        st.info("Tags: {nome}, {pedido}, {status}, {obs_status}, {data_previsao}")
        
        m1 = st.text_area("Status: Solicitado (Criação)", value=c.get('msg_solicitado',''))
        m2 = st.text_area("Status: Registro", value=c.get('msg_registro',''))
        m3 = st.text_area("Status: Entregue", value=c.get('msg_entregue',''))
        m4 = st.text_area("Status: Em processamento", value=c.get('msg_em_processamento',''))
        m5 = st.text_area("Status: Em execução", value=c.get('msg_em_execucao',''))
        m6 = st.text_area("Status: Pendente", value=c.get('msg_pendente',''))
        m7 = st.text_area("Status: Cancelado", value=c.get('msg_cancelado',''))
        
        if st.form_submit_button("Salvar Configurações"):
            tpls = {'Solicitado': m1, 'Registro': m2, 'Entregue': m3, 'Em processamento': m4, 'Em execução': m5, 'Pendente': m6, 'Cancelado': m7}
            salvar_configuracao(grp, tpls)
            st.success("Salvo!"); st.rerun()

# --- NOVO POP-UP DE CRIAÇÃO ---
@st.dialog("➕ Nova Tarefa")
def dialog_nova_tarefa():
    df_ped = buscar_pedidos_para_tarefa()
    
    if df_ped.empty:
        st.warning("Não há pedidos disponíveis.")
        return

    # Selectbox começando vazio
    opcoes = df_ped.apply(lambda x: f"{x['codigo']} | {x['nome_cliente']}", axis=1)
    idx_ped = st.selectbox(
        "Buscar Pedido (Selecione o Cliente)", 
        range(len(df_ped)), 
        format_func=lambda x: opcoes[x],
        index=None,
        placeholder="Escolha uma opção..."
    )

    # Lógica para mostrar dados SÓ DEPOIS de selecionar
    if idx_ped is not None:
        sel_ped = df_ped.iloc[idx_ped]
        
        st.markdown("---")
        # Informações do Produto
        c_prod1, c_prod2 = st.columns([2, 1])
        with c_prod1:
            st.write(f"📦 **Produto:** {sel_ped['nome_produto']}")
            st.caption(f"Categoria: {sel_ped['categoria_produto']}")
        with c_prod2:
            st.info(f"Obs Pedido: {sel_ped['obs_pedido'] if sel_ped['obs_pedido'] else 'Nenhuma'}")
        
        # Formulário
        with st.form("form_create_task"):
            label_data = "Data Previsão Entrega"
            if "SERVIÇO" in str(sel_ped['categoria_produto']).upper():
                label_data = "Data Previsão Início"
            
            # DATA FORMATO BR
            d_prev = st.date_input(label_data, value=date.today(), format="DD/MM/YYYY")
            obs_tar = st.text_area("Observação da Tarefa", height=100)
            av_cli = st.checkbox("Enviar aviso de início ao cliente?", value=True)
            
            if st.form_submit_button("Criar Tarefa"):
                conn = get_conn()
                dados_msg = {}
                if conn:
                    cur = conn.cursor()
                    cur.execute("SELECT codigo, nome_cliente, telefone_cliente, nome_produto FROM pedidos WHERE id=%s", (int(sel_ped['id']),))
                    res = cur.fetchone()
                    conn.close()
                    if res:
                         dados_msg = {'codigo_pedido': res[0], 'nome_cliente': res[1], 'telefone_cliente': res[2], 'nome_produto': res[3]}
                
                if criar_tarefa(sel_ped['id'], d_prev, obs_tar, dados_msg, av_cli):
                    st.success("Tarefa criada com sucesso!")
                    st.rerun()

# --- APP PRINCIPAL ---
def app_tarefas():
    # LAYOUT DO TOPO
    c_title, c_btn = st.columns([4, 1])
    with c_title:
        st.markdown("## ✅ CONTROLE DE TAREFAS")
    with c_btn:
        if st.button("➕ Nova Tarefa", type="primary"):
            dialog_nova_tarefa()
    
    st.divider()
    
    # ABAS
    tab1, tab2 = st.tabs(["📋 Gerenciar Tarefas", "⚙️ Configurações"])

    # ABA 1: GERENCIAR
    with tab1:
        df_tar = buscar_tarefas_lista()
        if not df_tar.empty:
            # FORMATAR DATA NA TABELA (VISUAL)
            df_tar['Data Prev.'] = pd.to_datetime(df_tar['data_previsao']).dt.strftime('%d/%m/%Y')
            
            c1, c2 = st.columns(2)
            with c1: txt_bus = st.text_input("🔎 Buscar (Pedido, Cliente, Produto)")
            with c2: sel_st = st.multiselect("Filtrar Status", df_tar['status'].unique())
            
            if txt_bus:
                df_tar = df_tar[df_tar['codigo_pedido'].str.contains(txt_bus, case=False) | 
                                df_tar['nome_cliente'].str.contains(txt_bus, case=False) |
                                df_tar['nome_produto'].str.contains(txt_bus, case=False)]
            if sel_st:
                df_tar = df_tar[df_tar['status'].isin(sel_st)]
            
            # Tabela simplificada
            st.dataframe(
                df_tar[['codigo_pedido', 'status', 'Data Prev.', 'nome_cliente', 'nome_produto']], 
                use_container_width=True, 
                hide_index=True
            )
            
            st.markdown("### Ações da Tarefa")
            for i, row in df_tar.iterrows():
                with st.expander(f"📌 {row['codigo_pedido']} | {row['nome_cliente']} ({row['status']})"):
                    c_info, c_btns = st.columns([2, 1])
                    with c_info:
                        st.write(f"**Produto:** {row['nome_produto']}")
                        st.write(f"**Previsão:** {row['Data Prev.']}") # Usa a data formatada
                        st.info(f"**Obs Tarefa:** {row['observacao_tarefa']}")
                    
                    with c_btns:
                        # BOTÕES RENOMEADOS
                        c_b1, c_b2, c_b3 = st.columns(3)
                        if c_b1.button("Cliente", key=f"vc_{row['id']}", help="Ver Cliente"): ver_cliente(row['nome_cliente'], row['cpf_cliente'], row['telefone_cliente'])
                        if c_b2.button("Produção", key=f"vp_{row['id']}", help="Ver Produto"): ver_produto(row['nome_produto'], row['categoria_produto'])
                        if c_b3.button("Pedido", key=f"vpe_{row['id']}", help="Ver Pedido"): ver_pedido(row['codigo_pedido'], row['obs_pedido'])
                        
                        st.divider()
                        
                        if st.button("🔄 Atualizar Status", key=f"st_{row['id']}", use_container_width=True): dialog_status(row)
                        if st.button("✏️ Editar Tarefa", key=f"ed_{row['id']}", use_container_width=True): dialog_editar(row)
                        
                        # BOTÃO EXCLUIR COM CONFIRMAÇÃO DUPLA
                        if st.button("🗑️ Excluir", key=f"del_{row['id']}", use_container_width=True):
                            dialog_confirmar_exclusao(row['id'])
        else:
            st.info("Nenhuma tarefa encontrada.")

    # ABA 2: CONFIG
    with tab2:
        st.write("Ajuste as mensagens automáticas enviadas via WhatsApp.")
        if st.button("Abrir Configuração de Tarefa"): dialog_config()

if __name__ == "__main__":
    app_tarefas()