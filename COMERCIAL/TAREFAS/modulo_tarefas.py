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

# --- FUNÇÕES AUXILIARES ---
def listar_modelos_mensagens():
    """Busca os modelos de mensagem cadastrados no W-API para este módulo"""
    conn = get_conn()
    if conn:
        try:
            # Filtra apenas modelos do módulo TAREFAS
            query = "SELECT chave_status FROM wapi_templates WHERE modulo = 'TAREFAS' ORDER BY chave_status ASC"
            df = pd.read_sql(query, conn)
            conn.close()
            return df['chave_status'].tolist()
        except:
            conn.close()
    return []

# --- FUNÇÕES DE BANCO ---

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
        # Adicionado JOIN com clientes_usuarios para trazer o email para a pesquisa
        query = """
            SELECT t.id, t.id_pedido, t.data_previsao, t.observacao_tarefa, t.status, t.data_criacao,
                   p.codigo as codigo_pedido, p.nome_cliente, p.cpf_cliente, p.telefone_cliente,
                   p.nome_produto, p.categoria_produto, p.observacao as obs_pedido,
                   c.email as email_cliente
            FROM tarefas t
            JOIN pedidos p ON t.id_pedido = p.id
            LEFT JOIN clientes_usuarios c ON p.id_cliente = c.id
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

def atualizar_status_tarefa(id_tarefa, novo_status, obs_status, dados_completos, avisar, modelo_msg_escolhido="Automático (Padrão)"):
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
                    # Lógica de seleção do modelo (igual ao Pedidos)
                    if modelo_msg_escolhido and modelo_msg_escolhido != "Automático (Padrão)":
                        chave = modelo_msg_escolhido
                    else:
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
    
    # Carrega opções de modelos do W-API
    lista_modelos = listar_modelos_mensagens()
    opcoes_msg = ["Automático (Padrão)"] + lista_modelos

    with st.form("form_st_tar"):
        novo_st = st.selectbox("Novo Status", lst_status, index=idx)
        modelo_escolhido = st.selectbox("Modelo de Mensagem", opcoes_msg, help="Selecione 'Automático' para usar a mensagem padrão do status.")
        obs_st = st.text_area("Observação")
        avisar = st.checkbox("Avisar Cliente?", value=True)
        
        if st.form_submit_button("Atualizar"):
            if atualizar_status_tarefa(tarefa['id'], novo_st, obs_st, tarefa, avisar, modelo_escolhido):
                st.success("Atualizado!"); st.rerun()

@st.dialog("⚠️ Excluir Tarefa")
def dialog_confirmar_exclusao(id_tarefa):
    st.error("Tem certeza que deseja excluir esta tarefa?")
    st.warning("Esta ação não pode ser desfeita.")
    if st.button("Confirmar Exclusão", type="primary"):
        if excluir_tarefa(id_tarefa): 
            st.success("Tarefa excluída!")
            st.rerun()

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
    # Cabeçalho com Botão Novo no Topo (Estilo Pedidos)
    c_title, c_btn = st.columns([5, 1])
    c_title.markdown("## ✅ CONTROLE DE TAREFAS")
    if c_btn.button("➕ Nova Tarefa", type="primary", use_container_width=False):
        dialog_nova_tarefa()
    
    df_tar = buscar_tarefas_lista()
    
    # --- FILTROS DE PESQUISA (Estilo Pedidos) ---
    with st.expander("🔍 Filtros de Pesquisa", expanded=True):
        # Linha 1: Busca Geral e Categorias
        cf1, cf2 = st.columns([3, 1.5])
        busca_geral = cf1.text_input("🔍 Buscar (Nome, Email, Produto, Obs)", placeholder="Comece a digitar...")
        
        opcoes_cats = df_tar['categoria_produto'].unique() if not df_tar.empty else []
        f_cats = cf2.multiselect("Categoria", options=opcoes_cats, placeholder="Filtrar Categorias")
        
        # Linha 2: Filtro de Data
        cd1, cd2, cd3 = st.columns([1.5, 1.5, 3])
        op_data = cd1.selectbox("Filtro de Data (Previsão)", ["Todo o período", "Igual a", "Antes de", "Depois de"])
        
        # Formato brasileiro no date_input
        data_ref = cd2.date_input("Data Referência", value=date.today(), format="DD/MM/YYYY")

        # --- APLICAÇÃO DOS FILTROS ---
        if not df_tar.empty:
            # 1. Filtro Texto Geral
            if busca_geral:
                mask = (
                    df_tar['nome_cliente'].str.contains(busca_geral, case=False, na=False) |
                    df_tar['nome_produto'].str.contains(busca_geral, case=False, na=False) |
                    df_tar['observacao_tarefa'].str.contains(busca_geral, case=False, na=False) |
                    df_tar['email_cliente'].str.contains(busca_geral, case=False, na=False)
                )
                df_tar = df_tar[mask]
            
            # 2. Filtro de Categoria
            if f_cats:
                df_tar = df_tar[df_tar['categoria_produto'].isin(f_cats)]
            
            # 3. Filtro de Data
            if op_data != "Todo o período":
                df_data = pd.to_datetime(df_tar['data_previsao']).dt.date
                if op_data == "Igual a":
                    df_tar = df_tar[df_data == data_ref]
                elif op_data == "Antes de":
                    df_tar = df_tar[df_data < data_ref]
                elif op_data == "Depois de":
                    df_tar = df_tar[df_data > data_ref]

    # --- PAGINAÇÃO / LIMITE DE VISUALIZAÇÃO ---
    st.markdown("---")
    col_res, col_pag = st.columns([4, 1])
    with col_pag:
        qtd_view = st.selectbox("Visualizar:", [10, 20, 50, 100, "Todos"], index=0)
    
    # Fatia o Dataframe conforme a seleção
    df_exibir = df_tar.copy()
    if qtd_view != "Todos":
        df_exibir = df_tar.head(int(qtd_view))
    
    with col_res:
        st.caption(f"Exibindo {len(df_exibir)} de {len(df_tar)} tarefas encontradas.")

    # --- LISTAGEM DAS TAREFAS ---
    if not df_exibir.empty:
        for i, row in df_exibir.iterrows():
            # Cor do Status (Lógica simples visual)
            cor_status = "🔴"
            if row['status'] == 'Entregue' or row['status'] == 'Concluído': cor_status = "🟢"
            elif row['status'] == 'Em execução': cor_status = "🟠"
            elif row['status'] == 'Solicitado': cor_status = "🔵"

            # Formata data
            data_fmt = pd.to_datetime(row['data_previsao']).strftime('%d/%m/%Y')

            with st.expander(f"{cor_status} {row['codigo_pedido']} - {row['nome_cliente']} | Prev: {data_fmt}"):
                st.write(f"**Produto:** {row['nome_produto']} ({row['categoria_produto']})")
                st.write(f"**Obs:** {row['observacao_tarefa']}")
                
                c1, c2, c3 = st.columns(3)
                if c1.button("🔄 Status", key=f"st_{row['id']}"): dialog_status(row)
                if c2.button("✏️ Editar", key=f"ed_{row['id']}"): dialog_editar(row)
                if c3.button("🗑️ Excluir", key=f"del_{row['id']}"): dialog_confirmar_exclusao(row['id'])
    else:
        st.info("Nenhuma tarefa encontrada com os filtros atuais.")

if __name__ == "__main__":
    app_tarefas()