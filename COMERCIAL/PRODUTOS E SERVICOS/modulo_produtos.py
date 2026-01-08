import streamlit as st
import pandas as pd
import psycopg2
import os
import re
import time
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

def salvar_tabela_generica(schema, tabela, df_original, df_editado):
    """Função genérica para salvar edições feitas via st.data_editor"""
    conn = get_conn()
    if not conn: return False, "Sem conexão"
    try:
        cur = conn.cursor()
        pk = 'id' 
        
        # Identifica IDs originais para saber o que deletar
        if pk in df_original.columns:
            ids_originais = set(df_original[pk].dropna().astype(int).tolist())
        else:
            ids_originais = set() 

        # 1. PROCESSAR DELEÇÕES
        if pk in df_original.columns:
            ids_editados = set()
            for _, row in df_editado.iterrows():
                if pd.notna(row.get(pk)) and row.get(pk) != '':
                    try: ids_editados.add(int(row[pk]))
                    except: pass
            
            ids_del = ids_originais - ids_editados
            if ids_del:
                ids_str = ",".join(map(str, ids_del))
                cur.execute(f"DELETE FROM {schema}.{tabela} WHERE {pk} IN ({ids_str})")

        # 2. PROCESSAR INSERÇÕES E ATUALIZAÇÕES
        for index, row in df_editado.iterrows():
            colunas_validas = list(row.index)
            # Ignora colunas automáticas de timestamp se não quiser forçar update nelas
            cols_ignore = ['data_criacao', 'data_atualizacao']
            colunas_validas = [c for c in colunas_validas if c not in cols_ignore]

            row_id = row.get(pk)
            eh_novo = pd.isna(row_id) or row_id == '' or row_id is None
            
            if eh_novo:
                # INSERT
                cols_insert = [c for c in colunas_validas if c != pk]
                vals_insert = [row[c] for c in cols_insert]
                placeholders = ", ".join(["%s"] * len(cols_insert))
                cols_str = ", ".join(cols_insert)
                if cols_insert:
                    cur.execute(f"INSERT INTO {schema}.{tabela} ({cols_str}) VALUES ({placeholders})", vals_insert)
            elif int(row_id) in ids_originais:
                # UPDATE
                cols_update = [c for c in colunas_validas if c != pk]
                vals_update = [row[c] for c in cols_update]
                vals_update.append(int(row_id)) 
                if cols_update:
                    set_clause = ", ".join([f"{c} = %s" for c in cols_update])
                    cur.execute(f"UPDATE {schema}.{tabela} SET {set_clause} WHERE {pk} = %s", vals_update)

        conn.commit(); conn.close()
        return True, "Dados salvos com sucesso!"
    except Exception as e:
        if conn: conn.close()
        return False, str(e)

# --- FUNÇÕES DE BANCO ---

def buscar_pedidos_para_tarefa():
    """Busca pedidos para vincular à nova tarefa."""
    conn = get_conn()
    if conn:
        query = """
            SELECT id, codigo, nome_cliente, nome_produto, categoria_produto, 
                   observacao as obs_pedido, status as status_pedido,
                   id_cliente, id_produto 
            FROM pedidos 
            ORDER BY data_criacao DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def buscar_tarefas_lista():
    """Lista tarefas buscando dados VIVOS de cliente/produto via ID."""
    conn = get_conn()
    if conn:
        query = """
            SELECT t.id, t.id_pedido, t.id_cliente, t.id_produto, 
                   t.data_previsao, t.observacao_tarefa, t.status, t.data_criacao,
                   
                   p.codigo as codigo_pedido, p.observacao as obs_pedido,
                   
                   c.nome as nome_cliente, c.cpf as cpf_cliente, 
                   c.telefone as telefone_cliente, c.email as email_cliente,
                   
                   pr.nome as nome_produto, pr.tipo as categoria_produto

            FROM tarefas t
            LEFT JOIN pedidos p ON t.id_pedido = p.id
            LEFT JOIN admin.clientes c ON t.id_cliente = c.id
            LEFT JOIN produtos_servicos pr ON t.id_produto = pr.id
            ORDER BY t.data_criacao DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def buscar_historico_tarefa(id_tarefa):
    conn = get_conn()
    if conn:
        query = "SELECT data_mudanca, status_novo, observacao FROM tarefas_historico WHERE id_tarefa = %s ORDER BY data_mudanca DESC"
        df = pd.read_sql(query, conn, params=(int(id_tarefa),))
        conn.close()
        return df
    return pd.DataFrame()

def criar_tarefa(id_pedido, id_cliente, id_produto, data_prev, obs_tarefa, dados_pedido, avisar_cli):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            sql = """
                INSERT INTO tarefas (id_pedido, id_cliente, id_produto, data_previsao, observacao_tarefa, status) 
                VALUES (%s, %s, %s, %s, %s, 'Solicitado') 
                RETURNING id
            """
            cur.execute(sql, (int(id_pedido), int(id_cliente), int(id_produto), data_prev, obs_tarefa))
            
            id_tarefa = cur.fetchone()[0]
            cur.execute("INSERT INTO tarefas_historico (id_tarefa, status_novo, observacao) VALUES (%s, 'Solicitado', 'Tarefa Criada')", (id_tarefa,))
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
        except Exception as e: 
            st.error(f"Erro SQL: {e}")
            if conn: conn.close()
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
@st.dialog("👤 Dados do Cliente")
def ver_cliente(nome, cpf, tel, email):
    st.write(f"**Nome:** {nome}")
    st.write(f"**CPF:** {cpf}")
    st.write(f"**Telefone:** {tel}")
    st.write(f"**E-mail:** {email}")

@st.dialog("👁️ Detalhes da Tarefa")
def dialog_visualizar(tarefa):
    st.markdown(f"### Tarefa: {tarefa['codigo_pedido']}")
    st.write(f"**Cliente:** {tarefa['nome_cliente']}")
    st.write(f"**Produto:** {tarefa['nome_produto']}")
    st.write(f"**Categoria:** {tarefa['categoria_produto']}")
    st.markdown("---")
    st.write(f"**Status Atual:** {tarefa['status']}")
    st.write(f"**Previsão:** {pd.to_datetime(tarefa['data_previsao']).strftime('%d/%m/%Y')}")
    st.info(f"**Observação da Tarefa:**\n{tarefa['observacao_tarefa']}")
    st.warning(f"**Observação Original do Pedido:**\n{tarefa['obs_pedido']}")

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

@st.dialog("📜 Histórico")
def dialog_historico(id_tarefa):
    st.write("Histórico de alterações:")
    df_hist = buscar_historico_tarefa(id_tarefa)
    if not df_hist.empty:
        df_hist.columns = ["Data/Hora", "Status", "Obs"]
        st.dataframe(df_hist, use_container_width=True, hide_index=True)
    else: st.info("Sem registros.")

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
    if df_ped.empty: 
        st.warning("Sem pedidos.")
        return
        
    opcoes = df_ped.apply(lambda x: f"{x['codigo']} | {x['nome_cliente']}", axis=1)
    idx_ped = st.selectbox("Selecione o Pedido", range(len(df_ped)), format_func=lambda x: opcoes[x], index=None)
    
    if idx_ped is not None:
        sel = df_ped.iloc[idx_ped]
        with st.form("form_create_task"):
            st.write(f"**Cliente:** {sel['nome_cliente']}")
            st.write(f"**Produto:** {sel['nome_produto']}")
            st.divider()
            
            d_prev = st.date_input("Data Previsão", value=date.today(), format="DD/MM/YYYY")
            obs_tar = st.text_area("Observação")
            
            if st.form_submit_button("Criar Tarefa"):
                dados_msg = {
                    'codigo_pedido': sel['codigo'], 
                    'nome_cliente': sel['nome_cliente'], 
                    'telefone_cliente': None, 
                    'nome_produto': sel['nome_produto']
                }
                
                sucesso = criar_tarefa(
                    id_pedido=sel['id'], 
                    id_cliente=sel['id_cliente'],
                    id_produto=sel['id_produto'],
                    data_prev=d_prev, 
                    obs_tarefa=obs_tar, 
                    dados_pedido=dados_msg, 
                    avisar_cli=True
                )
                
                if sucesso:
                    st.success("Tarefa criada com sucesso!")
                    time.sleep(1); st.rerun()

# --- APP PRINCIPAL ---
def app_tarefas():
    st.markdown("## ✅ Módulo de Tarefas")

    # --- CRIAÇÃO DAS ABAS (SUBMENUS) ---
    tab_tarefas, tab_tabelas = st.tabs(["📋 Tarefas", "🗃️ Tabelas"])

    # ------------------------------------
    # ABA 1: TAREFAS (INTERFACE ORIGINAL)
    # ------------------------------------
    with tab_tarefas:
        c_title, c_btn = st.columns([5, 1])
        if c_btn.button("➕ Nova Tarefa", type="primary", use_container_width=True):
            dialog_nova_tarefa()
        
        df_tar = buscar_tarefas_lista()
        
        # --- FILTROS DE PESQUISA ---
        with st.expander("🔍 Filtros de Pesquisa", expanded=True):
            cf1, cf2, cf3 = st.columns([3, 1.5, 1.5])
            busca_geral = cf1.text_input("🔍 Buscar (Nome, Email, Produto, Obs)", placeholder="Comece a digitar...")
            
            opcoes_status = df_tar['status'].unique().tolist() if not df_tar.empty else []
            padrao_status = ["Solicitado"] if "Solicitado" in opcoes_status else None
            f_status = cf2.multiselect("Status", options=opcoes_status, default=padrao_status, placeholder="Filtrar Status")

            opcoes_cats = df_tar['categoria_produto'].unique() if not df_tar.empty else []
            f_cats = cf3.multiselect("Categoria", options=opcoes_cats, placeholder="Filtrar Categorias")
            
            cd1, cd2, cd3 = st.columns([1.5, 1.5, 3])
            op_data = cd1.selectbox("Filtro de Data (Previsão)", ["Todo o período", "Igual a", "Antes de", "Depois de"])
            data_ref = cd2.date_input("Data Referência", value=date.today(), format="DD/MM/YYYY")

            if not df_tar.empty:
                if busca_geral:
                    mask = (
                        df_tar['nome_cliente'].str.contains(busca_geral, case=False, na=False) |
                        df_tar['nome_produto'].str.contains(busca_geral, case=False, na=False) |
                        df_tar['observacao_tarefa'].str.contains(busca_geral, case=False, na=False) |
                        df_tar['email_cliente'].str.contains(busca_geral, case=False, na=False)
                    )
                    df_tar = df_tar[mask]
                
                if f_status:
                    df_tar = df_tar[df_tar['status'].isin(f_status)]

                if f_cats:
                    df_tar = df_tar[df_tar['categoria_produto'].isin(f_cats)]
                
                if op_data != "Todo o período":
                    df_data = pd.to_datetime(df_tar['data_previsao']).dt.date
                    if op_data == "Igual a":
                        df_tar = df_tar[df_data == data_ref]
                    elif op_data == "Antes de":
                        df_tar = df_tar[df_data < data_ref]
                    elif op_data == "Depois de":
                        df_tar = df_tar[df_data > data_ref]

        st.markdown("---")
        col_res, col_pag = st.columns([4, 1])
        with col_pag:
            qtd_view = st.selectbox("Visualizar:", [10, 20, 50, 100, "Todos"], index=0)
        
        df_exibir = df_tar.copy()
        if qtd_view != "Todos":
            df_exibir = df_tar.head(int(qtd_view))
        
        with col_res:
            st.caption(f"Exibindo {len(df_exibir)} de {len(df_tar)} tarefas encontradas.")

        if not df_exibir.empty:
            for i, row in df_exibir.iterrows():
                stt = row['status']
                cor_status = "🔴"
                if stt in ['Entregue', 'Concluído', 'Pago']: cor_status = "🟢"
                elif stt in ['Em execução', 'Em processamento', 'Pendente']: cor_status = "🟠"
                elif stt == 'Solicitado': cor_status = "🔵"

                data_fmt = pd.to_datetime(row['data_previsao']).strftime('%d/%m/%Y')
                titulo_card = f"{cor_status} [{stt.upper()}] {row['codigo_pedido']} - {row['nome_cliente']} | 📅 Prev: {data_fmt}"

                with st.expander(titulo_card):
                    st.write(f"**Produto:** {row['nome_produto']} ({row['categoria_produto']})")
                    st.write(f"**Obs:** {row['observacao_tarefa']}")
                    
                    c1, c2, c3, c4, c5, c6 = st.columns(6)
                    
                    # CORREÇÃO AQUI: Adicionado índice 'i' para garantir unicidade da chave
                    if c1.button("👤 Cliente", key=f"cli_{row['id']}_{i}"): ver_cliente(row['nome_cliente'], row['cpf_cliente'], row['telefone_cliente'], row['email_cliente'])
                    if c2.button("👁️ Ver", key=f"ver_{row['id']}_{i}"): dialog_visualizar(row)
                    if c3.button("🔄 Status", key=f"st_{row['id']}_{i}"): dialog_status(row)
                    if c4.button("✏️ Editar", key=f"ed_{row['id']}_{i}"): dialog_editar(row)
                    if c5.button("📜 Hist.", key=f"his_{row['id']}_{i}"): dialog_historico(row['id'])
                    if c6.button("🗑️ Excluir", key=f"del_{row['id']}_{i}"): dialog_confirmar_exclusao(row['id'])
        else:
            st.info("Nenhuma tarefa encontrada com os filtros atuais.")

    # ------------------------------------
    # ABA 2: TABELAS (NOVO SUBMENU)
    # ------------------------------------
    with tab_tabelas:
        st.markdown("#### 🗃️ Gestão de Tabela: Tarefas")
        st.caption("Edite diretamente os dados da tabela no banco de dados. Use com cuidado.")

        tabela_alvo = "tarefas" # Define a tabela padrão
        schema_alvo = "public"  # Padrão mais provável

        # Seletor para caso o usuário queira mudar para 'admin.tarefas'
        opcao_tabela = st.radio("Selecionar Tabela:", ["Tarefas (Padrão)", "Tarefas (Admin)"], horizontal=True)
        if opcao_tabela == "Tarefas (Admin)":
            schema_alvo = "admin"

        conn = get_conn()
        if conn:
            try:
                # Carrega os dados brutos da tabela
                query = f"SELECT * FROM {schema_alvo}.{tabela_alvo} ORDER BY id DESC"
                df_tab = pd.read_sql(query, conn)
                conn.close()

                st.write(f"Lendo tabela: `{schema_alvo}.{tabela_alvo}`")
                
                df_editado = st.data_editor(
                    df_tab,
                    key="editor_tabela_tarefas",
                    use_container_width=True,
                    num_rows="dynamic",
                    disabled=["id", "data_criacao", "data_atualizacao"] # Protege campos automáticos
                )

                if st.button("💾 Salvar Alterações na Tabela", type="primary"):
                    with st.spinner("Salvando alterações..."):
                        ok, msg = salvar_tabela_generica(schema_alvo, tabela_alvo, df_tab, df_editado)
                        if ok:
                            st.success(msg)
                            time.sleep(1.5)
                            st.rerun()
                        else:
                            st.error(f"Erro ao salvar: {msg}")
            
            except Exception as e:
                if conn: conn.close()
                st.error(f"Erro ao carregar a tabela '{schema_alvo}.{tabela_alvo}'. Verifique se ela existe.")
                st.info("Dica: Se você acabou de criar a tabela no banco 'public', use a opção 'Tarefas (Padrão)'.")

if __name__ == "__main__":
    app_produtos()