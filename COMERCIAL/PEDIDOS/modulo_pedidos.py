import streamlit as st
import pandas as pd
import psycopg2
import time
import re
from datetime import datetime, date
import modulo_wapi

try:
    import conexao
except ImportError:
    st.error("Erro crítico: Arquivo conexao.py não localizado.")

# --- CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")
        return None

# =============================================================================
# 1. FUNÇÕES AUXILIARES
# =============================================================================

def listar_modelos_mensagens():
    conn = get_conn()
    if conn:
        try:
            query = "SELECT chave_status FROM wapi_templates WHERE modulo = 'PEDIDOS' ORDER BY chave_status ASC"
            df = pd.read_sql(query, conn)
            conn.close()
            return df['chave_status'].tolist()
        except: conn.close()
    return []

def listar_tabelas_schema(schema_name):
    conn = get_conn()
    if conn:
        try:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name"
            df = pd.read_sql(query, conn, params=(schema_name,))
            conn.close()
            return df['table_name'].tolist()
        except: conn.close()
    return []

# =============================================================================
# 2. LÓGICA FINANCEIRA E DE PEDIDOS
# =============================================================================

def registrar_movimentacao_financeira(conn, dados_pedido, tipo_lancamento, valor):
    """
    Registra movimentação no extrato financeiro (CREDITO ou DEBITO).
    """
    try:
        cur = conn.cursor()
        id_cliente = dados_pedido['id_cliente']
        
        # 1. Buscar Saldo Anterior do Cliente
        cur.execute("""
            SELECT saldo_novo 
            FROM cliente.extrato_carteira_por_produto 
            WHERE id_cliente = %s 
            ORDER BY id DESC LIMIT 1
        """, (str(id_cliente),))
        res = cur.fetchone()
        saldo_anterior = float(res[0]) if res else 0.0
        
        # 2. Calcular Novo Saldo
        valor_float = float(valor)
        if tipo_lancamento == 'CREDITO':
            saldo_novo = saldo_anterior + valor_float
        else: # DEBITO
            saldo_novo = saldo_anterior - valor_float
            
        # 3. Inserir no Extrato
        # Usamos 'PEDIDOS' como origem fixa para rastreabilidade
        cur.execute("""
            INSERT INTO cliente.extrato_carteira_por_produto (
                id_cliente, data_lancamento, tipo_lancamento, 
                produto_vinculado, origem_lancamento, 
                valor_lancado, saldo_anterior, saldo_novo, nome_usuario
            ) VALUES (%s, NOW(), %s, %s, 'PEDIDOS', %s, %s, %s, 'Sistema')
        """, (
            str(id_cliente), 
            tipo_lancamento, 
            f"Pedido {dados_pedido['codigo']} - {dados_pedido['nome_produto']}", 
            valor_float, 
            saldo_anterior, 
            saldo_novo
        ))
        return True
    except Exception as e:
        print(f"Erro ao registrar financeiro: {e}")
        return False

def registrar_custo_carteira_upsert(conn, dados_cliente, dados_produto, valor_custo, origem_custo_txt):
    """
    Verifica se já existe custo para este Cliente + ORIGEM.
    Se existir -> Atualiza (Valor e Data).
    Se não existir -> Insere novo.
    """
    try:
        cur = conn.cursor()
        
        id_user = str(dados_cliente.get('id_usuario_vinculo', ''))
        nome_user = str(dados_cliente.get('nome_usuario_vinculo', ''))
        
        if id_user == 'None' or not id_user: 
            id_user = '0'
            nome_user = 'Sem Vínculo'

        # 1. Verifica se já existe registro para este Cliente e esta ORIGEM
        sql_check = """
            SELECT id FROM cliente.valor_custo_carteira_cliente 
            WHERE id_cliente = %s AND origem_custo = %s
        """
        cur.execute(sql_check, (str(dados_cliente['id']), str(origem_custo_txt)))
        resultado = cur.fetchone()

        if resultado:
            # --- ATUALIZAR (UPDATE) ---
            id_existente = resultado[0]
            sql_update = """
                UPDATE cliente.valor_custo_carteira_cliente SET
                    valor_custo = %s,
                    nome_usuario = %s,
                    id_usuario = %s,
                    data_criacao = NOW()
                WHERE id = %s
            """
            cur.execute(sql_update, (
                float(valor_custo),
                nome_user, id_user,
                id_existente
            ))
        else:
            # --- INSERIR (INSERT) ---
            sql_insert = """
                INSERT INTO cliente.valor_custo_carteira_cliente (
                    id_cliente, nome_cliente,
                    id_usuario, nome_usuario,
                    id_produto, nome_produto,
                    origem_custo, valor_custo,
                    data_criacao
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """
            cur.execute(sql_insert, (
                str(dados_cliente['id']), dados_cliente['nome'],
                id_user, nome_user,
                str(dados_produto['id']), dados_produto['nome'],
                str(origem_custo_txt), float(valor_custo)
            ))
            
        return True, ""
    except Exception as e:
        print(f"Erro ao salvar custo carteira: {e}") 
        return False, str(e)

def criar_pedido_novo_fluxo(cliente, produto, qtd, valor_unitario, valor_total, valor_custo_informado, origem_custo_txt, avisar_cliente, observacao):
    codigo = f"PED-{datetime.now().strftime('%y%m%d%H%M')}"
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO pedidos (codigo, id_cliente, nome_cliente, cpf_cliente, telefone_cliente,
                                     id_produto, nome_produto, categoria_produto, quantidade, valor_unitario, valor_total,
                                     custo_carteira, origem_custo, data_solicitacao, observacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s) RETURNING id
            """, (codigo, int(cliente['id']), cliente['nome'], cliente['cpf'], cliente['telefone'],
                  int(produto['id']), produto['nome'], produto['tipo'], int(qtd), float(valor_unitario), float(valor_total),
                  float(valor_custo_informado), str(origem_custo_txt), observacao))
            
            id_novo = cur.fetchone()[0]
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao) VALUES (%s, 'Solicitado', 'Criado via Novo Fluxo')", (id_novo,))
            
            # Tenta registrar/atualizar o custo na carteira
            res_upsert = registrar_custo_carteira_upsert(conn, cliente, produto, valor_custo_informado, origem_custo_txt)
            if not res_upsert[0]:
                print(f"⚠️ Aviso: Custo não salvo na carteira. Erro: {res_upsert[1]}")
            
            conn.commit()
            conn.close()
            
            msg_whats = ""
            if avisar_cliente and cliente['telefone']:
                try:
                    inst = modulo_wapi.buscar_instancia_ativa()
                    if inst:
                        tpl = modulo_wapi.buscar_template("PEDIDOS", "criacao")
                        if tpl:
                            msg = tpl.replace("{nome}", str(cliente['nome']).split()[0]).replace("{pedido}", codigo).replace("{produto}", str(produto['nome']))
                            modulo_wapi.enviar_msg_api(inst[0], inst[1], cliente['telefone'], msg)
                            msg_whats = " (WhatsApp Enviado)"
                except: pass

            return True, f"Pedido {codigo} criado!{msg_whats}"

        except Exception as e: return False, str(e)
    return False, "Erro conexão"

# =============================================================================
# 3. CRUD E FUNÇÕES GERAIS
# =============================================================================

def buscar_clientes():
    conn = get_conn()
    if conn:
        query = """
            SELECT c.id, c.nome, c.cpf, c.telefone, c.email, c.id_usuario_vinculo, u.nome as nome_usuario_vinculo
            FROM admin.clientes c
            LEFT JOIN clientes_usuarios u ON c.id_usuario_vinculo = u.id
            ORDER BY c.nome
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def buscar_produtos():
    conn = get_conn()
    if conn:
        df = pd.read_sql("SELECT id, codigo, nome, tipo, preco, origem_custo FROM produtos_servicos WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close()
        return df
    return pd.DataFrame()

def buscar_historico_pedido(id_pedido):
    conn = get_conn()
    if conn:
        query = "SELECT data_mudanca, status_novo, observacao FROM pedidos_historico WHERE id_pedido = %s ORDER BY data_mudanca DESC"
        df = pd.read_sql(query, conn, params=(int(id_pedido),))
        conn.close()
        return df
    return pd.DataFrame()

def atualizar_status_pedido(id_pedido, novo_status, dados_pedido, avisar, obs, modelo_msg):
    # --- REGRA DE VALIDAÇÃO ---
    if dados_pedido['status'] == novo_status:
        return False, f"⚠️ O pedido já está com o status '{novo_status}'. Nenhuma alteração realizada."
    
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            obs_hist = obs
            coluna_data = ""
            
            if novo_status == "Solicitado":
                coluna_data = ", data_solicitacao = NOW()"
            elif novo_status == "Pago":
                coluna_data = ", data_pago = NOW()"
            elif novo_status == "Pendente":
                coluna_data = ", data_pendente = NOW()"
            elif novo_status == "Cancelado":
                coluna_data = ", data_cancelado = NOW()"

            sql_update = f"UPDATE pedidos SET status=%s, observacao=%s, data_atualizacao=NOW(){coluna_data} WHERE id=%s"
            cur.execute(sql_update, (novo_status, obs, id_pedido))
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao) VALUES (%s, %s, %s)", (id_pedido, novo_status, obs_hist))
            
            # --- REGRA FINANCEIRA ---
            if novo_status == "Pago":
                registrar_movimentacao_financeira(conn, dados_pedido, "CREDITO", dados_pedido['valor_total'])
            elif novo_status == "Cancelado":
                registrar_movimentacao_financeira(conn, dados_pedido, "DEBITO", dados_pedido['valor_total'])
            
            conn.commit(); conn.close()
            
            if avisar and dados_pedido['telefone_cliente']:
                inst = modulo_wapi.buscar_instancia_ativa()
                if inst:
                    chave = modelo_msg if modelo_msg != "Automático (Padrão)" else novo_status.lower().replace(" ", "_")
                    tpl = modulo_wapi.buscar_template("PEDIDOS", chave)
                    if tpl:
                        msg = tpl.replace("{nome}", str(dados_pedido['nome_cliente']).split()[0]).replace("{pedido}", str(dados_pedido['codigo'])).replace("{status}", novo_status).replace("{produto}", str(dados_pedido['nome_produto']))
                        modulo_wapi.enviar_msg_api(inst[0], inst[1], dados_pedido['telefone_cliente'], msg)
            return True, "Status atualizado com sucesso!"
        except Exception as e:
            print(e); return False, str(e)
    return False, "Erro conexão"

def excluir_pedido_db(id_pedido):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM pedidos WHERE id=%s", (id_pedido,))
            conn.commit(); conn.close()
            return True
        except: return False
    return False

def editar_dados_pedido_completo(id_pedido, nova_qtd, novo_valor, dados_antigos, novo_custo_carteira, carteira_vinculada, origem_custo):
    total = nova_qtd * novo_valor
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE pedidos SET quantidade=%s, valor_unitario=%s, valor_total=%s, custo_carteira=%s, data_atualizacao=NOW()
                WHERE id=%s
            """, (nova_qtd, novo_valor, total, float(novo_custo_carteira), id_pedido))
            conn.commit(); conn.close()
            return True, ""
        except Exception as e: return False, str(e)
    return False, "Erro BD"

def salvar_tabela_generica(schema, tabela, df_original, df_editado):
    conn = get_conn()
    if not conn: return False, "Sem conexão"
    try:
        cur = conn.cursor()
        pk = 'id' 
        
        if pk in df_original.columns:
            ids_originais = set(df_original[pk].dropna().astype(int).tolist())
        else:
            ids_originais = set() 

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

        for index, row in df_editado.iterrows():
            colunas_validas = list(row.index)
            cols_ignore = ['data_criacao', 'data_atualizacao', 'data_solicitacao', 'data_pago', 'data_pendente', 'data_cancelado']
            colunas_validas = [c for c in colunas_validas if c not in cols_ignore]

            row_id = row.get(pk)
            eh_novo = pd.isna(row_id) or row_id == '' or row_id is None
            
            if eh_novo:
                cols_insert = [c for c in colunas_validas if c != pk]
                vals_insert = [row[c] for c in cols_insert]
                placeholders = ", ".join(["%s"] * len(cols_insert))
                cols_str = ", ".join(cols_insert)
                if cols_insert:
                    cur.execute(f"INSERT INTO {schema}.{tabela} ({cols_str}) VALUES ({placeholders})", vals_insert)
            elif int(row_id) in ids_originais:
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

# --- ESTADO (MODALS) ---
def abrir_modal(tipo, pedido=None):
    st.session_state['modal_ativo'] = tipo
    st.session_state['pedido_ativo'] = pedido

def fechar_modal():
    st.session_state['modal_ativo'] = None
    st.session_state['pedido_ativo'] = None

# =============================================================================
# 4. COMPONENTE DE NOVO PEDIDO
# =============================================================================

def renderizar_novo_pedido_tab():
    st.markdown("### ➕ Novo Pedido")
    
    # Carregar dados iniciais
    df_c = buscar_clientes()
    df_p = buscar_produtos()
    
    if df_c.empty or df_p.empty: 
        st.warning("Cadastre clientes e produtos antes.")
        return

    # --- INICIALIZAÇÃO DE ESTADO ---
    if 'np_cli_idx' not in st.session_state: st.session_state.np_cli_idx = 0
    if 'np_prod_idx' not in st.session_state: st.session_state.np_prod_idx = 0
    
    prod_inicial = df_p.iloc[st.session_state.np_prod_idx]
    if 'np_val' not in st.session_state: st.session_state.np_val = float(prod_inicial['preco'] or 0.0)
    if 'np_qtd' not in st.session_state: st.session_state.np_qtd = 1
    if 'np_origem' not in st.session_state: st.session_state.np_origem = prod_inicial.get('origem_custo', 'Geral') or 'Geral'
    if 'np_custo' not in st.session_state: st.session_state.np_custo = 0.0
    
    # --- HELPER DE CUSTO ---
    def buscar_custo_referencia(id_cliente, id_produto):
        custo = 0.0
        conn_chk = get_conn()
        if conn_chk:
            try:
                cur = conn_chk.cursor()
                cur.execute("SELECT valor_custo FROM cliente.valor_custo_carteira_cliente WHERE id_cliente = %s AND id_produto = %s", (str(id_cliente), str(id_produto)))
                chk = cur.fetchone()
                if chk: custo = float(chk[0])
                conn_chk.close()
            except: conn_chk.close()
        return custo

    # --- CALLBACKS ---
    def on_change_produto():
        idx = st.session_state.np_prod_idx 
        prod = df_p.iloc[idx]
        
        st.session_state.np_val = float(prod['preco'] or 0.0)
        origem = prod.get('origem_custo', 'Geral')
        st.session_state.np_origem = origem if origem else 'Geral'
        
        idx_c = st.session_state.np_cli_idx
        cli = df_c.iloc[idx_c]
        st.session_state.np_custo = buscar_custo_referencia(cli['id'], prod['id'])

    def on_change_cliente():
        idx_c = st.session_state.np_cli_idx
        idx_p = st.session_state.np_prod_idx
        
        cli = df_c.iloc[idx_c]
        prod = df_p.iloc[idx_p]
        st.session_state.np_custo = buscar_custo_referencia(cli['id'], prod['id'])

    def atualizar_calculo():
        pass

    if st.session_state.np_custo == 0.0:
        cli_atual = df_c.iloc[st.session_state.np_cli_idx]
        prod_atual = df_p.iloc[st.session_state.np_prod_idx]
        st.session_state.np_custo = buscar_custo_referencia(cli_atual['id'], prod_atual['id'])

    with st.container(border=True):
        # --- INTERFACE ---
        c1, c2 = st.columns(2)
        
        # 1. Cliente
        st.session_state.np_cli_idx = c1.selectbox(
            "1. Cliente", 
            range(len(df_c)), 
            index=st.session_state.np_cli_idx,
            format_func=lambda x: f"{df_c.iloc[x]['nome']} / {df_c.iloc[x]['cpf']} / {df_c.iloc[x]['telefone']}", 
            key="np_cli_selector", 
            on_change=lambda: st.session_state.update({'np_cli_idx': st.session_state.np_cli_selector}) or on_change_cliente()
        )
        
        # 2. Produto
        st.session_state.np_prod_idx = c2.selectbox(
            "3. Produto", 
            range(len(df_p)), 
            index=st.session_state.np_prod_idx,
            format_func=lambda x: df_p.iloc[x]['nome'], 
            key="np_prod_selector",
            on_change=lambda: st.session_state.update({'np_prod_idx': st.session_state.np_prod_selector}) or on_change_produto()
        )
        
        c2.info(f"📍 **Origem:** {st.session_state.np_origem}")
        
        st.divider()
        
        # 4. Qtd e Valor
        c3, c4, c5 = st.columns(3)
        
        qtd = c3.number_input("Qtd", min_value=1, key="np_qtd", on_change=atualizar_calculo)
        val = c4.number_input("Valor Unit.", min_value=0.0, format="%.2f", step=1.0, key="np_val", on_change=atualizar_calculo)
        
        total = st.session_state.np_qtd * st.session_state.np_val
        c5.metric("Total", f"R$ {total:.2f}")
        
        st.divider()
        
        # 5. Custo
        c_custo = st.number_input("Valor de Custo (Referência)", 
                                  step=1.0, 
                                  help="Valor registrado para controle de custo deste cliente.",
                                  key="np_custo")
        
        # 6. Observação
        obs = st.text_area("Observação do Pedido", placeholder="Detalhes adicionais...")

        avisar = st.checkbox("Avisar WhatsApp?", value=True)
        
        if st.button("✅ Criar Pedido", type="primary", use_container_width=True):
            cli_final = df_c.iloc[st.session_state.np_cli_idx]
            prod_final = df_p.iloc[st.session_state.np_prod_idx]
            
            ok, res = criar_pedido_novo_fluxo(
                cli_final, prod_final, 
                st.session_state.np_qtd, st.session_state.np_val, total, 
                st.session_state.np_custo, st.session_state.np_origem, 
                avisar, obs
            )
            if ok: 
                st.success(res)
                time.sleep(1.5)
                st.rerun()
            else: st.error(res)

# =============================================================================
# 5. DIALOGS DE AÇÃO
# =============================================================================

@st.dialog("✏️ Editar", width="large")
def dialog_editar(ped):
    with st.form("fe"):
        st.markdown(f"#### Editando: {ped['codigo']}")
        c_i1, c_i2 = st.columns(2)
        c_i1.text_input("Cliente", value=ped['nome_cliente'], disabled=True)
        c_i2.text_input("Produto", value=ped['nome_produto'], disabled=True)

        st.divider()
        custo_atual = float(ped['custo_carteira'] or 0.0)
        novo_custo = st.number_input("Custo Carteira (R$)", value=custo_atual, step=0.01)

        c_d1, c_d2 = st.columns(2)
        nq = c_d1.number_input("Quantidade", 1, value=int(ped['quantidade']))
        nv = c_d2.number_input("Valor Unitário", 0.0, value=float(ped['valor_unitario']))
        st.info(f"Novo Total: R$ {nq*nv:.2f}")

        if st.form_submit_button("💾 Salvar Alterações"):
            ok, msg = editar_dados_pedido_completo(ped['id'], nq, nv, ped, novo_custo, None, None)
            if ok: st.success(f"Salvo!"); time.sleep(1); fechar_modal(); st.rerun()
            else: st.error(f"Erro: {msg}")

@st.dialog("🔄 Status")
def dialog_status(ped):
    st.write(f"🏢 **Cliente:** {ped['nome_cliente']}")
    st.divider()
    lst = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
    try: idx = lst.index(ped['status']) 
    except: idx = 0
    mods = ["Automático (Padrão)"] + listar_modelos_mensagens()
    with st.form("fs"):
        ns = st.selectbox("Status", lst, index=idx)
        mod = st.selectbox("Msg", mods)
        obs = st.text_area("Obs")
        av = st.checkbox("Avisar?", value=True)
        if st.form_submit_button("Atualizar"):
            ok, msg = atualizar_status_pedido(ped['id'], ns, ped, av, obs, mod)
            if ok:
                st.success(msg)
                time.sleep(1)
                fechar_modal()
                st.rerun()
            else:
                st.warning(msg)

    st.divider(); st.caption("Histórico")
    st.dataframe(buscar_historico_pedido(ped['id']), hide_index=True)

@st.dialog("📜 Histórico")
def dialog_historico(id_pedido, codigo):
    st.markdown(f"### Histórico do Pedido: {codigo}")
    df = buscar_historico_pedido(id_pedido)
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum histórico encontrado.")

@st.dialog("🗑️ Excluir")
def dialog_excluir(pid):
    st.warning("Confirmar?")
    if st.button("Sim", type="primary"):
        if excluir_pedido_db(pid): st.success("Apagado!"); time.sleep(1); fechar_modal(); st.rerun()

@st.dialog("📝 Tarefa")
def dialog_tarefa(ped):
    with st.form("ft"):
        dt = st.date_input("Previsão", datetime.now())
        obs = st.text_area("Obs")
        if st.form_submit_button("Criar"):
            conn = get_conn(); cur = conn.cursor()
            cur.execute("INSERT INTO tarefas (id_pedido, id_cliente, id_produto, data_previsao, observacao_tarefa, status) VALUES (%s,%s,%s,%s,%s,'Solicitado')", (ped['id'], ped['id_cliente'], ped['id_produto'], dt, obs))
            conn.commit(); conn.close()
            st.success("Criada!"); time.sleep(1); fechar_modal(); st.rerun()

# =============================================================================
# 5. APP PRINCIPAL
# =============================================================================

def app_pedidos():
    tab_novo, tab_lista, tab_param, tab_admin = st.tabs(["➕ Novo Pedido", "📋 Lista de Pedidos", "⚙️ Parâmetros", "🗃️ Tabelas Admin"])

    # ABA 1: NOVO PEDIDO (SUB MENU PRIMEIRO)
    with tab_novo:
        renderizar_novo_pedido_tab()

    # ABA 2: LISTA DE PEDIDOS
    with tab_lista:
        if 'modal_ativo' not in st.session_state: st.session_state.update({'modal_ativo': None, 'pedido_ativo': None})
        
        conn = get_conn()
        if conn:
            # FILTROS DE PESQUISA AVANÇADOS
            with st.expander("🔍 Filtros de Pesquisa", expanded=True):
                c1, c2, c3, c4 = st.columns(4)
                
                filtro_cliente = c1.text_input("Cliente", placeholder="Nome do cliente")
                filtro_produto = c2.text_input("Produto", placeholder="Nome do produto")
                opcoes_status = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
                filtro_status = c3.multiselect("Status", options=opcoes_status)
                filtro_datas = c4.date_input("Período (Criação)", value=[])

            # QUERY
            query_base = """
                SELECT p.*, c.nome_empresa, c.email as email_cliente 
                FROM pedidos p 
                LEFT JOIN admin.clientes c ON p.id_cliente = c.id 
                WHERE 1=1
            """
            params = []

            if filtro_cliente:
                query_base += " AND p.nome_cliente ILIKE %s"
                params.append(f"%{filtro_cliente}%")
            
            if filtro_produto:
                query_base += " AND p.nome_produto ILIKE %s"
                params.append(f"%{filtro_produto}%")
            
            if filtro_status:
                placeholders = ",".join(["%s"] * len(filtro_status))
                query_base += f" AND p.status IN ({placeholders})"
                params.extend(filtro_status)
            
            if len(filtro_datas) == 2:
                query_base += " AND p.data_criacao BETWEEN %s AND %s"
                params.append(filtro_datas[0])
                params.append(filtro_datas[1])

            query_base += " ORDER BY p.data_criacao DESC LIMIT 10"

            df = pd.read_sql(query_base, conn, params=params)
            conn.close()
            
            st.divider()
            
            if not df.empty:
                for _, row in df.iterrows():
                    cor = "🔴"; 
                    if row['status'] == 'Pago': cor = "🟢"
                    elif row['status'] == 'Pendente': cor = "🟠"
                    elif row['status'] == 'Solicitado': cor = "🔵"
                    
                    empresa_show = f"({row['nome_empresa']})" if row.get('nome_empresa') else ""
                    
                    with st.expander(f"{cor} [{row['status']}] {row['codigo']} - {row['nome_cliente']} {empresa_show} | R$ {row['valor_total']:.2f}"):
                        
                        # --- LAYOUT AMIGÁVEL ATUALIZADO ---
                        st.markdown("#### 📋 Detalhes do Pedido")
                        
                        gc1, gc2, gc3 = st.columns(3)
                        
                        with gc1:
                            st.caption("👤 Dados do Cliente")
                            st.write(f"**Nome:** {row['nome_cliente']}")
                            st.write(f"**CPF:** {row['cpf_cliente']}")
                            st.write(f"**Telefone:** {row['telefone_cliente']}")
                            st.write(f"**Email:** {row.get('email_cliente') or '-'}")

                        with gc2:
                            st.caption("📦 Produto e Valores")
                            st.write(f"**Produto:** {row['nome_produto']}")
                            st.write(f"**Categoria:** {row['categoria_produto']}")
                            st.write(f"**Qtd:** {row['quantidade']}")
                            st.write(f"**Total:** :green[R$ {row['valor_total']:.2f}]")
                            st.write(f"**Custo:** :red[R$ {row['custo_carteira']:.2f}]" if pd.notna(row['custo_carteira']) else "Custo: -")

                        with gc3:
                            st.caption("📅 Datas e Status")
                            st.write(f"**Criação:** {row['data_criacao'].strftime('%d/%m/%Y %H:%M')}")
                            st.write(f"**Solicitado:** {row['data_solicitacao'].strftime('%d/%m %H:%M') if pd.notna(row['data_solicitacao']) else '-'}")
                            st.write(f"**Pago:** {row['data_pago'].strftime('%d/%m %H:%M') if pd.notna(row['data_pago']) else '-'}")
                            st.write(f"**Origem Custo:** {row.get('origem_custo', '-')}")

                        if row['observacao']:
                            st.info(f"**Observação:** {row['observacao']}")
                            
                        st.divider()
                        c1, c2, c3, c4, c5, c6 = st.columns(6)
                        ts = int(time.time())
                        c1.button("Cliente", key=f"c_{row['id']}_{ts}", on_click=abrir_modal, args=('cliente', row), use_container_width=True)
                        c2.button("Editar", key=f"e_{row['id']}_{ts}", on_click=abrir_modal, args=('editar', row), use_container_width=True)
                        c3.button("Status", key=f"s_{row['id']}_{ts}", on_click=abrir_modal, args=('status', row), use_container_width=True)
                        c4.button("Histórico", key=f"h_{row['id']}_{ts}", on_click=abrir_modal, args=('historico', row), use_container_width=True)
                        c5.button("Excluir", key=f"d_{row['id']}_{ts}", on_click=abrir_modal, args=('excluir', row), use_container_width=True)
                        c6.button("Tarefa", key=f"t_{row['id']}_{ts}", on_click=abrir_modal, args=('tarefa', row), use_container_width=True)
            else: st.info("Nenhum pedido encontrado com os filtros atuais.")
    
    # ABA 3: PARÂMETROS
    with tab_param:
        st.markdown("#### ⚙️ Edição Técnica da Tabela Pedidos")
        conn = get_conn()
        if conn:
            df_pedidos_raw = pd.read_sql("SELECT * FROM pedidos ORDER BY id DESC LIMIT 50", conn)
            st.markdown("**Tabela Pedidos:**")
            st.dataframe(df_pedidos_raw, height=200)
            
            st.markdown("---")
            st.markdown("**Tabela Custos (cliente.valor_custo_carteira_cliente):**")
            try:
                df_custos = pd.read_sql("SELECT * FROM cliente.valor_custo_carteira_cliente ORDER BY id DESC LIMIT 50", conn)
                st.dataframe(df_custos, height=200)
            except: st.warning("Tabela de custos ainda não criada.")
            conn.close()

    # ABA 4: TABELAS ADMIN
    with tab_admin:
        st.markdown("#### 🗃️ Gestão de Tabelas (Schema: Admin)")
        tabelas = listar_tabelas_schema('admin')
        
        if tabelas:
            sel_tabela = st.selectbox("Selecione a Tabela para Editar:", tabelas)
            st.divider()
            
            if sel_tabela:
                conn = get_conn()
                if conn:
                    try:
                        query = f"SELECT * FROM admin.{sel_tabela} ORDER BY id"
                        df_tab = pd.read_sql(query, conn)
                        conn.close()
                        
                        st.caption(f"Editando tabela: **admin.{sel_tabela}**")
                        
                        df_tab_editado = st.data_editor(
                            df_tab,
                            key=f"editor_admin_{sel_tabela}",
                            use_container_width=True,
                            num_rows="dynamic"
                        )
                        
                        if st.button(f"💾 Salvar Alterações em {sel_tabela}", type="primary"):
                            with st.spinner(f"Processando alterações em admin.{sel_tabela}..."):
                                ok, msg = salvar_tabela_generica('admin', sel_tabela, df_tab, df_tab_editado)
                                if ok:
                                    st.success(f"Tabela {sel_tabela} atualizada com sucesso!")
                                    time.sleep(1.5)
                                    st.rerun()
                                else:
                                    st.error(f"Erro ao salvar: {msg}")

                    except Exception as e:
                        if conn: conn.close()
                        st.error(f"Erro ao carregar tabela: {e}")

    # Roteador de Modais
    m = st.session_state['modal_ativo']; p = st.session_state['pedido_ativo']
    
    if m == 'cliente' and p is not None: 
        st.dialog("👤 Cliente")(lambda: st.write(f"Nome: {p['nome_cliente']}\nCPF: {p['cpf_cliente']}\nTel: {p['telefone_cliente']}"))()
        fechar_modal()
    elif m == 'editar' and p is not None: dialog_editar(p)
    elif m == 'status' and p is not None: dialog_status(p)
    elif m == 'historico' and p is not None: dialog_historico(p['id'], p['codigo'])
    elif m == 'excluir' and p is not None: dialog_excluir(p['id'])
    elif m == 'tarefa' and p is not None: dialog_tarefa(p)

if __name__ == "__main__":
    app_pedidos()