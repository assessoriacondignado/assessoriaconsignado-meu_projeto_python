import streamlit as st
import pandas as pd
import psycopg2
import time
import re
import os
import sys
from datetime import datetime, date
import modulo_wapi

# Ajuste de path para importar módulos da raiz e de COMERCIAL
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
diretorio_comercial = os.path.dirname(diretorio_atual) # Pasta COMERCIAL
raiz_projeto = os.path.dirname(diretorio_comercial)    # Raiz do Projeto

if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)

try:
    import conexao
except ImportError:
    st.error("Erro crítico: Arquivo conexao.py não localizado.")

# Importação do módulo de configurações para templates
try:
    from COMERCIAL import modulo_comercial_configuracoes
except ImportError:
    modulo_comercial_configuracoes = None
    st.warning("Aviso: modulo_comercial_configuracoes não encontrado. Templates podem falhar.")

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
    """Busca os modelos de mensagem cadastrados no Config para este módulo"""
    if modulo_comercial_configuracoes:
        return modulo_comercial_configuracoes.listar_chaves_config("PEDIDOS")
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

        sql_check = """
            SELECT id FROM cliente.valor_custo_carteira_cliente 
            WHERE id_cliente = %s AND origem_custo = %s
        """
        cur.execute(sql_check, (str(dados_cliente['id']), str(origem_custo_txt)))
        resultado = cur.fetchone()

        if resultado:
            id_existente = resultado[0]
            sql_update = """
                UPDATE cliente.valor_custo_carteira_cliente SET
                    valor_custo = %s,
                    nome_usuario = %s,
                    id_usuario = %s,
                    data_criacao = NOW()
                WHERE id = %s
            """
            cur.execute(sql_update, (float(valor_custo), nome_user, id_user, id_existente))
        else:
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
            
            res_upsert = registrar_custo_carteira_upsert(conn, cliente, produto, valor_custo_informado, origem_custo_txt)
            if not res_upsert[0]:
                print(f"⚠️ Aviso: Custo não salvo na carteira. Erro: {res_upsert[1]}")
            
            conn.commit()
            conn.close()
            
            msg_whats = ""
            if avisar_cliente and cliente['telefone'] and modulo_comercial_configuracoes:
                try:
                    inst = modulo_wapi.buscar_instancia_ativa()
                    if inst:
                        tpl = modulo_comercial_configuracoes.buscar_template_config("PEDIDOS", "criacao")
                        if tpl:
                            msg = tpl.replace("{nome}", str(cliente['nome']).split()[0]) \
                                     .replace("{pedido}", codigo) \
                                     .replace("{produto}", str(produto['nome']))
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
    if dados_pedido['status'] == novo_status:
        return False, f"⚠️ O pedido já está com o status '{novo_status}'. Nenhuma alteração realizada."
    
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            obs_hist = obs
            coluna_data = ""
            if novo_status == "Solicitado": coluna_data = ", data_solicitacao = NOW()"
            elif novo_status == "Pago": coluna_data = ", data_pago = NOW()"
            elif novo_status == "Pendente": coluna_data = ", data_pendente = NOW()"
            elif novo_status == "Cancelado": coluna_data = ", data_cancelado = NOW()"

            sql_update = f"UPDATE pedidos SET status=%s, observacao=%s, data_atualizacao=NOW(){coluna_data} WHERE id=%s"
            cur.execute(sql_update, (novo_status, obs, id_pedido))
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao) VALUES (%s, %s, %s)", (id_pedido, novo_status, obs_hist))
            
            if novo_status == "Pago":
                registrar_movimentacao_financeira(conn, dados_pedido, "CREDITO", dados_pedido['valor_total'])
            elif novo_status == "Cancelado":
                registrar_movimentacao_financeira(conn, dados_pedido, "DEBITO", dados_pedido['valor_total'])
            
            conn.commit(); conn.close()
            
            if avisar and dados_pedido['telefone_cliente'] and modulo_comercial_configuracoes:
                inst = modulo_wapi.buscar_instancia_ativa()
                if inst:
                    chave = modelo_msg if modelo_msg and modelo_msg != "Automático (Padrão)" else novo_status.lower().replace(" ", "_")
                    tpl = modulo_comercial_configuracoes.buscar_template_config("PEDIDOS", chave)
                    if tpl:
                        msg = tpl.replace("{nome}", str(dados_pedido['nome_cliente']).split()[0]) \
                                 .replace("{pedido}", str(dados_pedido['codigo'])) \
                                 .replace("{status}", novo_status) \
                                 .replace("{produto}", str(dados_pedido['nome_produto']))
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

def editar_dados_pedido_completo(id_pedido, dados_novos):
    try:
        conn = get_conn()
        if not conn: return False, "Erro de conexão"
        cur = conn.cursor()
        total = float(dados_novos['qtd']) * float(dados_novos['valor'])
        sql = """
            UPDATE pedidos SET 
                id_cliente=%s, nome_cliente=%s, cpf_cliente=%s, telefone_cliente=%s,
                id_produto=%s, nome_produto=%s, categoria_produto=%s,
                quantidade=%s, valor_unitario=%s, valor_total=%s,
                custo_carteira=%s, origem_custo=%s, observacao=%s,
                data_atualizacao=NOW()
            WHERE id=%s
        """
        cur.execute(sql, (
            int(dados_novos['cliente']['id']), str(dados_novos['cliente']['nome']), str(dados_novos['cliente']['cpf']), str(dados_novos['cliente']['telefone']),
            int(dados_novos['produto']['id']), str(dados_novos['produto']['nome']), str(dados_novos['produto']['tipo']),
            int(dados_novos['qtd']), float(dados_novos['valor']), float(total),
            float(dados_novos['custo']), str(dados_novos['origem']), str(dados_novos['obs']),
            int(id_pedido)
        ))
        conn.commit(); conn.close()
        return True, "Pedido atualizado completo!"
    except Exception as e: return False, str(e)

# =============================================================================
# 4. COMPONENTE DE NOVO PEDIDO (DEFINIÇÃO DA FUNÇÃO QUE FALTAVA)
# =============================================================================

def renderizar_novo_pedido_tab():
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

    def atualizar_calculo(): pass

    if st.session_state.np_custo == 0.0:
        cli_atual = df_c.iloc[st.session_state.np_cli_idx]
        prod_atual = df_p.iloc[st.session_state.np_prod_idx]
        st.session_state.np_custo = buscar_custo_referencia(cli_atual['id'], prod_atual['id'])

    with st.container(border=True):
        c1, c2 = st.columns(2)
        st.session_state.np_cli_idx = c1.selectbox(
            "1. Cliente", range(len(df_c)), index=st.session_state.np_cli_idx,
            format_func=lambda x: f"{df_c.iloc[x]['nome']} / {df_c.iloc[x]['cpf']}", 
            key="np_cli_selector", on_change=lambda: st.session_state.update({'np_cli_idx': st.session_state.np_cli_selector}) or on_change_cliente()
        )
        st.session_state.np_prod_idx = c2.selectbox(
            "3. Produto", range(len(df_p)), index=st.session_state.np_prod_idx,
            format_func=lambda x: df_p.iloc[x]['nome'], 
            key="np_prod_selector", on_change=lambda: st.session_state.update({'np_prod_idx': st.session_state.np_prod_selector}) or on_change_produto()
        )
        c2.info(f"📍 **Origem:** {st.session_state.np_origem}")
        st.divider()
        c3, c4, c5 = st.columns(3)
        qtd = c3.number_input("Qtd", min_value=1, key="np_qtd", on_change=atualizar_calculo)
        val = c4.number_input("Valor Unit.", min_value=0.0, format="%.2f", step=1.0, key="np_val", on_change=atualizar_calculo)
        total = st.session_state.np_qtd * st.session_state.np_val
        c5.metric("Total", f"R$ {total:.2f}")
        st.divider()
        c_custo = st.number_input("Valor de Custo (Referência)", step=1.0, key="np_custo")
        obs = st.text_area("Observação", placeholder="Detalhes...")
        avisar = st.checkbox("Avisar WhatsApp?", value=True)
        
        if st.button("✅ Criar Pedido", type="primary", use_container_width=True):
            cli_final = df_c.iloc[st.session_state.np_cli_idx]
            prod_final = df_p.iloc[st.session_state.np_prod_idx]
            ok, res = criar_pedido_novo_fluxo(
                cli_final, prod_final, st.session_state.np_qtd, st.session_state.np_val, total, 
                st.session_state.np_custo, st.session_state.np_origem, avisar, obs
            )
            if ok: st.success(res); time.sleep(1.5); st.rerun()
            else: st.error(res)

# =============================================================================
# 5. PAINÉIS LATERAIS (Novo Padrão)
# =============================================================================

def painel_dados_cliente(ped):
    st.markdown(f"### 👤 Cliente: {ped['nome_cliente']}")
    st.write(f"**CPF:** {ped['cpf_cliente']}")
    st.write(f"**Telefone:** {ped['telefone_cliente']}")
    st.write(f"**E-mail:** {ped.get('email_cliente', '-')}")

def painel_editar_pedido(ped):
    st.markdown(f"### ✏️ Editar: {ped['codigo']}")
    df_c = buscar_clientes()
    df_p = buscar_produtos()
    
    if df_c.empty or df_p.empty: st.error("Dados base vazios."); return

    with st.form("form_painel_editar"):
        # Selects
        try: curr_cli_idx = df_c[df_c['id'] == ped['id_cliente']].index[0]
        except: curr_cli_idx = 0
        idx_cli = st.selectbox("Cliente", range(len(df_c)), index=int(curr_cli_idx), format_func=lambda x: f"{df_c.iloc[x]['nome']} ({df_c.iloc[x]['cpf']})")
        sel_cli = df_c.iloc[idx_cli]
        
        try: curr_prod_idx = df_p[df_p['id'] == ped['id_produto']].index[0]
        except: curr_prod_idx = 0
        idx_prod = st.selectbox("Produto", range(len(df_p)), index=int(curr_prod_idx), format_func=lambda x: df_p.iloc[x]['nome'])
        sel_prod = df_p.iloc[idx_prod]

        st.divider()
        c_v1, c_v2 = st.columns(2)
        nq = c_v1.number_input("Qtd", min_value=1, value=int(ped['quantidade']))
        nv = c_v2.number_input("Valor", min_value=0.0, value=float(ped['valor_unitario']), format="%.2f")
        
        c_c1, c_c2 = st.columns(2)
        ncusto = c_c1.number_input("Custo (R$)", value=float(ped['custo_carteira'] or 0.0), step=0.01)
        norigem = c_c2.text_input("Origem", value=ped.get('origem_custo', ''))
        
        nobs = st.text_area("Observação", value=ped['observacao'] or "")
        st.info(f"Novo Total: R$ {nq*nv:.2f}")

        if st.form_submit_button("💾 Salvar"):
            dados_novos = {
                'cliente': sel_cli, 'produto': sel_prod, 'qtd': nq, 'valor': nv,
                'custo': ncusto, 'origem': norigem, 'obs': nobs
            }
            ok, msg = editar_dados_pedido_completo(ped['id'], dados_novos)
            if ok: 
                st.success("Salvo!"); time.sleep(1)
                st.session_state.ped_panel_active = False
                st.rerun()
            else: st.error(f"Erro: {msg}")

def painel_status_pedido(ped):
    st.markdown(f"### 🔄 Status: {ped['codigo']}")
    lst = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
    try: idx = lst.index(ped['status']) 
    except: idx = 0
    mods = ["Automático (Padrão)"] + listar_modelos_mensagens()
    
    with st.form("form_painel_status"):
        ns = st.selectbox("Novo Status", lst, index=idx)
        mod = st.selectbox("Modelo Msg", mods)
        obs = st.text_area("Observação")
        av = st.checkbox("Avisar Cliente?", value=True)
        
        if st.form_submit_button("Atualizar Status", type="primary"):
            ok, msg = atualizar_status_pedido(ped['id'], ns, ped, av, obs, mod)
            if ok:
                st.success(msg); time.sleep(1)
                st.session_state.ped_panel_active = False
                st.rerun()
            else: st.warning(msg)

def painel_historico_pedido(id_pedido):
    st.markdown("### 📜 Histórico")
    df = buscar_historico_pedido(id_pedido)
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else: st.info("Sem histórico.")

def painel_excluir_pedido(pid):
    st.markdown("### 🗑️ Excluir Pedido")
    st.warning("Esta ação é irreversível.")
    if st.button("Confirmar Exclusão", type="primary", use_container_width=True):
        if excluir_pedido_db(pid): 
            st.success("Apagado!"); time.sleep(1)
            st.session_state.ped_panel_active = False
            st.rerun()

def painel_tarefa_pedido(ped):
    st.markdown("### 📝 Criar Tarefa")
    with st.form("form_painel_tarefa"):
        dt = st.date_input("Previsão", datetime.now())
        obs = st.text_area("Obs da Tarefa")
        if st.form_submit_button("Criar"):
            conn = get_conn(); cur = conn.cursor()
            cur.execute("INSERT INTO tarefas (id_pedido, id_cliente, id_produto, data_previsao, observacao_tarefa, status) VALUES (%s,%s,%s,%s,%s,'Solicitado')", (ped['id'], ped['id_cliente'], ped['id_produto'], dt, obs))
            conn.commit(); conn.close()
            st.success("Criada!"); time.sleep(1); 
            st.session_state.ped_panel_active = False; st.rerun()

# =============================================================================
# 6. APP PRINCIPAL
# =============================================================================

def app_pedidos():
    tab_novo, tab_lista, tab_param = st.tabs(["➕ Novo Pedido", "📋 Lista de Pedidos", "⚙️ Parâmetros"])

    # ABA 1: NOVO PEDIDO (SUB MENU PRIMEIRO)
    with tab_novo:
        renderizar_novo_pedido_tab()

    # ABA 2: LISTA DE PEDIDOS (LAYOUT DUAS COLUNAS)
    with tab_lista:
        # Inicialização do Estado do Painel
        if 'ped_panel_active' not in st.session_state: st.session_state.ped_panel_active = False
        if 'ped_panel_data' not in st.session_state: st.session_state.ped_panel_data = None
        if 'ped_panel_type' not in st.session_state: st.session_state.ped_panel_type = None 
        if 'ped_panel_width' not in st.session_state: st.session_state.ped_panel_width = 40

        # Layout Colunas
        if st.session_state.ped_panel_active:
            w = st.session_state.ped_panel_width
            col_lista, col_painel = st.columns([100-w, w])
        else:
            col_lista = st.container()
            col_painel = None

        # --- COLUNA ESQUERDA: LISTA ---
        with col_lista:
            conn = get_conn()
            if conn:
                # FILTROS
                with st.expander("🔍 Filtros de Pesquisa", expanded=True):
                    c1, c2, c3, c4 = st.columns(4)
                    filtro_cliente = c1.text_input("Cliente", placeholder="Nome")
                    filtro_produto = c2.text_input("Produto", placeholder="Nome")
                    opcoes_status = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
                    filtro_status = c3.multiselect("Status", options=opcoes_status)
                    filtro_datas = c4.date_input("Período", value=[])

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
                    params.append(filtro_datas[0]); params.append(filtro_datas[1])

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
                        
                        emp_show = f"({row['nome_empresa']})" if row.get('nome_empresa') else ""
                        
                        with st.expander(f"{cor} [{row['status']}] {row['codigo']} - {row['nome_cliente']} {emp_show} | R$ {row['valor_total']:.2f}"):
                            
                            gc1, gc2, gc3 = st.columns(3)
                            with gc1:
                                st.caption("👤 Cliente")
                                st.write(f"**{row['nome_cliente']}**")
                                st.write(f"CPF: {row['cpf_cliente']}")
                                st.write(f"Tel: {row['telefone_cliente']}")
                            with gc2:
                                st.caption("📦 Produto")
                                st.write(f"**{row['nome_produto']}**")
                                st.write(f"Qtd: {row['quantidade']} | Total: :green[{row['valor_total']:.2f}]")
                                st.write(f"**Custo:** :red[R$ {row['custo_carteira']:.2f}]" if pd.notna(row['custo_carteira']) else "Custo: -")
                            with gc3:
                                st.caption("📅 Info")
                                st.write(f"Criado: {row['data_criacao'].strftime('%d/%m/%y')}")
                                st.write(f"Origem: {row.get('origem_custo', '-')}")

                            if row['observacao']: st.info(f"Obs: {row['observacao']}")
                                
                            st.divider()
                            
                            # Helper para abrir painel
                            def abrir_painel(tipo, dados):
                                st.session_state.ped_panel_active = True
                                st.session_state.ped_panel_type = tipo
                                st.session_state.ped_panel_data = dados

                            # Botões de Ação
                            b1, b2, b3, b4, b5, b6 = st.columns(6)
                            ts = int(time.time())
                            
                            if b1.button("👤", key=f"c_{row['id']}_{ts}", help="Dados Cliente"):
                                abrir_painel('cliente', row); st.rerun()
                            if b2.button("✏️", key=f"e_{row['id']}_{ts}", help="Editar"):
                                abrir_painel('editar', row); st.rerun()
                            if b3.button("🔄", key=f"s_{row['id']}_{ts}", help="Status"):
                                abrir_painel('status', row); st.rerun()
                            if b4.button("📜", key=f"h_{row['id']}_{ts}", help="Histórico"):
                                abrir_painel('historico', row); st.rerun()
                            if b5.button("📝", key=f"t_{row['id']}_{ts}", help="Tarefa"):
                                abrir_painel('tarefa', row); st.rerun()
                            if b6.button("🗑️", key=f"d_{row['id']}_{ts}", help="Excluir"):
                                abrir_painel('excluir', row); st.rerun()
                else: st.info("Nenhum pedido encontrado.")

        # --- COLUNA DIREITA: PAINEL ---
        if col_painel and st.session_state.ped_panel_active:
            with col_painel:
                with st.container(border=True):
                    # Cabeçalho Painel
                    cp1, cp2 = st.columns([1, 3])
                    if cp1.button("✖ Fechar", type="primary"):
                        st.session_state.ped_panel_active = False
                        st.rerun()
                    
                    st.session_state.ped_panel_width = cp2.slider("Largura (%)", 20, 90, st.session_state.ped_panel_width, label_visibility="collapsed")
                    st.divider()

                    # Roteador
                    tipo = st.session_state.ped_panel_type
                    dados = st.session_state.ped_panel_data
                    
                    if tipo == 'cliente': painel_dados_cliente(dados)
                    elif tipo == 'editar': painel_editar_pedido(dados)
                    elif tipo == 'status': painel_status_pedido(dados)
                    elif tipo == 'historico': painel_historico_pedido(dados['id'])
                    elif tipo == 'tarefa': painel_tarefa_pedido(dados)
                    elif tipo == 'excluir': painel_excluir_pedido(dados['id'])
    
    # ABA 3: PARÂMETROS
    with tab_param:
        conn = get_conn()
        if conn:
            df_pedidos_raw = pd.read_sql("SELECT * FROM pedidos ORDER BY id DESC LIMIT 50", conn)
            st.markdown("**Tabela Pedidos:**"); st.dataframe(df_pedidos_raw, height=200)
            st.markdown("---")
            st.markdown("**Tabela Custos:**")
            try:
                df_custos = pd.read_sql("SELECT * FROM cliente.valor_custo_carteira_cliente ORDER BY id DESC LIMIT 50", conn)
                st.dataframe(df_custos, height=200)
            except: st.warning("Tabela custos não existe.")
            conn.close()

if __name__ == "__main__":
    app_pedidos()