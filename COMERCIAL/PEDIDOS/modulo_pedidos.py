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
    # st.warning("Aviso: modulo_comercial_configuracoes não encontrado. Templates podem falhar.")

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
# 1. FUNÇÕES AUXILIARES E DE NEGÓCIO (MANTIDAS)
# =============================================================================

def listar_modelos_mensagens():
    """Busca os modelos de mensagem cadastrados no Config para este módulo"""
    if modulo_comercial_configuracoes:
        return modulo_comercial_configuracoes.listar_chaves_config("PEDIDOS")
    return []

def registrar_movimentacao_financeira(conn, dados_pedido, tipo_lancamento, valor):
    try:
        cur = conn.cursor()
        id_cliente = dados_pedido['id_cliente']
        
        cur.execute("""
            SELECT saldo_novo 
            FROM cliente.extrato_carteira_por_produto 
            WHERE id_cliente = %s 
            ORDER BY id DESC LIMIT 1
        """, (str(id_cliente),))
        res = cur.fetchone()
        saldo_anterior = float(res[0]) if res else 0.0
        
        valor_float = float(valor)
        if tipo_lancamento == 'CREDITO':
            saldo_novo = saldo_anterior + valor_float
        else: # DEBITO
            saldo_novo = saldo_anterior - valor_float
            
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
            
            conn.commit()
            conn.close()
            
            msg_whats = ""
            if avisar_cliente and cliente['telefone'] and modulo_comercial_configuracoes:
                try:
                    inst = modulo_wapi.buscar_instancia_ativa()
                    if inst:
                        tpl = modulo_comercial_configuracoes.buscar_template_config("PEDIDOS", "criacao")
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
# 4. COMPONENTE DE NOVO PEDIDO
# =============================================================================

def renderizar_novo_pedido_tab():
    df_c = buscar_clientes()
    df_p = buscar_produtos()
    
    if df_c.empty or df_p.empty: 
        st.warning("Cadastre clientes e produtos antes.")
        return

    if 'np_cli_idx' not in st.session_state: st.session_state.np_cli_idx = 0
    if 'np_prod_idx' not in st.session_state: st.session_state.np_prod_idx = 0
    
    # Lógica de atualização de estado (Mantida igual ao original)
    prod_inicial = df_p.iloc[st.session_state.np_prod_idx]
    if 'np_val' not in st.session_state: st.session_state.np_val = float(prod_inicial['preco'] or 0.0)
    if 'np_qtd' not in st.session_state: st.session_state.np_qtd = 1
    if 'np_origem' not in st.session_state: st.session_state.np_origem = prod_inicial.get('origem_custo', 'Geral') or 'Geral'
    if 'np_custo' not in st.session_state: st.session_state.np_custo = 0.0
    
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
# 5. PAINÉIS DE RENDERIZAÇÃO (Layout Gaveta)
# =============================================================================

def renderizar_dados_cliente(ped):
    st.markdown(f"#### 👤 Dados do Cliente")
    st.write(f"**Nome:** {ped['nome_cliente']}")
    st.write(f"**CPF:** {ped['cpf_cliente']}")
    st.write(f"**Telefone:** {ped['telefone_cliente']}")
    st.write(f"**E-mail:** {ped.get('email_cliente', '-')}")
    if ped.get('nome_empresa'):
        st.write(f"**Empresa:** {ped['nome_empresa']}")

def renderizar_editar_pedido(ped):
    st.markdown(f"#### ✏️ Editar Dados")
    df_c = buscar_clientes()
    df_p = buscar_produtos()
    
    if df_c.empty or df_p.empty: st.error("Dados base vazios."); return

    with st.form("form_gaveta_editar_ped"):
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
        nv = c_v2.number_input("Valor Unit.", min_value=0.0, value=float(ped['valor_unitario']), format="%.2f")
        
        c_c1, c_c2 = st.columns(2)
        ncusto = c_c1.number_input("Custo Carteira (R$)", value=float(ped['custo_carteira'] or 0.0), step=0.01)
        norigem = c_c2.text_input("Origem Custo", value=ped.get('origem_custo', ''))
        
        nobs = st.text_area("Observação", value=ped['observacao'] or "")
        st.info(f"💰 Novo Total: R$ {nq*nv:.2f}")

        if st.form_submit_button("💾 Salvar Alterações", type="primary"):
            dados_novos = {
                'cliente': sel_cli, 'produto': sel_prod, 'qtd': nq, 'valor': nv,
                'custo': ncusto, 'origem': norigem, 'obs': nobs
            }
            ok, msg = editar_dados_pedido_completo(ped['id'], dados_novos)
            if ok: 
                st.success("Salvo!"); time.sleep(1)
                st.session_state.ped_selecionado = None # Força recarregamento ao clicar de novo
                st.session_state.ped_aba_ativa = None
                st.rerun()
            else: st.error(f"Erro: {msg}")

def renderizar_status_pedido(ped):
    st.markdown(f"#### 🔄 Atualizar Status")
    lst = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
    try: idx = lst.index(ped['status']) 
    except: idx = 0
    mods = ["Automático (Padrão)"] + listar_modelos_mensagens()
    
    with st.form("form_gaveta_status_ped"):
        ns = st.selectbox("Novo Status", lst, index=idx)
        mod = st.selectbox("Modelo Mensagem", mods)
        obs = st.text_area("Observação da Mudança")
        av = st.checkbox("Avisar Cliente (WhatsApp)?", value=True)
        
        if st.form_submit_button("✅ Confirmar Novo Status", type="primary"):
            ok, msg = atualizar_status_pedido(ped['id'], ns, ped, av, obs, mod)
            if ok:
                st.success(msg); time.sleep(1)
                st.session_state.ped_selecionado = None
                st.session_state.ped_aba_ativa = None
                st.rerun()
            else: st.warning(msg)

def renderizar_historico_pedido(id_pedido):
    st.markdown("#### 📜 Histórico de Movimentações")
    df = buscar_historico_pedido(id_pedido)
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else: st.info("Sem histórico registrado.")

def renderizar_excluir_pedido(ped):
    st.markdown(f"#### 🗑️ Excluir Pedido: {ped['codigo']}")
    st.warning("⚠️ Esta ação é irreversível e removerá o histórico financeiro associado.")
    
    col_del = st.columns([1, 1])
    if col_del[0].button("Sim, Excluir Permanentemente", type="primary", use_container_width=True):
        if excluir_pedido_db(ped['id']): 
            st.success("Pedido excluído com sucesso!"); time.sleep(1)
            st.session_state.ped_selecionado = None
            st.session_state.ped_aba_ativa = None
            st.rerun()

def renderizar_tarefa_pedido(ped):
    st.markdown("#### 📝 Criar Tarefa Vinculada")
    with st.form("form_gaveta_tarefa_ped"):
        dt = st.date_input("Data de Previsão", datetime.now())
        obs = st.text_area("Descrição da Tarefa")
        if st.form_submit_button("Criar Tarefa"):
            conn = get_conn(); cur = conn.cursor()
            cur.execute("INSERT INTO tarefas (id_pedido, id_cliente, id_produto, data_previsao, observacao_tarefa, status) VALUES (%s,%s,%s,%s,%s,'Solicitado')", (ped['id'], ped['id_cliente'], ped['id_produto'], dt, obs))
            conn.commit(); conn.close()
            st.success("Tarefa criada!"); time.sleep(1); 
            st.session_state.ped_aba_ativa = None; st.rerun()

# =============================================================================
# 6. APP PRINCIPAL
# =============================================================================

def app_pedidos():
    tab_novo, tab_lista, tab_param = st.tabs(["➕ Novo Pedido", "📋 Lista de Pedidos", "⚙️ Parâmetros"])

    # ABA 1: NOVO PEDIDO (MANTIDA)
    with tab_novo:
        renderizar_novo_pedido_tab()

    # ABA 2: LISTA DE PEDIDOS (LAYOUT MASTER-DETAIL 30/70)
    with tab_lista:
        # Inicializa Estados
        if 'ped_selecionado' not in st.session_state: st.session_state.ped_selecionado = None
        if 'ped_aba_ativa' not in st.session_state: st.session_state.ped_aba_ativa = None

        # Layout fixo
        col_lista, col_detalhe = st.columns([0.3, 0.7])

        # --- COLUNA ESQUERDA: LISTA ---
        with col_lista:
            st.markdown("##### 🔍 Filtros & Lista")
            # Filtros Compactos
            f1, f2 = st.columns(2)
            filtro_txt = f1.text_input("Busca", placeholder="Nome/Cod", label_visibility="collapsed")
            filtro_stt = f2.selectbox("St", ["Todos", "Solicitado", "Pendente", "Pago"], label_visibility="collapsed")
            
            conn = get_conn()
            if conn:
                query_base = """
                    SELECT p.*, c.nome_empresa, c.email as email_cliente 
                    FROM pedidos p 
                    LEFT JOIN admin.clientes c ON p.id_cliente = c.id 
                    WHERE 1=1
                """
                params = []
                if filtro_txt:
                    query_base += " AND (p.nome_cliente ILIKE %s OR p.codigo ILIKE %s)"
                    params.extend([f"%{filtro_txt}%", f"%{filtro_txt}%"])
                
                if filtro_stt != "Todos":
                    query_base += " AND p.status = %s"
                    params.append(filtro_stt)

                query_base += " ORDER BY p.data_criacao DESC LIMIT 20" # Limitado para performance
                df = pd.read_sql(query_base, conn, params=params)
                conn.close()

                if not df.empty:
                    for i, row in df.iterrows():
                        # Lógica Visual
                        is_selected = (st.session_state.ped_selecionado is not None and 
                                       st.session_state.ped_selecionado['id'] == row['id'])
                        
                        cor = "🔴"
                        if row['status'] == 'Pago': cor = "🟢"
                        elif row['status'] == 'Pendente': cor = "🟠"
                        elif row['status'] == 'Solicitado': cor = "🔵"
                        
                        border_style = True 
                        icon_sel = "👉 " if is_selected else ""

                        with st.container(border=border_style):
                            st.write(f"**{icon_sel}{row['nome_cliente']}**")
                            st.caption(f"{cor} {row['codigo']} | R$ {row['valor_total']:.2f}")
                            
                            if st.button("Ver Detalhes >", key=f"sel_ped_{row['id']}", use_container_width=True):
                                st.session_state.ped_selecionado = row.to_dict()
                                st.session_state.ped_aba_ativa = None # Reseta gaveta
                                st.rerun()
                else:
                    st.info("Nenhum pedido.")
            else:
                st.error("Sem conexão.")

        # --- COLUNA DIREITA: DETALHES (FIXO) ---
        with col_detalhe:
            ped = st.session_state.ped_selecionado
            
            if ped:
                with st.container(border=True):
                    # Cabeçalho Fixo
                    st.title(f"{ped['nome_cliente']}")
                    st.caption(f"Pedido: {ped['codigo']} | Status: {ped['status']} | Data: {ped['data_criacao']}")
                    
                    st.divider()
                    
                    # Menu de Ações (Barra Horizontal)
                    c_b1, c_b2, c_b3, c_b4, c_b5, c_b6 = st.columns(6)
                    
                    def set_aba(nome):
                        st.session_state.ped_aba_ativa = nome
                        st.rerun()

                    # Lógica de estilo para botão ativo
                    ativa = st.session_state.ped_aba_ativa
                    
                    if c_b1.button("👤 Cliente", use_container_width=True, type="primary" if ativa == 'cliente' else "secondary"): set_aba('cliente')
                    if c_b2.button("✏️ Editar", use_container_width=True, type="primary" if ativa == 'editar' else "secondary"): set_aba('editar')
                    if c_b3.button("🔄 Status", use_container_width=True, type="primary" if ativa == 'status' else "secondary"): set_aba('status')
                    if c_b4.button("📜 Histórico", use_container_width=True, type="primary" if ativa == 'historico' else "secondary"): set_aba('historico')
                    if c_b5.button("📝 Tarefa", use_container_width=True, type="primary" if ativa == 'tarefa' else "secondary"): set_aba('tarefa')
                    if c_b6.button("🗑️ Excluir", use_container_width=True, type="primary" if ativa == 'excluir' else "secondary"): set_aba('excluir')

                # Área de Conteúdo "Gaveta" (Renderiza abaixo do menu)
                aba = st.session_state.ped_aba_ativa
                
                if aba:
                    with st.container(border=True):
                        if aba == 'cliente': renderizar_dados_cliente(ped)
                        elif aba == 'editar': renderizar_editar_pedido(ped)
                        elif aba == 'status': renderizar_status_pedido(ped)
                        elif aba == 'historico': renderizar_historico_pedido(ped['id'])
                        elif aba == 'tarefa': renderizar_tarefa_pedido(ped)
                        elif aba == 'excluir': renderizar_excluir_pedido(ped)
                else:
                    st.info("👆 Selecione uma opção acima para gerenciar o pedido.")

            else:
                # Estado Vazio
                st.container(border=True).markdown(
                    """
                    <div style='text-align: center; padding: 50px;'>
                        <h3>⬅️ Selecione um pedido na lista</h3>
                        <p>Os detalhes, financeiro e opções de gerenciamento aparecerão aqui.</p>
                    </div>
                    """, 
                    unsafe_allow_html=True
                )
    
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