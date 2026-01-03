import streamlit as st
import pandas as pd
import psycopg2
import time
import re
from datetime import datetime
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

# --- FUNÇÕES AUXILIARES ---
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

def listar_carteiras_ativas():
    conn = get_conn()
    if conn:
        try:
            query = "SELECT nome_carteira FROM cliente.carteiras_config WHERE status = 'ATIVO' ORDER BY nome_carteira"
            df = pd.read_sql(query, conn)
            conn.close()
            return df['nome_carteira'].tolist()
        except: conn.close()
    return []

# --- LÓGICA FINANCEIRA ---
def processar_movimentacao_automatica(conn, dados_pedido, tipo_lancamento):
    try:
        cur = conn.cursor()
        
        # 1. Identificar a Origem de Custo do Produto
        cur.execute("SELECT origem_custo FROM produtos_servicos WHERE id = %s", (int(dados_pedido['id_produto']),))
        res_prod = cur.fetchone()
        if not res_prod or not res_prod[0]:
            return False, "Produto sem 'Origem de Custo' definida."
        
        origem = res_prod[0]
        cpf_cliente = dados_pedido['cpf_cliente']

        # 2. Localizar a Carteira na Lista do Cliente
        cur.execute("""
            SELECT nome_carteira 
            FROM cliente.cliente_carteira_lista 
            WHERE cpf_cliente = %s AND origem_custo = %s
            LIMIT 1
        """, (cpf_cliente, origem))
        res_lista = cur.fetchone()
        
        if not res_lista:
            return False, f"O cliente não possui carteira vinculada para a origem '{origem}'."
        
        nome_carteira = res_lista[0]

        # 3. Identificar a Tabela SQL da Carteira
        cur.execute("""
            SELECT nome_tabela_transacoes 
            FROM cliente.carteiras_config 
            WHERE nome_carteira = %s AND status = 'ATIVO'
            LIMIT 1
        """, (nome_carteira,))
        res_config = cur.fetchone()
        
        if not res_config:
            return False, f"Configuração técnica da carteira '{nome_carteira}' não encontrada."
            
        tabela_sql = res_config[0]

        # 4. Calcular Valores e Motivo
        valor = float(dados_pedido['valor_total'])
        codigo_pedido = dados_pedido['codigo']
        
        cur.execute(f"SELECT saldo_novo FROM {tabela_sql} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_cliente,))
        res_saldo = cur.fetchone()
        saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
        
        if tipo_lancamento == 'CREDITO':
            saldo_novo = saldo_anterior + valor
            motivo = f"Compra Pedido {codigo_pedido}"
        else: # DEBITO
            saldo_novo = saldo_anterior - valor
            motivo = f"Cancelada Pedido {codigo_pedido}"

        # 5. Inserir Transação
        sql_insert = f"""
            INSERT INTO {tabela_sql} 
            (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao)
            VALUES (%s, %s, %s, 'PEDIDO', %s, %s, %s, %s, NOW())
        """
        cur.execute(sql_insert, (cpf_cliente, dados_pedido['nome_cliente'], motivo, tipo_lancamento, valor, saldo_anterior, saldo_novo))
        
        return True, f"{tipo_lancamento} de R$ {valor:.2f} na carteira '{nome_carteira}'"

    except Exception as e:
        return False, f"Erro financeiro: {str(e)}"

# --- CRUD PEDIDOS (FUNCIONALIDADES ORIGINAIS) ---
def buscar_clientes():
    conn = get_conn()
    if conn:
        df = pd.read_sql("SELECT id, nome, cpf, telefone, email FROM admin.clientes ORDER BY nome", conn)
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

def criar_pedido(cliente, produto, qtd, valor_unitario, valor_total, avisar_cliente, add_lista=False, nome_lista="", custo_lista=0.0, origem_custo=""):
    codigo = f"PEDIDO-{datetime.now().strftime('%y%m%d%H%M')}"
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pedidos (codigo, id_cliente, nome_cliente, cpf_cliente, telefone_cliente,
                                     id_produto, nome_produto, categoria_produto, quantidade, valor_unitario, valor_total,
                                     nome_carteira, custo_carteira, origem_custo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (codigo, int(cliente['id']), cliente['nome'], cliente['cpf'], cliente['telefone'],
                  int(produto['id']), produto['nome'], produto['tipo'], int(qtd), float(valor_unitario), float(valor_total),
                  nome_lista, float(custo_lista), origem_custo))
            
            id_novo = cur.fetchone()[0]
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao) VALUES (%s, 'Solicitado', 'Criado')", (id_novo,))
            
            msg_lista = ""
            if add_lista and nome_lista:
                cpf_limpo_cli = re.sub(r'\D', '', str(cliente['cpf']))
                
                cur.execute("""
                    SELECT u.cpf, u.nome FROM admin.clientes c
                    JOIN clientes_usuarios u ON c.id_usuario_vinculo = u.id
                    WHERE regexp_replace(c.cpf, '[^0-9]', '', 'g') = %s LIMIT 1
                """, (cpf_limpo_cli,))
                res_u = cur.fetchone()
                cpf_u = res_u[0] if res_u else None
                nome_u = res_u[1] if res_u else None

                cur.execute("""
                    SELECT id FROM cliente.cliente_carteira_lista 
                    WHERE cpf_cliente = %s AND nome_carteira = %s AND origem_custo = %s
                """, (cliente['cpf'], nome_lista, origem_custo))
                existe = cur.fetchone()

                if existe:
                    cur.execute("""
                        UPDATE cliente.cliente_carteira_lista 
                        SET custo_carteira = %s, cpf_usuario = %s, nome_usuario = %s
                        WHERE id = %s
                    """, (float(custo_lista), cpf_u, nome_u, existe[0]))
                    msg_lista = " (Custo atualizado na lista)"
                else:
                    cur.execute("""
                        INSERT INTO cliente.cliente_carteira_lista 
                        (cpf_cliente, nome_cliente, nome_carteira, custo_carteira, origem_custo, cpf_usuario, nome_usuario, nome_produto) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (cliente['cpf'], cliente['nome'], nome_lista, float(custo_lista), origem_custo, cpf_u, nome_u, produto['nome']))
                    msg_lista = " (Adicionado à lista)"
            
            conn.commit(); conn.close()
            
            if avisar_cliente and cliente['telefone']:
                inst = modulo_wapi.buscar_instancia_ativa()
                if inst:
                    tpl = modulo_wapi.buscar_template("PEDIDOS", "criacao")
                    if tpl:
                        msg = tpl.replace("{nome}", str(cliente['nome']).split()[0]).replace("{pedido}", codigo).replace("{produto}", str(produto['nome']))
                        modulo_wapi.enviar_msg_api(inst[0], inst[1], cliente['telefone'], msg)
            
            return True, f"Pedido {codigo} criado!{msg_lista}"
        except Exception as e: return False, str(e)
    return False, "Erro conexão"

def atualizar_status_pedido(id_pedido, novo_status, dados_pedido, avisar, obs, modelo_msg):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            obs_hist = obs
            msg_fin = ""
            
            if novo_status == "Pago":
                ok, msg_fin = processar_movimentacao_automatica(conn, dados_pedido, 'CREDITO')
                if ok: obs_hist += f" | {msg_fin}"
                else: obs_hist += f" | ⚠️ Erro Fin: {msg_fin}"
                
            elif novo_status == "Cancelado":
                ok, msg_fin = processar_movimentacao_automatica(conn, dados_pedido, 'DEBITO')
                if ok: obs_hist += f" | {msg_fin}"
                else: obs_hist += f" | ⚠️ Erro Fin: {msg_fin}"

            cur.execute("UPDATE pedidos SET status=%s, observacao=%s, data_atualizacao=NOW() WHERE id=%s", (novo_status, obs, id_pedido))
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao) VALUES (%s, %s, %s)", (id_pedido, novo_status, obs_hist))
            
            conn.commit(); conn.close()
            
            if avisar and dados_pedido['telefone_cliente']:
                inst = modulo_wapi.buscar_instancia_ativa()
                if inst:
                    chave = modelo_msg if modelo_msg != "Automático (Padrão)" else novo_status.lower().replace(" ", "_")
                    tpl = modulo_wapi.buscar_template("PEDIDOS", chave)
                    if tpl:
                        msg = tpl.replace("{nome}", str(dados_pedido['nome_cliente']).split()[0]).replace("{pedido}", str(dados_pedido['codigo'])).replace("{status}", novo_status).replace("{produto}", str(dados_pedido['nome_produto']))
                        modulo_wapi.enviar_msg_api(inst[0], inst[1], dados_pedido['telefone_cliente'], msg)
            return True
        except Exception as e:
            print(e); return False
    return False

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
            
            msg_extra = ""
            if carteira_vinculada and carteira_vinculada != "N/A":
                cur.execute("""
                    UPDATE cliente.cliente_carteira_lista 
                    SET custo_carteira = %s 
                    WHERE cpf_cliente = %s AND nome_carteira = %s AND origem_custo = %s
                """, (novo_custo_carteira, dados_antigos['cpf_cliente'], carteira_vinculada, origem_custo))
                if cur.rowcount > 0:
                    msg_extra = " (Custo atualizado na Carteira)"

            conn.commit(); conn.close()
            return True, msg_extra
        except Exception as e: return False, str(e)
    return False, "Erro BD"

# --- FUNÇÕES PARA ABA PARÂMETROS (EDIÇÃO DIRETA) ---
def carregar_tabela_pedidos_completa():
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT * FROM pedidos ORDER BY id DESC", conn)
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def salvar_alteracoes_pedidos_geral(df_original, df_editado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        ids_originais = set(df_original['id'].dropna().astype(int).tolist())
        
        # 1. Detectar Deletes
        ids_editados_atuais = set()
        for _, row in df_editado.iterrows():
            if pd.notna(row.get('id')) and row.get('id') != '':
                try: ids_editados_atuais.add(int(row['id']))
                except: pass
        
        ids_del = ids_originais - ids_editados_atuais
        if ids_del:
            ids_str = ",".join(map(str, ids_del))
            cur.execute(f"DELETE FROM pedidos WHERE id IN ({ids_str})")

        # 2. Upsert (Update/Insert)
        for index, row in df_editado.iterrows():
            # Protege campos automáticos de edição manual direta se necessário
            # Aqui permitimos edição de quase tudo para correção, exceto id
            colunas_db = [c for c in row.index if c not in ['id', 'data_criacao']] 
            valores = [row[c] for c in colunas_db]
            row_id = row.get('id')
            
            eh_novo = pd.isna(row_id) or row_id == '' or row_id is None
            
            if eh_novo:
                cols_str = ", ".join(colunas_db)
                placeholders = ", ".join(["%s"] * len(colunas_db))
                cur.execute(f"INSERT INTO pedidos ({cols_str}) VALUES ({placeholders})", valores)
            elif int(row_id) in ids_originais:
                set_clause = ", ".join([f"{c} = %s" for c in colunas_db])
                valores_update = valores + [int(row_id)]
                cur.execute(f"UPDATE pedidos SET {set_clause} WHERE id = %s", valores_update)

        conn.commit(); conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar tabela: {e}")
        if conn: conn.close()
        return False

# --- FUNÇÕES DE ESTADO ---
def abrir_modal(tipo, pedido=None):
    st.session_state['modal_ativo'] = tipo
    st.session_state['pedido_ativo'] = pedido

def fechar_modal():
    st.session_state['modal_ativo'] = None
    st.session_state['pedido_ativo'] = None

# --- DIALOGS ---
@st.dialog("➕ Novo Pedido", width="large")
def dialog_novo_pedido():
    df_c = buscar_clientes()
    df_p = buscar_produtos()
    if df_c.empty or df_p.empty: 
        st.warning("Cadastre clientes e produtos.")
        return

    c1, c2 = st.columns(2)
    ic = c1.selectbox("Cliente", range(len(df_c)), format_func=lambda x: df_c.iloc[x]['nome'])
    ip = c2.selectbox("Produto", range(len(df_p)), format_func=lambda x: df_p.iloc[x]['nome'])
    
    cli = df_c.iloc[ic]
    prod = df_p.iloc[ip]
    
    origem_produto = prod.get('origem_custo') if prod.get('origem_custo') else "Não definida"
    
    carteira_vinculada = None
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome_carteira FROM cliente.carteiras_config WHERE id_produto = %s AND status = 'ATIVO' LIMIT 1", (int(prod['id']),))
            res = cur.fetchone()
            if res: carteira_vinculada = res[0]
            conn.close()
        except: conn.close()

    cart_display = carteira_vinculada if carteira_vinculada else "Não localizada"
    st.info(f"📦 **Item:** {prod['nome']}\n📍 **Origem:** {origem_produto}\n💼 **Carteira:** {cart_display}")

    c3, c4 = st.columns(2)
    qtd = c3.number_input("Qtd", 1, value=1)
    val = c4.number_input("Valor Unitário (Valentia)", 0.0, value=float(prod['preco'] or 0.0))
    st.markdown(f"### Total: R$ {qtd*val:.2f}")
    
    st.divider()
    
    check_default = True if carteira_vinculada else False
    add_cart = st.checkbox("Incluir/Atualizar na Lista de Carteira?", value=check_default)
    
    n_cart = ""; c_cart = 0.0
    if add_cart:
        if carteira_vinculada:
            n_cart = carteira_vinculada
            st.text_input("Carteira Destino", value=n_cart, disabled=True)
            custo_atual = 0.0
            if conn:
                try:
                    conn = get_conn()
                    cur = conn.cursor()
                    cur.execute("SELECT custo_carteira FROM cliente.cliente_carteira_lista WHERE cpf_cliente=%s AND nome_carteira=%s AND origem_custo=%s", (cli['cpf'], n_cart, origem_produto))
                    r_cus = cur.fetchone()
                    if r_cus: 
                        custo_atual = float(r_cus[0])
                        st.caption("ℹ️ Cliente com lista vinculada. Atualize o custo abaixo.")
                    else:
                        st.caption("ℹ️ Cliente sem lista. Será criado um novo vínculo.")
                    conn.close()
                except: pass
            
            c_cart = st.number_input("Custo do Desconto (Carteira)", 0.0, value=custo_atual, step=0.01)
        else:
            st.warning("Este produto não tem carteira vinculada.")
            l_carts = listar_carteiras_ativas()
            n_cart = st.selectbox("Selecione Manualmente", [""] + l_carts)
            c_cart = st.number_input("Custo", 0.0, step=0.01)

    st.divider()
    avisar = st.checkbox("Avisar WhatsApp?", value=True)
    
    if st.button("Criar Pedido", type="primary", use_container_width=True):
        if add_cart and not n_cart: st.error("Selecione a carteira.")
        else:
            ok, res = criar_pedido(cli, prod, qtd, val, qtd*val, avisar, add_cart, n_cart, c_cart, origem_produto)
            if ok: 
                st.success(res)
                time.sleep(1.5)
                fechar_modal()
                st.rerun()
            else: st.error(res)

@st.dialog("✏️ Editar", width="large")
def dialog_editar(ped):
    origem_atual = "N/A"
    carteira_atual = "N/A"
    custo_atual_lista = 0.0
    conn = get_conn()
    if conn:
        try:
            df_prod_info = pd.read_sql(f"SELECT origem_custo FROM produtos_servicos WHERE id = {ped['id_produto']}", conn)
            if not df_prod_info.empty: origem_atual = df_prod_info.iloc[0]['origem_custo']
            df_cart_info = pd.read_sql(f"SELECT nome_carteira FROM cliente.carteiras_config WHERE id_produto = {ped['id_produto']}", conn)
            if not df_cart_info.empty: carteira_atual = df_cart_info.iloc[0]['nome_carteira']
            if carteira_atual != "N/A":
                cur = conn.cursor()
                cur.execute("SELECT custo_carteira FROM cliente.cliente_carteira_lista WHERE cpf_cliente = %s AND nome_carteira = %s AND origem_custo = %s", (ped['cpf_cliente'], carteira_atual, origem_atual))
                res_c = cur.fetchone()
                if res_c: custo_atual_lista = float(res_c[0])
            conn.close()
        except: conn.close()

    with st.form("fe"):
        st.markdown(f"#### Editando: {ped['codigo']}")
        c_i1, c_i2 = st.columns(2)
        c_i1.text_input("Cliente", value=ped['nome_cliente'], disabled=True)
        c_i2.text_input("Produto", value=ped['nome_produto'], disabled=True)

        st.divider()
        st.markdown("##### Dados Financeiros")
        c_f1, c_f2, c_f3 = st.columns(3)
        c_f1.text_input("Carteira (Bloqueado)", value=carteira_atual if carteira_atual else "N/A", disabled=True)
        c_f2.text_input("Origem (Bloqueado)", value=origem_atual if origem_atual else "N/A", disabled=True)
        novo_custo = c_f3.number_input("Custo Carteira (R$)", value=custo_atual_lista, step=0.01)

        st.markdown("##### Detalhes do Pedido")
        c_d1, c_d2 = st.columns(2)
        nq = c_d1.number_input("Quantidade", 1, value=int(ped['quantidade']))
        nv = c_d2.number_input("Valor Unitário (Valentia)", 0.0, value=float(ped['valor_unitario']))
        st.info(f"Novo Total: R$ {nq*nv:.2f}")

        if st.form_submit_button("💾 Salvar Alterações"):
            ok, msg = editar_dados_pedido_completo(ped['id'], nq, nv, ped, novo_custo, carteira_atual, origem_atual)
            if ok: st.success(f"Salvo!{msg}"); time.sleep(1); fechar_modal(); st.rerun()
            else: st.error(f"Erro: {msg}")

@st.dialog("🔄 Status")
def dialog_status(ped):
    st.write(f"🏢 **Empresa:** {ped.get('nome_empresa', '-')}")
    st.write(f"👤 **Cliente:** {ped['nome_cliente']} | **CPF:** {ped['cpf_cliente']}")
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
            if atualizar_status_pedido(ped['id'], ns, ped, av, obs, mod):
                st.success("Atualizado!"); time.sleep(1); fechar_modal(); st.rerun()
    st.divider(); st.caption("Histórico")
    st.dataframe(buscar_historico_pedido(ped['id']), hide_index=True)

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

# --- APP ---
def app_pedidos():
    st.markdown("## 🛒 Módulo de Pedidos")
    
    # NOVAS ABAS DE NAVEGAÇÃO
    tab_lista, tab_param = st.tabs(["📋 Lista de Pedidos", "⚙️ Parâmetros"])

    # ABA 1: LISTA (ORIGINAL)
    with tab_lista:
        if 'modal_ativo' not in st.session_state: st.session_state.update({'modal_ativo': None, 'pedido_ativo': None})
        
        c_t, c_b = st.columns([5, 1])
        c_b.button("➕ Novo Pedido", type="primary", on_click=abrir_modal, args=('novo', None), use_container_width=True)
        
        conn = get_conn()
        if conn:
            df = pd.read_sql("""
                SELECT p.*, c.nome_empresa, c.email as email_cliente 
                FROM pedidos p 
                LEFT JOIN admin.clientes c ON p.id_cliente = c.id 
                ORDER BY p.data_criacao DESC
            """, conn)
            conn.close()
            
            with st.expander("🔍 Filtros de Pesquisa", expanded=True):
                c1, c2, c3 = st.columns([3, 1.5, 1.5])
                busca = c1.text_input("Buscar")
                status = c2.multiselect("Status", df['status'].unique() if not df.empty else [])
                if not df.empty:
                    if busca: df = df[df['nome_cliente'].str.contains(busca, case=False) | df['nome_produto'].str.contains(busca, case=False)]
                    if status: df = df[df['status'].isin(status)]
            st.divider()
            
            pag_size = 10
            total_pags = (len(df) // pag_size) + 1
            pag = st.selectbox("Página", range(1, total_pags + 1)) if total_pags > 1 else 1
            subset = df.iloc[(pag-1)*pag_size : pag*pag_size]
            
            if not subset.empty:
                for _, row in subset.iterrows():
                    cor = "🔴"; 
                    if row['status'] == 'Pago': cor = "🟢"
                    elif row['status'] == 'Pendente': cor = "🟠"
                    elif row['status'] == 'Solicitado': cor = "🔵"
                    
                    empresa_show = f"({row['nome_empresa']})" if row.get('nome_empresa') else ""
                    custo_show = f" | Custo: R$ {float(row['custo_carteira'] or 0):.2f}" if row.get('custo_carteira') else ""
                    
                    with st.expander(f"{cor} [{row['status']}] {row['codigo']} - {row['nome_cliente']} {empresa_show} | R$ {row['valor_total']:.2f}{custo_show}"):
                        st.write(f"**Produto:** {row['nome_produto']} | **Data:** {row['data_criacao'].strftime('%d/%m %H:%M')}")
                        if row.get('nome_carteira'):
                            st.caption(f"Carteira: {row['nome_carteira']} | Origem: {row.get('origem_custo', '-')}")
                            
                        c1, c2, c3, c4, c5, c6 = st.columns(6)
                        ts = int(time.time())
                        c1.button("👤", key=f"c_{row['id']}_{ts}", on_click=abrir_modal, args=('cliente', row))
                        c2.button("✏️", key=f"e_{row['id']}_{ts}", on_click=abrir_modal, args=('editar', row))
                        c3.button("🔄", key=f"s_{row['id']}_{ts}", on_click=abrir_modal, args=('status', row))
                        c4.button("📜", key=f"h_{row['id']}_{ts}", on_click=abrir_modal, args=('historico', row))
                        c5.button("🗑️", key=f"d_{row['id']}_{ts}", on_click=abrir_modal, args=('excluir', row))
                        c6.button("📝", key=f"t_{row['id']}_{ts}", on_click=abrir_modal, args=('tarefa', row))
            else: st.info("Nenhum pedido.")
    
    # ABA 2: PARÂMETROS (EDIÇÃO DIRETA)
    with tab_param:
        st.markdown("#### ⚙️ Edição Técnica da Tabela Pedidos")
        st.caption("Use com cautela. Permite editar dados brutos do sistema.")
        
        df_pedidos_raw = carregar_tabela_pedidos_completa()
        if not df_pedidos_raw.empty:
            # Configuração do Editor
            df_editado = st.data_editor(
                df_pedidos_raw,
                key="editor_pedidos",
                use_container_width=True,
                num_rows="dynamic",
                disabled=["id", "data_criacao", "data_atualizacao"] # Protege campos de sistema
            )
            
            if st.button("💾 Salvar Alterações na Tabela", type="primary"):
                with st.spinner("Salvando..."):
                    if salvar_alteracoes_pedidos_geral(df_pedidos_raw, df_editado):
                        st.success("Dados atualizados com sucesso!")
                        time.sleep(1)
                        st.rerun()
        else:
            st.info("A tabela de pedidos está vazia.")

    # Roteador de Modais (Mantido fora das abas para funcionar globalmente)
    m = st.session_state['modal_ativo']; p = st.session_state['pedido_ativo']
    if m == 'novo': dialog_novo_pedido()
    elif m == 'cliente' and p is not None: 
        st.dialog("👤 Cliente")(lambda: st.write(f"Nome: {p['nome_cliente']}\nCPF: {p['cpf_cliente']}\nTel: {p['telefone_cliente']}"))()
        fechar_modal()
    elif m == 'editar' and p is not None: dialog_editar(p)
    elif m == 'status' and p is not None: dialog_status(p)
    elif m == 'historico' and p is not None: dialog_historico(p['id'], p['codigo'])
    elif m == 'excluir' and p is not None: dialog_excluir(p['id'])
    elif m == 'tarefa' and p is not None: dialog_tarefa(p)

if __name__ == "__main__":
    app_pedidos()