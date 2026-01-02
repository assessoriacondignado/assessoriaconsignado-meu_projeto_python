import streamlit as st
import pandas as pd
import psycopg2
import time
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
        cur.execute("""
            SELECT nome_tabela_transacoes, nome_carteira 
            FROM cliente.carteiras_config 
            WHERE id_produto = %s AND status = 'ATIVO'
        """, (int(dados_pedido['id_produto']),))
        config = cur.fetchone()
        
        if config:
            tabela_carteira = config[0]
            nome_carteira = config[1]
            cpf = dados_pedido['cpf_cliente']
            valor = float(dados_pedido['valor_total'])
            
            cur.execute(f"SELECT saldo_novo FROM {tabela_carteira} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf,))
            res_saldo = cur.fetchone()
            saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
            
            if tipo_lancamento == 'DEBITO':
                saldo_novo = saldo_anterior - valor
                motivo = f"Cancelamento Pedido #{dados_pedido['codigo']} - Estorno"
            else:
                saldo_novo = saldo_anterior + valor
                motivo = f"Compra Pedido #{dados_pedido['codigo']} - {dados_pedido['nome_produto']}"
            
            sql_insert = f"""
                INSERT INTO {tabela_carteira} 
                (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao)
                VALUES (%s, %s, %s, 'PEDIDO', %s, %s, %s, %s, NOW())
            """
            cur.execute(sql_insert, (cpf, dados_pedido['nome_cliente'], motivo, tipo_lancamento, valor, saldo_anterior, saldo_novo))
            return True, f"{tipo_lancamento} de R$ {valor:.2f} em '{nome_carteira}'"
    except Exception as e:
        print(f"Erro mov: {e}")
        return False, str(e)
    return False, None

# --- CRUD PEDIDOS ---
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
        df = pd.read_sql("SELECT id, codigo, nome, tipo, preco FROM produtos_servicos WHERE ativo = TRUE ORDER BY nome", conn)
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

def criar_pedido(cliente, produto, qtd, valor_unitario, valor_total, avisar_cliente, add_lista=False, nome_lista="", custo_lista=0.0):
    codigo = f"PEDIDO-{datetime.now().strftime('%y%m%d%H%M')}"
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pedidos (codigo, id_cliente, nome_cliente, cpf_cliente, telefone_cliente,
                                     id_produto, nome_produto, categoria_produto, quantidade, valor_unitario, valor_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (codigo, int(cliente['id']), cliente['nome'], cliente['cpf'], cliente['telefone'],
                  int(produto['id']), produto['nome'], produto['tipo'], int(qtd), float(valor_unitario), float(valor_total)))
            id_novo = cur.fetchone()[0]
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao) VALUES (%s, 'Solicitado', 'Criado')", (id_novo,))
            
            if add_lista and nome_lista:
                try:
                    cur.execute("INSERT INTO cliente.cliente_carteira_lista (cpf_cliente, nome_cliente, nome_carteira, custo_carteira) VALUES (%s, %s, %s, %s)", 
                                (cliente['cpf'], cliente['nome'], nome_lista, float(custo_lista)))
                except: pass
            
            conn.commit(); conn.close()
            
            if avisar_cliente and cliente['telefone']:
                inst = modulo_wapi.buscar_instancia_ativa()
                if inst:
                    tpl = modulo_wapi.buscar_template("PEDIDOS", "criacao")
                    if tpl:
                        msg = tpl.replace("{nome}", str(cliente['nome']).split()[0]).replace("{pedido}", codigo).replace("{produto}", str(produto['nome']))
                        modulo_wapi.enviar_msg_api(inst[0], inst[1], cliente['telefone'], msg)
            return True, codigo
        except Exception as e: return False, str(e)
    return False, "Erro conexão"

def atualizar_status_pedido(id_pedido, novo_status, dados_pedido, avisar, obs, modelo_msg):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE pedidos SET status=%s, observacao=%s, data_atualizacao=NOW() WHERE id=%s", (novo_status, obs, id_pedido))
            obs_hist = obs
            
            if novo_status == "Pago":
                ok, msg = processar_movimentacao_automatica(conn, dados_pedido, 'CREDITO')
                if ok: obs_hist += f" | {msg}"
            elif novo_status == "Cancelado":
                ok, msg = processar_movimentacao_automatica(conn, dados_pedido, 'DEBITO')
                if ok: obs_hist += f" | {msg}"
                
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
        except: return False
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

def editar_dados_pedido(id_pedido, nova_qtd, novo_valor, novo_cli, novo_prod):
    total = nova_qtd * novo_valor
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE pedidos SET id_cliente=%s, nome_cliente=%s, cpf_cliente=%s, telefone_cliente=%s,
                id_produto=%s, nome_produto=%s, categoria_produto=%s, quantidade=%s, valor_unitario=%s, valor_total=%s, data_atualizacao=NOW()
                WHERE id=%s
            """, (novo_cli['id'], novo_cli['nome'], novo_cli['cpf'], novo_cli['telefone'], novo_prod['id'], novo_prod['nome'], novo_prod['tipo'], nova_qtd, novo_valor, total, id_pedido))
            conn.commit(); conn.close()
            return True
        except: return False
    return False

# --- FUNÇÕES DE ESTADO ---
def abrir_modal(tipo, pedido=None):
    st.session_state['modal_ativo'] = tipo
    st.session_state['pedido_ativo'] = pedido

def fechar_modal():
    st.session_state['modal_ativo'] = None
    st.session_state['pedido_ativo'] = None

# --- DIALOGS ---
@st.dialog("➕ Novo Pedido")
def dialog_novo_pedido():
    df_c = buscar_clientes()
    df_p = buscar_produtos()
    if df_c.empty or df_p.empty: st.warning("Cadastre clientes e produtos."); return

    c1, c2 = st.columns(2)
    ic = c1.selectbox("Cliente", range(len(df_c)), format_func=lambda x: df_c.iloc[x]['nome'])
    ip = c2.selectbox("Produto", range(len(df_p)), format_func=lambda x: df_p.iloc[x]['nome'])
    cli, prod = df_c.iloc[ic], df_p.iloc[ip]
    
    c3, c4 = st.columns(2)
    qtd = c3.number_input("Qtd", 1, value=1)
    val = c4.number_input("Valor", 0.0, value=float(prod['preco'] or 0.0))
    st.markdown(f"### Total: R$ {qtd*val:.2f}")
    
    st.divider()
    st.markdown("📂 **Lista de Carteira (Opcional)**")
    add_cart = st.checkbox("Incluir na Lista?", value=False)
    n_cart = ""; c_cart = 0.0
    if add_cart:
        l_carts = listar_carteiras_ativas()
        col_l1, col_l2 = st.columns(2)
        n_cart = col_l1.selectbox("Carteira", [""] + l_carts)
        c_cart = col_l2.number_input("Custo", 0.0, step=0.01)

    st.divider()
    avisar = st.checkbox("Avisar WhatsApp?", value=True)
    
    if st.button("Criar Pedido", type="primary", use_container_width=True):
        if add_cart and not n_cart: st.error("Selecione a carteira.")
        else:
            ok, res = criar_pedido(cli, prod, qtd, val, qtd*val, avisar, add_cart, n_cart, c_cart)
            if ok: 
                st.success("Criado!"); time.sleep(1); fechar_modal(); st.rerun()
            else: st.error(res)

@st.dialog("✏️ Editar")
def dialog_editar(ped):
    df_c = buscar_clientes(); df_p = buscar_produtos()
    with st.form("fe"):
        try: ic = df_c[df_c['nome'] == ped['nome_cliente']].index[0]
        except: ic = 0
        try: ip = df_p[df_p['nome'] == ped['nome_produto']].index[0]
        except: ip = 0
        
        sel_c = st.selectbox("Cliente", range(len(df_c)), index=int(ic), format_func=lambda x: df_c.iloc[x]['nome'])
        sel_p = st.selectbox("Produto", range(len(df_p)), index=int(ip), format_func=lambda x: df_p.iloc[x]['nome'])
        nq = st.number_input("Qtd", 1, value=int(ped['quantidade']))
        nv = st.number_input("Valor", 0.0, value=float(ped['valor_unitario']))
        if st.form_submit_button("Salvar"):
            if editar_dados_pedido(ped['id'], nq, nv, df_c.iloc[sel_c], df_p.iloc[sel_p]):
                st.success("Salvo!"); time.sleep(1); fechar_modal(); st.rerun()

@st.dialog("🔄 Status")
def dialog_status(ped):
    lst = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
    try: idx = lst.index(ped['status']) 
    except: idx = 0
    mods = ["Automático (Padrão)"] + listar_modelos_mensagens()
    
    with st.form("fs"):
        ns = st.selectbox("Status", lst, index=idx)
        mod = st.selectbox("Msg", mods)
        obs = st.text_area("Obs")
        av = st.checkbox("Avisar?", value=True)
        if ns == "Pago": st.info("ℹ️ Lançará CRÉDITO.")
        if ns == "Cancelado": st.warning("⚠️ Lançará DÉBITO.")
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
    if 'modal_ativo' not in st.session_state: st.session_state.update({'modal_ativo': None, 'pedido_ativo': None})

    c_t, c_b = st.columns([5, 1])
    c_t.markdown("## 🛒 Pedidos")
    c_b.button("➕ Novo", type="primary", on_click=abrir_modal, args=('novo', None))

    conn = get_conn()
    if conn:
        df = pd.read_sql("SELECT p.*, c.email as email_cliente FROM pedidos p LEFT JOIN clientes_usuarios c ON p.id_cliente = c.id ORDER BY p.data_criacao DESC", conn)
        conn.close()

        with st.expander("🔍 Filtros", expanded=True):
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
                cor = "🔴"
                if row['status'] == 'Pago': cor = "🟢"
                elif row['status'] == 'Pendente': cor = "🟠"
                elif row['status'] == 'Solicitado': cor = "🔵"
                
                with st.expander(f"{cor} [{row['status']}] {row['codigo']} - {row['nome_cliente']} | R$ {row['valor_total']:.2f}"):
                    st.write(f"**Produto:** {row['nome_produto']} | **Data:** {row['data_criacao'].strftime('%d/%m %H:%M')}")
                    c1, c2, c3, c4, c5, c6 = st.columns(6)
                    
                    # Gera uma chave única usando ID + Timestamp para evitar conflito de renderização
                    ts = int(time.time())
                    c1.button("👤", key=f"c_{row['id']}_{ts}", on_click=abrir_modal, args=('cliente', row))
                    c2.button("✏️", key=f"e_{row['id']}_{ts}", on_click=abrir_modal, args=('editar', row))
                    c3.button("🔄", key=f"s_{row['id']}_{ts}", on_click=abrir_modal, args=('status', row))
                    c4.button("📜", key=f"h_{row['id']}_{ts}", on_click=abrir_modal, args=('historico', row))
                    c5.button("🗑️", key=f"d_{row['id']}_{ts}", on_click=abrir_modal, args=('excluir', row))
                    c6.button("📝", key=f"t_{row['id']}_{ts}", on_click=abrir_modal, args=('tarefa', row))
        else:
            st.info("Nenhum pedido.")

    # Roteador de Modais
    m = st.session_state['modal_ativo']
    p = st.session_state['pedido_ativo']
    
    if m == 'novo': dialog_novo_pedido()
    elif m == 'cliente' and p is not None: 
        st.dialog("👤 Cliente")(lambda: st.write(f"Nome: {p['nome_cliente']}\nCPF: {p['cpf_cliente']}\nTel: {p['telefone_cliente']}"))()
        fechar_modal() # Fecha logo após renderizar pois é apenas visualização simples
    elif m == 'editar' and p is not None: dialog_editar(p)
    elif m == 'status' and p is not None: dialog_status(p)
    elif m == 'historico' and p is not None: dialog_historico(p['id'], p['codigo'])
    elif m == 'excluir' and p is not None: dialog_excluir(p['id'])
    elif m == 'tarefa' and p is not None: dialog_tarefa(p)

if __name__ == "__main__":
    app_pedidos()