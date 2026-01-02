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

# --- NOVA FUNÇÃO: MOVIMENTAÇÃO AUTOMÁTICA (CRÉDITO/DÉBITO) ---
def processar_movimentacao_automatica(conn, dados_pedido, tipo_lancamento):
    """
    Lança CRÉDITO ou DÉBITO na carteira do cliente dependendo do status do pedido.
    """
    try:
        cur = conn.cursor()
        
        # 1. Verifica se existe carteira configurada para este produto
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
            
            # 2. Busca o saldo anterior
            cur.execute(f"SELECT saldo_novo FROM {tabela_carteira} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf,))
            res_saldo = cur.fetchone()
            saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
            
            # 3. Define Operação (Crédito ou Débito)
            if tipo_lancamento == 'DEBITO':
                saldo_novo = saldo_anterior - valor
                motivo = f"Cancelamento Pedido #{dados_pedido['codigo']} - Estorno"
            else: # CREDITO
                saldo_novo = saldo_anterior + valor
                motivo = f"Compra Pedido #{dados_pedido['codigo']} - {dados_pedido['nome_produto']}"
            
            # 4. Insere o Lançamento
            sql_insert = f"""
                INSERT INTO {tabela_carteira} 
                (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao)
                VALUES (%s, %s, %s, 'PEDIDO', %s, %s, %s, %s, NOW())
            """
            cur.execute(sql_insert, (cpf, dados_pedido['nome_cliente'], motivo, tipo_lancamento, valor, saldo_anterior, saldo_novo))
            
            return True, f"{tipo_lancamento} de R$ {valor:.2f} em '{nome_carteira}'"
            
    except Exception as e:
        print(f"Erro movimentacao auto: {e}")
        return False, str(e)
        
    return False, None # Produto sem carteira vinculada

# --- FUNÇÕES DE CRUD ---
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

def criar_pedido(cliente, produto, qtd, valor_unitario, valor_total, avisar_cliente):
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
            
            # Atualiza o status do pedido
            cur.execute("UPDATE pedidos SET status=%s, observacao=%s, data_atualizacao=NOW() WHERE id=%s", (novo_status, obs, id_pedido))
            
            obs_historico = obs
            
            # --- INTEGRAÇÃO FINANCEIRA (NOVO) ---
            if novo_status == "Pago":
                ok_mov, msg_mov = processar_movimentacao_automatica(conn, dados_pedido, 'CREDITO')
                if ok_mov: obs_historico += f" | {msg_mov}"
            
            elif novo_status == "Cancelado":
                ok_mov, msg_mov = processar_movimentacao_automatica(conn, dados_pedido, 'DEBITO')
                if ok_mov: obs_historico += f" | {msg_mov}"
            # ------------------------------------

            # Grava histórico
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao) VALUES (%s, %s, %s)", (id_pedido, novo_status, obs_historico))
            
            conn.commit(); conn.close()
            
            # Envio de WhatsApp
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
            print(f"Erro ao atualizar: {e}")
            return False
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

# --- DIALOGS ---
@st.dialog("➕ Novo Pedido")
def dialog_novo_pedido():
    df_c = buscar_clientes()
    df_p = buscar_produtos()
    if df_c.empty or df_p.empty: st.warning("Cadastre clientes e produtos."); return

    with st.form("form_new"):
        c1, c2 = st.columns(2)
        ic = c1.selectbox("Cliente", range(len(df_c)), format_func=lambda x: df_c.iloc[x]['nome'])
        ip = c2.selectbox("Produto", range(len(df_p)), format_func=lambda x: df_p.iloc[x]['nome'])
        cli, prod = df_c.iloc[ic], df_p.iloc[ip]
        
        c3, c4 = st.columns(2)
        qtd = c3.number_input("Qtd", 1, value=1)
        val = c4.number_input("Valor Un.", 0.0, value=float(prod['preco'] or 0.0))
        st.write(f"Total: R$ {qtd*val:.2f}")
        avisar = st.checkbox("Avisar WhatsApp?", value=True)
        
        if st.form_submit_button("Criar"):
            ok, res = criar_pedido(cli, prod, qtd, val, qtd*val, avisar)
            if ok: st.success("Criado!"); time.sleep(1); st.rerun()
            else: st.error(res)

@st.dialog("✏️ Editar")
def dialog_editar(ped):
    df_c = buscar_clientes(); df_p = buscar_produtos()
    with st.form("form_edit"):
        try: ic_ini = df_c[df_c['nome'] == ped['nome_cliente']].index[0]
        except: ic_ini = 0
        try: ip_ini = df_p[df_p['nome'] == ped['nome_produto']].index[0]
        except: ip_ini = 0

        ic = st.selectbox("Cliente", range(len(df_c)), index=int(ic_ini), format_func=lambda x: df_c.iloc[x]['nome'])
        ip = st.selectbox("Produto", range(len(df_p)), index=int(ip_ini), format_func=lambda x: df_p.iloc[x]['nome'])
        nq = st.number_input("Qtd", 1, value=int(ped['quantidade']))
        nv = st.number_input("Valor", 0.0, value=float(ped['valor_unitario']))
        if st.form_submit_button("Salvar"):
            if editar_dados_pedido(ped['id'], nq, nv, df_c.iloc[ic], df_p.iloc[ip]): st.success("Salvo!"); st.rerun()

@st.dialog("🔄 Status")
def dialog_status(ped):
    lst = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
    try: idx = lst.index(ped['status']) 
    except: idx = 0
    mods = ["Automático (Padrão)"] + listar_modelos_mensagens()
    
    with st.form("form_st"):
        ns = st.selectbox("Status", lst, index=idx)
        mod = st.selectbox("Modelo Msg", mods)
        obs = st.text_area("Obs")
        av = st.checkbox("Avisar?", value=True)
        
        if ns == "Pago":
            st.info("ℹ️ Será lançado um CRÉDITO na carteira do cliente.")
        if ns == "Cancelado":
            st.warning("⚠️ Será lançado um DÉBITO (estorno) na carteira do cliente.")

        if st.form_submit_button("Atualizar"):
            if atualizar_status_pedido(ped['id'], ns, ped, av, obs, mod): st.success("Atualizado!"); st.rerun()
            
    st.divider(); st.caption("Histórico")
    st.dataframe(buscar_historico_pedido(ped['id']), hide_index=True)

@st.dialog("🗑️ Excluir")
def dialog_excluir(pid):
    st.warning("Confirmar exclusão?")
    if st.button("Sim", type="primary"):
        if excluir_pedido_db(pid): st.success("Excluído!"); st.rerun()

@st.dialog("📝 Nova Tarefa")
def dialog_tarefa(ped):
    with st.form("form_task"):
        dt = st.date_input("Previsão", datetime.now())
        obs = st.text_area("Descrição")
        if st.form_submit_button("Criar"):
            conn = get_conn(); cur = conn.cursor()
            cur.execute("INSERT INTO tarefas (id_pedido, id_cliente, id_produto, data_previsao, observacao_tarefa, status) VALUES (%s,%s,%s,%s,%s,'Solicitado')", (ped['id'], ped['id_cliente'], ped['id_produto'], dt, obs))
            conn.commit(); conn.close()
            st.success("Tarefa Criada!"); st.rerun()

# --- COMPONENTE ISOLADO (@st.fragment) ---
@st.fragment
def cartao_pedido(row):
    cor = "🔴"
    if row['status'] == 'Pago': cor = "🟢"
    elif row['status'] == 'Pendente': cor = "🟠"
    elif row['status'] == 'Solicitado': cor = "🔵"
    
    label = f"{cor} [{row['status'].upper()}] {row['codigo']} - {row['nome_cliente']} | R$ {row['valor_total']:.2f}"
    
    with st.expander(label):
        st.write(f"**Produto:** {row['nome_produto']}")
        st.write(f"**Data:** {row['data_criacao'].strftime('%d/%m %H:%M')}")
        
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        if c1.button("👤", key=f"c_{row['id']}", help="Cliente"): 
            st.info(f"Cliente: {row['nome_cliente']}\nCPF: {row['cpf_cliente']}\nTel: {row['telefone_cliente']}")
        if c2.button("✏️", key=f"e_{row['id']}", help="Editar"): dialog_editar(row)
        if c3.button("🔄", key=f"s_{row['id']}", help="Status"): dialog_status(row)
        if c4.button("📜", key=f"h_{row['id']}", help="Histórico"): 
            st.dataframe(buscar_historico_pedido(row['id']), hide_index=True)
        if c5.button("🗑️", key=f"d_{row['id']}", help="Excluir"): dialog_excluir(row['id'])
        if c6.button("📝", key=f"t_{row['id']}", help="Tarefa"): dialog_tarefa(row)

# --- APP PRINCIPAL ---
def app_pedidos():
    c_t, c_b = st.columns([5, 1])
    c_t.markdown("## 🛒 Pedidos")
    if c_b.button("➕ Novo", type="primary"): dialog_novo_pedido()

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
        
        start = (pag - 1) * pag_size
        end = start + pag_size
        subset = df.iloc[start:end]

        if not subset.empty:
            for _, row in subset.iterrows():
                cartao_pedido(row)
        else:
            st.info("Nenhum pedido.")

if __name__ == "__main__":
    app_pedidos()