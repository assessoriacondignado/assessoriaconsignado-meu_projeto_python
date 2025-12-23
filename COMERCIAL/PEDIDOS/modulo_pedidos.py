import streamlit as st
import pandas as pd
import psycopg2
import os
from datetime import datetime
import modulo_wapi  # Integração centralizada

try:
    import conexao
except ImportError:
    st.error("Erro crítico: Arquivo conexao.py não localizado.")

# --- CONEXÃO COM BANCO ---
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
    """Busca os modelos de mensagem cadastrados no W-API para este módulo"""
    conn = get_conn()
    if conn:
        try:
            # Filtra apenas modelos do módulo PEDIDOS
            query = "SELECT chave_status FROM wapi_templates WHERE modulo = 'PEDIDOS' ORDER BY chave_status ASC"
            df = pd.read_sql(query, conn)
            conn.close()
            return df['chave_status'].tolist()
        except:
            conn.close()
    return []

# --- FUNÇÕES DE BANCO DE DADOS (CRUD) ---
def buscar_clientes():
    conn = get_conn()
    if conn:
        df = pd.read_sql("SELECT id, nome, cpf, telefone FROM clientes_usuarios WHERE ativo = TRUE ORDER BY nome", conn)
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
            
            id_novo_pedido = cur.fetchone()[0]
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao) VALUES (%s, %s, %s)", 
                        (id_novo_pedido, 'Solicitado', 'Pedido criado no sistema.'))
            conn.commit()
            conn.close()
            
            # --- NOTIFICAÇÕES VIA W-API CENTRALIZADO ---
            instancia = modulo_wapi.buscar_instancia_ativa()
            if instancia and avisar_cliente and cliente['telefone']:
                inst_id, inst_token = instancia
                template = modulo_wapi.buscar_template("PEDIDOS", "criacao")
                if template:
                    msg_final = template.replace("{nome}", str(cliente['nome']).split()[0]) \
                                        .replace("{pedido}", codigo) \
                                        .replace("{produto}", str(produto['nome']))
                    modulo_wapi.enviar_msg_api(inst_id, inst_token, cliente['telefone'], msg_final)
            
            return True, codigo
        except Exception as e:
            return False, str(e)
    return False, "Erro conexão"

def editar_dados_pedido(id_pedido, nova_qtd, novo_valor_unit, novo_cliente, novo_produto):
    novo_total = nova_qtd * novo_valor_unit
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE pedidos 
                SET id_cliente = %s, nome_cliente = %s, cpf_cliente = %s, telefone_cliente = %s,
                    id_produto = %s, nome_produto = %s, categoria_produto = %s,
                    quantidade = %s, valor_unitario = %s, valor_total = %s, 
                    data_atualizacao = NOW()
                WHERE id = %s
            """, (int(novo_cliente['id']), novo_cliente['nome'], novo_cliente['cpf'], novo_cliente['telefone'],
                  int(novo_produto['id']), novo_produto['nome'], novo_produto['tipo'],
                  int(nova_qtd), float(novo_valor_unit), float(novo_total), int(id_pedido)))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar dados: {e}")
            return False
    return False

def atualizar_status_pedido(id_pedido, novo_status, dados_pedido, avisar_cliente, obs_status_texto, modelo_msg_escolhido="Automático (Padrão)"):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE pedidos SET status = %s, observacao = %s, data_atualizacao = NOW() WHERE id = %s", 
                        (novo_status, obs_status_texto, int(id_pedido)))
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao, data_mudanca) VALUES (%s, %s, %s, NOW())", 
                        (int(id_pedido), novo_status, obs_status_texto))
            conn.commit()
            conn.close()
            
            if avisar_cliente and dados_pedido['telefone_cliente']:
                instancia = modulo_wapi.buscar_instancia_ativa()
                if instancia:
                    # Lógica de seleção do modelo
                    if modelo_msg_escolhido and modelo_msg_escolhido != "Automático (Padrão)":
                        chave_msg = modelo_msg_escolhido
                    else:
                        chave_msg = novo_status.lower().replace(" ", "_")
                    
                    template = modulo_wapi.buscar_template("PEDIDOS", chave_msg)
                    
                    if template:
                        msg_final = template.replace("{nome}", str(dados_pedido['nome_cliente']).split()[0]) \
                                            .replace("{pedido}", str(dados_pedido['codigo'])) \
                                            .replace("{status}", novo_status) \
                                            .replace("{obs_status}", obs_status_texto) \
                                            .replace("{produto}", str(dados_pedido['nome_produto']))
                        modulo_wapi.enviar_msg_api(instancia[0], instancia[1], dados_pedido['telefone_cliente'], msg_final)
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar status: {e}")
            return False
    return False

def excluir_pedido_db(id_pedido):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM pedidos WHERE id = %s", (id_pedido,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao excluir: {e}")
            return False
    return False

def buscar_historico_pedido(id_pedido):
    conn = get_conn()
    if conn:
        query = "SELECT data_mudanca, status_novo, observacao FROM pedidos_historico WHERE id_pedido = %s ORDER BY data_mudanca DESC"
        df = pd.read_sql(query, conn, params=(int(id_pedido),))
        conn.close()
        return df
    return pd.DataFrame()

# --- POP-UPS (DIALOGS) ---
@st.dialog("➕ Novo Pedido")
def dialog_novo_pedido():
    df_c = buscar_clientes()
    df_p = buscar_produtos()
    
    if df_c.empty or df_p.empty:
        st.warning("É necessário ter Clientes e Produtos cadastrados.")
        return

    with st.form("form_novo_pedido"):
        st.write("Preencha os dados do pedido:")
        c1, c2 = st.columns(2)
        idx_cli = c1.selectbox("Cliente", range(len(df_c)), format_func=lambda x: df_c.iloc[x]['nome'])
        idx_prod = c2.selectbox("Produto", range(len(df_p)), format_func=lambda x: df_p.iloc[x]['nome'])
        
        cli_selecionado = df_c.iloc[idx_cli]
        prod_selecionado = df_p.iloc[idx_prod]
        
        c3, c4 = st.columns(2)
        qtd = c3.number_input("Quantidade", min_value=1, value=1, step=1)
        # Permite editar o valor unitário, puxando o padrão do produto
        valor_unit = c4.number_input("Valor Unitário (R$)", min_value=0.0, value=float(prod_selecionado['preco'] or 0.0), step=0.5, format="%.2f")
        
        # Cálculo do total
        total = qtd * valor_unit
        st.markdown(f"### 💰 Total: R$ {total:.2f}")
        
        avisar = st.checkbox("Enviar confirmação no WhatsApp?", value=True)

        if st.form_submit_button("✅ Confirmar Pedido"):
            ok, res = criar_pedido(cli_selecionado, prod_selecionado, qtd, valor_unit, total, avisar)
            if ok:
                st.success(f"Pedido {res} criado com sucesso!")
                st.rerun()
            else:
                st.error(f"Erro: {res}")

@st.dialog("👤 Detalhes do Cliente")
def ver_cliente(nome, cpf, tel):
    st.write(f"**Nome:** {nome}")
    st.write(f"**CPF:** {cpf}")
    st.write(f"**Telefone:** {tel}")

@st.dialog("✏️ Editar Pedido")
def dialog_editar_dados(pedido):
    df_clientes = buscar_clientes()
    df_produtos = buscar_produtos()
    if df_clientes.empty or df_produtos.empty: return
    
    with st.form("form_editar_dados"):
        idx_cli = st.selectbox("Cliente", range(len(df_clientes)), format_func=lambda x: df_clientes.iloc[x]['nome'])
        idx_prod = st.selectbox("Produto", range(len(df_produtos)), format_func=lambda x: df_produtos.iloc[x]['nome'])
        nova_qtd = st.number_input("Quantidade", min_value=1, value=int(pedido['quantidade']))
        novo_preco = st.number_input("Valor Unitário", min_value=0.0, value=float(pedido['valor_unitario']))
        
        novo_total = nova_qtd * novo_preco
        st.write(f"Novo Total: R$ {novo_total:.2f}")

        if st.form_submit_button("💾 Salvar"):
            if editar_dados_pedido(pedido['id'], nova_qtd, novo_preco, df_clientes.iloc[idx_cli], df_produtos.iloc[idx_prod]):
                st.success("Atualizado!")
                st.rerun()

@st.dialog("🔄 Atualizar Status")
def dialog_status_pedido(pedido):
    status_opcoes = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
    
    # Carrega opções de modelos do W-API
    lista_modelos = listar_modelos_mensagens()
    opcoes_msg = ["Automático (Padrão)"] + lista_modelos
    
    with st.form("form_status_update"):
        novo = st.selectbox("Novo Status", status_opcoes)
        modelo_escolhido = st.selectbox("Modelo de Mensagem", opcoes_msg, help="Selecione 'Automático' para usar a mensagem padrão do status.")
        obs = st.text_area("Observação")
        avisar = st.checkbox("Avisar cliente?", value=True)
        
        if st.form_submit_button("Atualizar"):
            if atualizar_status_pedido(pedido['id'], novo, pedido, avisar, obs, modelo_escolhido):
                st.success("Status Alterado!")
                st.rerun()

@st.dialog("📜 Histórico")
def dialog_historico(id_pedido, codigo_pedido):
    st.write(f"Histórico de: **{codigo_pedido}**")
    df_hist = buscar_historico_pedido(id_pedido)
    if not df_hist.empty:
        df_hist.columns = ["Data/Hora", "Status", "Obs"]
        st.dataframe(df_hist, use_container_width=True, hide_index=True)
    else: st.info("Sem registros.")

@st.dialog("⚠️ Excluir Pedido")
def dialog_excluir(id_pedido):
    st.error("Tem certeza que deseja excluir este pedido?")
    st.warning("Esta ação não pode ser desfeita.")
    if st.button("Confirmar Exclusão", type="primary"):
        if excluir_pedido_db(id_pedido):
            st.success("Pedido excluído!")
            st.rerun()

# --- APP PRINCIPAL ---
def app_pedidos():
    # Cabeçalho com Botão Novo no Topo (Ajustado)
    c_title, c_btn = st.columns([5, 1])
    c_title.markdown("## 🛒 Módulo de Pedidos") 
    # use_container_width=False para o botão ficar da largura do texto
    if c_btn.button("➕ Novo Pedido", type="primary", use_container_width=False):
        dialog_novo_pedido()

    conn = get_conn()
    if conn:
        try:
            # Query com JOIN para trazer o e-mail do cliente para pesquisa
            query = """
                SELECT p.*, c.email as email_cliente 
                FROM pedidos p
                LEFT JOIN clientes_usuarios c ON p.id_cliente = c.id
                ORDER BY p.data_criacao DESC
            """
            df = pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Erro na query: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()

        # --- FILTROS DE PESQUISA (Unificado e Melhorado) ---
        with st.expander("🔍 Filtros de Pesquisa", expanded=True):
            # Linha 1: Busca Geral e Categorias
            cf1, cf2 = st.columns([3, 1.5])
            busca_geral = cf1.text_input("🔍 Buscar (Nome, Email, Telefone, Produto)", placeholder="Comece a digitar...")
            
            opcoes_cats = df['categoria_produto'].unique() if not df.empty else []
            f_cats = cf2.multiselect("Categoria", options=opcoes_cats, placeholder="Filtrar Categorias")
            
            # Linha 2: Filtro de Data
            cd1, cd2, cd3 = st.columns([1.5, 1.5, 3])
            op_data = cd1.selectbox("Filtro de Data", ["Todo o período", "Igual a", "Antes de", "Depois de"])
            
            # Formato brasileiro no date_input
            data_ref = cd2.date_input("Data Referência", value=datetime.today(), format="DD/MM/YYYY")

            # --- APLICAÇÃO DOS FILTROS ---
            if not df.empty:
                # 1. Filtro Texto Geral (Unificado)
                if busca_geral:
                    mask = (
                        df['nome_cliente'].str.contains(busca_geral, case=False, na=False) |
                        df['nome_produto'].str.contains(busca_geral, case=False, na=False) |
                        df['telefone_cliente'].str.contains(busca_geral, case=False, na=False) |
                        df['email_cliente'].str.contains(busca_geral, case=False, na=False)
                    )
                    df = df[mask]
                
                # 2. Filtro de Categoria
                if f_cats:
                    df = df[df['categoria_produto'].isin(f_cats)]
                
                # 3. Filtro de Data
                if op_data != "Todo o período":
                    df_data = pd.to_datetime(df['data_criacao']).dt.date
                    if op_data == "Igual a":
                        df = df[df_data == data_ref]
                    elif op_data == "Antes de":
                        df = df[df_data < data_ref]
                    elif op_data == "Depois de":
                        df = df[df_data > data_ref]

        # --- PAGINAÇÃO / LIMITE DE VISUALIZAÇÃO ---
        st.markdown("---")
        col_res, col_pag = st.columns([4, 1])
        with col_pag:
            qtd_view = st.selectbox("Visualizar:", [10, 20, 50, 100, "Todos"], index=0)
        
        # Fatia o Dataframe conforme a seleção
        df_exibir = df.copy()
        if qtd_view != "Todos":
            df_exibir = df.head(int(qtd_view))
        
        with col_res:
            st.caption(f"Exibindo {len(df_exibir)} de {len(df)} pedidos encontrados.")

        # --- LISTAGEM DOS PEDIDOS ---
        if not df_exibir.empty:
            for i, row in df_exibir.iterrows():
                # Cor do status
                cor_status = "🔴"
                if row['status'] == 'Pago': cor_status = "🟢"
                elif row['status'] == 'Pendente': cor_status = "🟠"
                elif row['status'] == 'Solicitado': cor_status = "🔵"
                
                with st.expander(f"{cor_status} {row['codigo']} - {row['nome_cliente']} | R$ {row['valor_total']:.2f}"):
                    st.write(f"**Produto:** {row['nome_produto']} ({row['categoria_produto']})")
                    # Formata data visualmente
                    data_fmt = pd.to_datetime(row['data_criacao']).strftime('%d/%m/%Y %H:%M')
                    st.write(f"**Data:** {data_fmt}")
                    
                    c1, c2, c3, c4, c5 = st.columns(5)
                    if c1.button("👤 Cliente", key=f"c_{row['id']}"): ver_cliente(row['nome_cliente'], row['cpf_cliente'], row['telefone_cliente'])
                    if c2.button("✏️ Dados", key=f"e_{row['id']}"): dialog_editar_dados(row)
                    if c3.button("🔄 Status", key=f"s_{row['id']}"): dialog_status_pedido(row)
                    if c4.button("📜 Hist.", key=f"h_{row['id']}"): dialog_historico(row['id'], row['codigo'])
                    
                    # Botão Excluir
                    if c5.button("🗑️ Excluir", key=f"del_{row['id']}"): dialog_excluir(row['id'])
        else:
            st.info("Nenhum pedido encontrado com os filtros atuais.")
    else:
        st.info("Sem conexão com o banco.")

if __name__ == "__main__":
    app_pedidos()