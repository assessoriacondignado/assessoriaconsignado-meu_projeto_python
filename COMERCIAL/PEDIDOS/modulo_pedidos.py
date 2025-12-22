import streamlit as st
import pandas as pd
import psycopg2
import os
import re
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

def criar_pedido(cliente, produto, qtd, valor_total, avisar_cliente, avisar_grupo):
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
                  int(produto['id']), produto['nome'], produto['tipo'], int(qtd), float(produto['preco']), float(valor_total)))
            
            id_novo_pedido = cur.fetchone()[0]
            cur.execute("INSERT INTO pedidos_historico (id_pedido, status_novo, observacao) VALUES (%s, %s, %s)", 
                        (id_novo_pedido, 'Solicitado', 'Pedido criado no sistema.'))
            conn.commit()
            conn.close()
            
            # --- NOTIFICAÇÕES VIA W-API CENTRALIZADO ---
            instancia = modulo_wapi.buscar_instancia_ativa()
            if instancia:
                inst_id, inst_token = instancia
                
                # Aviso Grupo (Opcional: Pode ser configurado via template também futuramente)
                if avisar_grupo: 
                    # Simples hardcode ou busca template 'grupo_novo_pedido' se desejar
                    pass 

                # Aviso Cliente
                if avisar_cliente and cliente['telefone']:
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

def atualizar_status_pedido(id_pedido, novo_status, dados_pedido, avisar_cliente, obs_status_texto):
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
                    # Busca template centralizado: chaves ex: 'pago', 'registro', 'pendente'
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

def buscar_historico_pedido(id_pedido):
    conn = get_conn()
    if conn:
        query = "SELECT data_mudanca, status_novo, observacao FROM pedidos_historico WHERE id_pedido = %s ORDER BY data_mudanca DESC"
        df = pd.read_sql(query, conn, params=(int(id_pedido),))
        conn.close()
        return df
    return pd.DataFrame()

# --- POP-UPS (DIALOGS) ---
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
        if st.form_submit_button("💾 Salvar"):
            if editar_dados_pedido(pedido['id'], nova_qtd, novo_preco, df_clientes.iloc[idx_cli], df_produtos.iloc[idx_prod]):
                st.success("Atualizado!")
                st.rerun()

@st.dialog("🔄 Atualizar Status")
def dialog_status_pedido(pedido):
    status_opcoes = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
    with st.form("form_status_update"):
        novo = st.selectbox("Novo Status", status_opcoes)
        obs = st.text_area("Observação")
        avisar = st.checkbox("Avisar cliente?", value=True)
        if st.form_submit_button("Atualizar"):
            if atualizar_status_pedido(pedido['id'], novo, pedido, avisar, obs):
                st.success("Status Alterado!")
                st.rerun()

@st.dialog("📜 Histórico")
def dialog_historico(id_pedido, codigo_pedido):
    df_hist = buscar_historico_pedido(id_pedido)
    if not df_hist.empty:
        df_hist.columns = ["Data/Hora", "Status", "Obs"]
        st.dataframe(df_hist, use_container_width=True, hide_index=True)
    else: st.info("Sem registros.")

# --- APP PRINCIPAL ---
def app_pedidos():
    st.markdown("## 🛒 Módulo de Pedidos") 
    tab1, tab2 = st.tabs(["📝 Novo", "🔎 Gerenciar"])
    
    with tab1:
        df_c = buscar_clientes()
        df_p = buscar_produtos()
        if not df_c.empty and not df_p.empty:
            with st.form("form_novo_pedido"):
                c1, c2 = st.columns(2)
                cli = df_c.iloc[c1.selectbox("Cliente", range(len(df_c)), format_func=lambda x: df_c.iloc[x]['nome'])]
                prod = df_p.iloc[c2.selectbox("Produto", range(len(df_p)), format_func=lambda x: df_p.iloc[x]['nome'])]
                q = st.number_input("Qtd", min_value=1, value=1)
                total = q * float(prod['preco'])
                st.metric("Total", f"R$ {total:.2f}")
                if st.form_submit_button("✅ Finalizar"):
                    ok, res = criar_pedido(cli, prod, q, total, True, True)
                    if ok: st.success(f"Pedido {res} criado!")
        else: st.warning("Cadastre clientes e produtos primeiro.")

    with tab2:
        conn = get_conn()
        if conn:
            df = pd.read_sql("SELECT * FROM pedidos ORDER BY data_criacao DESC", conn)
            conn.close()
            if not df.empty:
                for i, row in df.iterrows():
                    with st.expander(f"📦 {row['codigo']} - {row['nome_cliente']} ({row['status']})"):
                        st.write(f"Item: {row['nome_produto']} | Total: R$ {row['valor_total']:.2f}")
                        c1, c2, c3, c4 = st.columns(4)
                        if c1.button("👤 Cliente", key=f"c_{row['id']}"): ver_cliente(row['nome_cliente'], row['cpf_cliente'], row['telefone_cliente'])
                        if c2.button("✏️ Dados", key=f"e_{row['id']}"): dialog_editar_dados(row)
                        if c3.button("🔄 Status", key=f"s_{row['id']}"): dialog_status_pedido(row)
                        if c4.button("📜 Hist.", key=f"h_{row['id']}"): dialog_historico(row['id'], row['codigo'])

if __name__ == "__main__":
    app_pedidos()