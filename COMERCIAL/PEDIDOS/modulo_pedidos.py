import streamlit as st
import pandas as pd
import psycopg2
import os
import requests
import json
import re
from datetime import datetime

# Tentativa de importação robusta da conexão
try:
    import conexao
except ImportError:
    st.error("Erro crítico: Arquivo conexao.py não localizado.")

# --- CONFIGURAÇÕES DE DIRETÓRIO DINÂMICO ---
# Substituímos o caminho fixo /root/ para funcionar na nuvem
BASE_DIR = os.path.join(os.getcwd(), "COMERCIAL", "PEDIDOS")

try:
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR, exist_ok=True)
except PermissionError:
    # Se falhar no Streamlit Cloud, usa o diretório temporário padrão do Linux
    BASE_DIR = "/tmp"

# --- CONEXÃO COM BANCO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host,
            port=conexao.port,
            database=conexao.database,
            user=conexao.user,
            password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")
        return None

# --- INTEGRAÇÃO W-API ---
def buscar_instancia_ativa():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
            res = cur.fetchone()
            conn.close()
            return res 
        except: return None
    return None

def enviar_whatsapp(numero, mensagem):
    dados_instancia = buscar_instancia_ativa()
    if not dados_instancia:
        return False, "Sem instância cadastrada."
    
    instance_id, token = dados_instancia
    BASE_URL = "https://api.w-api.app/v1"
    url = f"{BASE_URL}/message/send-text?instanceId={instance_id}"
    
    if "@g.us" in str(numero):
        numero_limpo = str(numero)
    else:
        numero_limpo = re.sub(r'\D', '', str(numero)) 
        if len(numero_limpo) < 12 and not numero_limpo.startswith("55"):
             numero_limpo = "55" + numero_limpo

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "phone": numero_limpo, 
        "message": mensagem,
        "delayMessage": 3
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code in [200, 201]:
            return True, "Enviado"
        else:
            return False, str(response.json())
    except Exception as e:
        return False, f"Erro Requisição: {str(e)}"

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

def buscar_configuracao():
    conn = get_conn()
    if conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM config_pedidos WHERE id = 1")
        colunas = [desc[0] for desc in cur.description]
        res = cur.fetchone()
        conn.close()
        if res:
            return dict(zip(colunas, res))
    return {}

def salvar_configuracao(grupo_id, templates):
    conn = get_conn()
    if conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE config_pedidos SET 
                grupo_aviso_id = %s, msg_criacao = %s, msg_pago = %s, 
                msg_registrar = %s, msg_pendente = %s, msg_cancelado = %s
            WHERE id = 1
        """, (grupo_id, templates['msg_criacao'], templates['msg_pago'], 
              templates['msg_registrar'], templates['msg_pendente'], templates['msg_cancelado']))
        conn.commit()
        conn.close()

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
            
            # --- NOTIFICAÇÕES ---
            config = buscar_configuracao()
            if avisar_grupo and config.get('grupo_aviso_id'):
                grupo_destino = str(config['grupo_aviso_id']).strip()
                if "@" not in grupo_destino: grupo_destino += "@g.us"
                msg_grupo = f"🔔 *NOVO PEDIDO NO SITE*\n\n📄 Cód: {codigo}\n👤 Cliente: {cliente['nome']}\n📦 Item: {produto['nome']}\n💰 Valor: R$ {valor_total:.2f}"
                enviar_whatsapp(grupo_destino, msg_grupo)
            
            if avisar_cliente and cliente['telefone']:
                template = config.get('msg_criacao', '')
                if template:
                    msg_final = template.replace("{nome}", str(cliente['nome']).split()[0]).replace("{pedido}", codigo).replace("{produto}", str(produto['nome']))
                    enviar_whatsapp(cliente['telefone'], msg_final)
            
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
            
            if avisar_cliente:
                config = buscar_configuracao()
                campo_msg = "msg_registrar" if novo_status == "Registro" else f"msg_{novo_status.lower()}"
                template = config.get(campo_msg, '')
                if template and dados_pedido['telefone_cliente']:
                    msg_final = template.replace("{nome}", str(dados_pedido['nome_cliente']).split()[0]) \
                                        .replace("{pedido}", str(dados_pedido['codigo'])) \
                                        .replace("{status}", novo_status) \
                                        .replace("{obs_status}", obs_status_texto) \
                                        .replace("{produto}", str(dados_pedido['nome_produto']))
                    enviar_whatsapp(dados_pedido['telefone_cliente'], msg_final)
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar status: {e}")
            return False
    return False

def excluir_pedido(id_pedido):
    conn = get_conn()
    if conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM pedidos WHERE id = %s", (int(id_pedido),))
        conn.commit()
        conn.close()
        return True
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

@st.dialog("📦 Detalhes do Produto")
def ver_produto(nome, cat):
    st.write(f"**Produto:** {nome}")
    st.write(f"**Categoria:** {cat}")

@st.dialog("⚙️ Configurar Mensagens")
def dialog_configuracao():
    conf = buscar_configuracao()
    with st.form("form_config_pedidos"):
        st.subheader("Integração W-API")
        grupo = st.text_input("ID do Grupo (Apenas números)", value=conf.get('grupo_aviso_id', ''))
        st.caption("Tags: {nome}, {pedido}, {produto}, {obs_status}")
        tpls = {
            'msg_criacao': st.text_area("Novo Pedido", value=conf.get('msg_criacao', '')),
            'msg_pago': st.text_area("Pago", value=conf.get('msg_pago', '')),
            'msg_registrar': st.text_area("Registro", value=conf.get('msg_registrar', '')),
            'msg_pendente': st.text_area("Pendente", value=conf.get('msg_pendente', '')),
            'msg_cancelado': st.text_area("Cancelado", value=conf.get('msg_cancelado', ''))
        }
        if st.form_submit_button("Salvar Configurações"):
            salvar_configuracao(grupo, tpls)
            st.success("Salvo!")
            st.rerun()

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
    tab1, tab2, tab3 = st.tabs(["📝 Novo", "🔎 Gerenciar", "⚙️ Config"])
    
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

    with tab3:
        if st.button("⚙️ Configurar Mensagens"): dialog_configuracao()

if __name__ == "__main__":
    app_pedidos()