import streamlit as st
import pandas as pd
import psycopg2
import os
import requests
import json
import re
from datetime import datetime
import conexao

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
BASE_DIR = "/root/meu_sistema/COMERCIAL/PEDIDOS"
if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR, exist_ok=True)

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
    # 1. Busca dados da instância
    dados_instancia = buscar_instancia_ativa()
    if not dados_instancia:
        print("❌ Erro W-API: Nenhuma instância cadastrada.")
        return False, "Sem instância cadastrada."
    
    instance_id, token = dados_instancia
    
    BASE_URL = "https://api.w-api.app/v1"
    url = f"{BASE_URL}/message/send-text?instanceId={instance_id}"
    
    # AJUSTE GRUPO: Verifica se é grupo (tem @g.us)
    # Se tiver @g.us, não limpamos os caracteres. Se não, aplicamos a limpeza de telefone.
    if "@g.us" in str(numero):
        numero_limpo = str(numero)
    else:
        # Limpeza para telefones normais
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
        print(f"📡 Enviando para {numero_limpo} via instância {instance_id}...")
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        res_json = response.json()
        
        if response.status_code == 200 or response.status_code == 201:
            print("✅ Mensagem enviada!")
            return True, "Enviado"
        else:
            erro_msg = f"Erro API: {res_json}"
            print(f"❌ {erro_msg}")
            return False, str(res_json)
            
    except Exception as e:
        print(f"❌ Erro de Requisição: {str(e)}")
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
    
    id_cliente = int(cliente['id'])
    id_produto = int(produto['id'])
    val_total = float(valor_total)
    val_unit = float(produto['preco'])
    qtd_int = int(qtd)
    
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pedidos (codigo, id_cliente, nome_cliente, cpf_cliente, telefone_cliente,
                                     id_produto, nome_produto, categoria_produto, quantidade, valor_unitario, valor_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (codigo, id_cliente, cliente['nome'], cliente['cpf'], cliente['telefone'],
                  id_produto, produto['nome'], produto['tipo'], qtd_int, val_unit, val_total))
            
            id_novo_pedido = cur.fetchone()[0]
            
            cur.execute("""
                INSERT INTO pedidos_historico (id_pedido, status_novo, observacao)
                VALUES (%s, %s, %s)
            """, (id_novo_pedido, 'Solicitado', 'Pedido criado no sistema.'))

            conn.commit()
            conn.close()
            
            # --- NOTIFICAÇÕES ---
            config = buscar_configuracao()
            
            # AJUSTE GRUPO: Adiciona @g.us se o usuário digitou só números
            if avisar_grupo and config.get('grupo_aviso_id'):
                grupo_destino = str(config['grupo_aviso_id']).strip()
                if "@" not in grupo_destino:
                    grupo_destino += "@g.us"
                
                msg_grupo = f"🔔 *NOVO PEDIDO NO SITE*\n\n📄 Cód: {codigo}\n👤 Cliente: {cliente['nome']}\n📦 Item: {produto['nome']}\n💰 Valor: R$ {val_total:.2f}"
                enviar_whatsapp(grupo_destino, msg_grupo)
            
            if avisar_cliente and cliente['telefone']:
                template = config.get('msg_criacao', '')
                if template:
                    msg_final = template.replace("{nome}", str(cliente['nome']).split()[0]).replace("{pedido}", codigo).replace("{produto}", str(produto['nome']))
                    sucesso, erro = enviar_whatsapp(cliente['telefone'], msg_final)
                    if not sucesso: st.toast(f"⚠️ Aviso ao cliente falhou: {erro}")
                else:
                    st.toast("ℹ️ Mensagem de 'Novo Pedido' não configurada.")
            
            return True, codigo
        except Exception as e:
            return False, str(e)
    return False, "Erro conexão"

def editar_dados_pedido(id_pedido, nova_qtd, novo_valor_unit, novo_cliente, novo_produto):
    novo_total = nova_qtd * novo_valor_unit
    id_cli = int(novo_cliente['id'])
    id_prod = int(novo_produto['id'])
    
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
            """, (id_cli, novo_cliente['nome'], novo_cliente['cpf'], novo_cliente['telefone'],
                  id_prod, novo_produto['nome'], novo_produto['tipo'],
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
            cur.execute("""
                UPDATE pedidos 
                SET status = %s, observacao = %s, data_atualizacao = NOW() 
                WHERE id = %s
            """, (novo_status, obs_status_texto, int(id_pedido)))
            
            cur.execute("""
                INSERT INTO pedidos_historico (id_pedido, status_novo, observacao, data_mudanca)
                VALUES (%s, %s, %s, NOW())
            """, (int(id_pedido), novo_status, obs_status_texto))
            
            conn.commit()
            conn.close()
            
            if avisar_cliente:
                config = buscar_configuracao()
                
                # AJUSTE NOME DO CAMPO NO BANCO (Mapeamento Registro -> msg_registrar)
                campo_msg = f"msg_{novo_status.lower()}" 
                if novo_status == "Registro": # Ajuste para o status Registro usar o campo existente
                    campo_msg = "msg_registrar"
                
                template = config.get(campo_msg, '')
                
                if template:
                    if dados_pedido['telefone_cliente']:
                        # AJUSTE TARG PRODUTO: Adicionado .replace("{produto}", ...)
                        msg_final = template.replace("{nome}", str(dados_pedido['nome_cliente']).split()[0]) \
                                            .replace("{pedido}", str(dados_pedido['codigo'])) \
                                            .replace("{status}", novo_status) \
                                            .replace("{obs_status}", obs_status_texto) \
                                            .replace("{produto}", str(dados_pedido['nome_produto'])) # <-- CORREÇÃO AQUI
                        
                        sucesso, erro = enviar_whatsapp(dados_pedido['telefone_cliente'], msg_final)
                        if sucesso:
                            st.toast(f"✅ Mensagem de '{novo_status}' enviada!")
                        else:
                            st.toast(f"❌ Falha no envio: {erro}")
                else:
                    st.toast(f"⚠️ Atenção: O texto para o status '{novo_status}' está vazio nas configurações.")
                    
            return True
        except Exception as e:
            st.error(f"Erro ao registrar histórico: {e}")
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
        query = """
            SELECT data_mudanca, status_novo, observacao 
            FROM pedidos_historico 
            WHERE id_pedido = %s 
            ORDER BY data_mudanca DESC
        """
        df = pd.read_sql(query, conn, params=(int(id_pedido),))
        conn.close()
        return df
    return pd.DataFrame()

# --- POP-UPS VISUALIZAR ---
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
        
        # AJUSTE: Aviso sobre o código do grupo
        grupo = st.text_input("ID do Grupo (Digite apenas o código numérico)", value=conf.get('grupo_aviso_id', ''))
        st.caption("O sistema adicionará @g.us automaticamente se necessário.")
        
        st.subheader("Modelos de Mensagem (Para o Cliente)")
        st.info("Tags disponíveis: {nome}, {pedido}, {produto} e {obs_status}")
        
        msg_new = st.text_area("Novo Pedido Criado", value=conf.get('msg_criacao', ''))
        msg_pago = st.text_area("Status: Pago", value=conf.get('msg_pago', ''))
        # AJUSTE NOME: "Registro"
        msg_reg = st.text_area("Status: Registro", value=conf.get('msg_registrar', ''))
        msg_pend = st.text_area("Status: Pendente", value=conf.get('msg_pendente', ''))
        msg_canc = st.text_area("Status: Cancelado", value=conf.get('msg_cancelado', ''))
        
        if st.form_submit_button("Salvar Configurações"):
            tpls = {'msg_criacao': msg_new, 'msg_pago': msg_pago, 'msg_registrar': msg_reg, 'msg_pendente': msg_pend, 'msg_cancelado': msg_canc}
            salvar_configuracao(grupo, tpls)
            st.success("Configuração Salva!")
            st.rerun()

@st.dialog("✏️ Editar Informações do Pedido")
def dialog_editar_dados(pedido):
    st.write(f"Editando: **{pedido['codigo']}**")
    
    df_clientes = buscar_clientes()
    df_produtos = buscar_produtos()

    if df_clientes.empty or df_produtos.empty:
        st.error("Erro ao carregar listas.")
        return

    idx_cli_atual = 0
    idx_prod_atual = 0
    try: idx_cli_atual = df_clientes[df_clientes['id'] == pedido['id_cliente']].index[0]
    except: pass
    try: idx_prod_atual = df_produtos[df_produtos['id'] == pedido['id_produto']].index[0]
    except: pass

    with st.form("form_editar_dados"):
        st.markdown("##### Dados Principais")
        opcoes_cli = df_clientes.apply(lambda x: f"{x['nome']} | CPF: {x['cpf']}", axis=1)
        idx_cli = st.selectbox("Alterar Cliente", range(len(df_clientes)), index=int(idx_cli_atual), format_func=lambda x: opcoes_cli[x])
        cli_selecionado = df_clientes.iloc[idx_cli]

        opcoes_prod = df_produtos.apply(lambda x: f"{x['nome']} (R$ {x['preco']})", axis=1)
        idx_prod = st.selectbox("Alterar Produto", range(len(df_produtos)), index=int(idx_prod_atual), format_func=lambda x: opcoes_prod[x])
        prod_selecionado = df_produtos.iloc[idx_prod]

        st.markdown("---")
        st.markdown("##### Valores")
        col1, col2 = st.columns(2)
        with col1:
            nova_qtd = st.number_input("Quantidade", min_value=1, value=int(pedido['quantidade']))
        with col2:
            novo_preco = st.number_input("Valor Unitário (R$)", min_value=0.0, value=float(pedido['valor_unitario']), format="%.2f")
            
        st.caption(f"Novo Total será: R$ {(nova_qtd * novo_preco):.2f}")
        
        if st.form_submit_button("💾 Salvar Dados"):
            if editar_dados_pedido(pedido['id'], nova_qtd, novo_preco, cli_selecionado, prod_selecionado):
                st.success("Dados atualizados com sucesso!")
                st.rerun()

@st.dialog("🔄 Status do Pedido")
def dialog_status_pedido(pedido):
    st.write(f"Pedido: **{pedido['codigo']}**")
    # AJUSTE: Nome do status alterado de Registrar para Registro
    status_opcoes = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
    idx = 0
    if pedido['status'] in status_opcoes: idx = status_opcoes.index(pedido['status'])
    
    with st.form("form_status_update"):
        novo_status = st.selectbox("Novo Status", status_opcoes, index=idx)
        
        obs_status = st.text_area("Observação / Informação Adicional", 
                                  placeholder="Digite aqui (Ex: Link do comprovante...). Isso ficará salvo no histórico.")
        
        avisar = st.checkbox("📱 Enviar mensagem ao cliente?", value=True)
        
        if st.form_submit_button("Atualizar Status"):
            if atualizar_status_pedido(pedido['id'], novo_status, pedido, avisar, obs_status):
                st.success("Histórico atualizado com sucesso!")
                st.rerun()

@st.dialog("📜 Registros do Pedido")
def dialog_historico(id_pedido, codigo_pedido):
    st.write(f"Histórico completo de: **{codigo_pedido}**")
    
    df_hist = buscar_historico_pedido(id_pedido)
    
    if not df_hist.empty:
        df_hist['data_mudanca'] = pd.to_datetime(df_hist['data_mudanca']).dt.strftime('%d/%m/%Y %H:%M')
        df_hist.columns = ["Data/Hora", "Status", "Observação"]
        st.dataframe(df_hist, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum registro histórico encontrado.")

# --- APP PRINCIPAL ---
def app_pedidos():
    st.markdown("## PEDIDOS") 
    
    tab1, tab2, tab3 = st.tabs(["📝 Novo Pedido", "🔎 Gerenciar Pedidos", "⚙️ Configurações"])
    
    with tab1:
        df_clientes = buscar_clientes()
        df_produtos = buscar_produtos()
        
        if df_clientes.empty or df_produtos.empty:
            st.warning("Cadastre Clientes e Produtos antes de criar um pedido.")
        else:
            with st.form("form_novo_pedido", clear_on_submit=True):
                c1, c2 = st.columns(2)
                with c1:
                    opcoes_cli = df_clientes.apply(lambda x: f"{x['nome']} | CPF: {x['cpf']}", axis=1)
                    idx_cli = st.selectbox("Selecione o Cliente", range(len(df_clientes)), format_func=lambda x: opcoes_cli[x])
                    cliente_selecionado = df_clientes.iloc[idx_cli]
                with c2:
                    opcoes_prod = df_produtos.apply(lambda x: f"{x['nome']} (R$ {x['preco']})", axis=1)
                    idx_prod = st.selectbox("Selecione o Produto", range(len(df_produtos)), format_func=lambda x: opcoes_prod[x])
                    produto_selecionado = df_produtos.iloc[idx_prod]
                
                c3, c4, c5 = st.columns(3)
                with c3:
                    qtd = st.number_input("Quantidade", min_value=1, value=1)
                with c4:
                    valor_unit = float(produto_selecionado['preco'] or 0.0)
                    valor_manual = st.number_input("Valor Unitário (R$)", value=valor_unit, format="%.2f")
                with c5:
                    total_calc = valor_manual * qtd
                    st.metric("Total do Pedido", f"R$ {total_calc:.2f}")

                st.markdown("---")
                st.write("**Notificações W-API:**")
                ck_cli = st.checkbox("Enviar confirmação para o Cliente?", value=True)
                ck_grp = st.checkbox("Avisar Grupo Interno?", value=True)
                
                if st.form_submit_button("✅ Finalizar Pedido", type="primary"):
                    ok, msg = criar_pedido(cliente_selecionado, produto_selecionado, qtd, total_calc, ck_cli, ck_grp)
                    if ok:
                        st.success(f"Pedido Criado! Código: {msg}")
                    else:
                        st.error(f"Erro: {msg}")

    with tab2:
        conn = get_conn()
        if conn:
            query = "SELECT * FROM pedidos ORDER BY data_criacao DESC"
            df_pedidos = pd.read_sql(query, conn)
            conn.close()
            
            if not df_pedidos.empty:
                filtro = st.text_input("🔎 Pesquisar (Nome, Código, CPF)", placeholder="Digite para buscar...")
                if filtro:
                    df_pedidos = df_pedidos[
                        df_pedidos['nome_cliente'].str.contains(filtro, case=False) |
                        df_pedidos['codigo'].str.contains(filtro, case=False) |
                        df_pedidos['cpf_cliente'].str.contains(filtro, case=False)
                    ]
                
                st.dataframe(df_pedidos[['codigo', 'data_criacao', 'nome_cliente', 'nome_produto', 'valor_total', 'status']], use_container_width=True, hide_index=True)
                
                st.markdown("### Ações")
                for i, row in df_pedidos.iterrows():
                    with st.expander(f"📦 {row['codigo']} - {row['nome_cliente']} ({row['status']})"):
                        c_info, c_acoes = st.columns([2, 1])
                        with c_info:
                             st.write(f"**Produto:** {row['nome_produto']} ({row['categoria_produto']})")
                             st.write(f"**Valor:** R$ {row['valor_total']:.2f}")
                             st.caption(f"Criado em: {row['data_criacao']}")
                        with c_acoes:
                            if st.button("👤 Ver Cliente", key=f"vc_{row['id']}"):
                                ver_cliente(row['nome_cliente'], row['cpf_cliente'], row['telefone_cliente'])
                            if st.button("📦 Ver Produto", key=f"vp_{row['id']}"):
                                ver_produto(row['nome_produto'], row['categoria_produto'])
                            
                            c_ed_dados, c_ed_status, c_del = st.columns(3)
                            with c_ed_dados:
                                if st.button("✏️ Editar Dados", key=f"ed_dad_{row['id']}", help="Alterar Cliente/Produto/Qtd/Valor"):
                                    dialog_editar_dados(row)

                            with c_ed_status:
                                if st.button("🔄 Status", key=f"st_{row['id']}"):
                                    dialog_status_pedido(row)
                            
                            with c_del:
                                if st.button("🗑️", key=f"del_{row['id']}", help="Excluir"):
                                    excluir_pedido(row['id'])
                                    st.rerun()
                        
                        st.markdown("---")
                        if st.button(f"📜 Registros do Pedido", key=f"hist_{row['id']}", use_container_width=True):
                            dialog_historico(row['id'], row['codigo'])
            else:
                st.info("Nenhum pedido encontrado.")

    with tab3:
        st.write("Configurações de avisos do módulo de pedidos.")
        if st.button("⚙️ Abrir Configurações de Mensagem"):
            dialog_configuracao()

if __name__ == "__main__":
    app_pedidos()