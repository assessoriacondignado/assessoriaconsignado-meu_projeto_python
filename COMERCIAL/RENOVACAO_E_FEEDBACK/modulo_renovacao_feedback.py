import streamlit as st
import pandas as pd
import psycopg2
import os
import sys
import re
import time
from datetime import datetime, date
import modulo_wapi # Integração

# Ajuste de path para importar módulos da raiz e de COMERCIAL
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
diretorio_comercial = os.path.dirname(diretorio_atual)
raiz_projeto = os.path.dirname(diretorio_comercial)

if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)

try: 
    import conexao
except ImportError: 
    st.error("Erro crítico: conexao.py não encontrado.")

try:
    from COMERCIAL import modulo_comercial_configuracoes
except ImportError:
    modulo_comercial_configuracoes = None

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password
        )
    except: return None

# =============================================================================
# 1. FUNÇÕES DE BANCO
# =============================================================================

def buscar_pedidos_disponiveis():
    conn = get_conn()
    if conn:
        query = "SELECT id, codigo, nome_cliente, nome_produto, telefone_cliente FROM pedidos ORDER BY data_criacao DESC"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def listar_rf():
    conn = get_conn()
    if conn:
        query = """
            SELECT rf.id, rf.id_pedido, rf.data_criacao, rf.data_previsao, rf.status, rf.observacao,
                   p.codigo as codigo_pedido, p.nome_cliente, p.nome_produto, p.categoria_produto,
                   c.cpf as cpf_cliente, c.telefone as telefone_cliente, c.email as email_cliente
            FROM renovacao_feedback rf
            JOIN pedidos p ON rf.id_pedido = p.id
            LEFT JOIN clientes_usuarios c ON p.id_cliente = c.id
            ORDER BY rf.data_criacao DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def buscar_historico_rf(id_rf):
    conn = get_conn()
    if conn:
        query = "SELECT data_mudanca, status_novo, observacao FROM renovacao_feedback_historico WHERE id_rf = %s ORDER BY data_mudanca DESC"
        df = pd.read_sql(query, conn, params=(int(id_rf),))
        conn.close()
        return df
    return pd.DataFrame()

def criar_registro_rf(id_pedido, data_prev, obs, dados_pedido, avisar):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO renovacao_feedback (id_pedido, data_previsao, observacao, status) VALUES (%s, %s, %s, 'Entrada') RETURNING id", 
                        (id_pedido, data_prev, obs))
            id_novo = cur.fetchone()[0]
            cur.execute("INSERT INTO renovacao_feedback_historico (id_rf, status_novo, observacao) VALUES (%s, 'Entrada', 'Registro criado')", (id_novo,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro: {e}")
    return False

def atualizar_status_rf(id_rf, novo_status, obs, dados_rf, avisar):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE renovacao_feedback SET status=%s, data_atualizacao=NOW() WHERE id=%s", (novo_status, id_rf))
            cur.execute("INSERT INTO renovacao_feedback_historico (id_rf, status_novo, observacao) VALUES (%s, %s, %s)", (id_rf, novo_status, obs))
            
            if avisar and dados_rf.get('telefone_cliente') and modulo_comercial_configuracoes:
                cur.execute("SELECT mensagem_padrao FROM admin.status WHERE modulo='RENOVACAO' AND status_relacionado=%s", (novo_status,))
                res_msg = cur.fetchone()
                
                if res_msg and res_msg[0]:
                    template = res_msg[0]
                    msg_final = template.replace("{nome}", str(dados_rf['nome_cliente']).split()[0]) \
                                        .replace("{nome_completo}", str(dados_rf['nome_cliente'])) \
                                        .replace("{pedido}", str(dados_rf['codigo_pedido'])) \
                                        .replace("{status}", novo_status) \
                                        .replace("{produto}", str(dados_rf['nome_produto'])) \
                                        .replace("{obs_status}", obs)
                    
                    instancia = modulo_wapi.buscar_instancia_ativa()
                    if instancia:
                        modulo_wapi.enviar_msg_api(instancia[0], instancia[1], dados_rf['telefone_cliente'], msg_final)

            conn.commit()
            conn.close()
            return True
        except Exception as e: st.error(f"Erro: {e}")
    return False

def editar_rf_dados(id_rf, nova_data, nova_obs):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE renovacao_feedback SET data_previsao=%s, observacao=%s WHERE id=%s", (nova_data, nova_obs, id_rf))
            conn.commit()
            conn.close()
            return True
        except: return False
    return False

def excluir_rf_db(id_rf):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM renovacao_feedback WHERE id = %s", (id_rf,))
            conn.commit()
            conn.close()
            return True
        except: return False
    return False

# =============================================================================
# 2. PAINÉIS DE RENDERIZAÇÃO (MASTER-DETAIL)
# =============================================================================

def renderizar_novo_rf_tab():
    st.markdown("### ➕ Novo Registro de Renovação/Feedback")
    df_ped = buscar_pedidos_disponiveis()
    
    if df_ped.empty:
        st.warning("Nenhum pedido encontrado para gerar renovação.")
        return

    opcoes = df_ped.apply(lambda x: f"{x['codigo']} | {x['nome_cliente']} - {x['nome_produto']}", axis=1)
    
    with st.container(border=True):
        idx_ped = st.selectbox("Selecione o Pedido", range(len(df_ped)), format_func=lambda x: opcoes[x], index=None)
        
        if idx_ped is not None:
            sel = df_ped.iloc[idx_ped]
            st.info(f"Gerando registro para: **{sel['nome_cliente']}**")
            
            with st.form("form_novo_rf_tab"):
                d_prev = st.date_input("Data Previsão", value=date.today())
                obs = st.text_area("Observação inicial", placeholder="Motivo da renovação ou feedback...")
                
                if st.form_submit_button("Criar Registro", type="primary"):
                    id_p = int(sel['id'])
                    # O parâmetro dados_pedido/avisar está como None/False pois a função criar original
                    # não implementava o envio de msg na criação explicitamente no código anterior
                    # (apenas no fluxo pós-venda). Mantendo padrão.
                    if criar_registro_rf(id_p, d_prev, obs, None, False):
                        st.success("Criado com sucesso!")
                        time.sleep(1)
                        st.rerun()

def renderizar_detalhes_rf(rf):
    st.markdown(f"#### 👁️ Detalhes: {rf['codigo_pedido']}")
    st.write(f"**Cliente:** {rf['nome_cliente']}")
    st.write(f"**Produto:** {rf['nome_produto']}")
    st.write(f"**Categoria:** {rf['categoria_produto']}")
    st.markdown("---")
    st.write(f"**Status:** {rf['status']}")
    st.write(f"**Previsão:** {pd.to_datetime(rf['data_previsao']).strftime('%d/%m/%Y')}")
    st.info(f"**Observação:**\n{rf['observacao']}")

def renderizar_dados_cliente_rf(rf):
    st.markdown(f"#### 👤 Dados do Cliente")
    st.write(f"**Nome:** {rf['nome_cliente']}")
    st.write(f"**CPF:** {rf['cpf_cliente']}")
    st.write(f"**Telefone:** {rf['telefone_cliente']}")
    st.write(f"**E-mail:** {rf['email_cliente']}")

def renderizar_editar_rf(rf):
    st.markdown(f"#### ✏️ Editar Registro")
    with st.form("form_gaveta_edit_rf"):
        n_data = st.date_input("Data Previsão", value=pd.to_datetime(rf['data_previsao']))
        n_obs = st.text_area("Observação", value=rf['observacao'])
        
        if st.form_submit_button("💾 Salvar", type="primary"):
            if editar_rf_dados(rf['id'], n_data, n_obs):
                st.success("Atualizado!")
                time.sleep(1)
                st.session_state.rf_selecionado = None
                st.rerun()
            else:
                st.error("Erro ao salvar.")

def renderizar_status_rf(rf):
    st.markdown(f"#### 🔄 Atualizar Status")
    
    # Histórico
    df_h = buscar_historico_rf(rf['id'])
    if not df_h.empty:
        st.caption("Histórico:")
        df_h.columns = ["Data", "Status", "Obs"]
        st.dataframe(df_h, use_container_width=True, hide_index=True, height=150)
    
    st.markdown("---")
    
    # Form
    status_opcoes = ["Entrada", "Em Análise", "Concluído", "Pendente", "Cancelado"]
    try: idx = status_opcoes.index(rf['status'])
    except: idx = 0
    
    with st.form("form_gaveta_st_rf"):
        novo = st.selectbox("Novo Status", status_opcoes, index=idx)
        obs = st.text_area("Observação da mudança")
        enviar_whats = st.checkbox("📱 Enviar mensagem automática?", value=True)
        
        if st.form_submit_button("Confirmar Atualização", type="primary"):
            if atualizar_status_rf(rf['id'], novo, obs, rf, enviar_whats):
                st.success("Status atualizado!")
                time.sleep(1)
                st.session_state.rf_selecionado = None
                st.rerun()

def renderizar_excluir_rf(rf):
    st.markdown(f"#### 🗑️ Excluir Registro")
    st.warning("Tem certeza que deseja apagar este registro de renovação?")
    
    if st.button("Sim, confirmar exclusão", type="primary"):
        if excluir_rf_db(rf['id']):
            st.success("Apagado!")
            time.sleep(1)
            st.session_state.rf_selecionado = None
            st.rerun()

# =============================================================================
# 3. APP PRINCIPAL
# =============================================================================

def app_renovacao_feedback():
    # --- CORREÇÃO DE ESTILO: APLICAR APENAS AO BLOCO PRINCIPAL ---
    # Usando o seletor 'section[data-testid="stMainBlock"]' para não vazar para a sidebar
    st.markdown("""
        <style>
        section[data-testid="stMainBlock"] div.stButton > button {
            background-color: #FF4B4B !important;
            color: white !important;
            border-color: #FF4B4B !important;
        }
        section[data-testid="stMainBlock"] div.stButton > button:hover {
            background-color: #FF0000 !important;
            border-color: #FF0000 !important;
            color: white !important;
        }
        </style>
    """, unsafe_allow_html=True)

    tab_novo, tab_gestao = st.tabs(["➕ Novo Registro", "📋 Gestão de Renovações"])

    # --- ABA 1: NOVO ---
    with tab_novo:
        renderizar_novo_rf_tab()

    # --- ABA 2: GESTÃO ---
    with tab_gestao:
        if 'rf_selecionado' not in st.session_state: st.session_state.rf_selecionado = None
        if 'rf_aba_ativa' not in st.session_state: st.session_state.rf_aba_ativa = None

        col_lista, col_detalhe = st.columns([0.3, 0.7])

        # --- COLUNA ESQUERDA ---
        with col_lista:
            st.markdown("##### 🔍 Filtros")
            busca = st.text_input("Buscar", placeholder="Cli/Prod/Obs", label_visibility="collapsed")
            
            df = listar_rf()
            
            if not df.empty:
                f_stt = st.multiselect("Status", options=df['status'].unique(), placeholder="Status")
                
                # Aplicação Filtros
                if busca:
                    mask = (
                        df['nome_cliente'].str.contains(busca, case=False, na=False) |
                        df['nome_produto'].str.contains(busca, case=False, na=False) |
                        df['observacao'].str.contains(busca, case=False, na=False)
                    )
                    df = df[mask]
                
                if f_stt:
                    df = df[df['status'].isin(f_stt)]
                
                st.markdown(f"**Total:** {len(df)}")
                st.markdown("---")

                # Lista
                for i, row in df.iterrows():
                    stt = row['status']
                    cor = "🔴"
                    if stt == 'Concluído': cor = "🟢"
                    elif stt == 'Em Análise': cor = "🟠"
                    elif stt == 'Entrada': cor = "🔵"
                    
                    with st.container(border=True):
                        st.write(f"**{row['nome_cliente']}**")
                        st.caption(f"{cor} {stt} | {pd.to_datetime(row['data_previsao']).strftime('%d/%m')}")
                        st.caption(f"{row['nome_produto']}")
                        
                        if st.button("Ver >", key=f"sel_rf_{row['id']}", use_container_width=True):
                            st.session_state.rf_selecionado = row.to_dict()
                            st.session_state.rf_aba_ativa = "detalhes"
                            st.rerun()
            else:
                st.info("Nenhum registro.")

        # --- COLUNA DIREITA ---
        with col_detalhe:
            rf = st.session_state.rf_selecionado
            
            if rf:
                with st.container(border=True):
                    st.title(f"{rf['nome_cliente']}")
                    st.caption(f"Pedido: {rf['codigo_pedido']} | Produto: {rf['nome_produto']}")
                    st.divider()

                    # Menu
                    opcoes = [
                        ("👁️ Detalhes", "detalhes"),
                        ("👤 Cliente", "cliente"),
                        ("✏️ Editar", "editar"),
                        ("🔄 Status", "status"),
                        ("🗑️ Excluir", "excluir")
                    ]
                    
                    cols = st.columns(len(opcoes), gap="small")
                    for col, (lbl, key) in zip(cols, opcoes):
                        tipo = "primary" if st.session_state.rf_aba_ativa == key else "secondary"
                        if col.button(lbl, key=f"btn_rf_top_{key}", type=tipo, use_container_width=True):
                            st.session_state.rf_aba_ativa = key
                            st.rerun()

                # Conteúdo
                aba = st.session_state.rf_aba_ativa
                with st.container(border=True):
                    if aba == 'detalhes': renderizar_detalhes_rf(rf)
                    elif aba == 'cliente': renderizar_dados_cliente_rf(rf)
                    elif aba == 'editar': renderizar_editar_rf(rf)
                    elif aba == 'status': renderizar_status_rf(rf)
                    elif aba == 'excluir': renderizar_excluir_rf(rf)

            else:
                st.container(border=True).markdown(
                    """
                    <div style='text-align: center; padding: 50px;'>
                        <h3>⬅️ Selecione um registro</h3>
                        <p>Detalhes e opções aparecerão aqui.</p>
                    </div>
                    """, 
                    unsafe_allow_html=True
                )

if __name__ == "__main__":
    app_renovacao_feedback()