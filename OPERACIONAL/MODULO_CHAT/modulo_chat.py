import streamlit as st
import pandas as pd
import psycopg2
import time
from datetime import datetime
import modulo_wapi  # Reutiliza suas funções de envio
import conexao

# --- CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

# --- FUNÇÕES DE BUSCA ---
def listar_contatos_recentes():
    """Busca números com interação recente na tabela wapi_numeros"""
    conn = get_conn()
    if conn:
        try:
            # Ordena pelos que tiveram interação mais recente
            query = """
                SELECT id, telefone, nome_cliente, data_ultima_interacao 
                FROM wapi_numeros 
                ORDER BY data_ultima_interacao DESC 
                LIMIT 50
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except: 
            conn.close()
    return pd.DataFrame()

def buscar_mensagens(telefone):
    """Busca histórico de mensagens (Enviadas e Recebidas)"""
    conn = get_conn()
    if conn:
        try:
            # Busca logs vinculados a este telefone
            query = """
                SELECT data_hora, tipo, mensagem, nome_contato, status 
                FROM wapi_logs 
                WHERE telefone = %s 
                ORDER BY data_hora ASC
            """
            df = pd.read_sql(query, conn, params=(str(telefone),))
            conn.close()
            return df
        except: 
            conn.close()
    return pd.DataFrame()

# --- INTERFACE DO CHAT ---
def app_chat_screen():
    # CSS para ajustar altura e visual
    st.markdown("""
        <style>
        .stChatMessage { padding: 1rem; border-radius: 10px; margin-bottom: 10px; }
        .stChatMessage[data-testid="stChatMessageUser"] { background-color: #dcf8c6; }
        div[data-testid="column"] { overflow: auto; height: 80vh; }
        </style>
    """, unsafe_allow_html=True)

    st.subheader("💬 Atendimento WhatsApp")

    # Layout: Coluna Esquerda (Contatos) | Coluna Direita (Chat)
    col_lista, col_chat = st.columns([1, 3])

    # --- COLUNA DA ESQUERDA: LISTA DE CONTATOS ---
    with col_lista:
        st.markdown("### 📥 Conversas")
        if st.button("🔄 Atualizar Lista"):
            st.rerun()
            
        df_contatos = listar_contatos_recentes()
        
        if not df_contatos.empty:
            # Seletor de contato (usando radio para simular lista clicável)
            # Criamos um label amigável: "Nome (Telefone)"
            opcoes = df_contatos.apply(lambda x: f"{x['nome_cliente'] or 'Desconhecido'} | {x['telefone']}", axis=1)
            escolha = st.radio("Selecione:", options=opcoes, label_visibility="collapsed")
            
            # Extrai o telefone da seleção
            telefone_selecionado = escolha.split(" | ")[1]
            
            # Pega dados completos do selecionado
            dados_contato = df_contatos[df_contatos['telefone'] == telefone_selecionado].iloc[0]
            st.session_state['chat_telefone_atual'] = telefone_selecionado
            st.session_state['chat_nome_atual'] = dados_contato['nome_cliente']
        else:
            st.info("Nenhuma conversa iniciada.")
            st.session_state['chat_telefone_atual'] = None

    # --- COLUNA DA DIREITA: JANELA DE MENSAGENS ---
    with col_chat:
        telefone = st.session_state.get('chat_telefone_atual')
        
        if telefone:
            # Cabeçalho da conversa
            nome_cli = st.session_state.get('chat_nome_atual') or "Cliente"
            st.markdown(f"#### 👤 {nome_cli} ({telefone})")
            st.divider()

            # Área de Mensagens (Container com scroll)
            chat_container = st.container(height=400)
            
            # Carrega mensagens
            df_msgs = buscar_mensagens(telefone)
            
            with chat_container:
                if not df_msgs.empty:
                    for _, row in df_msgs.iterrows():
                        # Define quem enviou (User = Nós/Atendente, Assistant = Cliente)
                        # Ajuste conforme sua lógica: 'ENVIADA' somos nós, 'RECEBIDA' é o cliente
                        role = "user" if row['tipo'] == 'ENVIADA' else "assistant"
                        avatar = "👤" if role == "assistant" else "🎧"
                        
                        with st.chat_message(role, avatar=avatar):
                            st.write(row['mensagem'])
                            st.caption(f"{row['data_hora'].strftime('%d/%m %H:%M')} - {row['nome_contato'] or ''}")
                else:
                    st.caption("Nenhuma mensagem trocada ainda.")

            # Área de Envio
            with st.container():
                c_input, c_btn = st.columns([4, 1])
                texto_msg = c_input.chat_input("Digite sua mensagem...")
                
                # Opção de identificar o usuário
                usuario_logado = st.session_state.get('usuario_nome', 'Atendente')
                enviar_como = st.checkbox(f"Assinar como {usuario_logado}?", value=True)
                
                if texto_msg:
                    # Prepara a mensagem (adiciona assinatura se marcado)
                    msg_final = texto_msg
                    if enviar_como:
                        msg_final += f"\n\n~ {usuario_logado}"

                    # Busca instância ativa para envio
                    instancia = modulo_wapi.buscar_instancia_ativa()
                    if instancia:
                        with st.spinner("Enviando..."):
                            res = modulo_wapi.enviar_msg_api(instancia[0], instancia[1], telefone, msg_final)
                            
                            if res.get('success') or res.get('messageId'):
                                # O Webhook deve salvar o log, mas para garantir feedback visual rápido:
                                st.success("Enviada!")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error(f"Erro no envio: {res}")
                    else:
                        st.error("Nenhuma instância do WhatsApp conectada.")

        else:
            st.info("👈 Selecione uma conversa ao lado para iniciar o atendimento.")
            st.markdown("---")
            
            # Opção de iniciar nova conversa manual se não existir na lista
            st.write("Ou inicie uma nova conversa:")
            novo_num = st.text_input("Novo Número (apenas dígitos, ex: 5511999999999)")
            if st.button("Abrir Conversa"):
                if novo_num:
                    st.session_state['chat_telefone_atual'] = novo_num
                    st.session_state['chat_nome_atual'] = "Novo Contato"
                    st.rerun()