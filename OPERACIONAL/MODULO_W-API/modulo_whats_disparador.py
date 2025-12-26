import streamlit as st
import pandas as pd
import psycopg2
import requests
import re
import conexao

BASE_URL = "https://api.w-api.app/v1"

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

def enviar_msg_api(instance_id, token, to, message):
    url = f"{BASE_URL}/message/send-text?instanceId={instance_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    contato_limpo = to if "@g.us" in str(to) else re.sub(r'[^0-9]', '', str(to))
    payload = {"phone": contato_limpo, "message": message, "delayMessage": 3}
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        return res.json()
    except Exception as e: 
        return {"success": False, "error": str(e)}

def app_disparador():
    st.markdown("### 📤 Enviar Mensagem")
    try:
        conn = get_conn()
        df_inst = pd.read_sql("SELECT nome, api_instance_id, api_token FROM wapi_instancias", conn)
        df_cli = pd.read_sql("SELECT nome, telefone FROM clientes_usuarios WHERE ativo = TRUE", conn)
        conn.close()

        if not df_inst.empty:
            inst_sel = st.selectbox("Selecione a Instância", df_inst['nome'].tolist())
            row_inst = df_inst[df_inst['nome'] == inst_sel].iloc[0]
            
            tipo_dest = st.radio("Destino", ["Cliente", "Manual"], horizontal=True)
            if tipo_dest == "Cliente":
                cli_sel = st.selectbox("Selecionar Cliente", df_cli['nome'].tolist())
                destino = df_cli[df_cli['nome'] == cli_sel].iloc[0]['telefone']
                st.caption(f"Telefone: {destino}")
            else:
                destino = st.text_input("Número (DDI+DDD+Número)")

            msg = st.text_area("Conteúdo da Mensagem")
            if st.button("🚀 Enviar Agora"):
                if destino and msg:
                    res = enviar_msg_api(row_inst['api_instance_id'], row_inst['api_token'], destino, msg)
                    if res.get('messageId') or res.get('success'):
                        st.success("Solicitação enviada! O log será gerado automaticamente pelo Webhook.")
                    else:
                        st.error(f"Falha no envio: {res}")
                else: st.warning("Preencha o destino e a mensagem.")
        else: st.warning("Nenhuma instância configurada.")
    except Exception as e: st.error(f"Erro ao carregar dados: {e}")