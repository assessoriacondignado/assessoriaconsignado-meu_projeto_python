import streamlit as st
import pandas as pd
import psycopg2
import requests
import re
import time
from datetime import datetime

# --- IMPORTAÇÃO ROBUSTA DA CONEXÃO ---
try: 
    import conexao
except ImportError:
    st.error("Erro crítico: Arquivo conexao.py não localizado no servidor.")

def get_conn():
    return psycopg2.connect(
        host=conexao.host, 
        port=conexao.port, 
        database=conexao.database, 
        user=conexao.user, 
        password=conexao.password
    )

# ==========================================================
# 1. FUNÇÕES DE API (W-API)
# ==========================================================
BASE_URL = "https://api.w-api.app/v1"

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

def obter_qrcode_api(instance_id, token):
    url = f"{BASE_URL}/instance/qr-code"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"instanceId": instance_id, "image": "enable"}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        return res.content if res.status_code == 200 else None
    except: return None

def obter_otp_api(instance_id, token, phone):
    url = f"{BASE_URL}/instance/connect-phone"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"instanceId": instance_id, "phone": phone}
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        return res.json()
    except: return None

def checar_status_api(instance_id, token):
    url = f"{BASE_URL}/instance/status-instance"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"instanceId": instance_id}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        return res.json() if res.status_code == 200 else {"state": "erro"}
    except: return {"state": "erro"}

# ==========================================================
# 2. POP-UPS (DIÁLOGOS)
# ==========================================================
@st.dialog("📷 Conectar QR Code")
def dialog_qrcode(inst_id, token):
    img = obter_qrcode_api(inst_id, token)
    if img: 
        st.image(img, width=300)
        st.info("Escaneie para conectar a instância.")
    else: st.error("Erro ao carregar QR Code da API.")

@st.dialog("🔢 Conectar via Código (OTP)")
def dialog_otp(inst_id, token):
    phone = st.text_input("Número com DDI (Ex: 5511999999999)")
    if st.button("Gerar Código"):
        res = obter_otp_api(inst_id, token, phone)
        if res and res.get('code'):
            st.code(res['code'], language="text")
            st.success("Insira este código no seu aparelho WhatsApp.")
        else: st.error("Erro ao gerar código OTP.")

@st.dialog("📝 Editar Instância")
def dialog_editar(id_db, nome, inst_id, token):
    new_nome = st.text_input("Nome Identificador", value=nome)
    new_id = st.text_input("Instance ID", value=inst_id)
    new_token = st.text_input("Token de Acesso", value=token)
    if st.button("Salvar Alterações"):
        try:
            conn = get_conn(); cur = conn.cursor()
            cur.execute("UPDATE wapi_instancias SET nome=%s, api_instance_id=%s, api_token=%s WHERE id=%s", (new_nome, new_id, new_token, id_db))
            conn.commit(); conn.close()
            st.success("Configurações atualizadas!")
            time.sleep(1); st.rerun()
        except Exception as e: st.error(f"Erro ao salvar: {e}")

# ==========================================================
# 3. INTERFACE PRINCIPAL
# ==========================================================
def app_wapi():
    st.markdown("## 📱 Módulo W-API")
    tab1, tab2, tab3, tab4 = st.tabs(["📤 Disparador", "🤖 Instâncias", "📝 Modelos", "📋 Registros"])

    with tab1:
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

    with tab2:
        st.markdown("### 🤖 Gerenciar Instâncias")
        try:
            conn = get_conn()
            df_list = pd.read_sql("SELECT id, nome, api_instance_id, api_token FROM wapi_instancias", conn)
            conn.close()

            if not df_list.empty:
                for _, inst in df_list.iterrows():
                    with st.expander(f"Instância: **{inst['nome']}**"):
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            if st.button("📷 QR Code", key=f"qr_{inst['id']}"): dialog_qrcode(inst['api_instance_id'], inst['api_token'])
                            if st.button("📊 Status", key=f"st_{inst['id']}"):
                                res_st = checar_status_api(inst['api_instance_id'], inst['api_token'])
                                st.write(f"Estado: **{res_st.get('state')}**")
                        with c2:
                            if st.button("🔢 Código OTP", key=f"otp_{inst['id']}"): dialog_otp(inst['api_instance_id'], inst['api_token'])
                            if st.button("📝 Editar", key=f"ed_{inst['id']}"): dialog_editar(inst['id'], inst['nome'], inst['api_instance_id'], inst['api_token'])
                        with c3:
                            if st.button("❌ Excluir", key=f"del_{inst['id']}"):
                                conn = get_conn(); cur = conn.cursor()
                                cur.execute("DELETE FROM wapi_instancias WHERE id=%s", (inst['id'],))
                                conn.commit(); conn.close()
                                st.warning("Removida."); time.sleep(1); st.rerun()
            else: st.info("Nenhuma instância cadastrada.")
        except: pass

    with tab4:
        st.markdown("### 📋 Histórico de Mensagens (Webhook)")
        try:
            conn = get_conn()
            # Query otimizada para mostrar Instância e Contato com nomes resolvidos pelo Webhook
            query = """
                SELECT data_hora, instance_id as "Instância", nome_contato as "Contato", 
                       tipo as "Fluxo", telefone, mensagem, status 
                FROM wapi_logs 
                ORDER BY data_hora DESC 
                LIMIT 50
            """
            df_logs = pd.read_sql(query, conn)
            conn.close()
            if not df_logs.empty:
                df_logs['data_hora'] = pd.to_datetime(df_logs['data_hora']).dt.strftime('%d/%m/%Y %H:%M')
                st.dataframe(df_logs, use_container_width=True, hide_index=True)
            else: st.info("Histórico vazio.")
        except Exception as e: st.error(f"Erro ao carregar logs: {e}")

if __name__ == "__main__":
    app_wapi()