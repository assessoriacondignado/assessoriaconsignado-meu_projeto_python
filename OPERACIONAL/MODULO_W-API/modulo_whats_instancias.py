import streamlit as st
import pandas as pd
import psycopg2
import requests
import time
import conexao

# --- CONFIGURAÇÕES ---
BASE_URL = "https://api.w-api.app/v1"

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

# --- FUNÇÕES DE API ---
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

# --- DIALOGS ---
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

# --- INTERFACE ---
def app_instancias():
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