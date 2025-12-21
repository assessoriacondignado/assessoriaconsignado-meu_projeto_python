import streamlit as st
import pandas as pd
import psycopg2
import requests
import re
import time
from datetime import datetime

# Importação da conexão
try: 
    import conexao
except ImportError:
    st.error("Erro: Arquivo conexao.py não encontrado.")

# --- CONEXÃO COM BANCO ---
def get_conn():
    return psycopg2.connect(host=conexao.host, port=conexao.port, database=conexao.database, user=conexao.user, password=conexao.password)

# ==========================================================
# 1. FUNÇÕES DE API E LOGS
# ==========================================================
BASE_URL = "https://api.w-api.app/v1"

def enviar_msg_api(instance_id, token, to, message):
    url = f"{BASE_URL}/message/send-text?instanceId={instance_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    contato_limpo = to if "@g.us" in to else re.sub(r'[^0-9]', '', to)
    payload = {"phone": contato_limpo, "message": message, "delayMessage": 3}
    try:
        res = requests.post(url, json=payload, headers=headers)
        return res.json()
    except Exception as e: 
        return {"success": False, "error": str(e)}

def salvar_log(instance_id, telefone, mensagem, tipo="ENVIADA", status="Sucesso", nome=""):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO wapi_logs (instance_id, telefone, mensagem, tipo, status, nome_contato) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (instance_id, telefone, mensagem, tipo, status, nome))
        conn.commit()
    except: pass
    finally: conn.close()

# --- FUNÇÕES DE INSTÂNCIA (API) ---
def obter_qrcode_api(instance_id, token):
    url = f"{BASE_URL}/instance/qr-code"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"instanceId": instance_id, "image": "enable"}
    try:
        res = requests.get(url, headers=headers, params=params)
        return res.content if res.status_code == 200 else None
    except: return None

def obter_otp_api(instance_id, token, phone):
    url = f"{BASE_URL}/instance/connect-phone"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"instanceId": instance_id, "phone": phone}
    try:
        res = requests.post(url, json=payload, headers=headers)
        return res.json()
    except: return None

def checar_status_api(instance_id, token):
    url = f"{BASE_URL}/instance/status-instance"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"instanceId": instance_id}
    try:
        res = requests.get(url, headers=headers, params=params)
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
        st.info("Escaneie para conectar.")
    else: st.error("Erro ao carregar QR Code.")

@st.dialog("🔢 Conectar via Código (OTP)")
def dialog_otp(inst_id, token):
    phone = st.text_input("Número com DDI (Ex: 5511999999999)")
    if st.button("Gerar Código"):
        res = obter_otp_api(inst_id, token, phone)
        if res and res.get('code'):
            st.code(res['code'], language="text")
            st.success("Insira este código no seu WhatsApp.")
        else: st.error("Erro ao gerar código OTP.")

@st.dialog("📝 Editar Instância")
def dialog_editar(id_db, nome, inst_id, token):
    new_nome = st.text_input("Nome", value=nome)
    new_id = st.text_input("Instance ID", value=inst_id)
    new_token = st.text_input("Token", value=token)
    if st.button("Salvar Alterações"):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE wapi_instancias SET nome=%s, api_instance_id=%s, api_token=%s WHERE id=%s", (new_nome, new_id, new_token, id_db))
        conn.commit()
        conn.close()
        st.success("Atualizado!")
        time.sleep(1)
        st.rerun()

# ==========================================================
# 4. INTERFACE PRINCIPAL
# ==========================================================
def app_wapi():
    st.markdown("## 📱 Módulo W-API")
    tab1, tab2, tab3, tab4 = st.tabs(["📤 Disparador", "🤖 Instâncias", "📝 Modelos", "📋 Registros"])

    with tab1:
        st.markdown("### 📤 Disparar Mensagem")
        conn = get_conn()
        df_inst = pd.read_sql("SELECT nome, api_instance_id, api_token FROM wapi_instancias", conn)
        df_cli = pd.read_sql("SELECT nome, telefone, id_grupo_whats FROM clientes_usuarios", conn)
        conn.close()

        if not df_inst.empty:
            inst_sel = st.selectbox("Instância", df_inst['nome'].tolist())
            row_inst = df_inst[df_inst['nome'] == inst_sel].iloc[0]
            tipo_envio = st.radio("Tipo", ["Para Cliente", "Manual"], horizontal=True)
            
            destino_final = ""
            nome_cli = ""
            if tipo_envio == "Para Cliente":
                cli_sel = st.selectbox("Selecione o Cliente", df_cli['nome'].tolist())
                row_cli = df_cli[df_cli['nome'] == cli_sel].iloc[0]
                nome_cli = row_cli['nome']
                opcoes = ["Telefone Pessoal"]
                if row_cli['id_grupo_whats']: opcoes.append("Grupo do Cliente")
                escolha = st.radio("Destino", opcoes, horizontal=True)
                destino_final = row_cli['id_grupo_whats'] if escolha == "Grupo do Cliente" else row_cli['telefone']
                st.info(f"📍 Destino: {destino_final}")
            else:
                destino_final = st.text_input("Número/ID Grupo")

            msg = st.text_area("Mensagem")
            if st.button("🚀 Enviar"):
                res = enviar_msg_api(row_inst['api_instance_id'], row_inst['api_token'], destino_final, msg)
                if res.get('messageId') or res.get('success') is True:
                    st.success("Enviado!")
                    salvar_log(inst_sel, destino_final, msg, "ENVIADA", "Sucesso", nome_cli)
                else:
                    st.error("Erro no envio.")
                    salvar_log(inst_sel, destino_final, msg, "ENVIADA", "Erro", nome_cli)

    with tab2:
        st.markdown("### 🤖 Gerenciar Instâncias")
        conn = get_conn()
        df_list = pd.read_sql("SELECT id, nome, api_instance_id, api_token FROM wapi_instancias", conn)
        conn.close()

        if not df_list.empty:
            for _, inst in df_list.iterrows():
                with st.expander(f"Instância: **{inst['nome']}**"):
                    st.write(f"**ID:** `{inst['api_instance_id']}`")
                    st.write(f"**Token:** `{'*'*10 + inst['api_token'][-5:]}`")
                    
                    col_bt1, col_bt2, col_bt3 = st.columns(3)
                    with col_bt1:
                        if st.button("📷 QR Code", key=f"qr_{inst['id']}"):
                            dialog_qrcode(inst['api_instance_id'], inst['api_token'])
                        if st.button("📊 Status", key=f"st_{inst['id']}"):
                            res_st = checar_status_api(inst['api_instance_id'], inst['api_token'])
                            st.write(f"Estado: **{res_st.get('state')}**")
                    
                    with col_bt2:
                        if st.button("🔢 Código OTP", key=f"otp_{inst['id']}"):
                            dialog_otp(inst['api_instance_id'], inst['api_token'])
                        if st.button("📝 Editar", key=f"ed_{inst['id']}"):
                            dialog_editar(inst['id'], inst['nome'], inst['api_instance_id'], inst['api_token'])
                    
                    with col_bt3:
                        if st.button("❌ Excluir", key=f"del_{inst['id']}"):
                            conn = get_conn()
                            cur = conn.cursor()
                            cur.execute("DELETE FROM wapi_instancias WHERE id=%s", (inst['id'],))
                            conn.commit()
                            conn.close()
                            st.warning("Excluída!")
                            time.sleep(1)
                            st.rerun()
        else:
            st.info("Nenhuma instância cadastrada.")

    # ATUALIZAÇÃO DA TAB 4: Sincronização com Webhook/ngrok
    with tab4:
        st.markdown("### 📋 Histórico de Mensagens")
        try:
            conn = get_conn()
            # Seleciona as colunas preenchidas pelo webhook_wapi.py
            query = """
                SELECT data_hora, tipo, nome_contato, telefone, mensagem, status 
                FROM wapi_logs 
                ORDER BY data_hora DESC 
                LIMIT 50
            """
            df_logs = pd.read_sql(query, conn)
            conn.close()
            
            if not df_logs.empty:
                df_logs['data_hora'] = pd.to_datetime(df_logs['data_hora']).dt.strftime('%d/%m/%Y %H:%M')
                
                # Identificação visual de Entrada/Saída
                df_logs['Fluxo'] = df_logs['tipo'].apply(
                    lambda x: "📥 RECEBIDA" if x == 'RECEBIDA' else "📤 ENVIADA"
                )
                
                st.dataframe(
                    df_logs[['data_hora', 'Fluxo', 'nome_contato', 'telefone', 'mensagem', 'status']], 
                    column_config={
                        "data_hora": "Data/Hora",
                        "Fluxo": "Direção",
                        "nome_contato": "Cliente/PushName",
                        "telefone": "WhatsApp",
                        "mensagem": "Conteúdo",
                        "status": "Situação"
                    },
                    use_container_width=True, 
                    hide_index=True
                )
            else:
                st.info("Aguardando novas interações para exibir no histórico.")
        except Exception as e:
            st.error(f"Erro ao carregar o histórico: {e}")

if __name__ == "__main__":
    app_wapi()