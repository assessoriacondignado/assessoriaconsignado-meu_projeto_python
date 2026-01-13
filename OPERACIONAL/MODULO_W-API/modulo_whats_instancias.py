import streamlit as st
import pandas as pd
import psycopg2
import time
import conexao
# Importa o módulo central da W-API para evitar duplicação de código
import modulo_wapi

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

# --- DIALOGS ---
@st.dialog("📷 Conectar QR Code")
def dialog_qrcode(inst_id, token):
    # Uso da função centralizada
    img = modulo_wapi.obter_qrcode_api(inst_id, token)
    if img: 
        try:
            # Tenta renderizar como imagem
            st.image(img, width=300)
            st.info("Escaneie para conectar a instância.")
        except Exception:
            # Se falhar (ex: PIL.UnidentifiedImageError), exibe o conteúdo como texto
            st.warning("Resposta da API não é uma imagem (provavelmente mensagem de status):")
            st.code(img, language="text")
    else: st.error("Erro ao carregar QR Code da API.")

@st.dialog("🔢 Conectar via Código (OTP)")
def dialog_otp(inst_id, token):
    phone = st.text_input("Número com DDI (Ex: 5511999999999)")
    if st.button("Gerar Código"):
        # Uso da função centralizada
        res = modulo_wapi.obter_otp_api(inst_id, token, phone)
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
                            # Uso da função centralizada
                            res_st = modulo_wapi.checar_status_api(inst['api_instance_id'], inst['api_token'])
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