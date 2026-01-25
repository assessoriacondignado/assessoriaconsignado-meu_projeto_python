import streamlit as st
import pandas as pd
import psycopg2
import time
import requests
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
            # Ajustado para usar o schema admin.wapi_instancias
            cur.execute("UPDATE admin.wapi_instancias SET nome=%s, api_instance_id=%s, api_token=%s WHERE id=%s", (new_nome, new_id, new_token, id_db))
            conn.commit(); conn.close()
            st.success("Configurações atualizadas!")
            time.sleep(1); st.rerun()
        except Exception as e: st.error(f"Erro ao salvar: {e}")

# --- INTERFACE ---
def app_instancias():
    st.markdown("### 🤖 Gerenciar Instâncias")
    try:
        conn = get_conn()
        # CORREÇÃO: Schema explícito 'admin.wapi_instancias'
        df_list = pd.read_sql("SELECT id, nome, api_instance_id, api_token FROM admin.wapi_instancias", conn)
        conn.close()

        if not df_list.empty:
            for _, inst in df_list.iterrows():
                with st.expander(f"Instância: **{inst['nome']}**"):
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        if st.button("📷 QR Code", key=f"qr_{inst['id']}"): dialog_qrcode(inst['api_instance_id'], inst['api_token'])
                        
                        # --- ATUALIZAÇÃO DE STATUS (CORRIGIDA) ---
                        if st.button("📊 Atualizar Status", key=f"st_{inst['id']}"):
                            try:
                                url = f"https://api.w-api.app/v1/webhook/update-webhook-message-status?instanceId={inst['api_instance_id']}"
                                headers = {
                                    "Content-Type": "application/x-www-form-urlencoded",
                                    "Authorization": f"Bearer {inst['api_token']}"
                                }
                                response = requests.post(url, headers=headers)
                                
                                # Tenta decodificar JSON apenas se houver conteúdo
                                try:
                                    res_json = response.json()
                                    if res_json.get("error") is False:
                                        st.success(res_json.get("message", "Webhook de presença atualizado."))
                                    else:
                                        st.error(f"Erro API: {res_json.get('message', 'Erro desconhecido')}")
                                except ValueError:
                                    # Se falhar ao ler JSON, mas o status for 200, considera sucesso parcial ou exibe o texto
                                    if response.status_code == 200:
                                        st.success(f"Comando enviado com sucesso! (Status 200)")
                                        # Opcional: mostrar o retorno texto se quiser debugar
                                        # st.code(response.text)
                                    else:
                                        st.error(f"Falha na requisição (Status: {response.status_code})")
                                        with st.expander("Ver detalhes do erro"):
                                            st.code(response.text)
                                    
                            except Exception as e:
                                st.error(f"Erro de execução: {e}")
                        # --- FIM DA ATUALIZAÇÃO ---

                    with c2:
                        if st.button("🔢 Código OTP", key=f"otp_{inst['id']}"): dialog_otp(inst['api_instance_id'], inst['api_token'])
                        if st.button("📝 Editar", key=f"ed_{inst['id']}"): dialog_editar(inst['id'], inst['nome'], inst['api_instance_id'], inst['api_token'])
                    with c3:
                        if st.button("❌ Excluir", key=f"del_{inst['id']}"):
                            conn = get_conn(); cur = conn.cursor()
                            # CORREÇÃO: Schema explícito 'admin.wapi_instancias'
                            cur.execute("DELETE FROM admin.wapi_instancias WHERE id=%s", (inst['id'],))
                            conn.commit(); conn.close()
                            st.warning("Removida."); time.sleep(1); st.rerun()
        else: st.info("Nenhuma instância cadastrada.")
    except Exception as e: 
        st.error(f"Erro ao carregar instâncias: {e}")