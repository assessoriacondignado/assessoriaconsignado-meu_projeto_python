import streamlit as st
import pandas as pd
import psycopg2
import time
import requests
import conexao
# Importa o módulo central da W-API para usar a limpeza
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
    img = modulo_wapi.obter_qrcode_api(inst_id, token)
    if img: 
        try:
            st.image(img, width=300)
            st.info("Escaneie para conectar a instância.")
        except Exception:
            st.warning("Resposta da API não é uma imagem (provavelmente mensagem de status):")
            st.code(img, language="text")
    else: st.error("Erro ao carregar QR Code da API.")

@st.dialog("🔢 Conectar via Código (OTP)")
def dialog_otp(inst_id, token):
    st.markdown("Insira o número **com DDI** para solicitar o código (o WhatsApp exige o formato internacional aqui).")
    phone = st.text_input("Ex: 5511999999999")
    if st.button("Gerar Código"):
        if phone:
            # Aqui enviamos o phone como digitado, pois a API precisa do DDI para conectar
            res = modulo_wapi.obter_otp_api(inst_id, token, phone)
            if res and res.get('code'):
                st.code(res['code'], language="text")
                st.success("Insira este código no seu aparelho WhatsApp.")
            else: st.error("Erro ao gerar código OTP.")
        else:
            st.warning("Digite o número.")

@st.dialog("📝 Editar Instância")
def dialog_editar(id_db, nome, inst_id, token):
    new_nome = st.text_input("Nome Identificador", value=nome)
    new_id = st.text_input("Instance ID", value=inst_id)
    new_token = st.text_input("Token de Acesso", value=token)
    if st.button("Salvar Alterações"):
        try:
            conn = get_conn(); cur = conn.cursor()
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
        df_list = pd.read_sql("SELECT id, nome, api_instance_id, api_token, status FROM admin.wapi_instancias", conn)
        conn.close()

        if not df_list.empty:
            for _, inst in df_list.iterrows():
                # Define cor do status para facilitar visualização
                status_bd = inst.get('status', 'N/A')
                cor_status = "green" if status_bd == 'conectado' else "red"
                
                with st.expander(f"Instância: **{inst['nome']}** | Status DB: :{cor_status}[{status_bd}]"):
                    
                    # --- BOTÃO CARREGAR INFO ---
                    if st.button("🔄 Carregar Info / Verificar Status", key=f"info_{inst['id']}"):
                        with st.spinner("Consultando API..."):
                            # 1. Tenta pegar Info Completa (Foto/Nome)
                            info = modulo_wapi.obter_info_instancia(inst['api_instance_id'], inst['api_token'])
                            
                            sucesso_info = False
                            if info and not info.get('error'):
                                sucesso_info = True
                                ci1, ci2 = st.columns([1, 3])
                                with ci1:
                                    if info.get('profilePicUrl'):
                                        st.image(info['profilePicUrl'], width=100)
                                    else: st.info("Sem foto")
                                with ci2:
                                    # Pega o número bruto da API
                                    raw_number = info.get('ownerJid', '')
                                    # --- APLICA LIMPEZA PARA VISUALIZAÇÃO ---
                                    clean_number = modulo_wapi.limpar_telefone(raw_number) if raw_number else "Desconhecido"
                                    
                                    st.write(f"**Nome:** {info.get('profileName', 'Desconhecido')}")
                                    st.write(f"**Número Conectado:** {clean_number}")
                            
                            # 2. Lógica de Atualização e Fallback
                            novo_status = None
                            
                            if sucesso_info:
                                novo_status = 'conectado'
                                st.success("Dados recuperados com sucesso!")
                            else:
                                # Se falhou Info, tenta checar apenas o STATUS DA CONEXÃO
                                st.warning("Não foi possível obter foto/perfil. Verificando conexão básica...")
                                
                                if info and info.get('error'):
                                    st.caption(f"Debug Info API: Code {info.get('status_code')} - {info.get('message')}")

                                status_check = modulo_wapi.checar_status_api(inst['api_instance_id'], inst['api_token'])
                                estado_real = status_check.get('state')
                                
                                if estado_real in ['open', 'connected']:
                                    novo_status = 'conectado'
                                    st.success(f"Instância está ONLINE! (State: {estado_real})")
                                else:
                                    novo_status = 'desconectado'
                                    st.error(f"Instância parece estar offline. (State: {estado_real})")

                            # 3. Atualiza o Banco se houver um status definido
                            if novo_status:
                                try:
                                    conn_up = get_conn(); cur_up = conn_up.cursor()
                                    cur_up.execute("UPDATE admin.wapi_instancias SET status = %s WHERE id = %s", (novo_status, inst['id']))
                                    conn_up.commit(); conn_up.close()
                                    if novo_status != status_bd:
                                        st.toast(f"Status atualizado para: {novo_status}", icon="💾")
                                        time.sleep(1)
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Erro ao atualizar banco: {e}")

                    st.divider()

                    c1, c2, c3 = st.columns(3)
                    with c1:
                        if st.button("📷 QR Code", key=f"qr_{inst['id']}"): dialog_qrcode(inst['api_instance_id'], inst['api_token'])
                        
                        if st.button("📊 Forçar Webhook", key=f"st_{inst['id']}"):
                            try:
                                url = f"https://api.w-api.app/v1/webhook/update-webhook-message-status?instanceId={inst['api_instance_id']}"
                                headers = {
                                    "Content-Type": "application/x-www-form-urlencoded",
                                    "Authorization": f"Bearer {inst['api_token']}"
                                }
                                response = requests.post(url, headers=headers)
                                try:
                                    res_json = response.json()
                                    if res_json.get("error") is False:
                                        st.success(res_json.get("message", "Webhook atualizado."))
                                    else:
                                        st.error(f"Erro API: {res_json.get('message')}")
                                except:
                                    st.code(f"Status: {response.status_code}\n{response.text}")
                            except Exception as e:
                                st.error(f"Erro: {e}")

                    with c2:
                        if st.button("🔢 Código OTP", key=f"otp_{inst['id']}"): dialog_otp(inst['api_instance_id'], inst['api_token'])
                        if st.button("📝 Editar", key=f"ed_{inst['id']}"): dialog_editar(inst['id'], inst['nome'], inst['api_instance_id'], inst['api_token'])
                    with c3:
                        if st.button("❌ Excluir", key=f"del_{inst['id']}"):
                            conn = get_conn(); cur = conn.cursor()
                            cur.execute("DELETE FROM admin.wapi_instancias WHERE id=%s", (inst['id'],))
                            conn.commit(); conn.close()
                            st.warning("Removida."); time.sleep(1); st.rerun()
        else: st.info("Nenhuma instância cadastrada.")
    except Exception as e: 
        st.error(f"Erro ao carregar instâncias: {e}")