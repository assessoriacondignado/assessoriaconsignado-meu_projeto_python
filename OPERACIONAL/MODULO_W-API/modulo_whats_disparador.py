import streamlit as st
import pandas as pd
import psycopg2
import requests
import re
import conexao
import base64
# Importa o módulo WAPI para usar a função de envio e limpeza
import modulo_wapi 

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

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
            
            # Seleção do Destinatário
            tipo_dest = st.radio("Destino", ["Cliente", "Manual"], horizontal=True)
            
            destino = None # Inicializa variável

            if tipo_dest == "Cliente":
                cli_sel = st.selectbox("Selecionar Cliente", df_cli['nome'].tolist())
                # Pega o telefone bruto do banco
                telefone_bruto = df_cli[df_cli['nome'] == cli_sel].iloc[0]['telefone']
                
                # --- APLICA LIMPEZA (Remove 55 e ajusta 9º dígito) ---
                destino = modulo_wapi.limpar_telefone(telefone_bruto)
                
                st.caption(f"Telefone Original: {telefone_bruto} | Formatado: {destino}")
            else:
                telefone_manual = st.text_input("Número (DDD+Número)")
                
                # --- APLICA LIMPEZA NO INPUT MANUAL ---
                if telefone_manual:
                    destino = modulo_wapi.limpar_telefone(telefone_manual)
                    st.caption(f"Número processado: {destino}")

            # Conteúdo da Mensagem
            msg = st.text_area("Texto / Legenda da Mídia")
            
            # --- UPLOAD DE MÍDIA ---
            st.markdown("##### 📎 Anexar Arquivo (Opcional)")
            arquivo = st.file_uploader("Envie imagem, áudio, vídeo ou documento", 
                                     type=['png', 'jpg', 'jpeg', 'pdf', 'mp3', 'mp4', 'ogg', 'wav'])

            if st.button("🚀 Enviar Agora"):
                if destino:
                    # Lógica de Envio
                    res = {}
                    
                    if arquivo:
                        # Processo de envio de Mídia
                        with st.spinner("Processando arquivo..."):
                            try:
                                # Converte o arquivo para Base64
                                bytes_data = arquivo.getvalue()
                                b64_encoded = base64.b64encode(bytes_data).decode('utf-8')
                                mime_type = arquivo.type
                                
                                # Monta a string Data URI scheme
                                base64_full = f"data:{mime_type};base64,{b64_encoded}"
                                
                                # O modulo_wapi já faz a limpeza novamente por segurança
                                res = modulo_wapi.enviar_midia_api(
                                    row_inst['api_instance_id'], 
                                    row_inst['api_token'], 
                                    destino, 
                                    base64_full, 
                                    arquivo.name, 
                                    msg
                                )
                            except Exception as e:
                                st.error(f"Erro ao processar arquivo: {e}")
                                return
                    elif msg:
                        # Processo de envio apenas Texto
                        res = modulo_wapi.enviar_msg_api(
                            row_inst['api_instance_id'], 
                            row_inst['api_token'], 
                            destino, 
                            msg
                        )
                    else:
                        st.warning("Escreva uma mensagem ou anexe um arquivo.")
                        return

                    # Feedback
                    if res.get('messageId') or res.get('success'):
                        st.success("Enviado com sucesso!")
                    else:
                        st.error(f"Falha no envio: {res}")
                else: 
                    st.warning("Preencha o número de destino.")
        else: 
            st.warning("Nenhuma instância configurada.")
    except Exception as e: 
        st.error(f"Erro ao carregar dados: {e}")