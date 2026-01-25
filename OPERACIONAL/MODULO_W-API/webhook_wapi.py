import sys
import os
from flask import Flask, request, jsonify
import psycopg2
import re
import json
from datetime import datetime

# --- CONFIGURAÇÃO DE CAMINHO DINÂMICO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Adiciona raiz do projeto para importar conexao.py
sys.path.append(os.path.dirname(os.path.dirname(BASE_DIR)))

try:
    import conexao
    print("✅ Conexão importada com sucesso no Webhook!", flush=True)
except Exception as e:
    print(f"❌ Erro crítico no conexao.py: {e}", flush=True)

app = Flask(__name__)

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, 
            database=conexao.database, user=conexao.user, 
            password=conexao.password
        )
    except: return None

def gerenciar_numero_e_log(instance_id, telefone, mensagem, tipo, push_name):
    """
    Registra ou atualiza o número e salva o log da mensagem.
    """
    if not telefone: return

    conn = get_conn()
    if not conn: return

    try:
        cur = conn.cursor()
        
        # --- ETAPA 1: Gerenciar wapi_numeros ---
        cur.execute("SELECT id, id_cliente, nome_cliente FROM wapi_numeros WHERE telefone = %s", (telefone,))
        res_num = cur.fetchone()
        
        id_cliente_final = None
        nome_cliente_final = None 
        nome_contato_log = push_name 

        if res_num:
            # Já existe: Atualiza data
            cur.execute("UPDATE wapi_numeros SET data_ultima_interacao = NOW() WHERE telefone = %s", (telefone,))
            id_cliente_final = res_num[1]
            nome_cliente_final = res_num[2]
            if nome_cliente_final:
                nome_contato_log = nome_cliente_final
        else:
            # Novo Número: Tenta auto-vincular
            busca_tel = f"%{telefone[-8:]}"
            cur.execute("SELECT id, nome FROM admin.clientes WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
            res_cli = cur.fetchone()
            
            if res_cli:
                id_cliente_final = res_cli[0]
                nome_cliente_final = res_cli[1]
                nome_contato_log = nome_cliente_final
            
            # Insere novo número
            cur.execute("""
                INSERT INTO wapi_numeros (telefone, id_cliente, nome_cliente, data_ultima_interacao) 
                VALUES (%s, %s, %s, NOW())
            """, (telefone, id_cliente_final, nome_cliente_final))
        
        # --- ETAPA 2: Gravar Log ---
        sql_log = """
            INSERT INTO wapi_logs (instance_id, telefone, mensagem, tipo, status, nome_contato, id_cliente, nome_cliente) 
            VALUES (%s, %s, %s, %s, 'Sucesso', %s, %s, %s)
        """
        cur.execute(sql_log, (instance_id, telefone, mensagem or "", tipo, nome_contato_log, id_cliente_final, nome_cliente_final))
        
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erro no banco: {e}", flush=True)
        if conn: conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    if not dados: return jsonify({"status": "vazio"}), 200
    
    event = dados.get("event")
    
    # Lista atualizada com 'webhookDelivery'
    eventos_aceitos = ["message.received", "message.sent", "message.upsert", "webhookReceived", "webhookDelivery"]
    
    if event not in eventos_aceitos:
        return jsonify({"status": "ignorado"}), 200

    instance_id = dados.get("instanceId", "PADRAO")
    
    # Ignora grupos
    if dados.get("isGroup") is True: 
        return jsonify({"status": "grupo_ignorado"}), 200

    # Determina fluxo (Enviada/Recebida)
    # No seu JSON, 'fromMe' está na raiz
    is_from_me = dados.get("fromMe") is True
    tipo_log = "ENVIADA" if is_from_me else "RECEBIDA"

    # Captura objetos principais (com suporte a 'remetente' do seu JSON)
    sender = dados.get("sender") or dados.get("remetente", {})
    chat = dados.get("chat", {})
    
    telefone_bruto = None
    push_name = "Desconhecido"

    if is_from_me:
        # LÓGICA DE ENVIO (webhookDelivery)
        # O destinatário está em chat -> id
        telefone_bruto = chat.get("id")
        push_name = "Sistema/Atendente"
    else:
        # LÓGICA DE RECEBIMENTO
        # O remetente está em remetente -> id
        telefone_bruto = sender.get("id") or dados.get("from")
        push_name = sender.get("pushName", "Cliente")

    # Tratamento da Mensagem
    msg_content = dados.get("msgContent", {})
    mensagem = (
        msg_content.get("text") or 
        msg_content.get("conversation") or 
        msg_content.get("body") or 
        msg_content.get("extendedTextMessage", {}).get("text") or 
        ""
    )
    
    if not mensagem:
        if "imageMessage" in msg_content: mensagem = "[Imagem]"
        elif "audioMessage" in msg_content: mensagem = "[Áudio]"
        elif "documentMessage" in msg_content: mensagem = "[Documento]"

    if telefone_bruto:
        # Limpeza do telefone
        telefone_limpo = re.sub(r'[^0-9]', '', str(telefone_bruto))
        
        # Ajuste de DDI e 9º dígito
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            if int(telefone_limpo[4]) >= 6:
                telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
        
        gerenciar_numero_e_log(instance_id, telefone_limpo, mensagem, tipo_log, push_name)
        return jsonify({"status": "processado"}), 200

    return jsonify({"status": "erro_dados_insuficientes"}), 200

if __name__ == '__main__':
    # Porta 5001 para evitar conflito com Streamlit
    app.run(host='0.0.0.0', port=5001)