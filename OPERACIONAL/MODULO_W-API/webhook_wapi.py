import sys
import os
from flask import Flask, request, jsonify
import psycopg2
import re
import json

# --- CONFIGURAÇÃO DE CAMINHO DINÂMICO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

try:
    import conexao
    print("✅ Conexão importada com sucesso no Webhook!", flush=True)
except Exception as e:
    print(f"❌ Erro crítico no conexao.py: {e}", flush=True)

app = Flask(__name__)

def get_conn():
    return psycopg2.connect(
        host=conexao.host, 
        port=conexao.port, 
        database=conexao.database, 
        user=conexao.user, 
        password=conexao.password
    )

def salvar_log_webhook(instance_id, telefone, mensagem, tipo, nome=""):
    try:
        conn = get_conn()
        cur = conn.cursor()
        sql = """
            INSERT INTO wapi_logs (instance_id, telefone, mensagem, tipo, status, nome_contato) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        # Garante que campos nulos não quebrem o banco
        valores = (instance_id, telefone, mensagem or "", tipo, 'Sucesso', nome or "")
        cur.execute(sql, valores)
        conn.commit()
        print(f"💾 DADOS GRAVADOS ({tipo}) -> Tel: {telefone} | Msg: '{mensagem}'", flush=True)
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao gravar no banco: {e}", flush=True)

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    
    # --- DIAGNÓSTICO ---
    print("\n" + "="*40, flush=True)
    print("🔍 EVENTO RECEBIDO NO WEBHOOK", flush=True)
    if dados:
        print(f"Evento: {dados.get('event')} | FromMe: {dados.get('fromMe')}", flush=True)
    
    event = dados.get("event")
    instance_id = dados.get("instanceId")
    
    # 1. Identifica se é envio ou recebimento
    # Agora aceita 'webhookDelivery' conforme capturado no Webhook.site
    is_from_me = dados.get("fromMe") is True or event in ["message.sent", "webhookDelivery"]
    tipo_log = "ENVIADA" if is_from_me else "RECEBIDA"

    # 2. Captura o Telefone (Lógica aprimorada para envios)
    # Se for envio, o telefone do cliente está em 'chat'['id']
    telefone_bruto = ""
    if is_from_me:
        telefone_bruto = dados.get("chat", {}).get("id") or dados.get("to")
    else:
        telefone_bruto = dados.get("sender", {}).get("id") or dados.get("remoteJid")

    # 3. Captura o Nome
    nome_contato = "Contato WhatsApp"
    if is_from_me:
        # No envio, costumamos usar o nome da sua instância ou deixar vazio
        nome_contato = "Assessoria Consignado"
    else:
        nome_contato = dados.get("sender", {}).get("pushName") or "Cliente"

    # 4. Captura a Mensagem (Lógica para texto simples ou estendido)
    msg_content = dados.get("msgContent") or {}
    mensagem = (
        msg_content.get("text") or 
        msg_content.get("conversation") or 
        msg_content.get("body") or 
        msg_content.get("extendedTextMessage", {}).get("text") or
        dados.get("content") or ""
    )

    if dados.get("isGroup") is True:
        return jsonify({"status": "ignorado_grupo"}), 200

    if telefone_bruto:
        telefone_limpo = re.sub(r'[^0-9]', '', str(telefone_bruto))
        # Ajuste para o nono dígito em números brasileiros
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            if int(telefone_limpo[4]) >= 6:
                telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
        
        salvar_log_webhook(instance_id, telefone_limpo, mensagem, tipo_log, nome_contato)
        return jsonify({"status": "sucesso", "tipo": tipo_log}), 200

    return jsonify({"status": "erro_identificacao"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)