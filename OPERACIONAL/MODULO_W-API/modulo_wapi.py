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
    
    # --- DIAGNÓSTICO FORÇADO ---
    print("\n" + "="*40, flush=True)
    print("🔍 EVENTO RECEBIDO NO WEBHOOK", flush=True)
    
    if not dados:
        print("⚠️ Aviso: Corpo da requisição vazio.", flush=True)
        return jsonify({"status": "vazio"}), 200

    event = dados.get("event")
    instance_id = dados.get("instanceId")
    
    # --- DETECÇÃO INTELIGENTE DE FLUXO ---
    # Se 'fromMe' for True, a mensagem saiu do Robô (ENVIADA)
    is_from_me = (
        dados.get("fromMe") is True or 
        dados.get("data", {}).get("key", {}).get("fromMe") is True or
        event == "message.sent"
    )
    
    tipo_log = "ENVIADA" if is_from_me else "RECEBIDA"
    
    # Captura o telefone (remetente ou destinatário)
    sender_id = dados.get("sender", {}).get("id") or dados.get("to") or dados.get("remoteJid")
    if not sender_id and dados.get("data"):
        sender_id = dados.get("data", {}).get("key", {}).get("remoteJid")

    nome_contato = dados.get("sender", {}).get("pushName") or dados.get("pushName") or "Contato"

    # --- CAPTURA DE MENSAGEM ---
    msg_content = dados.get("msgContent") or dados.get("data", {}).get("message") or {}
    mensagem = (
        msg_content.get("text") or 
        msg_content.get("conversation") or 
        msg_content.get("body") or 
        dados.get("content") or 
        ""
    )

    if dados.get("isGroup") is True:
        print(f"ℹ️ Grupo ignorado.", flush=True)
        return jsonify({"status": "ignorado"}), 200

    if sender_id:
        telefone_limpo = re.sub(r'[^0-9]', '', str(sender_id))
        salvar_log_webhook(instance_id, telefone_limpo, mensagem, tipo_log, nome_contato)
        return jsonify({"status": "sucesso", "tipo": tipo_log}), 200

    print("⚠️ Não foi possível identificar o telefone no JSON.", flush=True)
    return jsonify({"status": "erro_identificacao"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)