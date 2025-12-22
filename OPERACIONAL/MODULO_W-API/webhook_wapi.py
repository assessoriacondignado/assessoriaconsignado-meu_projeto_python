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
    print("✅ Conexão importada com sucesso no Webhook!")
except Exception as e:
    print(f"❌ Erro crítico no conexao.py: {e}")

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
    if mensagem is None:
        mensagem = ""
        
    try:
        conn = get_conn()
        cur = conn.cursor()
        sql = """
            INSERT INTO wapi_logs (instance_id, telefone, mensagem, tipo, status, nome_contato) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        valores = (instance_id, telefone, mensagem, tipo, 'Sucesso', nome)
        
        cur.execute(sql, valores)
        conn.commit()
        print(f"💾 DADOS GRAVADOS ({tipo}) -> Nome: {nome} | Tel: {telefone} | Msg: '{mensagem}'")
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro ao gravar no banco: {e}")

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    
    # --- DIAGNÓSTICO ---
    print("\n" + "="*40)
    print("🔍 EVENTO RECEBIDO NO WEBHOOK")
    # print(json.dumps(dados, indent=2)) 
    print("="*40 + "\n")
    
    event = dados.get("event")
    
    # Define eventos permitidos para RECEBIDAS e ENVIADAS
    eventos_recebidos = ["webhookReceived", "message.received", "messages.upsert"]
    eventos_enviados = ["message.sent"]

    if dados and (event in eventos_recebidos or event in eventos_enviados or "event" not in dados):
        instance_id = dados.get("instanceId")
        
        # Determina o tipo do log
        tipo_log = "RECEBIDA"
        if event in eventos_enviados:
            tipo_log = "ENVIADA"
            
        # Captura remetente/destinatário
        sender = dados.get("sender", {})
        remetente = sender.get("id", "") 
        nome_push = sender.get("pushName") or sender.get("name") or "Contato WhatsApp"

        # --- CAPTURA DO CONTEÚDO ---
        msg_content = dados.get("msgContent", {})
        mensagem = (
            msg_content.get("text") or 
            msg_content.get("conversation") or 
            msg_content.get("body") or 
            msg_content.get("caption") or 
            dados.get("content") or 
            ""
        )
        
        if not mensagem:
            extended = msg_content.get("extendedTextMessage", {})
            mensagem = extended.get("text") or extended.get("body") or ""

        # Ignora grupos para log limpo
        if dados.get("isGroup") is True:
            return jsonify({"status": "ignorado_grupo"}), 200

        # Normalização do telefone
        telefone_limpo = re.sub(r'[^0-9]', '', str(remetente))
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            try:
                if int(telefone_limpo[4]) >= 6:
                    telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
            except: pass 

        # Grava o log (Seja entrada ou saída)
        salvar_log_webhook(instance_id, telefone_limpo, mensagem, tipo_log, nome_push)
        return jsonify({"status": "sucesso"}), 200

    return jsonify({"status": "evento_ignorado"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)