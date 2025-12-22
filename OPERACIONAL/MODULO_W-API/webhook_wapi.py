import sys
import os
from flask import Flask, request, jsonify
import psycopg2
import re
import json # Adicionado para formatar os logs de diagnóstico

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

def salvar_log_recebido(instance_id, telefone, mensagem, nome=""):
    if mensagem is None:
        mensagem = ""
        
    try:
        conn = get_conn()
        cur = conn.cursor()
        sql = """
            INSERT INTO wapi_logs (instance_id, telefone, mensagem, tipo, status, nome_contato) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        valores = (instance_id, telefone, mensagem, 'RECEBIDA', 'Sucesso', nome)
        
        cur.execute(sql, valores)
        conn.commit()
        print(f"💾 DADOS GRAVADOS -> Nome: {nome} | Tel: {telefone} | Msg: '{mensagem}'")
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro ao gravar no banco: {e}")

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    
    # --- DIAGNÓSTICO: VERIFICAÇÃO DO CÓDIGO RECEBIDO ---
    # Isso permite que você identifique erros de estrutura no terminal do VS Code
    print("\n" + "="*40)
    print("🔍 DIAGNÓSTICO: CONTEÚDO BRUTO RECEBIDO")
    print(json.dumps(dados, indent=2)) 
    print("="*40 + "\n")
    
    # Valida eventos de mensagem (Expandido para capturar diferentes tipos da W-API)
    eventos_permitidos = ["webhookReceived", "message.received", "messages.upsert"]
    if dados and (dados.get("event") in eventos_permitidos or "event" not in dados):
        instance_id = dados.get("instanceId")
        sender = dados.get("sender", {})
        remetente = sender.get("id", "") 
        nome_push = sender.get("pushName") or sender.get("name") or "Contato via Whats"

        # --- CAPTURA ROBUSTA DO CONTEÚDO (EVITA MENSAGENS VAZIAS) ---
        msg_content = dados.get("msgContent", {})
        
        # Tenta extrair o texto de todos os locais possíveis onde ele pode estar escondido
        mensagem = (
            msg_content.get("text") or 
            msg_content.get("conversation") or 
            msg_content.get("body") or 
            msg_content.get("caption") or 
            dados.get("content") or 
            ""
        )
        
        # Caso especial para mensagens citadas ou textos estendidos
        if not mensagem:
            extended = msg_content.get("extendedTextMessage", {})
            mensagem = extended.get("text") or extended.get("body") or ""
        # -----------------------------------------------------------

        if dados.get("isGroup") is True:
            print(f"ℹ️ Grupo ignorado: {remetente}")
            return jsonify({"status": "ignorado"}), 200

        telefone_limpo = re.sub(r'[^0-9]', '', str(remetente))
        
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            try:
                primeiro_digito = int(telefone_limpo[4])
                if primeiro_digito >= 6:
                    telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
            except:
                pass 

        salvar_log_recebido(instance_id, telefone_limpo, mensagem, nome_push)
        return jsonify({"status": "sucesso"}), 200

    return jsonify({"status": "evento_ignorado"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)