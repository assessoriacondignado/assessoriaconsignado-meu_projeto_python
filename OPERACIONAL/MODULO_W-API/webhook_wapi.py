import sys
import os
from flask import Flask, request, jsonify
import psycopg2
import re

# --- CONFIGURAÇÃO DE CAMINHO DINÂMICO ---
# Garante que o Python localize o conexao.py independente de onde o script é chamado
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
    # Retorna a conexão com o banco de dados PostgreSQL
    return psycopg2.connect(
        host=conexao.host, 
        port=conexao.port, 
        database=conexao.database, 
        user=conexao.user, 
        password=conexao.password
    )

def salvar_log_recebido(instance_id, telefone, mensagem, nome=""):
    # Garante que mensagem nunca seja None para evitar erros de inserção no banco
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
    
    # Valida se o evento recebido é de mensagem
    if dados and dados.get("event") == "webhookReceived":
        instance_id = dados.get("instanceId")
        sender = dados.get("sender", {})
        remetente = sender.get("id", "") 
        nome_push = sender.get("pushName", "Contato via Whats")

        # --- CAPTURA DO CONTEÚDO ---
        msg_content = dados.get("msgContent", {})
        mensagem = msg_content.get("text")
        
        if not mensagem:
            extended = msg_content.get("extendedTextMessage", {})
            mensagem = extended.get("text", "")
            
        if not mensagem:
            mensagem = msg_content.get("conversation", "")
        # ---------------------------

        # Ignora mensagens de grupos para não sobrecarregar o banco
        if dados.get("isGroup") is True:
            return jsonify({"status": "ignorado"}), 200

        # Limpa caracteres não numéricos
        telefone_limpo = re.sub(r'[^0-9]', '', str(remetente))
        
        # --- NORMALIZAÇÃO DO 9º DÍGITO ---
        # Adiciona o 9 extra para celulares brasileiros com 12 dígitos (55 + DDD + Numero)
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            try:
                primeiro_digito = int(telefone_limpo[4])
                # Se o número após o DDD começar com 6, 7, 8 ou 9, é celular
                if primeiro_digito >= 6:
                    telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
            except:
                pass 
        # ---------------------------------

        salvar_log_recebido(instance_id, telefone_limpo, mensagem, nome_push)
        return jsonify({"status": "sucesso"}), 200

    return jsonify({"status": "evento_ignorado"}), 200

if __name__ == '__main__':
    # No servidor Ubuntu, você deve rodar este script em background (ex: com PM2 ou screen)
    app.run(host='0.0.0.0', port=5000)