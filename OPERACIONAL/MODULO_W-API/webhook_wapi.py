import sys
import os

# Garante a importação do conexao.py na mesma pasta
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, request, jsonify
import psycopg2
import re

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
    # Garante que mensagem nunca seja None
    if mensagem is None:
        mensagem = ""
        
    conn = get_conn()
    cur = conn.cursor()
    try:
        sql = """
            INSERT INTO wapi_logs (instance_id, telefone, mensagem, tipo, status, nome_contato) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        valores = (instance_id, telefone, mensagem, 'RECEBIDA', 'Sucesso', nome)
        
        cur.execute(sql, valores)
        conn.commit()
        print(f"💾 DADOS GRAVADOS -> Nome: {nome} | Tel: {telefone} | Msg: '{mensagem}'")
        
    except Exception as e:
        print(f"❌ Erro ao gravar no banco: {e}")
    finally:
        cur.close()
        conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    
    if dados and dados.get("event") == "webhookReceived":
        instance_id = dados.get("instanceId")
        sender = dados.get("sender", {})
        remetente = sender.get("id", "") 
        nome_push = sender.get("pushName", "Contato via Whats")

        # --- CAPTURA DO CONTEÚDO (Lógica Mantida) ---
        msg_content = dados.get("msgContent", {})
        mensagem = msg_content.get("text")
        
        if not mensagem:
            extended = msg_content.get("extendedTextMessage", {})
            mensagem = extended.get("text", "")
            
        if not mensagem:
            mensagem = msg_content.get("conversation", "")
        # --------------------------------------------

        if dados.get("isGroup") is True:
            return jsonify({"status": "ignorado"}), 200

        # Limpa caracteres não numéricos
        telefone_limpo = re.sub(r'[^0-9]', '', remetente)
        
        # --- AJUSTE DE NORMALIZAÇÃO DO 9º DÍGITO ---
        # Verifica se é padrão BR (55) e tem 12 dígitos (Ex: 55 82 99025155)
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            try:
                # Pega o primeiro dígito após o DDD (índice 4 na string)
                # Ex: 55(0-1) 82(2-3) 9(4)...
                primeiro_digito = int(telefone_limpo[4])
                
                # Se começar com 6, 7, 8 ou 9, consideramos celular e adicionamos o 9 extra
                if primeiro_digito >= 6:
                    telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
                    # Resultado vira 13 dígitos: 55 82 9 99025155
            except:
                pass # Se der erro na conversão, mantém original
        # -------------------------------------------

        salvar_log_recebido(instance_id, telefone_limpo, mensagem, nome_push)
        
        return jsonify({"status": "sucesso"}), 200

    return jsonify({"status": "evento_ignorado"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)