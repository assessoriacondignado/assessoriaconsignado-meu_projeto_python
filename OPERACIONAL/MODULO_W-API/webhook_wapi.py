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
        host=conexao.host, port=conexao.port, 
        database=conexao.database, user=conexao.user, 
        password=conexao.password
    )

def buscar_nomes_sistema(instance_id_api, telefone, is_enviada, push_name_json):
    """Busca o nome amigável da instância e do contato no banco de dados"""
    nome_instancia = instance_id_api
    nome_contato = push_name_json or "Contato WhatsApp"
    
    try:
        conn = get_conn()
        cur = conn.cursor()
        
        # 1. Busca nome amigável da Instância
        cur.execute("SELECT nome FROM wapi_instancias WHERE api_instance_id = %s", (instance_id_api,))
        res_inst = cur.fetchone()
        if res_inst: nome_instancia = res_inst[0]
        
        # 2. Se for mensagem enviada, busca o nome do destinatário nas tabelas do sistema
        if is_enviada:
            tel_limpo = re.sub(r'[^0-9]', '', str(telefone))
            if len(tel_limpo) > 8:
                busca_tel = f"%{tel_limpo[-8:]}"
                # Busca na tabela de clientes administrativos
                cur.execute("SELECT nome FROM admin.clientes WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
                res_cli = cur.fetchone()
                if res_cli: 
                    nome_contato = res_cli[0]
                else:
                    # Busca na tabela de usuários do sistema
                    cur.execute("SELECT nome FROM clientes_usuarios WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
                    res_user = cur.fetchone()
                    if res_user: nome_contato = res_user[0]
        
        cur.close()
        conn.close()
    except: pass
    
    return nome_instancia, nome_contato

def salvar_log_webhook(instancia_nome, telefone, mensagem, tipo, nome_contato):
    try:
        conn = get_conn()
        cur = conn.cursor()
        sql = """
            INSERT INTO wapi_logs (instance_id, telefone, mensagem, tipo, status, nome_contato) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        valores = (instancia_nome, telefone, mensagem or "", tipo, 'Sucesso', nome_contato)
        cur.execute(sql, valores)
        conn.commit()
        print(f"💾 GRAVADO -> Instância: {instancia_nome} | Contato: {nome_contato} | Fluxo: {tipo}", flush=True)
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao gravar no banco: {e}", flush=True)

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    if not dados: return jsonify({"status": "vazio"}), 200

    print("\n" + "="*40, flush=True)
    print(f"🔍 EVENTO RECEBIDO: {dados.get('event')}", flush=True)
    
    event = dados.get("event")
    instance_id_api = dados.get("instanceId")
    
    # Identifica se a mensagem é enviada (saída) ou recebida (entrada)
    is_from_me = dados.get("fromMe") is True or event in ["message.sent", "webhookDelivery"]
    tipo_log = "ENVIADA" if is_from_me else "RECEBIDA"

    # Captura dados brutos do JSON
    telefone_bruto = ""
    push_name_json = ""
    
    if is_from_me:
        telefone_bruto = dados.get("chat", {}).get("id") or dados.get("to")
        push_name_json = dados.get("sender", {}).get("pushName")
    else:
        telefone_bruto = dados.get("sender", {}).get("id") or dados.get("remoteJid")
        push_name_json = dados.get("sender", {}).get("pushName")

    # Resolve os nomes amigáveis consultando o Banco de Dados
    nome_instancia, nome_contato = buscar_nomes_sistema(instance_id_api, telefone_bruto, is_from_me, push_name_json)

    # Extração robusta do conteúdo da mensagem
    msg_content = dados.get("msgContent") or {}
    mensagem = (
        msg_content.get("text") or msg_content.get("conversation") or 
        msg_content.get("body") or msg_content.get("extendedTextMessage", {}).get("text") or
        dados.get("content") or ""
    )

    # Filtro para ignorar logs de grupos
    if dados.get("isGroup") is True: return jsonify({"status": "ignorado"}), 200

    if telefone_bruto:
        telefone_limpo = re.sub(r'[^0-9]', '', str(telefone_bruto))
        # Normalização para o nono dígito brasileiro
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            if int(telefone_limpo[4]) >= 6:
                telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
        
        salvar_log_webhook(nome_instancia, telefone_limpo, mensagem, tipo_log, nome_contato)
        return jsonify({"status": "sucesso"}), 200

    return jsonify({"status": "erro"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)