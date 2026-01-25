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
    if not telefone: 
        print("❌ Telefone vazio. Abortando gravação.", flush=True)
        return

    conn = get_conn()
    if not conn: 
        print("❌ Sem conexão com banco.", flush=True)
        return

    try:
        cur = conn.cursor()
        
        # --- ETAPA 1: Gerenciar admin.wapi_numeros ---
        cur.execute("SELECT id, id_cliente, nome_cliente FROM admin.wapi_numeros WHERE telefone = %s", (telefone,))
        res_num = cur.fetchone()
        
        id_cliente_final = None
        nome_cliente_final = None 
        nome_contato_log = push_name 

        if res_num:
            # Já existe: Atualiza data
            cur.execute("UPDATE admin.wapi_numeros SET data_ultima_interacao = NOW() WHERE telefone = %s", (telefone,))
            id_cliente_final = res_num[1]
            nome_cliente_final = res_num[2]
            if nome_cliente_final: nome_contato_log = nome_cliente_final
        else:
            # Novo Número: Tenta auto-vincular
            busca_tel = f"%{telefone[-8:]}"
            cur.execute("SELECT id, nome FROM admin.clientes WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
            res_cli = cur.fetchone()
            
            if res_cli:
                id_cliente_final = res_cli[0]
                nome_cliente_final = res_cli[1]
                nome_contato_log = nome_cliente_final
                print(f"🔗 Auto-vínculo encontrado: {nome_cliente_final}", flush=True)
            
            cur.execute("""
                INSERT INTO admin.wapi_numeros (telefone, id_cliente, nome_cliente, data_ultima_interacao) 
                VALUES (%s, %s, %s, NOW())
            """, (telefone, id_cliente_final, nome_cliente_final))
        
        # --- ETAPA 2: Gravar Log em admin.wapi_logs ---
        sql_log = """
            INSERT INTO admin.wapi_logs (instance_id, telefone, mensagem, tipo, status, nome_contato, id_cliente, nome_cliente, data_hora) 
            VALUES (%s, %s, %s, %s, 'Sucesso', %s, %s, %s, NOW())
        """
        cur.execute(sql_log, (instance_id, telefone, mensagem or "", tipo, nome_contato_log, id_cliente_final, nome_cliente_final))
        
        conn.commit()
        
        icone = "➡️ ENVIADA" if tipo == "ENVIADA" else "⬅️ RECEBIDA"
        print(f"✅ GRAVADO NO BANCO: {icone} | Tel: {telefone} | Msg: {mensagem}", flush=True)
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erro no banco de dados: {e}", flush=True)
        if conn: conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    if not dados: return jsonify({"status": "vazio"}), 200
    
    # --- SALVAR JSON EM ARQUIVO (Backup/Debug) ---
    try:
        # Define o caminho da pasta
        pasta_json = os.path.join(BASE_DIR, "WAPI_WEBHOOK_JASON")
        if not os.path.exists(pasta_json):
            os.makedirs(pasta_json) # Cria a pasta se não existir

        # Cria nome do arquivo: YYYYMMDD_HHMMSS_evento.json
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        evento_nome = dados.get("event", "unknown")
        nome_arquivo = f"{timestamp}_{evento_nome}.json"
        caminho_arquivo = os.path.join(pasta_json, nome_arquivo)

        # Salva o JSON
        with open(caminho_arquivo, "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=4, ensure_ascii=False)
            
        print(f"📂 JSON salvo em: {nome_arquivo}", flush=True)
        
    except Exception as e:
        print(f"⚠️ Erro ao salvar arquivo JSON: {e}", flush=True)
    # ---------------------------------------------

    # DEBUG TERMINAL
    print("\n" + "-"*50, flush=True)
    # print(json.dumps(dados, indent=2, ensure_ascii=False), flush=True)

    event = dados.get("event")
    eventos_aceitos = ["message.received", "message.sent", "message.upsert", "webhookReceived", "webhookDelivery"]
    
    if event not in eventos_aceitos:
        print(f"⚠️ Evento ignorado: {event}", flush=True)
        return jsonify({"status": "ignorado"}), 200

    instance_id = dados.get("instanceId", "PADRAO")
    
    if dados.get("isGroup") is True: 
        return jsonify({"status": "grupo_ignorado"}), 200

    # LÓGICA DE IDENTIFICAÇÃO
    is_from_me = dados.get("fromMe") is True
    tipo_log = "ENVIADA" if is_from_me else "RECEBIDA"

    sender = dados.get("sender") or dados.get("remetente", {})
    chat = dados.get("chat", {})
    
    telefone_bruto = None
    push_name = "Desconhecido"

    if is_from_me:
        # ENVIO (webhookDelivery)
        telefone_bruto = chat.get("id")
        push_name = "Sistema/Atendente"
        print(f"📤 DETECTADO ENVIO (webhookDelivery)", flush=True)
    else:
        # RECEBIMENTO
        telefone_bruto = sender.get("id") or dados.get("from")
        push_name = sender.get("pushName", "Cliente")
        print(f"📥 DETECTADO RECEBIMENTO", flush=True)

    # LÓGICA DE MENSAGEM
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
        telefone_limpo = re.sub(r'[^0-9]', '', str(telefone_bruto))
        
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            if int(telefone_limpo[4]) >= 6:
                telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
        
        print(f"🔎 Processando: {telefone_limpo} | Msg: {mensagem}", flush=True)
        gerenciar_numero_e_log(instance_id, telefone_limpo, mensagem, tipo_log, push_name)
        return jsonify({"status": "processado"}), 200

    print("⚠️ Telefone não identificado no payload.", flush=True)
    return jsonify({"status": "erro_dados_insuficientes"}), 200

if __name__ == '__main__':
    # Porta 5001 para não conflitar
    app.run(host='0.0.0.0', port=5001)