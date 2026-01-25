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
    if not telefone: return

    conn = get_conn()
    if not conn: return

    try:
        cur = conn.cursor()
        
        # Verifica número
        cur.execute("SELECT id, id_cliente, nome_cliente FROM wapi_numeros WHERE telefone = %s", (telefone,))
        res_num = cur.fetchone()
        
        id_cliente_final = None
        nome_cliente_final = None 
        nome_contato_log = push_name 

        if res_num:
            cur.execute("UPDATE wapi_numeros SET data_ultima_interacao = NOW() WHERE telefone = %s", (telefone,))
            id_cliente_final = res_num[1]
            nome_cliente_final = res_num[2]
            if nome_cliente_final: nome_contato_log = nome_cliente_final
        else:
            busca_tel = f"%{telefone[-8:]}"
            cur.execute("SELECT id, nome FROM admin.clientes WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
            res_cli = cur.fetchone()
            
            if res_cli:
                id_cliente_final = res_cli[0]
                nome_cliente_final = res_cli[1]
                nome_contato_log = nome_cliente_final
            
            cur.execute("""
                INSERT INTO wapi_numeros (telefone, id_cliente, nome_cliente, data_ultima_interacao) 
                VALUES (%s, %s, %s, NOW())
            """, (telefone, id_cliente_final, nome_cliente_final))
        
        # Grava Log
        sql_log = """
            INSERT INTO wapi_logs (instance_id, telefone, mensagem, tipo, status, nome_contato, id_cliente, nome_cliente) 
            VALUES (%s, %s, %s, %s, 'Sucesso', %s, %s, %s)
        """
        cur.execute(sql_log, (instance_id, telefone, mensagem or "", tipo, nome_contato_log, id_cliente_final, nome_cliente_final))
        conn.commit()
        
        # --- PRINT NO TERMINAL PARA CONFIRMAR GRAVAÇÃO NO BANCO ---
        print(f"💾 [BANCO] Salvo: {tipo} | Tel: {telefone} | Msg: {mensagem}", flush=True)
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erro no banco: {e}", flush=True)
        if conn: conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    if not dados: return jsonify({"status": "vazio"}), 200
    
    # --- [DEBUG] MOSTRA O JSON INTEIRO NO TERMINAL ---
    print("\n" + "="*50, flush=True)
    print("⚡ RECEBIDO NO WEBHOOK (JSON BRUTO):", flush=True)
    print(json.dumps(dados, indent=2, ensure_ascii=False), flush=True)
    print("="*50 + "\n", flush=True)

    event = dados.get("event")
    eventos_aceitos = ["message.received", "message.sent", "message.upsert", "webhookReceived", "webhookDelivery"]
    
    if event not in eventos_aceitos:
        print(f"⚠️ Evento ignorado: {event}", flush=True)
        return jsonify({"status": "ignorado"}), 200

    instance_id = dados.get("instanceId", "PADRAO")
    
    if dados.get("isGroup") is True: 
        return jsonify({"status": "grupo_ignorado"}), 200

    # --- IDENTIFICAÇÃO DO TIPO (ENVIADA/RECEBIDA) ---
    is_from_me = dados.get("fromMe") is True
    tipo_log = "ENVIADA" if is_from_me else "RECEBIDA"

    # Captura objetos principais (Suporte a 'remetente' e 'sender')
    sender = dados.get("sender") or dados.get("remetente", {})
    chat = dados.get("chat", {})
    
    telefone_bruto = None
    push_name = "Desconhecido"

    # --- [IDENTIFICAÇÃO] LÓGICA DE EXTRAÇÃO DO NÚMERO ---
    if is_from_me:
        # SE FOR ENVIO: O destino está em 'chat' -> 'id'
        telefone_bruto = chat.get("id")
        push_name = "Sistema/Atendente"
        print(f"📤 DETECTADO ENVIO PARA: {telefone_bruto}", flush=True)
    else:
        # SE FOR RECEBIMENTO: A origem está em 'sender'/'remetente' -> 'id'
        telefone_bruto = sender.get("id") or dados.get("from")
        push_name = sender.get("pushName", "Cliente")
        print(f"📥 DETECTADO RECEBIMENTO DE: {telefone_bruto}", flush=True)

    # --- [IDENTIFICAÇÃO] LÓGICA DE EXTRAÇÃO DA MENSAGEM ---
    msg_content = dados.get("msgContent", {})
    
    # Tenta pegar o texto em várias posições possíveis do JSON
    mensagem = (
        msg_content.get("text") or 
        msg_content.get("conversation") or 
        msg_content.get("body") or 
        # O campo abaixo é o que veio no seu JSON de exemplo:
        msg_content.get("extendedTextMessage", {}).get("text") or 
        ""
    )
    
    # Se não achar texto, tenta identificar se é mídia
    if not mensagem:
        if "imageMessage" in msg_content: mensagem = "[Imagem]"
        elif "audioMessage" in msg_content: mensagem = "[Áudio]"
        elif "documentMessage" in msg_content: mensagem = "[Documento]"

    print(f"💬 CONTEÚDO CAPTURADO: {mensagem}", flush=True)

    if telefone_bruto:
        telefone_limpo = re.sub(r'[^0-9]', '', str(telefone_bruto))
        
        # Ajuste de DDI BR (55) e 9º dígito
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            if int(telefone_limpo[4]) >= 6:
                telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
        
        gerenciar_numero_e_log(instance_id, telefone_limpo, mensagem, tipo_log, push_name)
        return jsonify({"status": "processado"}), 200

    print("❌ ERRO: Telefone não identificado no JSON", flush=True)
    return jsonify({"status": "erro_dados_insuficientes"}), 200

if __name__ == '__main__':
    # Porta 5001 para evitar conflito com Streamlit
    app.run(host='0.0.0.0', port=5001)