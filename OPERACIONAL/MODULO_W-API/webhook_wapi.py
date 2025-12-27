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
    return psycopg2.connect(
        host=conexao.host, port=conexao.port, 
        database=conexao.database, user=conexao.user, 
        password=conexao.password
    )

def gerenciar_numero_e_log(instance_id, telefone, mensagem, tipo, push_name):
    """
    FLUXO:
    1. Verifica se o número existe em wapi_numeros.
    2. Se existir: Atualiza data e pega o ID do cliente.
    3. Se NÃO existir:
       3.1 Verifica se o número existe em admin.clientes (tentativa de auto-vínculo).
       3.2 Cria o registro em wapi_numeros (com ou sem ID).
    4. Grava o log em wapi_logs com os dados conciliados.
    """
    if not telefone: return

    conn = get_conn()
    try:
        cur = conn.cursor()
        
        # --- ETAPA 1: Gerenciar wapi_numeros ---
        cur.execute("SELECT id, id_cliente, nome_cliente FROM wapi_numeros WHERE telefone = %s", (telefone,))
        res_num = cur.fetchone()
        
        id_cliente_final = None
        nome_cliente_final = None # Nome do cadastro (se houver)
        nome_contato_log = push_name # Nome que aparecerá no log (pode ser o pushname ou o nome do cliente)

        if res_num:
            # 2.1 Já existe: Atualiza interação e pega dados
            cur.execute("UPDATE wapi_numeros SET data_ultima_interacao = NOW() WHERE telefone = %s", (telefone,))
            id_cliente_final = res_num[1]
            nome_cliente_final = res_num[2]
            if nome_cliente_final:
                nome_contato_log = nome_cliente_final
        else:
            # 2.2/2.3 Novo Número: Tenta achar no cadastro de clientes para auto-vincular
            # Busca flexível pelos últimos 8 dígitos para evitar erro de DDD/9º dígito
            busca_tel = f"%{telefone[-8:]}"
            cur.execute("SELECT id, nome FROM admin.clientes WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
            res_cli = cur.fetchone()
            
            if res_cli:
                # Achou cliente: Cria vinculado
                id_cliente_final = res_cli[0]
                nome_cliente_final = res_cli[1]
                nome_contato_log = nome_cliente_final
                print(f"🔗 Auto-vínculo: {telefone} -> {nome_cliente_final}")
            else:
                # Não achou: Cria sem vínculo (ID e Nome NULL)
                print(f"🆕 Novo número desconhecido: {telefone}")

            # Insere na tabela de números
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
        print(f"💾 LOG GRAVADO: {tipo} | {telefone} | Cli: {id_cliente_final}", flush=True)
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
    
    # Processa apenas mensagens ou envios
    if event not in ["message.received", "message.sent", "webhookReceived"]:
        return jsonify({"status": "ignorado"}), 200

    instance_id = dados.get("instanceId", "PADRAO")
    
    # Filtra grupos
    if dados.get("isGroup") is True: return jsonify({"status": "grupo_ignorado"}), 200

    # Determina fluxo (Enviada/Recebida)
    is_from_me = dados.get("fromMe") is True or event == "message.sent"
    tipo_log = "ENVIADA" if is_from_me else "RECEBIDA"

    # Captura dados
    sender = dados.get("sender", {})
    chat = dados.get("chat", {})
    
    if is_from_me:
        telefone_bruto = chat.get("id") or dados.get("to")
        push_name = "Sistema/Atendente"
    else:
        telefone_bruto = sender.get("id") or dados.get("from")
        push_name = sender.get("pushName", "Desconhecido")

    # Tratamento da Mensagem
    msg_content = dados.get("msgContent") or {}
    mensagem = (
        msg_content.get("text") or msg_content.get("conversation") or 
        msg_content.get("body") or msg_content.get("extendedTextMessage", {}).get("text") or ""
    )

    if telefone_bruto:
        # Limpeza do telefone
        telefone_limpo = re.sub(r'[^0-9]', '', str(telefone_bruto))
        if len(telefone_limpo) == 12 and telefone_limpo.startswith("55"):
            if int(telefone_limpo[4]) >= 6:
                telefone_limpo = f"{telefone_limpo[:4]}9{telefone_limpo[4:]}"
        
        gerenciar_numero_e_log(instance_id, telefone_limpo, mensagem, tipo_log, push_name)
        return jsonify({"status": "processado"}), 200

    return jsonify({"status": "erro_dados"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)