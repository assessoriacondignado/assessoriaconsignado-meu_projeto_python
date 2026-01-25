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

# Adiciona raiz do projeto
sys.path.append(os.path.dirname(os.path.dirname(BASE_DIR)))

try:
    import conexao
    print(" ✅ Conexão importada com sucesso!", flush=True)
except Exception as e:
    print(f" ❌ Erro no conexao.py: {e}", flush=True)

app = Flask(__name__)

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, 
            database=conexao.database, user=conexao.user, 
            password=conexao.password
        )
    except: return None

def limpar_telefone(telefone_bruto):
    """
    Remove caracteres não numéricos, trata 9º dígito
    e REMOVE O 55 (DDI BRASIL) antes de salvar.
    """
    if not telefone_bruto: return None
    temp = telefone_bruto.split('@')[0]
    limpo = re.sub(r'[^0-9]', '', temp)
    
    # Regra básica do 9º dígito BR
    if len(limpo) == 12 and limpo.startswith("55"):
        if int(limpo[4]) >= 6:
            limpo = f"{limpo[:4]}9{limpo[4:]}"

    # --- REMOVE O 55 ---
    if limpo.startswith("55") and len(limpo) >= 10:
        limpo = limpo[2:] 

    return limpo

def gerenciar_banco_dados(dados_proc):
    """
    Grava os dados processados no banco de dados.
    Agora identifica cliente por TELEFONE (Individual) ou ID_GRUPO_WHATS (Grupo).
    """
    conn = get_conn()
    if not conn: return
    
    try:
        cur = conn.cursor()
        
        telefone = dados_proc['telefone']
        is_group = dados_proc['is_group']
        id_grupo = dados_proc['id_grupo']
        push_name = dados_proc['nome_contato']
        
        id_cliente_final = None
        nome_cliente_final = None
        nome_para_log = push_name 

        # ======================================================================
        # NOVA LÓGICA DE IDENTIFICAÇÃO (GRUPO OU INDIVIDUAL)
        # ======================================================================

        # --- CENÁRIO 1: É GRUPO? Tenta achar cliente pelo ID do Grupo ---
        if is_group and id_grupo:
            # Busca exata pelo ID do grupo na tabela de clientes
            cur.execute("SELECT id, nome FROM admin.clientes WHERE id_grupo_whats = %s LIMIT 1", (id_grupo,))
            res_grupo = cur.fetchone()
            
            if res_grupo:
                id_cliente_final = res_grupo[0]
                nome_cliente_final = res_grupo[1]
                # Nota: Em grupos, mantemos o nome_para_log como quem enviou (pushName),
                # mas o vínculo (id_cliente) será da empresa dona do grupo.

        # --- CENÁRIO 2: NÃO É GRUPO? (Ou não achou pelo grupo) Tenta pelo Telefone ---
        # Só entra aqui se for conversa individual E tiver telefone
        if not is_group and telefone:
            
            # 2.1 Verifica se já existe na tabela de triagem (wapi_numeros)
            cur.execute("SELECT id, id_cliente, nome_cliente FROM admin.wapi_numeros WHERE telefone = %s", (telefone,))
            res_num = cur.fetchone()
            
            if res_num:
                cur.execute("UPDATE admin.wapi_numeros SET data_ultima_interacao = NOW() WHERE telefone = %s", (telefone,))
                id_cliente_final = res_num[1]
                nome_cliente_final = res_num[2]
                if nome_cliente_final: nome_para_log = nome_cliente_final
            else:
                # 2.2 Auto-Match na tabela clientes (Pelo Telefone)
                busca_tel = f"%{telefone[-8:]}"
                cur.execute("SELECT id, nome FROM admin.clientes WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
                res_cli = cur.fetchone()
                
                if res_cli:
                    id_cliente_final = res_cli[0]
                    nome_cliente_final = res_cli[1]
                    nome_para_log = nome_cliente_final
                
                # Registra na triagem
                cur.execute("""
                    INSERT INTO admin.wapi_numeros (telefone, id_cliente, nome_cliente, data_ultima_interacao) 
                    VALUES (%s, %s, %s, NOW())
                """, (telefone, id_cliente_final, nome_cliente_final))

        # ======================================================================
        # GRAVAÇÃO DO LOG
        # ======================================================================
        sql_log = """
            INSERT INTO admin.wapi_logs (
                instance_id, telefone, nome_contato, mensagem, tipo, 
                status, id_grupo, grupo, cpf_cliente, id_cliente, nome_cliente, data_hora
            ) VALUES (%s, %s, %s, %s, %s, 'Sucesso', %s, %s, NULL, %s, %s, NOW())
        """
        
        # O campo 'grupo' na tabela logs receberá o nome do cliente se for identificado, 
        # senão o próprio ID do grupo para referência.
        nome_grupo_log = nome_cliente_final if (is_group and nome_cliente_final) else id_grupo

        cur.execute(sql_log, (
            dados_proc['instance_id'],    
            telefone,                     
            nome_para_log,                
            dados_proc['mensagem'],       
            dados_proc['tipo'],           
            dados_proc['id_grupo'],       
            nome_grupo_log,               # Se achou cliente pelo grupo, salva o nome dele na coluna grupo também
            id_cliente_final,             # ID DO CLIENTE (Vindo do Grupo ou Telefone)
            nome_cliente_final            # NOME DO CLIENTE (Vindo do Grupo ou Telefone)
        ))
        
        conn.commit()
        
        categoria = "GRUPO" if is_group else "CLIENTE"
        match_status = f"✅ (ID: {id_cliente_final})" if id_cliente_final else "⚠️ (Não Identificado)"
        print(f"💾 LOG GRAVADO: {dados_proc['tipo']} / {categoria} / {match_status}", flush=True)
        
        cur.close(); conn.close()

    except Exception as e:
        print(f" ❌ Erro ao gravar no banco: {e}", flush=True)
        if conn: conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    if not dados: return jsonify({"status": "vazio"}), 200
    
    # --- 1. SALVA JSON (BACKUP) ---
    try:
        pasta_json = os.path.join(BASE_DIR, "WAPI_WEBHOOK_JASON")
        if not os.path.exists(pasta_json): os.makedirs(pasta_json)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        evento_nome = dados.get('event', 'msg')
        nome_arquivo = f"{timestamp}_{evento_nome}.json"
        
        with open(os.path.join(pasta_json, nome_arquivo), "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ Erro ao salvar JSON: {e}")

    # --- 2. FILTRO DE EVENTOS ---
    event = dados.get("event")
    eventos_aceitos = ["webhookReceived", "webhookDelivery", "message.received", "message.sent"]
    if event not in eventos_aceitos:
        return jsonify({"status": "ignorado"}), 200

    # --- 3. EXTRAÇÃO DE DADOS ---
    instance_id = dados.get("instanceId", "PADRAO")
    is_group = dados.get("isGroup") is True
    from_me = dados.get("fromMe") is True
    tipo_log = "ENVIADA" if from_me else "RECEBIDA"

    sender_data = dados.get("sender") or dados.get("remetente") or {}
    chat_data = dados.get("chat") or {}
    
    telefone_bruto = ""
    push_name = ""
    id_grupo = None

    if is_group:
        # GRUPO
        id_grupo = chat_data.get("id") # ID do Grupo (ex: 123456@g.us)
        
        if from_me:
             telefone_bruto = sender_data.get("id") 
             push_name = sender_data.get("pushName") or "Sistema"
        else:
            telefone_bruto = sender_data.get("id")
            push_name = sender_data.get("pushName") or "Membro do Grupo"
        
    else:
        # INDIVIDUAL
        if from_me:
            telefone_bruto = chat_data.get("id")
            push_name = "Cliente (Destino)"
        else:
            telefone_bruto = sender_data.get("id")
            push_name = sender_data.get("pushName") or "Cliente"

    telefone_limpo = limpar_telefone(telefone_bruto)

    # Conteúdo da Mensagem
    msg_content = dados.get("msgContent", {})
    mensagem = ""
    if "extendedTextMessage" in msg_content:
        mensagem = msg_content["extendedTextMessage"].get("text")
    elif "conversation" in msg_content:
        mensagem = msg_content.get("conversation")
    elif "text" in msg_content:
        mensagem = msg_content.get("text")
    else:
        mensagem = "Mídia/Outros"

    dados_processados = {
        "instance_id": instance_id,
        "telefone": telefone_limpo,
        "mensagem": mensagem,
        "tipo": tipo_log,
        "nome_contato": push_name,
        "is_group": is_group,
        "id_grupo": id_grupo
    }

    if telefone_limpo or id_grupo:
        gerenciar_banco_dados(dados_processados)
        return jsonify({"status": "processado"}), 200
    else:
        return jsonify({"status": "sem_identificacao"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)