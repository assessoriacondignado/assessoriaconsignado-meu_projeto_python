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
    Remove caracteres não numéricos, trata 9º dígito e remove prefixo 55 se houver.
    """
    if not telefone_bruto: return None
    temp = telefone_bruto.split('@')[0]
    limpo = re.sub(r'[^0-9]', '', temp)
    
    # Regra básica do 9º dígito BR
    if len(limpo) == 12 and limpo.startswith("55"):
        if int(limpo[4]) >= 6:
            limpo = f"{limpo[:4]}9{limpo[4:]}"

    # Remove o 55
    if limpo.startswith("55") and len(limpo) >= 10:
        limpo = limpo[2:] 

    return limpo

def gerenciar_banco_dados(dados_proc):
    """
    Grava os dados processados no banco de dados.
    Lógica de Cascata: Tenta Grupo -> Se falhar, Tenta Telefone.
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
        # 1. TENTATIVA 1: BUSCA PELO GRUPO (Se for mensagem de grupo)
        # ======================================================================
        if is_group and id_grupo:
            cur.execute("SELECT id, nome FROM admin.clientes WHERE id_grupo_whats = %s LIMIT 1", (id_grupo,))
            res_grupo = cur.fetchone()
            
            if res_grupo:
                id_cliente_final = res_grupo[0]
                nome_cliente_final = res_grupo[1]
                # Se achou pelo grupo, não precisamos mudar o nome_para_log (mantém quem enviou)
                # Mas sabemos que o "Cliente" pagador é a empresa do grupo.

        # ======================================================================
        # 2. TENTATIVA 2: BUSCA PELO TELEFONE (Se id_cliente ainda for None)
        # ======================================================================
        # Entra aqui se: (Não é grupo) OU (É grupo mas não achou o cadastro do grupo)
        if not id_cliente_final and telefone:
            
            # A) Verifica na tabela de triagem (wapi_numeros) se esse número já tem dono
            cur.execute("SELECT id, id_cliente, nome_cliente FROM admin.wapi_numeros WHERE telefone = %s", (telefone,))
            res_num = cur.fetchone()
            
            if res_num:
                # Já existe na triagem
                cur.execute("UPDATE admin.wapi_numeros SET data_ultima_interacao = NOW() WHERE telefone = %s", (telefone,))
                
                # Só pega o ID se tiver vinculado na triagem
                if res_num[1]: 
                    id_cliente_final = res_num[1]
                    nome_cliente_final = res_num[2]
            
            else:
                # B) Se não está na triagem, tenta Auto-Match na tabela clientes (Pelo Telefone)
                busca_tel = f"%{telefone[-8:]}"
                cur.execute("SELECT id, nome FROM admin.clientes WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
                res_cli = cur.fetchone()
                
                if res_cli:
                    id_cliente_final = res_cli[0]
                    nome_cliente_final = res_cli[1]
                    # Se achou pelo telefone pessoal, atualiza o nome do log para o nome oficial
                    if not is_group: nome_para_log = nome_cliente_final 
                
                # Registra esse novo número na tabela de triagem
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
        
        # Definição do nome do grupo para o log
        nome_grupo_log = None
        if is_group:
            # Se achamos o cliente, usa o nome dele como nome do grupo no log para facilitar leitura
            nome_grupo_log = nome_cliente_final if nome_cliente_final else id_grupo

        cur.execute(sql_log, (
            dados_proc['instance_id'],    
            telefone,                     
            nome_para_log,                
            dados_proc['mensagem'],       
            dados_proc['tipo'],           
            dados_proc['id_grupo'],       
            nome_grupo_log,               
            id_cliente_final,             
            nome_cliente_final            
        ))
        
        conn.commit()
        
        categoria = "GRUPO" if is_group else "INDIVIDUAL"
        match_status = f"✅ ID: {id_cliente_final} ({nome_cliente_final})" if id_cliente_final else "⚠️ Não Identificado"
        print(f"💾 LOG: {categoria} | {match_status}", flush=True)
        
        cur.close(); conn.close()

    except Exception as e:
        print(f" ❌ Erro ao gravar no banco: {e}", flush=True)
        if conn: conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    if not dados: return jsonify({"status": "vazio"}), 200
    
    # --- 1. JSON BACKUP ---
    try:
        pasta_json = os.path.join(BASE_DIR, "WAPI_WEBHOOK_JASON")
        if not os.path.exists(pasta_json): os.makedirs(pasta_json)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        evento_nome = dados.get('event', 'msg')
        with open(os.path.join(pasta_json, f"{timestamp}_{evento_nome}.json"), "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=4, ensure_ascii=False)
    except: pass

    # --- 2. FILTRO ---
    event = dados.get("event")
    if event not in ["webhookReceived", "webhookDelivery", "message.received", "message.sent"]:
        return jsonify({"status": "ignorado"}), 200

    # --- 3. EXTRAÇÃO ---
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
        id_grupo = chat_data.get("id") 
        if from_me:
             telefone_bruto = sender_data.get("id") 
             push_name = sender_data.get("pushName") or "Sistema"
        else:
            telefone_bruto = sender_data.get("id")
            push_name = sender_data.get("pushName") or "Membro"
    else:
        if from_me:
            telefone_bruto = chat_data.get("id")
            push_name = "Cliente (Destino)"
        else:
            telefone_bruto = sender_data.get("id")
            push_name = sender_data.get("pushName") or "Cliente"

    telefone_limpo = limpar_telefone(telefone_bruto)

    # Conteúdo
    msg_content = dados.get("msgContent", {})
    mensagem = msg_content.get("conversation") or \
               msg_content.get("extendedTextMessage", {}).get("text") or \
               msg_content.get("text") or "Mídia/Outros"

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