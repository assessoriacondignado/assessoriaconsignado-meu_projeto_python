import sys
import os
from flask import Flask, request, jsonify
import psycopg2
import re
import json
from datetime import datetime

# --- CONFIGURAÇÃO DE CAMINHO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

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
    """Remove caracteres não numéricos e o sufixo do whatsapp"""
    if not telefone_bruto: return None
    # Remove o @...
    temp = telefone_bruto.split('@')[0]
    # Deixa apenas números
    limpo = re.sub(r'[^0-9]', '', temp)
    
    # Regra básica do 9º dígito BR (opcional, mas recomendada para padronizar)
    if len(limpo) == 12 and limpo.startswith("55"):
        if int(limpo[4]) >= 6:
            limpo = f"{limpo[:4]}9{limpo[4:]}"
    return limpo

def gerenciar_banco_dados(dados_processados):
    """
    Grava no banco de dados seguindo a regra:
    - Se NÃO for grupo: Registra/Atualiza cliente em admin.wapi_numeros
    - Sempre: Grava log em admin.wapi_logs
    """
    conn = get_conn()
    if not conn: return
    
    telefone = dados_processados['telefone']
    is_group = dados_processados['is_group']
    push_name = dados_processados['nome_contato']
    
    try:
        cur = conn.cursor()
        
        id_cliente_final = None
        nome_cliente_final = None
        # Nome para o log assume o pushname, a menos que achemos o cliente no banco
        nome_para_log = push_name 

        # --- REGRA 1: SE O ISGRUPO for false, deve registrar cliente ---
        if not is_group and telefone:
            # Verifica se já existe na tabela de números capturados
            cur.execute("SELECT id, id_cliente, nome_cliente FROM admin.wapi_numeros WHERE telefone = %s", (telefone,))
            res_num = cur.fetchone()
            
            if res_num:
                # Atualiza interação
                cur.execute("UPDATE admin.wapi_numeros SET data_ultima_interacao = NOW() WHERE telefone = %s", (telefone,))
                id_cliente_final = res_num[1]
                nome_cliente_final = res_num[2]
                if nome_cliente_final: nome_para_log = nome_cliente_final
            else:
                # Tenta achar em clientes oficiais para vincular
                busca_tel = f"%{telefone[-8:]}"
                cur.execute("SELECT id, nome FROM admin.clientes WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
                res_cli = cur.fetchone()
                
                if res_cli:
                    id_cliente_final = res_cli[0]
                    nome_cliente_final = res_cli[1]
                    nome_para_log = nome_cliente_final
                
                # Registra novo número
                cur.execute("""
                    INSERT INTO admin.wapi_numeros (telefone, id_cliente, nome_cliente, data_ultima_interacao) 
                    VALUES (%s, %s, %s, NOW())
                """, (telefone, id_cliente_final, nome_cliente_final))

        # --- REGRA 2: GRAVAÇÃO DO LOG (Sempre ocorre) ---
        # Campos com * preenchidos conforme solicitação
        sql_log = """
            INSERT INTO admin.wapi_logs (
                instance_id, telefone, nome_contato, mensagem, tipo, 
                status, id_grupo, grupo, cpf_cliente, id_cliente, nome_cliente, data_hora
            ) VALUES (%s, %s, %s, %s, %s, 'Sucesso', %s, %s, NULL, %s, %s, NOW())
        """
        
        cur.execute(sql_log, (
            dados_processados['instance_id'],   # * instance_id
            telefone,                           # * telefone
            nome_para_log,                      # * nome_contato
            dados_processados['mensagem'],      # * mensagem
            dados_processados['tipo'],          # * tipo
            dados_processados['id_grupo'],      # * id_grupo
            dados_processados['id_grupo'],      # * grupo (usando ID como nome base se não houver outro)
            id_cliente_final,
            nome_cliente_final
        ))
        
        conn.commit()
        
        # LOG VISUAL NO CONSOLE
        categoria = "GRUPO" if is_group else "CLIENTE"
        print(f"💾 LOG GRAVADO: {dados_processados['tipo']} / {telefone} / {categoria}", flush=True)
        
        cur.close(); conn.close()

    except Exception as e:
        print(f" ❌ Erro ao gravar no banco: {e}", flush=True)
        if conn: conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    if not dados: return jsonify({"status": "vazio"}), 200
    
    # ==============================================================================
    # 1º PASSO: GRAVA NO WEBHOOK.LOG (ARQUIVO JSON) ANTES DE TUDO
    # ==============================================================================
    try:
        pasta_json = os.path.join(BASE_DIR, "WAPI_WEBHOOK_JASON")
        if not os.path.exists(pasta_json): os.makedirs(pasta_json)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        evento_nome = dados.get('event', 'msg')
        nome_arquivo = f"{timestamp}_{evento_nome}.json"
        
        with open(os.path.join(pasta_json, nome_arquivo), "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"Erro ao salvar JSON: {e}")

    # ==============================================================================
    # 2º PASSO: IDENTIFICAÇÃO E MAPEAMENTO DAS VARIÁVEIS
    # ==============================================================================
    event = dados.get("event")
    eventos_aceitos = ["webhookReceived", "webhookDelivery", "message.received", "message.sent"]
    
    if event not in eventos_aceitos:
        return jsonify({"status": "ignorado"}), 200

    # Variáveis Base
    instance_id = dados.get("instanceId", "PADRAO")
    is_group = dados.get("isGroup") is True
    from_me = dados.get("fromMe") is True
    
    # Determina o TIPO (ENVIADA ou RECEBIDA)
    tipo_log = "ENVIADA" if from_me else "RECEBIDA"

    # Determina TELEFONE e NOME
    # Se for envio (delivery), 'chat.id' costuma ser o destino (cliente), e 'remetente' sou eu.
    # Se for recebimento, 'sender.id' é quem mandou.
    
    sender_data = dados.get("sender") or dados.get("remetente") or {}
    
    telefone_bruto = ""
    push_name = ""
    id_grupo = None

    if is_group:
        # --- CENÁRIO: GRUPO (Recebido ou Enviado) ---
        # Regra: Webhook envia id do grupo + telefone do cliente (sender)
        id_grupo = dados.get("chat", {}).get("id")
        telefone_bruto = sender_data.get("id") # Quem mandou a msg no grupo
        push_name = sender_data.get("pushName") or "Membro do Grupo"
        
    else:
        # --- CENÁRIO: CLIENTE INDIVIDUAL ---
        if from_me:
            # Enviada PARA o cliente (chat.id é o cliente)
            telefone_bruto = dados.get("chat", {}).get("id")
            push_name = "Cliente (Destino)" # Em envios, as vezes não temos o nome atualizado do destino no hook
        else:
            # Recebida DO cliente (sender.id é o cliente)
            telefone_bruto = sender_data.get("id")
            push_name = sender_data.get("pushName") or "Cliente"

    # Limpeza do número
    telefone_limpo = limpar_telefone(telefone_bruto)

    # Conteúdo da Mensagem (Regra: extendedTextMessage)
    msg_content = dados.get("msgContent", {})
    mensagem = ""
    
    # Tenta pegar conforme hierarquia de prioridade
    if "extendedTextMessage" in msg_content:
        mensagem = msg_content["extendedTextMessage"].get("text")
    elif "conversation" in msg_content:
        mensagem = msg_content.get("conversation")
    elif "text" in msg_content:
        mensagem = msg_content.get("text")
    else:
        mensagem = "Conteúdo não textual (Imagem/Audio/Sticker)"

    # ==============================================================================
    # 3º PASSO: GRAVAÇÃO NO BANCO DE DADOS (LOG E PLANILHA)
    # ==============================================================================
    
    dados_processados = {
        "instance_id": instance_id,
        "telefone": telefone_limpo,
        "mensagem": mensagem,
        "tipo": tipo_log,
        "nome_contato": push_name,
        "is_group": is_group,
        "id_grupo": id_grupo
    }

    if telefone_limpo:
        gerenciar_banco_dados(dados_processados)
        return jsonify({"status": "processado"}), 200
    else:
        return jsonify({"status": "sem_telefone"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)