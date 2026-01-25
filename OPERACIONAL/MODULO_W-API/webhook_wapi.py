import sys
import os
from flask import Flask, request, jsonify
import psycopg2
import re
import json
from datetime import datetime
import pandas as pd

# Tenta importar streamlit
try:
    import streamlit as st
except ImportError:
    st = None

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

# ==============================================================================
#  INTERFACE VISUAL (STREAMLIT)
# ==============================================================================
def app_registros():
    if st is None: return
    st.markdown("### 📋 Histórico de Logs (Webhook)")
    st.markdown("---")
    conn = get_conn()
    if not conn:
        st.error("Erro ao conectar ao banco de dados.")
        return
    try:
        query = """
            SELECT id, instance_id, data_hora, tipo, telefone, id_cliente, nome_cliente,
                   nome_contato, id_grupo, grupo, mensagem, status 
            FROM admin.wapi_logs ORDER BY data_hora DESC LIMIT 500
        """
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
            if st.button("🔄 Atualizar Lista"): st.rerun()
        else:
            st.info("Nenhum registro encontrado.")
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
    finally:
        conn.close()

# ==============================================================================
#  FUNÇÕES BACKEND
# ==============================================================================

def limpar_telefone(telefone_bruto):
    """Remove 55 e formata."""
    if not telefone_bruto: return None
    if "@g.us" in str(telefone_bruto): return str(telefone_bruto).strip()
    temp = str(telefone_bruto).split('@')[0]
    limpo = re.sub(r'[^0-9]', '', temp)
    if len(limpo) == 12 and limpo.startswith("55"):
        if int(limpo[4]) >= 6: limpo = f"{limpo[:4]}9{limpo[4:]}"
    if limpo.startswith("55") and len(limpo) >= 10:
        limpo = limpo[2:] 
    return limpo

def processar_mensagem(dados_proc):
    """
    ABORDAGEM: SALVAR PRIMEIRO -> ATUALIZAR DEPOIS
    1. Insere o log bruto.
    2. Se for Grupo, busca cliente e atualiza o registro criado.
    """
    conn = get_conn()
    if not conn: return
    
    try:
        cur = conn.cursor()
        
        # --- PASSO 1: GRAVAR O BÁSICO (INSERT) ---
        # Salvamos o log imediatamente, sem ID de cliente ainda.
        # Usamos RETURNING id para saber qual linha acabamos de criar.
        
        sql_insert = """
            INSERT INTO admin.wapi_logs (
                data_hora, instance_id, telefone, nome_contato, mensagem, tipo, 
                status, id_grupo, grupo
            ) VALUES (NOW(), %s, %s, %s, %s, %s, 'Sucesso', %s, %s)
            RETURNING id
        """
        
        # Se for grupo, salvamos o ID do grupo na coluna 'grupo' temporariamente
        # ou o nome do grupo se vier do whats, para não ficar vazio.
        valor_grupo_inicial = dados_proc.get('nome_grupo') if dados_proc.get('nome_grupo') else dados_proc['id_grupo']

        cur.execute(sql_insert, (
            dados_proc['instance_id'],
            dados_proc['telefone'],
            dados_proc['nome_contato'],
            dados_proc['mensagem'],
            dados_proc['tipo'],
            dados_proc['id_grupo'],
            valor_grupo_inicial
        ))
        
        # Captura o ID do log que acabou de ser gerado (Ex: 35, 36...)
        novo_log_id = cur.fetchone()[0]
        conn.commit() # Salva garantido!
        
        print(f"💾 Log Básico Salvo! ID do Registro: {novo_log_id}", flush=True)

        # --- PASSO 2: GATILHO DE ATUALIZAÇÃO (UPDATE) ---
        # Agora, com calma, verificamos se precisamos buscar o cliente
        
        id_cliente_encontrado = None
        nome_cliente_encontrado = None
        
        # Verifica se é grupo e tem ID
        if dados_proc['is_group'] and dados_proc['id_grupo']:
            
            id_grupo_busca = str(dados_proc['id_grupo']).strip()
            print(f"🔎 Gatilho Grupo Acionado: Buscando '{id_grupo_busca}'...", flush=True)
            
            # Busca na tabela de clientes (com TRIM para segurança)
            cur.execute("SELECT id, nome FROM admin.clientes WHERE TRIM(id_grupo_whats) = %s LIMIT 1", (id_grupo_busca,))
            res_grupo = cur.fetchone()
            
            if res_grupo:
                id_cliente_encontrado = res_grupo[0]
                nome_cliente_encontrado = res_grupo[1]
                print(f"✅ Cliente Encontrado no Grupo: {nome_cliente_encontrado}", flush=True)
                
                # ATUALIZA O REGISTRO RECÉM CRIADO
                # Atualiza: id_cliente, nome_cliente e força o nome do cliente na coluna 'grupo' também
                sql_update = """
                    UPDATE admin.wapi_logs 
                    SET id_cliente = %s, 
                        nome_cliente = %s,
                        grupo = %s 
                    WHERE id = %s
                """
                cur.execute(sql_update, (
                    id_cliente_encontrado, 
                    nome_cliente_encontrado, 
                    nome_cliente_encontrado, # Atualiza coluna grupo com o nome do cliente
                    novo_log_id
                ))
                conn.commit()
                print("🔄 Registro Atualizado com Sucesso!", flush=True)
            else:
                print("⚠️ Grupo não vinculado a nenhum cliente.", flush=True)

        # --- PASSO EXTRA: Se não for grupo, tenta buscar pelo telefone (opcional, mas recomendado) ---
        elif not dados_proc['is_group'] and dados_proc['telefone']:
             cur.execute("SELECT id, nome FROM admin.clientes WHERE telefone = %s LIMIT 1", (dados_proc['telefone'],))
             res_cli = cur.fetchone()
             if res_cli:
                 id_cliente_encontrado = res_cli[0]
                 nome_cliente_encontrado = res_cli[1]
                 
                 cur.execute("UPDATE admin.wapi_logs SET id_cliente=%s, nome_cliente=%s WHERE id=%s", 
                             (id_cliente_encontrado, nome_cliente_encontrado, novo_log_id))
                 conn.commit()
                 print(f"✅ Cliente identificado por Telefone e atualizado.", flush=True)

        cur.close()
        conn.close()

    except Exception as e:
        if conn: conn.rollback()
        print(f"❌ Erro no processamento: {e}", flush=True)
        if conn: conn.close()

# ==============================================================================
#  SERVIDOR WEBHOOK
# ==============================================================================
@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    if not dados: return jsonify({"status": "vazio"}), 200
    
    # 1. Log JSON
    try:
        pasta_json = os.path.join(BASE_DIR, "WAPI_WEBHOOK_JASON")
        if not os.path.exists(pasta_json): os.makedirs(pasta_json)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        evento_nome = dados.get('event', 'msg')
        with open(os.path.join(pasta_json, f"{timestamp}_{evento_nome}.json"), "w", encoding="utf-8") as f:
            json.dump(dados, f, indent=4, ensure_ascii=False)
    except: pass

    # 2. Filtros
    event = dados.get("event")
    if event not in ["webhookReceived", "webhookDelivery", "message.received", "message.sent"]:
        return jsonify({"status": "ignorado"}), 200

    # 3. Extração
    instance_id = dados.get("instanceId", "PADRAO")
    is_group = dados.get("isGroup") is True
    from_me = dados.get("fromMe") is True
    tipo_log = "ENVIADA" if from_me else "RECEBIDA"

    sender_data = dados.get("sender") or dados.get("remetente") or {}
    chat_data = dados.get("chat") or {}
    
    telefone_bruto = ""
    push_name = ""
    id_grupo = None
    nome_grupo = None 

    if is_group:
        id_grupo = chat_data.get("id")
        nome_grupo = chat_data.get("name") or chat_data.get("subject")
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

    # Limpeza do telefone (Padrão: sem 55)
    telefone_limpo = limpar_telefone(telefone_bruto)

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
        "id_grupo": id_grupo,
        "nome_grupo": nome_grupo
    }

    if telefone_limpo or id_grupo:
        processar_mensagem(dados_processados)
        return jsonify({"status": "processado"}), 200
    else:
        return jsonify({"status": "sem_identificacao"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)