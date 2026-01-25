import sys
import os
from flask import Flask, request, jsonify
import psycopg2
import re
import json
from datetime import datetime
import pandas as pd

# Tenta importar streamlit, mas não falha se for rodar apenas como API/Webhook num env sem ele
try:
    import streamlit as st
except ImportError:
    st = None

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

# ==============================================================================
#  INTERFACE VISUAL (STREAMLIT) - ABA LOGS
# ==============================================================================
def app_registros():
    """
    Função chamada pelo sistema principal para exibir a tela de logs.
    """
    if st is None:
        print("Streamlit não instalado neste ambiente.")
        return

    st.markdown("### 📋 Histórico de Logs (Webhook)")
    st.markdown("---")

    conn = get_conn()
    if not conn:
        st.error("Erro ao conectar ao banco de dados.")
        return

    try:
        # Busca os últimos 500 registros
        query = """
            SELECT 
                instance_id,
                data_hora, 
                tipo, 
                telefone, 
                id_cliente,
                nome_cliente,
                nome_contato, 
                grupo, 
                mensagem, 
                status 
            FROM admin.wapi_logs 
            ORDER BY data_hora DESC 
            LIMIT 500
        """
        
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            st.dataframe(
                df, 
                use_container_width=True,
                hide_index=True,
                column_config={
                    "instance_id": "Instância",
                    "data_hora": st.column_config.DatetimeColumn("Data/Hora", format="DD/MM/YYYY HH:mm:ss"),
                    "tipo": "Tipo",
                    "telefone": "Telefone",
                    "id_cliente": "ID Cliente",
                    "nome_cliente": "Cliente Identificado",
                    "nome_contato": "Contato (PushName)",
                    "grupo": "Grupo / Origem",
                    "mensagem": "Conteúdo",
                    "status": "Status"
                }
            )
            
            if st.button("🔄 Atualizar Lista"):
                st.rerun()
        else:
            st.info("Nenhum registro encontrado.")

    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
    finally:
        conn.close()

# ==============================================================================
#  FUNÇÕES UTILITÁRIAS (BACKEND)
# ==============================================================================

def limpar_telefone(telefone_bruto):
    """
    Remove caracteres não numéricos, trata 9º dígito
    e REMOVE O 55 (DDI BRASIL) para padronizar no banco.
    """
    if not telefone_bruto: return None
    
    # Se for ID de grupo, retorna limpo (apenas strip)
    if "@g.us" in str(telefone_bruto):
        return str(telefone_bruto).strip()

    temp = str(telefone_bruto).split('@')[0]
    limpo = re.sub(r'[^0-9]', '', temp)
    
    # Regra básica do 9º dígito BR
    if len(limpo) == 12 and limpo.startswith("55"):
        if int(limpo[4]) >= 6:
            limpo = f"{limpo[:4]}9{limpo[4:]}"

    # --- REMOVE O 55 ---
    # Verifica se começa com 55 e tem tamanho de telefone (DD+NUMERO)
    if limpo.startswith("55") and len(limpo) >= 10:
        limpo = limpo[2:] 

    return limpo

def gerenciar_banco_dados(dados_proc):
    """
    Grava os dados processados no banco de dados.
    PRIORIDADE DE IDENTIFICAÇÃO:
    1. Busca Cliente pelo TELEFONE.
    2. Se falhar, Busca Cliente pelo ID DO GRUPO (se for grupo).
    3. Registra na Triagem.
    """
    conn = get_conn()
    if not conn: return
    
    try:
        cur = conn.cursor()
        
        telefone = dados_proc['telefone'] # Já vem sem o 55
        is_group = dados_proc['is_group']
        id_grupo = dados_proc['id_grupo'].strip() if dados_proc['id_grupo'] else None
        
        push_name = dados_proc['nome_contato']
        nome_grupo_orig = dados_proc.get('nome_grupo')
        
        id_cliente_final = None
        nome_cliente_final = None
        nome_para_log = push_name 

        # ======================================================================
        # 1. TENTATIVA PRIORITÁRIA: BUSCA PELO TELEFONE
        # ======================================================================
        # Verifica se o número que mandou mensagem é de um cliente cadastrado
        if telefone:
            # Busca pelos últimos 8 dígitos para garantir match
            busca_tel = f"%{telefone[-8:]}"
            cur.execute("SELECT id, nome FROM admin.clientes WHERE telefone LIKE %s LIMIT 1", (busca_tel,))
            res_cli = cur.fetchone()
            
            if res_cli:
                id_cliente_final = res_cli[0]
                nome_cliente_final = res_cli[1]
                # Se achou pelo telefone, este é o nome principal (exceto em grupos, onde mantemos pushname no contato)
                if not is_group: nome_para_log = nome_cliente_final
                
                print(f"✅ Identificado por TELEFONE: {nome_cliente_final}", flush=True)

        # ======================================================================
        # 2. TENTATIVA SECUNDÁRIA: BUSCA PELO GRUPO (Fallback)
        # ======================================================================
        # Se NÃO achou pelo telefone E é uma mensagem de grupo, tenta pelo ID do Grupo
        if not id_cliente_final and is_group and id_grupo:
            sql_grupo = "SELECT id, nome FROM admin.clientes WHERE TRIM(id_grupo_whats) = %s LIMIT 1"
            cur.execute(sql_grupo, (id_grupo,))
            res_grupo = cur.fetchone()
            
            if res_grupo:
                id_cliente_final = res_grupo[0]
                nome_cliente_final = res_grupo[1]
                print(f"✅ Identificado por GRUPO: {nome_cliente_final}", flush=True)

        # ======================================================================
        # 3. GESTÃO DA TABELA DE TRIAGEM (WAPI_NUMEROS)
        # ======================================================================
        if telefone:
            # Verifica se já existe na triagem
            cur.execute("SELECT id, id_cliente, nome_cliente FROM admin.wapi_numeros WHERE telefone = %s", (telefone,))
            res_num = cur.fetchone()
            
            if res_num:
                # Se já existe, atualiza data
                cur.execute("UPDATE admin.wapi_numeros SET data_ultima_interacao = NOW() WHERE telefone = %s", (telefone,))
                
                # Se ainda não tínhamos identificado o cliente, mas na triagem tem vínculo manual, usamos ele
                if not id_cliente_final and res_num[1]:
                    id_cliente_final = res_num[1]
                    nome_cliente_final = res_num[2]
                    if not is_group: nome_para_log = nome_cliente_final
                
                # Se agora identificamos (nos passos 1 ou 2), atualizamos a triagem para ficar igual
                if id_cliente_final and (res_num[1] != id_cliente_final):
                     cur.execute("UPDATE admin.wapi_numeros SET id_cliente = %s, nome_cliente = %s WHERE telefone = %s", 
                                 (id_cliente_final, nome_cliente_final, telefone))

            else:
                # Se não existe, insere novo registro na triagem
                cur.execute("""
                    INSERT INTO admin.wapi_numeros (telefone, id_cliente, nome_cliente, data_ultima_interacao) 
                    VALUES (%s, %s, %s, NOW())
                """, (telefone, id_cliente_final, nome_cliente_final))

        # ======================================================================
        # GRAVAÇÃO DO LOG FINAL
        # ======================================================================
        sql_log = """
            INSERT INTO admin.wapi_logs (
                instance_id, telefone, nome_contato, mensagem, tipo, 
                status, id_grupo, grupo, cpf_cliente, id_cliente, nome_cliente, data_hora
            ) VALUES (%s, %s, %s, %s, %s, 'Sucesso', %s, %s, NULL, %s, %s, NOW())
        """
        
        # Define o nome que aparecerá na coluna 'grupo'
        valor_grupo_nome = None
        if is_group:
            # Preferência visual: Nome do Cliente > Nome do Grupo (Whats) > ID do Grupo
            if nome_cliente_final:
                valor_grupo_nome = nome_cliente_final
            elif nome_grupo_orig:
                valor_grupo_nome = nome_grupo_orig
            else:
                valor_grupo_nome = id_grupo

        cur.execute(sql_log, (
            dados_proc['instance_id'],
            telefone,
            nome_para_log,
            dados_proc['mensagem'],
            dados_proc['tipo'],
            dados_proc['id_grupo'],
            valor_grupo_nome,
            id_cliente_final,
            nome_cliente_final
        ))
        
        conn.commit()
        
        categoria = "GRUPO" if is_group else "CLIENTE"
        status_ident = f"✅ {nome_cliente_final}" if nome_cliente_final else "⚠️ Não Identificado"
        print(f"💾 LOG: {categoria} | {telefone} | {status_ident}", flush=True)
        
        cur.close(); conn.close()

    except Exception as e:
        print(f" ❌ Erro ao gravar no banco: {e}", flush=True)
        if conn: conn.close()

# ==============================================================================
#  SERVIDOR WEBHOOK (FLASK)
# ==============================================================================
@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.json
    if not dados: return jsonify({"status": "vazio"}), 200
    
    # 1. Log JSON (Backup)
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

    # 2. Filtros
    event = dados.get("event")
    eventos_aceitos = ["webhookReceived", "webhookDelivery", "message.received", "message.sent"]
    if event not in eventos_aceitos:
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
            push_name = sender_data.get("pushName") or "Membro do Grupo"
    else:
        if from_me:
            telefone_bruto = chat_data.get("id")
            push_name = "Cliente (Destino)"
        else:
            telefone_bruto = sender_data.get("id")
            push_name = sender_data.get("pushName") or "Cliente"

    # Aplica a limpeza (Remove o 55)
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
        gerenciar_banco_dados(dados_processados)
        return jsonify({"status": "processado"}), 200
    else:
        return jsonify({"status": "sem_identificacao"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)