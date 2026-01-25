import psycopg2
import requests
import re
import json # Importante para tratar erros de JSON

try: 
    import conexao
except ImportError:
    print("Erro crítico: Arquivo conexao.py não localizado no servidor.")

def get_conn():
    """Estabelece conexão com o banco de dados usando as configurações do arquivo conexao.py"""
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        print(f"Erro de conexão DB: {e}")
        return None

# ==========================================================
# 0. FUNÇÃO UTILITÁRIA DE LIMPEZA (PADRÃO DO SISTEMA)
# ==========================================================
def limpar_telefone(telefone_bruto):
    """
    Remove caracteres não numéricos, trata 9º dígito
    e REMOVE O 55 (DDI BRASIL) para padronizar no banco e envio.
    """
    if not telefone_bruto: return None
    
    # Se for grupo (contém @g.us), retorna como está (apenas strip)
    if "@g.us" in str(telefone_bruto):
        return str(telefone_bruto).strip()

    # Limpeza básica
    temp = str(telefone_bruto).split('@')[0]
    limpo = re.sub(r'[^0-9]', '', temp)
    
    # Regra básica do 9º dígito BR (com 55)
    if len(limpo) == 12 and limpo.startswith("55"):
        if int(limpo[4]) >= 6:
            limpo = f"{limpo[:4]}9{limpo[4:]}"

    # --- REMOVE O 55 ---
    # Verifica se começa com 55 e tem tamanho de telefone (DD+NUMERO)
    if limpo.startswith("55") and len(limpo) >= 10:
        limpo = limpo[2:] 

    return limpo

# ==========================================================
# 1. FUNÇÕES DE API (W-API)
# ==========================================================
BASE_URL = "https://api.w-api.app/v1"

def enviar_msg_api(instance_id, token, to, message):
    """Envia mensagem de texto via API"""
    url = f"{BASE_URL}/message/send-text?instanceId={instance_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # Usa a função padronizada de limpeza
    contato_limpo = limpar_telefone(to)
    
    payload = {"phone": contato_limpo, "message": message, "delayMessage": 3}
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        return res.json()
    except Exception as e: 
        return {"success": False, "error": str(e)}

def enviar_midia_api(instance_id, token, to, base64_data, file_name, caption=""):
    """Envia arquivo/mídia via API (Base64)"""
    url = f"{BASE_URL}/message/send-media?instanceId={instance_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # Usa a função padronizada de limpeza
    contato_limpo = limpar_telefone(to)
    
    payload = {
        "phone": contato_limpo,
        "media": base64_data,
        "caption": caption,
        "fileName": file_name,
        "delayMessage": 3
    }
    
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=60)
        try:
            return res.json()
        except ValueError:
            return {"success": False, "error": f"Erro API (Não JSON): {res.text} - Code: {res.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def obter_qrcode_api(instance_id, token):
    """Obtém o buffer da imagem do QR Code"""
    url = f"{BASE_URL}/instance/qr-code"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"instanceId": instance_id, "image": "enable"}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        return res.content if res.status_code == 200 else None
    except: return None

def obter_otp_api(instance_id, token, phone):
    """Solicita o código de pareamento (OTP)"""
    url = f"{BASE_URL}/instance/connect-phone"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"instanceId": instance_id, "phone": phone}
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        return res.json()
    except: return None

def checar_status_api(instance_id, token):
    """Verifica o status da instância na API"""
    url = f"{BASE_URL}/instance/status-instance"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"instanceId": instance_id}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        if res.status_code == 200:
            return res.json()
        return {"state": "erro_api", "details": res.text}
    except Exception as e: return {"state": "erro_req", "details": str(e)}

def obter_info_instancia(instance_id, token):
    """Obtém informações do perfil (Foto, Nome, Número)"""
    url = f"{BASE_URL}/instance/info" 
    headers = {"Authorization": f"Bearer {token}"}
    params = {"instanceId": instance_id}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        
        # Se sucesso, retorna o JSON
        if res.status_code == 200:
            return res.json()
            
        # Se falha, retorna um dicionário com o erro para debug
        return {
            "error": True, 
            "status_code": res.status_code, 
            "message": res.text
        }
    except Exception as e: 
        return {"error": True, "message": str(e)}

# ==========================================================
# 2. FUNÇÕES DE SUPORTE (BANCO DE DADOS)
# ==========================================================

def buscar_instancia_ativa():
    """Retorna a primeira instância configurada no banco"""
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
            res = cur.fetchone()
            conn.close()
            return res 
        except: 
            conn.close()
            return None
    return None

def buscar_template(modulo, chave):
    """Busca o texto de um modelo de mensagem no banco"""
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT conteudo_mensagem FROM wapi_templates WHERE modulo = %s AND chave_status = %s", (modulo, chave))
            res = cur.fetchone()
            conn.close()
            return res[0] if res else ""
        except:
            conn.close()
            return ""
    return ""