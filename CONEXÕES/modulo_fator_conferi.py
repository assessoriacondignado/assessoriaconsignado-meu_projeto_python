import streamlit as st
import pandas as pd
import psycopg2
import requests
import json
import os
import time
import re
import base64
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
import conexao

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PASTA_JSON = os.path.join(BASE_DIR, "JSON")

try:
    if not os.path.exists(PASTA_JSON):
        os.makedirs(PASTA_JSON, exist_ok=True)
except Exception as e:
    st.error(f"Erro crítico de permissão ao criar pasta JSON: {e}")

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

# =============================================================================
# 1. FUNÇÕES AUXILIARES
# =============================================================================

def buscar_credenciais():
    conn = get_conn()
    cred = {"url": "https://fator.confere.link/api/", "token": ""}
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT key_conexao FROM conexoes.relacao WHERE nome_conexao ILIKE '%FATOR%' LIMIT 1")
            res = cur.fetchone()
            if res: cred["token"] = res[0]
        except: pass
        finally: conn.close()
    return cred

def buscar_valor_consulta_atual():
    conn = get_conn()
    valor = 0.50
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT valor_da_consulta FROM conexoes.fatorconferi_valor_da_consulta ORDER BY id DESC LIMIT 1")
            res = cur.fetchone()
            if res: valor = float(res[0])
        except: pass
        finally: conn.close()
    return valor

def formatar_cpf_cnpj_visual(valor):
    dado = re.sub(r'\D', '', str(valor))
    if len(dado) == 11:
        return f"{dado[:3]}.{dado[3:6]}.{dado[6:9]}-{dado[9:]}"
    elif len(dado) == 14:
        return f"{dado[:2]}.{dado[2:5]}.{dado[5:8]}/{dado[8:12]}-{dado[12:]}"
    return valor

def converter_data_banco(data_str):
    if not data_str: return None
    try:
        return datetime.strptime(data_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except: return None

def get_tag_text(element, tag_name):
    """Busca o texto de uma tag de forma insensível a maiúsculas/minúsculas"""
    if element is None: return None
    res = element.find(tag_name)
    if res is not None: return res.text
    res = element.find(tag_name.upper())
    if res is not None: return res.text
    res = element.find(tag_name.lower())
    if res is not None: return res.text
    return None

def find_tag(element, tag_name):
    """Encontra um elemento filho de forma insensível a maiúsculas/minúsculas"""
    if element is None: return None
    res = element.find(tag_name)
    if res is not None: return res
    res = element.find(tag_name.upper())
    if res is not None: return res
    res = element.find(tag_name.lower())
    if res is not None: return res
    return None

def parse_xml_to_dict(xml_string):
    try:
        xml_string = xml_string.replace('ISO-8859-1', 'UTF-8') 
        root = ET.fromstring(xml_string)
        dados = {}
        
        # 1. DADOS CADASTRAIS
        cad = find_tag(root, 'cadastrais')
        if cad is not None:
            dados['nome'] = get_tag_text(cad, 'nome')
            dados['cpf'] = get_tag_text(cad, 'cpf')
            dados['nascimento'] = get_tag_text(cad, 'nascto')
            dados['mae'] = get_tag_text(cad, 'nome_mae')
            dados['rg'] = get_tag_text(cad, 'rg')
            dados['titulo'] = get_tag_text(cad, 'titulo_eleitor')
            dados['sexo'] = get_tag_text(cad, 'sexo')
        
        # 2. TELEFONES
        telefones = []
        tm = find_tag(root, 'telefones_movel')
        if tm is not None:
            for child in tm:
                if 'telefone' in child.tag.lower():
                    telefones.append({
                        'numero': get_tag_text(child, 'numero'),
                        'tipo': 'MOVEL',
                        'prioridade': get_tag_text(child, 'prioridade')
                    })
        
        tf = find_tag(root, 'telefones_fixo')
        if tf is not None:
            for child in tf:
                if 'telefone' in child.tag.lower():
                    telefones.append({
                        'numero': get_tag_text(child, 'numero'),
                        'tipo': 'FIXO',
                        'prioridade': get_tag_text(child, 'prioridade')
                    })
        dados['telefones'] = telefones

        # 3. EMAILS
        emails = []
        em_root = find_tag(root, 'emails')
        if em_root is not None:
            for em in em_root:
                if 'email' in em.tag.lower() and em.text:
                    emails.append(em.text)
        dados['emails'] = emails

        # 4. ENDEREÇOS
        enderecos = []
        end_root = find_tag(root, 'enderecos')
        if end_root is not None:
            for end in end_root:
                if 'endereco' in end.tag.lower():
                    logr = get_tag_text(end, 'logradouro') or ""
                    num = get_tag_text(end, 'numero') or ""
                    comp = get_tag_text(end, 'complemento') or ""
                    rua_full = f"{logr}, {num} {comp}".strip().strip(',')
                    
                    enderecos.append({
                        'rua': rua_full,
                        'bairro': get_tag_text(end, 'bairro'),
                        'cidade': get_tag_text(end, 'cidade'),
                        'uf': get_tag_text(end, 'estado'),
                        'cep': get_tag_text(end, 'cep')
                    })
        dados['enderecos'] = enderecos
        
        return dados
    except Exception as e:
        return {"erro": f"Falha ao processar XML: {e}", "raw": xml_string}

def consultar_saldo_api():
    cred = buscar_credenciais()
    if not cred['token']: return False, 0.0
    url = f"{cred['url']}?acao=VER_SALDO&TK={cred['token']}"
    try:
        response = requests.get(url, timeout=10)
        valor_texto = response.text.strip()
        if '<' in valor_texto:
            try:
                root = ET.fromstring(valor_texto)
                valor_texto = root.text 
            except: pass
        saldo = float(valor_texto.replace(',', '.')) if valor_texto else 0.0
        
        conn = get_conn()
        if conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO conexoes.fatorconferi_registro_de_saldo (valor_saldo) VALUES (%s)", (saldo,))
            conn.commit(); conn.close()
            
        return True, saldo
    except Exception as e:
        return False, 0.0

def obter_origem_padronizada(nome_origem):
    """Busca o nome correto na tabela de origens para garantir integridade"""
    conn = get_conn()
    origem_final = nome_origem 
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT origem FROM conexoes.fatorconferi_origem_consulta_fator WHERE origem = %s", (nome_origem,))
            res = cur.fetchone()
            if res:
                origem_final = res[0]
            conn.close()
        except:
            if conn: conn.close()
    return origem_final

# =============================================================================
# 2. FUNÇÃO DE DÉBITO FINANCEIRO DINÂMICO
# =============================================================================

def processar_debito_automatico(origem_da_consulta, dados_consulta):
    """
    Regra de Débito:
    1. Localiza o valor na 'cliente.cliente_carteira_lista' (CPF + Origem).
    2. Localiza a tabela na 'cliente.carteiras_config' (Nome da Carteira).
    3. Lança o débito financeiro.
    """
    id_usuario_logado = st.session_state.get('usuario_id')
    if not id_usuario_logado:
        return False, "Usuário não logado."

    conn = get_conn()
    if not conn: return False, "Erro conexão DB."
    
    try:
        cur = conn.cursor()
        
        # Identificar o Cliente (Empresa) vinculado ao Usuário logado
        cur.execute("SELECT cpf, nome FROM admin.clientes WHERE id_usuario_vinculo = %s LIMIT 1", (id_usuario_logado,))
        res_pagador = cur.fetchone()
        if not res_pagador:
            conn.close(); return False, "Usuário sem cliente vinculado em admin.clientes."
        
        cpf_pagador = res_pagador[0]
        nome_pagador = res_pagador[1]

        # ETAPA 1: Buscar o VALOR e o NOME DA CARTEIRA na 'cliente.cliente_carteira_lista'
        cur.execute("""
            SELECT nome_carteira, custo_carteira 
            FROM cliente.cliente_carteira_lista 
            WHERE cpf_cliente = %s AND origem_custo = %s 
            LIMIT 1
        """, (cpf_pagador, origem_da_consulta))
        res_lista = cur.fetchone()

        if not res_lista:
            conn.close()
            return False, f"Cliente não possui a carteira '{origem_da_consulta}' na sua Lista de Carteiras."
        
        nome_carteira_vinculada = res_lista[0]
        valor_cobranca = float(res_lista[1])

        # ETAPA 2: Buscar a TABELA SQL na 'cliente.carteiras_config' usando o nome da carteira
        cur.execute("""
            SELECT nome_tabela_transacoes 
            FROM cliente.carteiras_config 
            WHERE nome_carteira = %s AND status = 'ATIVO' 
            LIMIT 1
        """, (nome_carteira_vinculada,))
        res_config = cur.fetchone()

        if not res_config:
            conn.close()
            return False, f"Configuração da tabela para '{nome_carteira_vinculada}' não encontrada ou inativa."
            
        tabela_sql = res_config[0]

        # ETAPA 3: Realizar o Lançamento de Débito
        cur.execute(f"SELECT saldo_novo FROM {tabela_sql} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_pagador,))
        res_saldo = cur.fetchone()
        saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
        
        novo_saldo = saldo_anterior - valor_cobranca
        cpf_consultado = dados_consulta.get('cpf', 'Desconhecido')
        motivo = f"Consulta Fator ({origem_da_consulta}): {cpf_consultado}"
        
        sql_insert = f"""
            INSERT INTO {tabela_sql}
            (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao)
            VALUES (%s, %s, %s, %s, 'DEBITO', %s, %s, %s, NOW())
        """
        cur.execute(sql_insert, (cpf_pagador, nome_pagador, motivo, origem_da_consulta, valor_cobranca, saldo_anterior, novo_saldo))
        
        conn.commit()
        conn.close()
        return True, f"Débito de R$ {valor_cobranca:.2f} na tabela {tabela_sql}."

    except Exception as e:
        if conn: conn.close()
        return False, f"Erro financeiro: {str(e)}"

# =============================================================================
# 3. FUNÇÃO DE SALVAMENTO DE DADOS (BASE PF)
# =============================================================================

def salvar_dados_fator_no_banco(dados_api):
    conn = get_conn()
    if not conn: return False, "Erro de conexão."
    
    try:
        cur = conn.cursor()
        cpf_limpo = re.sub(r'\D', '', str(dados_api.get('cpf', '')))
        
        if not cpf_limpo or len(cpf_limpo) != 11:
            return False, "CPF inválido."

        campos = {
            'nome': dados_api.get('nome'),
            'data_nascimento': converter_data_banco(dados_api.get('nascimento')),
            'rg': dados_api.get('rg'),
            'nome_mae': dados_api.get('mae')
        }
        
        query_dados = """
            INSERT INTO banco_pf.pf_dados (cpf, nome, data_nascimento, rg, nome_mae, data_criacao)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (cpf) DO UPDATE SET
                nome = COALESCE(EXCLUDED.nome, banco_pf.pf_dados.nome),
                data_nascimento = COALESCE(EXCLUDED.data_nascimento, banco_pf.pf_dados.data_nascimento),
                rg = COALESCE(EXCLUDED.rg, banco_pf.pf_dados.rg),
                nome_mae = COALESCE(EXCLUDED.nome_mae, banco_pf.pf_dados.nome_mae);
        """
        cur.execute(query_dados, (cpf_limpo, campos['nome'], campos['data_nascimento'], campos['rg'], campos['nome_mae']))
        
        count_tel = 0
        for tel in dados_api.get('telefones', []):
            num_limpo = re.sub(r'\D', '', str(tel['numero']))
            if num_limpo:
                cur.execute("SELECT 1 FROM banco_pf.pf_telefones WHERE cpf_ref = %s AND numero = %s", (cpf_limpo, num_limpo))
                if not cur.fetchone():
                    qualif = tel.get('prioridade', '').capitalize()
                    cur.execute("INSERT INTO banco_pf.pf_telefones (cpf_ref, numero, tag_qualificacao, data_atualizacao) VALUES (%s, %s, %s, CURRENT_DATE)", (cpf_limpo, num_limpo, qualif))
                    count_tel += 1

        count_email = 0
        for email in dados_api.get('emails', []):
            email_val = str(email).strip().lower()
            if email_val:
                cur.execute("SELECT 1 FROM banco_pf.pf_emails WHERE cpf_ref = %s AND email = %s", (cpf_limpo, email_val))
                if not cur.fetchone():
                    cur.execute("INSERT INTO banco_pf.pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_limpo, email_val))
                    count_email += 1

        count_end = 0
        for end in dados_api.get('enderecos', []):
            cep_limpo = re.sub(r'\D', '', str(end['cep']))
            if cep_limpo:
                cur.execute("SELECT 1 FROM banco_pf.pf_enderecos WHERE cpf_ref = %s AND cep = %s", (cpf_limpo, cep_limpo))
                if not cur.fetchone():
                    cur.execute("INSERT INTO banco_pf.pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_limpo, end['rua'], end['bairro'], end['cidade'], end['uf'], cep_limpo))
                    count_end += 1

        conn.commit(); conn.close()
        return True, f"Salvo! +{count_tel} Tels, +{count_email} Emails, +{count_end} End."

    except Exception as e:
        conn.close(); return False, f"Erro DB: {e}"

# =============================================================================
# 4. FLUXO DE CONSULTA (INTEGRADO COM FINANCEIRO E CACHE)
# =============================================================================

def realizar_consulta_cpf(cpf, origem="Teste Manual", forcar_nova=False):
    cpf_limpo_raw = ''.join(filter(str.isdigit, str(cpf)))
    if len(cpf_limpo_raw) <= 11: cpf_padrao = cpf_limpo_raw.zfill(11); tipo_registro = "CPF SIMPLES"
    else: cpf_padrao = cpf_limpo_raw.zfill(14); tipo_registro = "CNPJ SIMPLES"
    
    conn = get_conn()
    if not conn: return {"sucesso": False, "msg": "Erro DB."}
    
    try:
        cur = conn.cursor()
        
        # --- LÓGICA DE CACHE (COM REGISTRO DE LOG) ---
        if not forcar_nova:
            cur.execute("SELECT caminho_json FROM conexoes.fatorconferi_registo_consulta WHERE cpf_consultado = %s AND status_api = 'SUCESSO' ORDER BY id DESC LIMIT 1", (cpf_padrao,))
            res = cur.fetchone()
            if res and res[0] and os.path.exists(res[0]):
                try:
                    with open(res[0], 'r', encoding='utf-8') as f: 
                        dados = json.load(f)
                        if dados.get('nome') or dados.get('cpf'):
                            usr = st.session_state.get('usuario_nome', 'Sistema')
                            id_usr = st.session_state.get('usuario_id', 0)
                            
                            # Grava log de Cache no histórico
                            cur.execute("""
                                INSERT INTO conexoes.fatorconferi_registo_consulta 
                                (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora)
                                VALUES (%s, %s, %s, %s, 0, %s, 'SUCESSO', %s, %s, 'CACHE', NOW())
                            """, (tipo_registro, cpf_padrao, id_usr, usr, res[0], res[0], origem))
                            conn.commit()
                            
                            conn.close()
                            return {"sucesso": True, "dados": dados, "msg": "Cache recuperado."}
                except: pass
        
        # --- NOVA CONSULTA API ---
        cred = buscar_credenciais()
        if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token ausente."}
        
        url = f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}"
        resp = requests.get(url, timeout=30)
        resp.encoding = 'ISO-8859-1'
        xml = resp.text
        
        if "Não localizado" in xml or "erro" in xml.lower():
             conn.close(); return {"sucesso": False, "msg": "CPF não localizado.", "raw": xml}
        
        dados = parse_xml_to_dict(xml)
        
        if not dados.get('nome') and not dados.get('cpf'):
             conn.close()
             return {"sucesso": False, "msg": "Retorno vazio da API.", "raw": xml, "dados": dados}

        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, ensure_ascii=False, indent=4)
        
        # PROCESSAR DÉBITO FINANCEIRO DINÂMICO
        msg_financeira = ""
        ok_fin, txt_fin = processar_debito_automatico(origem, dados)
        if ok_fin:
            msg_financeira = f" | {txt_fin}"
        else:
            msg_financeira = f" | ⚠️ Falha Financeira: {txt_fin}"
        
        # Gravar log da nova consulta (valor pago é buscado via financeiro, mas logamos o custo padrão aqui)
        custo_padrao = buscar_valor_consulta_atual()
        usr = st.session_state.get('usuario_nome', 'Sistema')
        id_usr = st.session_state.get('usuario_id', 0)
        
        cur.execute("""
            INSERT INTO conexoes.fatorconferi_registo_consulta 
            (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora)
            VALUES (%s, %s, %s, %s, %s, %s, 'SUCESSO', %s, %s, 'PAGO', NOW())
        """, (tipo_registro, cpf_padrao, id_usr, usr, custo_padrao, path, path, origem))
        conn.commit(); conn.close()
        
        return {"sucesso": True, "dados": dados, "msg": "Consulta realizada." + msg_financeira}
        
    except Exception as e:
        if conn: conn.close()
        return {"sucesso": False, "msg": str(e)}

# =============================================================================
# 5. INTERFACE PRINCIPAL
# =============================================================================

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    tabs = st.tabs(["👥 Clientes", "🔍 Teste de Consulta", "💰 Saldo API", "📋 Histórico", "⚙️ Parâmetros"])

    with tabs[1]:
        st.markdown("#### 1.1 Consulta e Importação")
        c1, c2, c3 = st.columns([3, 1.5, 1.5])
        cpf_in = c1.text_input("CPF")
        forcar = c2.checkbox("Ignorar Histórico", value=False)
        
        if c3.button("🔍 Consultar", type="primary"):
            if cpf_in:
                with st.spinner("Buscando..."):
                    origem_padrao = obter_origem_padronizada("WEB USUÁRIO")
                    res = realizar_consulta_cpf(cpf_in, origem_padrao, forcar)
                    st.session_state['resultado_fator'] = res
        
        if 'resultado_fator' in st.session_state:
            res = st.session_state['resultado_fator']
            if res['sucesso']:
                if "msg" in res: st.success(res['msg'])
                
                st.divider()
                col_save, col_info = st.columns([1, 3])
                if col_save.button("💾 Salvar na Base PF", type="primary"):
                    with st.spinner("Processando..."):
                        ok_save, msg_save = salvar_dados_fator_no_banco(res['dados'])
                        if ok_save: st.success(msg_save)
                        else: st.error(msg_save)
                
                dados = res['dados']
                with st.expander("👤 Dados Pessoais", expanded=True):
                    dc1, dc2 = st.columns(2)
                    dc1.write(f"**Nome:** {dados.get('nome', '-')}")
                    dc1.write(f"**CPF:** {dados.get('cpf', '-')}")
                    dc2.write(f"**Mãe:** {dados.get('mae', '-')}")
                    dc2.write(f"**Nasc:** {dados.get('nascimento', '-')}")

                with st.expander(f"📞 Telefones ({len(dados.get('telefones', []))})", expanded=False):
                    st.table(pd.DataFrame(dados.get('telefones', [])))

                with st.expander(f"🏠 Endereços ({len(dados.get('enderecos', []))})", expanded=False):
                    for end in dados.get('enderecos', []):
                        st.write(f"📍 {end['rua']}, {end['bairro']} - {end['cidade']}/{end['uf']} (CEP: {end['cep']})")
                
                with st.expander(f"📧 E-mails ({len(dados.get('emails', []))})", expanded=False):
                    for em in dados.get('emails', []): st.write(f"✉️ {em}")
            else: st.error(res.get('msg', 'Erro'))

    with tabs[2]: 
        if st.button("🔄 Atualizar Saldo"): 
            ok, v = consultar_saldo_api()
            if ok: st.metric("Saldo Atual", f"R$ {v:.2f}")
            else: st.error("Erro ao consultar saldo.")
        
    with tabs[3]: 
        conn = get_conn()
        if conn: 
            st.dataframe(pd.read_sql("SELECT * FROM conexoes.fatorconferi_registo_consulta ORDER BY id DESC LIMIT 50", conn), use_container_width=True)
            conn.close()