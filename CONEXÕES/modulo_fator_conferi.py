import streamlit as st
import pandas as pd
import psycopg2
import requests
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, date

# --- IMPORTAÇÃO DE MÓDULOS EXTERNOS (MODULO PF CADASTRO) ---
try:
    # Ajusta o caminho para encontrar o módulo de cadastro
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../OPERACIONAL/BANCO DE PLANILHAS')))
    import modulo_pf_cadastro
except ImportError:
    st.error("Erro crítico: modulo_pf_cadastro.py não encontrado. Verifique a estrutura de pastas.")

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

def get_tag_text(element, tag_name):
    if element is None: return None
    res = element.find(tag_name)
    if res is not None: return res.text
    return None

def find_tag(element, tag_name):
    if element is None: return None
    res = element.find(tag_name)
    if res is not None: return res
    return None

def parse_xml_to_dict(xml_string):
    try:
        xml_string = xml_string.replace('ISO-8859-1', 'UTF-8') 
        root = ET.fromstring(xml_string)
        dados = {}
        
        # Dados Cadastrais
        cad = find_tag(root, 'cadastrais')
        if cad is not None:
            dados['nome'] = get_tag_text(cad, 'nome')
            dados['cpf'] = get_tag_text(cad, 'cpf')
            dados['nascimento'] = get_tag_text(cad, 'nascto')
            dados['mae'] = get_tag_text(cad, 'nome_mae')
            dados['rg'] = get_tag_text(cad, 'rg')
        
        # Telefones (Junta Fixo e Móvel numa lista só)
        telefones = []
        for tag_tipo in ['telefones_movel', 'telefones_fixo']:
            node = find_tag(root, tag_tipo)
            if node is not None:
                for child in node:
                    if 'telefone' in child.tag.lower():
                        telefones.append({
                            'numero': get_tag_text(child, 'numero'), 
                            'prioridade': get_tag_text(child, 'prioridade')
                        })
        dados['telefones'] = telefones

        # Emails (Lista de Strings)
        emails = []
        em_root = find_tag(root, 'emails')
        if em_root is not None:
            for em in em_root:
                if 'email' in em.tag.lower() and em.text: 
                    emails.append(em.text)
        dados['emails'] = emails

        # Endereços
        enderecos = []
        end_root = find_tag(root, 'enderecos')
        if end_root is not None:
            for end in end_root:
                if 'endereco' in end.tag.lower():
                    enderecos.append({
                        'rua': f"{get_tag_text(end, 'logradouro') or ''}, {get_tag_text(end, 'numero') or ''}".strip(', '),
                        'bairro': get_tag_text(end, 'bairro'),
                        'cidade': get_tag_text(end, 'cidade'),
                        'uf': get_tag_text(end, 'estado'),
                        'cep': get_tag_text(end, 'cep')
                    })
        dados['enderecos'] = enderecos
        return dados
    except Exception as e:
        return {"erro": f"Falha XML: {e}", "raw": xml_string}

# =============================================================================
# 2. SALVAR NO BANCO (INTEGRAÇÃO CORRIGIDA)
# =============================================================================

def salvar_dados_fator_no_banco(dados_api):
    """
    Processa o JSON/Dict e salva usando as regras do modulo_pf_cadastro.
    """
    if not dados_api: return False, "Sem dados para salvar."

    # 1. Tratamento CPF
    raw_cpf = str(dados_api.get('cpf', '')).strip()
    cpf_limpo = modulo_pf_cadastro.limpar_normalizar_cpf(raw_cpf)
    
    if not cpf_limpo or len(cpf_limpo) != 11:
        return False, f"CPF inválido: '{raw_cpf}'"

    nome_cliente = dados_api.get('nome') or "Cliente Fator"

    try:
        # --- ETAPA 1: CRIAÇÃO DO CADASTRO (Se não existir) ---
        dados_novo = {'nome': nome_cliente, 'cpf': cpf_limpo}
        modulo_pf_cadastro.salvar_pf(
            dados_gerais=dados_novo, 
            df_tel=pd.DataFrame(), df_email=pd.DataFrame(), 
            df_end=pd.DataFrame(), df_emp=pd.DataFrame(), df_contr=pd.DataFrame(), 
            modo="novo"
        )
        
        # --- ETAPA 2: ATUALIZAÇÃO (DADOS COMPLETOS) ---
        
        # A. Dados Gerais
        dados_editar = {
            'nome': nome_cliente,
            'cpf': cpf_limpo,
            'rg': dados_api.get('rg'),
            'nome_mae': dados_api.get('mae'),
            'data_nascimento': modulo_pf_cadastro.converter_data_br_iso(dados_api.get('nascimento'))
        }
        
        # B. Telefones (Correção de Leitura)
        lista_tels = []
        raw_telefones = dados_api.get('telefones', []) or []
        for t in raw_telefones:
            # Garante leitura se for dict {"numero": "..."} ou string direta
            val_num = t.get('numero') if isinstance(t, dict) else str(t)
            
            # Limpeza preventiva (remove caracteres não numéricos)
            val_limpo = re.sub(r'\D', '', str(val_num))
            
            # Validação
            num_val, erro = modulo_pf_cadastro.validar_formatar_telefone(val_limpo)
            if num_val and not erro:
                lista_tels.append({'numero': num_val})
        
        df_tels = pd.DataFrame(lista_tels)

        # C. E-mails (Correção de Leitura)
        lista_emails = []
        raw_emails = dados_api.get('emails', []) or []
        for e in raw_emails:
            # Garante leitura se for string "a@a.com" ou dict {"email": "..."}
            val_email = e if isinstance(e, str) else e.get('email')
            
            if val_email:
                val_email = str(val_email).strip().lower()
                if modulo_pf_cadastro.validar_email(val_email):
                    lista_emails.append({'email': val_email})
        
        df_emails = pd.DataFrame(lista_emails)

        # D. Endereços
        lista_ends = []
        raw_ends = dados_api.get('enderecos', []) or []
        for d in raw_ends:
            if isinstance(d, dict):
                cep_num, _, erro_cep = modulo_pf_cadastro.validar_formatar_cep(d.get('cep'))
                if (cep_num and not erro_cep) or d.get('rua'):
                    uf_val = str(d.get('uf', '')).upper()
                    if modulo_pf_cadastro.validar_uf(uf_val):
                         lista_ends.append({
                             'cep': cep_num, 'rua': d.get('rua'),
                             'bairro': d.get('bairro'), 'cidade': d.get('cidade'), 'uf': uf_val
                         })
        df_ends = pd.DataFrame(lista_ends)

        # CHAMA O SALVAMENTO FINAL
        ok, msg = modulo_pf_cadastro.salvar_pf(
            dados_gerais=dados_editar,
            df_tel=df_tels,
            df_email=df_emails,
            df_end=df_ends,
            df_emp=pd.DataFrame(),
            df_contr=pd.DataFrame(),
            modo="editar",
            cpf_original=cpf_limpo
        )
        
        if ok:
            return True, f"Consulta realizada. | ✅ Cadastro atualizado (Tel: {len(df_tels)}, Email: {len(df_emails)})"
        else:
            return False, f"Erro ao atualizar: {msg}"

    except Exception as e:
        return False, f"Erro integração: {str(e)}"

# =============================================================================
# 3. INTERFACE E CONSULTA
# =============================================================================

def realizar_consulta_cpf(cpf, ambiente, forcar_nova=False, id_cliente_pagador_manual=None):
    cpf_padrao = ''.join(filter(str.isdigit, str(cpf))).zfill(11)
    conn = get_conn()
    if not conn: return {"sucesso": False, "msg": "Erro DB."}
    
    # 1. Identificação
    id_usuario = st.session_state.get('usuario_id', 0)
    nome_usuario = st.session_state.get('usuario_nome', 'Sistema')
    
    # ... (Lógica de cache mantida igual) ...
    # Se quiser forçar nova ou não tiver cache:
    cred = buscar_credenciais()
    if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token ausente."}
    
    try:
        if not forcar_nova:
            # Tenta Cache
            cur = conn.cursor()
            cur.execute("SELECT caminho_json FROM conexoes.fatorconferi_registo_consulta WHERE cpf_consultado=%s AND status_api='SUCESSO' ORDER BY id DESC LIMIT 1", (cpf_padrao,))
            res = cur.fetchone()
            if res and res[0] and os.path.exists(res[0]):
                with open(res[0], 'r', encoding='utf-8') as f: dados = json.load(f)
                conn.close()
                return {"sucesso": True, "dados": dados, "msg": "Cache recuperado."}

        # Nova Consulta
        resp = requests.get(f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}", timeout=30)
        resp.encoding = 'ISO-8859-1'
        dados = parse_xml_to_dict(resp.text)
        
        if not dados.get('nome'):
            conn.close(); return {"sucesso": False, "msg": "Sem dados na API.", "dados": dados}
        
        if not dados.get('cpf'): dados['cpf'] = cpf_padrao

        # Salva JSON
        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4)
        
        # Registra no Banco
        custo = buscar_valor_consulta_atual()
        origem = buscar_origem_por_ambiente(ambiente)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO conexoes.fatorconferi_registo_consulta 
            (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, origem_consulta, data_hora, ambiente) 
            VALUES ('CPF SIMPLES', %s, %s, %s, %s, %s, 'SUCESSO', %s, NOW(), %s)
        """, (cpf_padrao, id_usuario, nome_usuario, custo, path, origem, ambiente))
        conn.commit(); conn.close()
        
        return {"sucesso": True, "dados": dados, "msg": "Consulta realizada."}

    except Exception as e:
        if conn: conn.close()
        return {"sucesso": False, "msg": str(e)}

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    
    c1, c2 = st.columns([3, 1])
    cpf_in = c1.text_input("CPF")
    if c2.button("Consultar", type="primary"):
        if cpf_in:
            with st.spinner("Consultando..."):
                res = realizar_consulta_cpf(cpf_in, "painel_fator")
                if res['sucesso']:
                    # TENTA SALVAR NO BANCO
                    ok, msg = salvar_dados_fator_no_banco(res['dados'])
                    if ok: st.success(msg)
                    else: st.warning(msg)
                    
                    with st.expander("Ver JSON"): st.json(res['dados'])
                else:
                    st.error(res.get('msg'))