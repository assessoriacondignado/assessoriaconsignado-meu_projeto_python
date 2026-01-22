import streamlit as st
import pandas as pd
import psycopg2
import requests
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, date

import conexao
import modulo_validadores as mv

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
# 1. FUNÇÕES AUXILIARES E SANITIZAÇÃO
# =============================================================================

def registrar_erro_importacao(cpf, erro_msg):
    try:
        data_hora = datetime.now().strftime("%d-%m-%Y_%H-%M")
        nome_arq = f"ERROIMPORTAÇÃO_{cpf}_{data_hora}.txt"
        caminho = os.path.join(PASTA_JSON, nome_arq)
        with open(caminho, "w", encoding="utf-8") as f:
            f.write(f"ERRO NA IMPORTAÇÃO FATOR CONFERI\n")
            f.write(f"CPF: {cpf}\n")
            f.write(f"DATA: {datetime.now()}\n")
            f.write(f"DETALHE DO ERRO:\n{str(erro_msg)}")
    except: pass

def sanitizar_valor_api(valor):
    """
    Remove espaços, converte '[]', '[ ]' e 'NULO' para None.
    """
    if valor is None:
        return None
    
    if isinstance(valor, list) and len(valor) == 0:
        return None
    
    if isinstance(valor, str):
        v = valor.strip()
        if v in ["[]", "[ ]"]:
            return None
        if v.upper() == "NULO":
            return None
        if v == "":
            return None
        return v
        
    return valor

# =============================================================================
# 2. PARSING (JSON E XML) - COM LOGICA MULTI-ITEM
# =============================================================================

def find_tag_insensitive(element, tag_name):
    if element is None: return None
    res = element.find(tag_name)
    if res is not None: return res
    res = element.find(tag_name.upper())
    if res is not None: return res
    res = element.find(tag_name.lower())
    if res is not None: return res
    for child in element:
        if child.tag.lower() == tag_name.lower():
            return child
    return None

def get_tag_text_insensitive(element, tag_name):
    node = find_tag_insensitive(element, tag_name)
    if node is not None and node.text:
        return sanitizar_valor_api(node.text)
    return None

def parse_xml_to_dict(texto_resposta):
    """
    Lê JSON/XML e 'achata' listas criando chaves indexadas (telefone_1, telefone_2...)
    """
    dados = {}
    
    # --- TENTATIVA 1: JSON ---
    try:
        json_bruto = json.loads(texto_resposta)
        
        # Copia campos raiz
        for k, v in json_bruto.items():
            dados[k] = sanitizar_valor_api(v)
            dados[k.lower()] = sanitizar_valor_api(v)

        # --- PROCESSAMENTO DE TELEFONES (Dispositivos) ---
        dispositivos = None
        for k in json_bruto.keys():
            if "dispositivos" in k.lower().strip():
                dispositivos = json_bruto[k]
                break
        
        telefones_lista = []
        if isinstance(dispositivos, list):
            for d in dispositivos:
                num = None
                for sub_k, sub_v in d.items():
                    if "numero" in sub_k.lower().replace("ú", "u"):
                        num = sanitizar_valor_api(sub_v)
                        break
                if num:
                    telefones_lista.append({'numero': num})
        
        dados['telefones'] = telefones_lista

        # Lógica de Achatamento (Flattening) para Telefones
        # Cria chaves: telefone_1, numero_1, telefone_2, numero_2...
        for i, item in enumerate(telefones_lista):
            idx = i + 1
            num = item['numero']
            dados[f'telefone_{idx}'] = num
            dados[f'numero_{idx}'] = num
            
            # Mantém compatibilidade com o [0] na raiz sem sufixo
            if i == 0:
                dados['telefone'] = num
                dados['numero'] = num
                dados['celular'] = num

        # --- PROCESSAMENTO DE ENDEREÇOS ---
        enderecos_lista = []
        lista_end_raw = None
        for k in json_bruto.keys():
            if "enderecos" in k.lower().strip():
                lista_end_raw = json_bruto[k]
                break
        
        if isinstance(lista_end_raw, list):
            for end in lista_end_raw:
                end_limpo = {k.strip().lower(): sanitizar_valor_api(v) for k, v in end.items()}
                enderecos_lista.append(end_limpo)
        
        dados['enderecos'] = enderecos_lista

        # Lógica de Achatamento (Flattening) para Endereços
        # Cria chaves: rua_1, cep_1, rua_2, cep_2...
        for i, item in enumerate(enderecos_lista):
            idx = i + 1
            # Mapeia campos comuns com sufixo
            for chave_end, valor_end in item.items():
                dados[f"{chave_end}_{idx}"] = valor_end # ex: rua_1, cep_1
            
            # Mantém compatibilidade com o [0] na raiz sem sufixo
            if i == 0:
                dados['rua'] = item.get('rua')
                dados['logradouro'] = item.get('rua')
                dados['bairro'] = item.get('bairro')
                dados['cidade'] = item.get('cidade')
                dados['uf'] = item.get('uf')
                dados['cep'] = item.get('cep')
        
        return dados

    except json.JSONDecodeError:
        pass
    except Exception:
        pass

    # --- TENTATIVA 2: XML (Legado) ---
    try:
        xml_string = texto_resposta.replace('ISO-8859-1', 'UTF-8') 
        root = ET.fromstring(xml_string)
        
        cad = find_tag_insensitive(root, 'cadastrais')
        if cad is not None:
            dados['nome'] = get_tag_text_insensitive(cad, 'nome')
            dados['cpf'] = get_tag_text_insensitive(cad, 'cpf')
            dados['nascimento'] = get_tag_text_insensitive(cad, 'nascto') 
            dados['mae'] = get_tag_text_insensitive(cad, 'nome_mae')
            dados['rg'] = get_tag_text_insensitive(cad, 'rg')
            dados['pai'] = get_tag_text_insensitive(cad, 'nome_pai')
            
            uf_res = get_tag_text_insensitive(cad, 'uf_emissao')
            if not uf_res: uf_res = get_tag_text_insensitive(cad, 'uf_rg')
            dados['uf_rg'] = uf_res
            dados['sexo'] = get_tag_text_insensitive(cad, 'sexo')
        
        telefones = []
        for tag_grupo in ['telefones_movel', 'telefones_fixo']:
            node = find_tag_insensitive(root, tag_grupo)
            if node is not None:
                for child in node:
                    if 'telefone' in child.tag.lower():
                        num = get_tag_text_insensitive(child, 'numero') or get_tag_text_insensitive(child, 'número')
                        if num:
                            telefones.append({'numero': num})
        
        dados['telefones'] = telefones
        
        # Achata telefones XML
        for i, item in enumerate(telefones):
            idx = i + 1
            dados[f'telefone_{idx}'] = item['numero']
            if i == 0:
                dados['telefone'] = item['numero']
                dados['numero'] = item['numero']

        emails = []
        em_root = find_tag_insensitive(root, 'emails')
        if em_root is not None:
            for em in em_root:
                if 'email' in em.tag.lower() and em.text: 
                    val = sanitizar_valor_api(em.text)
                    if val: emails.append(val)
        dados['emails'] = emails
        
        # Achata Emails XML
        for i, val in enumerate(emails):
            dados[f'email_{i+1}'] = val
            if i == 0: dados['email'] = val

        enderecos = []
        end_root = find_tag_insensitive(root, 'enderecos')
        if end_root is not None:
            for end in end_root:
                if 'endereco' in end.tag.lower():
                    enderecos.append({
                        'rua': get_tag_text_insensitive(end, 'logradouro') or get_tag_text_insensitive(end, 'rua'),
                        'bairro': get_tag_text_insensitive(end, 'bairro'),
                        'cidade': get_tag_text_insensitive(end, 'cidade'),
                        'uf': get_tag_text_insensitive(end, 'estado') or get_tag_text_insensitive(end, 'uf'),
                        'cep': get_tag_text_insensitive(end, 'cep')
                    })
        dados['enderecos'] = enderecos
        
        # Achata Endereços XML
        for i, item in enumerate(enderecos):
            idx = i + 1
            for k, v in item.items():
                dados[f"{k}_{idx}"] = v
            if i == 0:
                for k, v in item.items():
                    dados[k] = v
        
        return dados
    except Exception as e:
        return {"erro": f"Falha no Parser (XML/JSON): {e}", "raw": texto_resposta}

# =============================================================================
# 3. PROCESSO DE INSERÇÃO PRÓPRIO
# =============================================================================

def verificar_coluna_cpf(cur, tabela):
    try:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'banco_pf' AND table_name = '{tabela}' AND column_name IN ('cpf', 'cpf_ref')")
        res = cur.fetchone()
        if res: return res[0]
    except: pass
    return 'cpf_ref' 

def salvar_dados_fator_no_banco(dados_api):
    conn = get_conn()
    if not conn: return False, "Erro de conexão com o banco."
    
    raw_cpf = str(dados_api.get('cpf', '')).strip()
    cpf_limpo = mv.ValidadorDocumentos.cpf_para_sql(raw_cpf)
    
    if not cpf_limpo:
        return False, f"CPF inválido/irregular para importação: '{raw_cpf}'"

    try:
        cur = conn.cursor()
        
        def safe_get(key):
            v = dados_api.get(key)
            return v if v else None

        campos = {
            'nome': (safe_get('nome') or "CLIENTE IMPORTADO"),
            'rg': safe_get('rg'),
            'data_nascimento': mv.ValidadorData.para_sql(safe_get('nascimento')), 
            'nome_mae': safe_get('mae'),
            'nome_pai': safe_get('pai'),
            'uf_rg': safe_get('uf_rg'),
            'pis': safe_get('pis'),
            'cnh': safe_get('cnh'),
            'serie_ctps': safe_get('serie_ctps'),
            'nome_procurador': safe_get('nome_procurador'),
            'cpf_procurador': mv.ValidadorDocumentos.cpf_para_sql(safe_get('cpf_procurador'))
        }

        sql_pf = """
            INSERT INTO banco_pf.pf_dados (
                cpf, nome, rg, data_nascimento, nome_mae, nome_pai, uf_rg, 
                pis, cnh, serie_ctps, nome_procurador, cpf_procurador, data_criacao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (cpf) DO UPDATE SET
                nome = COALESCE(EXCLUDED.nome, banco_pf.pf_dados.nome),
                rg = COALESCE(EXCLUDED.rg, banco_pf.pf_dados.rg),
                data_nascimento = COALESCE(EXCLUDED.data_nascimento, banco_pf.pf_dados.data_nascimento),
                nome_mae = COALESCE(EXCLUDED.nome_mae, banco_pf.pf_dados.nome_mae),
                nome_pai = COALESCE(EXCLUDED.nome_pai, banco_pf.pf_dados.nome_pai),
                uf_rg = COALESCE(EXCLUDED.uf_rg, banco_pf.pf_dados.uf_rg)
        """
        cur.execute(sql_pf, (
            cpf_limpo, campos['nome'], campos['rg'], campos['data_nascimento'], 
            campos['nome_mae'], campos['nome_pai'], campos['uf_rg'],
            campos['pis'], campos['cnh'], campos['serie_ctps'], 
            campos['nome_procurador'], campos['cpf_procurador']
        ))

        # Salva TODOS os telefones na tabela filha
        raw_telefones = dados_api.get('telefones', []) or []
        count_tel = 0
        col_tel = verificar_coluna_cpf(cur, 'pf_telefones')
        
        for t in raw_telefones:
            val_bruto = str(t.get('numero', '')) if isinstance(t, dict) else str(t)
            tel_validado = mv.ValidadorContato.telefone_para_sql(val_bruto)
            
            if tel_validado:
                cur.execute(f"SELECT 1 FROM banco_pf.pf_telefones WHERE {col_tel}=%s AND numero=%s", (cpf_limpo, tel_validado))
                if not cur.fetchone():
                    cur.execute(f"INSERT INTO banco_pf.pf_telefones ({col_tel}, numero, data_atualizacao) VALUES (%s, %s, CURRENT_DATE)", (cpf_limpo, tel_validado))
                    count_tel += 1

        raw_emails = dados_api.get('emails', []) or []
        count_email = 0
        col_email = verificar_coluna_cpf(cur, 'pf_emails')

        for e in raw_emails:
            val_bruto = str(e)
            val_limpo = val_bruto.strip().lower()
            
            if mv.ValidadorContato.email_valido(val_limpo):
                cur.execute(f"SELECT 1 FROM banco_pf.pf_emails WHERE {col_email}=%s AND email=%s", (cpf_limpo, val_limpo))
                if not cur.fetchone():
                    cur.execute(f"INSERT INTO banco_pf.pf_emails ({col_email}, email) VALUES (%s, %s)", (cpf_limpo, val_limpo))
                    count_email += 1

        raw_ends = dados_api.get('enderecos', []) or []
        count_end = 0
        col_end = verificar_coluna_cpf(cur, 'pf_enderecos')

        for d in raw_ends:
            if isinstance(d, dict):
                cep_val = mv.ValidadorDocumentos.limpar_numero(d.get('cep'))
                if cep_val and len(cep_val) == 8: pass 
                else: cep_val = None

                rua_val = d.get('rua')
                if cep_val or rua_val:
                    cur.execute(f"SELECT 1 FROM banco_pf.pf_enderecos WHERE {col_end}=%s AND cep=%s AND rua=%s", (cpf_limpo, cep_val, rua_val))
                    if not cur.fetchone():
                        cur.execute(f"INSERT INTO banco_pf.pf_enderecos ({col_end}, cep, rua, bairro, cidade, uf) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_limpo, cep_val, rua_val, d.get('bairro'), d.get('cidade'), d.get('uf')))
                        count_end += 1

        conn.commit()
        conn.close()
        return True, f"✅ Dados inseridos com sucesso! (+{count_tel} Tels, +{count_email} Emails)"

    except Exception as e:
        if conn: conn.rollback(); conn.close()
        registrar_erro_importacao(cpf_limpo, e)
        return False, f"Erro na importação: {str(e)} (Log salvo na pasta JSON)"

def executar_distribuicao_dinamica(dados_api):
    conn = get_conn()
    if not conn:
        return [], ["Erro de conexão com o banco de dados."]

    sucessos = []
    erros = []

    try:
        df_map = pd.read_sql("SELECT tabela_referencia, tabela_referencia_coluna, jason_api_fatorconferi_coluna FROM conexoes.fatorconferi_conexao_tabelas", conn)
        
        if df_map.empty:
            conn.close()
            return [], ["Nenhum mapeamento de tabelas encontrado em 'conexoes.fatorconferi_conexao_tabelas'."]

        tabelas_destino = df_map['tabela_referencia'].unique()
        cur = conn.cursor()

        for tabela in tabelas_destino:
            try:
                regras = df_map[df_map['tabela_referencia'] == tabela]
                
                colunas_sql = []
                valores_insert = []
                
                tem_dado = False
                chaves_testadas = []

                for _, row in regras.iterrows():
                    col_sql = str(row['tabela_referencia_coluna']).strip()
                    chave_json = str(row['jason_api_fatorconferi_coluna']).strip()
                    chaves_testadas.append(chave_json)
                    
                    # 1. Pega valor (agora suporta telefone_1, telefone_2, etc.)
                    valor_raw = dados_api.get(chave_json)
                    if valor_raw is None:
                        valor_raw = dados_api.get(chave_json.lower())

                    valor = sanitizar_valor_api(valor_raw)
                    
                    if isinstance(valor, (list, dict)):
                        valor = str(valor)

                    # 2. Padronização CPF
                    if valor and ('cpf' in col_sql.lower() or 'cpf' in chave_json.lower()):
                        cpf_ajustado = mv.ValidadorDocumentos.cpf_para_sql(valor)
                        if cpf_ajustado: 
                            valor = cpf_ajustado
                    
                    colunas_sql.append(col_sql)
                    valores_insert.append(valor)
                    
                    if valor: 
                        tem_dado = True

                if not tem_dado:
                    erros.append(f"⚠️ Tabela '{tabela}' pulada: Nenhum dado encontrado na API para as chaves {chaves_testadas}.")
                    continue

                colunas_str = ", ".join(colunas_sql)
                placeholders = ", ".join(["%s"] * len(valores_insert))
                
                sql = f"INSERT INTO {tabela} ({colunas_str}) VALUES ({placeholders})"
                
                cur.execute(sql, tuple(valores_insert))
                
                sucessos.append(tabela)
            
            except Exception as e:
                conn.rollback() 
                erros.append(f"❌ Erro ao inserir em '{tabela}': {str(e)}")

        conn.commit()
        cur.close()
        conn.close()
        
        return sucessos, erros

    except Exception as e:
        if conn: conn.close()
        return [], [f"Erro crítico no processo de distribuição: {str(e)}"]

# =============================================================================
# 4. FUNÇÕES DE CONSULTA API E GESTÃO DE SALDO
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

def consultar_saldo_api():
    cred = buscar_credenciais()
    if not cred['token']: return False, 0.0
    url = f"{cred['url']}?acao=VER_SALDO&TK={cred['token']}"
    try:
        response = requests.get(url, timeout=10)
        valor_texto = response.text.strip()
        if '<' in valor_texto:
            try: root = ET.fromstring(valor_texto); valor_texto = root.text 
            except: pass
        saldo = float(valor_texto.replace(',', '.')) if valor_texto else 0.0
        
        conn = get_conn()
        if conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO conexoes.fatorconferi_registro_de_saldo (valor_saldo) VALUES (%s)", (saldo,))
            conn.commit(); conn.close()
        return True, saldo
    except Exception as e: return False, 0.0

def buscar_origem_por_ambiente(nome_ambiente):
    conn = get_conn()
    origem_padrao = "WEB USUÁRIO" 
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT origem FROM conexoes.fatorconferi_ambiente_consulta WHERE ambiente = %s LIMIT 1", (nome_ambiente,))
            res = cur.fetchone()
            if res: origem_padrao = res[0]
            conn.close()
        except:
            if conn: conn.close()
    return origem_padrao

def buscar_cliente_vinculado_ao_usuario(id_usuario):
    conn = get_conn()
    cliente = {"id": None, "nome": None}
    if conn and id_usuario:
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, nome FROM admin.clientes WHERE id_usuario_vinculo = %s LIMIT 1", (id_usuario,))
            res = cur.fetchone()
            if res:
                cliente["id"] = res[0]
                cliente["nome"] = res[1]
            conn.close()
        except:
            if conn: conn.close()
    return cliente

def processar_cobranca_novo_fluxo(conn, dados_cliente, origem_custo_chave):
    try:
        cur = conn.cursor()
        id_cli = str(dados_cliente['id'])
        
        sql_custo = "SELECT valor_custo, id_produto, nome_produto FROM cliente.valor_custo_carteira_cliente WHERE id_cliente = %s AND origem_custo = %s LIMIT 1"
        cur.execute(sql_custo, (id_cli, origem_custo_chave))
        res_custo = cur.fetchone()
        
        if not res_custo:
            return False, f"Custo não definido para o cliente na origem '{origem_custo_chave}'."
            
        valor_debitar = float(res_custo[0])
        id_prod_vinc = res_custo[1] 
        nome_prod_vinc = res_custo[2]
        
        if valor_debitar <= 0:
            return True, "Custo zero/gratuito."

        sql_saldo = "SELECT saldo_novo FROM cliente.extrato_carteira_por_produto WHERE id_cliente = %s ORDER BY id DESC LIMIT 1"
        cur.execute(sql_saldo, (id_cli,))
        res_saldo = cur.fetchone()
        saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
        
        saldo_novo = saldo_anterior - valor_debitar
        
        sql_insert = """
            INSERT INTO cliente.extrato_carteira_por_produto (
                produto_vinculado, id_cliente, nome_cliente,
                id_usuario, nome_usuario,
                origem_lancamento, data_lancamento, tipo_lancamento,
                valor_lancado, saldo_anterior, saldo_novo,
                id_produto
            ) VALUES (
                %s, %s, %s,
                %s, %s,
                %s, NOW(), 'DEBITO',
                %s, %s, %s,
                %s
            )
        """
        cur.execute(sql_insert, (
            nome_prod_vinc, id_cli, dados_cliente['nome'],
            str(dados_cliente.get('id_usuario', '0')), dados_cliente.get('nome_usuario', 'Sistema'),
            origem_custo_chave, 
            valor_debitar, saldo_anterior, saldo_novo,
            id_prod_vinc
        ))
        
        return True, f"Débito R$ {valor_debitar:.2f} OK. (Saldo: {saldo_novo:.2f})"

    except Exception as e:
        return False, f"Erro Cobrança: {str(e)}"

# --- FUNÇÃO PRINCIPAL DE CONSULTA ---
def realizar_consulta_cpf(cpf, ambiente, forcar_nova=False, id_cliente_pagador_manual=None):
    cpf_padrao = mv.ValidadorDocumentos.cpf_para_sql(cpf)
    if not cpf_padrao:
        return {"sucesso": False, "msg": f"CPF Inválido: {cpf}"}

    conn = get_conn()
    if not conn: return {"sucesso": False, "msg": "Erro DB."}
    
    id_usuario = st.session_state.get('usuario_id', 0)
    nome_usuario = st.session_state.get('usuario_nome', 'Sistema')
    
    dados_pagador = {"id": None, "nome": None, "id_usuario": id_usuario, "nome_usuario": nome_usuario}
    
    if id_cliente_pagador_manual:
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, nome FROM admin.clientes WHERE id=%s", (id_cliente_pagador_manual,))
            res = cur.fetchone()
            if res: 
                dados_pagador["id"] = res[0]
                dados_pagador["nome"] = res[1]
        except: pass
    else:
        d = buscar_cliente_vinculado_ao_usuario(id_usuario)
        dados_pagador["id"] = d['id']
        dados_pagador["nome"] = d['nome']

    origem_real = buscar_origem_por_ambiente(ambiente)

    try:
        cur = conn.cursor()
        
        if not forcar_nova:
            cur.execute("SELECT caminho_json FROM conexoes.fatorconferi_registo_consulta WHERE cpf_consultado=%s AND status_api='SUCESSO' ORDER BY id DESC LIMIT 1", (cpf_padrao,))
            res = cur.fetchone()
            if res and res[0] and os.path.exists(res[0]):
                with open(res[0], 'r', encoding='utf-8') as f: dados = json.load(f)
                conn.close()
                return {"sucesso": True, "dados": dados, "msg": "Cache recuperado."}

        custo_previsto = 0.0
        if dados_pagador['id']:
             sql_custo = "SELECT valor_custo FROM cliente.valor_custo_carteira_cliente WHERE id_cliente = %s AND origem_custo = %s LIMIT 1"
             cur.execute(sql_custo, (str(dados_pagador['id']), origem_real))
             res_custo = cur.fetchone()
             if res_custo:
                 custo_previsto = float(res_custo[0])
             else:
                 custo_previsto = buscar_valor_consulta_atual()

             sql_saldo = "SELECT saldo_novo FROM cliente.extrato_carteira_por_produto WHERE id_cliente = %s ORDER BY id DESC LIMIT 1"
             cur.execute(sql_saldo, (str(dados_pagador['id']),))
             res_s = cur.fetchone()
             saldo_atual = float(res_s[0]) if res_s else 0.0

             if saldo_atual < custo_previsto:
                 conn.close()
                 return {
                     "sucesso": False, 
                     "msg": f"🚫 Bloqueio Financeiro: Saldo insuficiente. (Custo: R$ {custo_previsto:.2f} | Saldo: R$ {saldo_atual:.2f})"
                 }

        cred = buscar_credenciais()
        if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token API ausente."}
        
        resp = requests.get(f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}", timeout=30)
        
        # --- PARSER ATUALIZADO (JSON/XML + LISTAS) ---
        dados = parse_xml_to_dict(resp.text)
        
        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        
        try:
            with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4)
        except Exception as e:
            st.error(f"Erro ao salvar arquivo JSON de log: {e}")

        if not dados.get('nome'): 
            conn.close()
            return {
                "sucesso": False, 
                "msg": f"Sem dados retornados ou erro de estrutura. (Log: {nome_arq})", 
                "dados": dados
            }
        
        if not dados.get('cpf'): dados['cpf'] = cpf_padrao

        cur.execute("INSERT INTO conexoes.fatorconferi_registo_consulta (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, origem_consulta, data_hora, id_cliente, nome_cliente, ambiente) VALUES ('CPF SIMPLES', %s, %s, %s, %s, %s, 'SUCESSO', %s, NOW(), %s, %s, %s)", 
                    (cpf_padrao, id_usuario, nome_usuario, custo_previsto, path, origem_real, dados_pagador['id'], dados_pagador['nome'], ambiente))
        
        msg_fin = ""
        if dados_pagador['id']:
            ok_fin, txt_fin = processar_cobranca_novo_fluxo(conn, dados_pagador, origem_real)
            if ok_fin:
                msg_fin = f" | {txt_fin}"
            else:
                msg_fin = f" | ⚠️ Erro Cobrança: {txt_fin}"
        
        conn.commit()
        conn.close()
        return {"sucesso": True, "dados": dados, "msg": "Consulta realizada." + msg_fin}
    except Exception as e:
        if conn: conn.close()
        return {"sucesso": False, "msg": str(e)}

# =============================================================================
# 5. GESTÃO DE PARÂMETROS E INTERFACE
# =============================================================================

def carregar_dados_genericos(nome_tabela):
    conn = get_conn()
    if conn:
        try: df = pd.read_sql(f"SELECT * FROM {nome_tabela} ORDER BY id DESC", conn); conn.close(); return df
        except: conn.close(); return None
    return None

def criar_tabela_ambiente():
    conn = get_conn()
    if conn:
        try: 
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_ambiente_consulta (id SERIAL PRIMARY KEY, ambiente VARCHAR(255), origem VARCHAR(255))")
            conn.commit(); conn.close(); return True
        except: conn.close(); return False
    return False

def salvar_alteracoes_genericas(nome_tabela, df_original, df_editado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        ids_orig = set(df_original['id'].dropna().astype(int).tolist())
        for index, row in df_editado.iterrows():
            cols = [c for c in row.index if c not in ['id', 'data_hora', 'data_criacao']]
            vals = [row[c] for c in cols]
            rid = row.get('id')
            if pd.isna(rid) or rid == '':
                pl = ", ".join(["%s"]*len(cols)); nm = ", ".join(cols)
                cur.execute(f"INSERT INTO {nome_tabela} ({nm}) VALUES ({pl})", vals)
            elif int(rid) in ids_orig:
                stset = ", ".join([f"{c}=%s" for c in cols])
                cur.execute(f"UPDATE {nome_tabela} SET {stset} WHERE id=%s", vals + [int(rid)])
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def listar_clientes_carteira():
    conn = get_conn()
    if conn:
        try: df = pd.read_sql("SELECT * FROM conexoes.fator_cliente_carteira ORDER BY id", conn); conn.close(); return df
        except: conn.close()
    return pd.DataFrame()

# =============================================================================
# 6. FUNÇÕES PARA MAPA DE DADOS
# =============================================================================

def criar_tabela_conexao_tabelas():
    conn = get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_conexao_tabelas (
                        id SERIAL PRIMARY KEY,
                        tabela_referencia TEXT,
                        tabela_referencia_coluna TEXT,
                        jason_api_fatorconferi_coluna TEXT
                    )
                """)
                conn.commit()
            return True
        except Exception as e:
            st.error(f"Erro ao criar tabela de conexão: {e}")
            return False
        finally:
            conn.close()

def listar_tabelas_disponiveis():
    conn = get_conn()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema || '.' || table_name 
                FROM information_schema.tables 
                WHERE table_schema IN ('banco_pf', 'conexoes', 'sistema_consulta') 
                ORDER BY table_schema, table_name
            """)
            return [r[0] for r in cur.fetchall()]
    except Exception as e:
        st.error(f"Erro ao listar tabelas: {e}")
        return []
    finally:
        conn.close()

def listar_colunas_geral(nome_tabela_completo):
    conn = get_conn()
    if not conn: return []
    try:
        parts = nome_tabela_completo.split('.')
        schema = parts[0] if len(parts) > 1 else 'public'
        tabela = parts[1] if len(parts) > 1 else parts[0]
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, tabela))
            return [r[0] for r in cur.fetchall()]
    except Exception as e:
        st.error(f"Erro ao listar colunas: {e}")
        return []
    finally:
        conn.close()

def listar_mapeamento_tabela(nome_tabela):
    conn = get_conn()
    if not conn: return {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tabela_referencia_coluna, jason_api_fatorconferi_coluna 
                FROM conexoes.fatorconferi_conexao_tabelas 
                WHERE tabela_referencia = %s
            """, (nome_tabela,))
            return {row[0]: row[1] for row in cur.fetchall()}
    except: return {}
    finally: conn.close()

def listar_todos_mapeamentos():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        return pd.read_sql("SELECT * FROM conexoes.fatorconferi_conexao_tabelas ORDER BY id DESC", conn)
    except Exception as e:
        st.error(f"Erro ao listar todos mapeamentos: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def salvar_mapeamento_grade(nome_tabela, df_mapeamento):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        for index, row in df_mapeamento.iterrows():
            col_sql = row['Coluna SQL']
            chave_json = str(row['Chave JSON API']).strip()
            
            cur.execute("""
                DELETE FROM conexoes.fatorconferi_conexao_tabelas 
                WHERE tabela_referencia = %s AND tabela_referencia_coluna = %s
            """, (nome_tabela, col_sql))
            
            if chave_json:
                cur.execute("""
                    INSERT INTO conexoes.fatorconferi_conexao_tabelas 
                    (tabela_referencia, tabela_referencia_coluna, jason_api_fatorconferi_coluna)
                    VALUES (%s, %s, %s)
                """, (nome_tabela, col_sql, chave_json))
        
        conn.commit()
        return True
    except Exception as e:
        if conn: conn.rollback()
        st.error(f"Erro ao salvar mapeamento: {e}")
        return False
    finally: conn.close()

# =============================================================================
# APP PRINCIPAL
# =============================================================================

def app_fator_conferi():
    criar_tabela_conexao_tabelas()

    st.markdown("### ⚡ Painel Fator Conferi")
    tabs = st.tabs(["👥 Clientes", "🔍 Teste de Consulta", "💰 Saldo API", "📋 Histórico", "⚙️ Parâmetros", "🗺️ Mapa de Dados"])

    with tabs[0]: 
        st.info("Gestão de Carteiras (Use o Módulo Clientes para criar novas)")
        df = listar_clientes_carteira()
        if not df.empty: st.dataframe(df, use_container_width=True)

    with tabs[1]:
        st.markdown("#### 1.1 Consulta e Importação")
        col_cli, col_cpf = st.columns([2, 2])
        
        id_cliente_teste = None
        conn = get_conn()
        if conn:
            try:
                df_clis = pd.read_sql("SELECT id, nome FROM admin.clientes ORDER BY nome", conn)
                opcoes_cli = {row['id']: row['nome'] for _, row in df_clis.iterrows()}
                id_cliente_teste = col_cli.selectbox("Cliente Pagador (Teste Manual)", options=[None] + list(opcoes_cli.keys()), format_func=lambda x: opcoes_cli[x] if x else "Usar Vínculo Automático")
            except: pass
            finally: conn.close()
            
        cpf_in = col_cpf.text_input("CPF Consultado")
        forcar = st.checkbox("Ignorar Histórico (Forçar Cobrança)", value=False)
        
        if st.button("🔍 Consultar", type="primary"):
            if cpf_in:
                with st.spinner("Buscando..."):
                    res = realizar_consulta_cpf(cpf_in, "teste_de_consulta_fatorconferi.cpf", forcar, id_cliente_teste)
                    st.session_state['resultado_fator'] = res

                    if res['sucesso']:
                        ok_s, msg_s = salvar_dados_fator_no_banco(res['dados'])
                        if ok_s: st.toast(f"{msg_s}", icon="💾")
                        else: st.error(f"Erro ao salvar na base PF: {msg_s}")
                        
                        lista_sucessos, lista_erros = executar_distribuicao_dinamica(res['dados'])
                        
                        if lista_sucessos:
                            msg_ok = ", ".join(lista_sucessos)
                            st.success(f"✅ Dados distribuídos com sucesso para: {msg_ok}")
                            
                        if lista_erros:
                            msg_erro = "\n".join(lista_erros)
                            st.error(f"⚠️ Relatório de Importação:\n{msg_erro}")
                            
        
        if 'resultado_fator' in st.session_state:
            res = st.session_state['resultado_fator']
            if res['sucesso']:
                if "msg" in res: st.success(res['msg'])
                with st.expander("Ver Dados Retornados", expanded=True): st.json(res['dados'])
            else: st.error(res.get('msg', 'Erro'))

    with tabs[2]: 
        if st.button("🔄 Atualizar"): 
            ok, v = consultar_saldo_api()
            if ok: st.metric("Saldo Atual", f"R$ {v:.2f}")
    
    with tabs[3]: 
        st.markdown("<p style='color: lightblue; font-size: 12px; margin-bottom: 0px;'>Tabela: conexoes.fatorconferi_registo_consulta</p>", unsafe_allow_html=True)
        conn = get_conn()
        if conn: 
            df_hist = pd.read_sql("SELECT * FROM conexoes.fatorconferi_registo_consulta ORDER BY id DESC LIMIT 20", conn)
            conn.close()
            event = st.dataframe(df_hist, on_select="rerun", selection_mode="single-row", use_container_width=True, hide_index=True)
            if len(event.selection.rows) > 0:
                idx = event.selection.rows[0]
                caminho_arq = df_hist.iloc[idx].get("caminho_json")
                if caminho_arq and os.path.exists(caminho_arq):
                    with open(caminho_arq, "r", encoding="utf-8") as f:
                        st.download_button(label=f"⬇️ Baixar JSON", data=f.read(), file_name=os.path.basename(caminho_arq), mime="application/json")
    
    with tabs[4]: 
        st.markdown("### 🛠️ Gestão de Tabelas do Sistema")
        opcoes_tabelas = {
            "1. Carteiras de Clientes": "conexoes.fator_cliente_carteira",
            "2. Origens de Consulta": "conexoes.fatorconferi_origem_consulta_fator",
            "3. Parâmetros Gerais": "conexoes.fatorconferi_parametros",
            "4. Registros de Consulta": "conexoes.fatorconferi_registo_consulta",
            "5. Tipos de Consulta": "conexoes.fatorconferi_tipo_consulta_fator",
            "6. Valores da Consulta": "conexoes.fatorconferi_valor_da_consulta",
            "7. Relação de Conexões": "conexoes.relacao",
            "8. Ambiente de Consulta": "conexoes.fatorconferi_ambiente_consulta"
        }
        tabela_escolhida = st.selectbox("Selecione a Tabela:", list(opcoes_tabelas.keys()))
        nome_sql = opcoes_tabelas[tabela_escolhida]
        if nome_sql:
            df_param = carregar_dados_genericos(nome_sql)
            if df_param is None:
                st.warning(f"A tabela `{nome_sql}` não foi encontrada.")
                if nome_sql == "conexoes.fatorconferi_ambiente_consulta":
                    if st.button("🛠️ Criar Tabela Ambiente Agora"): criar_tabela_ambiente(); st.rerun()
            else:
                df_editado = st.data_editor(df_param, key=f"editor_{nome_sql}", num_rows="dynamic", use_container_width=True)
                if st.button("💾 Salvar Alterações", type="primary"):
                    if salvar_alteracoes_genericas(nome_sql, df_param, df_editado): st.success("Salvo!"); time.sleep(1); st.rerun()

    with tabs[5]:
        st.subheader("⚙️ Mapeamento de Dados (API -> SQL)")
        st.info("Configure qual campo da API (JSON Key) deve ser salvo em qual coluna da tabela.")
        
        lista_tabelas = listar_tabelas_disponiveis()
        tabela_sel = st.selectbox("1. Selecione a Tabela Destino:", ["(Selecione)"] + lista_tabelas)
        
        if tabela_sel != "(Selecione)":
            colunas_db = listar_colunas_geral(tabela_sel)
            mapa_existente = listar_mapeamento_tabela(tabela_sel)
            colunas_pre_selecionadas = [c for c in mapa_existente.keys() if c in colunas_db]
            
            colunas_sel = st.multiselect(
                "2. Escolha as colunas para mapear:", 
                options=colunas_db, 
                default=colunas_pre_selecionadas
            )
            
            if colunas_sel:
                st.divider()
                st.markdown("#### 3. Editar Mapeamento")
                st.caption("Escreva o nome exato do campo da API na coluna da direita (ex: `nome`, `cpf`, `nascto`).")
                
                dados_grade = []
                for col in colunas_sel:
                    val_atual = mapa_existente.get(col, "")
                    dados_grade.append({
                        "Tabela Destino": tabela_sel,
                        "Coluna SQL": col, 
                        "Chave JSON API": val_atual
                    })
                
                df_grade = pd.DataFrame(dados_grade)
                
                df_editado = st.data_editor(
                    df_grade,
                    column_config={
                        "Tabela Destino": st.column_config.TextColumn(disabled=True),
                        "Coluna SQL": st.column_config.TextColumn(disabled=True),
                        "Chave JSON API": st.column_config.TextColumn(
                            help="Nome do campo que vem do Fator Conexo (ex: nome, cpf, rg)"
                        )
                    },
                    hide_index=True,
                    use_container_width=True,
                    num_rows="fixed"
                )
                
                if st.button("💾 Salvar Mapeamento", type="primary"):
                    if salvar_mapeamento_grade(tabela_sel, df_editado):
                        st.success(f"Mapeamento salvo com sucesso para a tabela **{tabela_sel}**!")
                        time.sleep(1.5)
                        st.rerun()
        
        st.divider()
        st.markdown("### 📋 Tabela Geral de Conexões (conexoes.fatorconferi_conexao_tabelas)")
        df_geral = listar_todos_mapeamentos()
        st.dataframe(df_geral, use_container_width=True)