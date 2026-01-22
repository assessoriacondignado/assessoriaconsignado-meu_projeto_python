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
    Remove espaços, quebras de linha e converte indicadores de vazio para None.
    """
    if valor is None: return None
    
    if isinstance(valor, str):
        v = valor.strip()
        if v.upper() in ["NULO", "NULL", "NONE"]: return None
        if v in ["[]", "[ ]", "{}"]: return None
        if v == "": return None
        return v
        
    return valor

# =============================================================================
# 2. PARSING XML ESTRITO
# =============================================================================

def _xml_para_dict_recursivo(elemento):
    texto = sanitizar_valor_api(elemento.text)
    if len(elemento) == 0:
        return texto

    resultado = {}
    for filho in elemento:
        tag = filho.tag
        if '}' in tag: tag = tag.split('}', 1)[1]
        valor_filho = _xml_para_dict_recursivo(filho)
        
        if tag in resultado:
            if isinstance(resultado[tag], list):
                resultado[tag].append(valor_filho)
            else:
                resultado[tag] = [resultado[tag], valor_filho]
        else:
            resultado[tag] = valor_filho
            
    return resultado

def parse_xml_to_dict(texto_xml):
    try:
        if isinstance(texto_xml, bytes):
            texto_xml = texto_xml.decode('utf-8', errors='ignore')
        texto_xml = texto_xml.replace('ISO-8859-1', 'UTF-8')
        
        try:
            return json.loads(texto_xml)
        except:
            pass

        root = ET.fromstring(texto_xml)
        dados = _xml_para_dict_recursivo(root)
        return dados if isinstance(dados, dict) else {}

    except Exception as e:
        return {"erro": f"Erro no Parser: {e}", "raw": texto_xml}

# =============================================================================
# 3. BUSCA INTELIGENTE POR CAMINHO (IGNORA ESPAÇOS NAS CHAVES)
# =============================================================================

def _navegar_recursivamente(dados_atuais, lista_chaves):
    """
    Navega no JSON seguindo a lista de chaves.
    AGORA COM .strip() NAS CHAVES DA API para ignorar espaços extras.
    """
    if not lista_chaves:
        return dados_atuais

    chave_atual = lista_chaves[0].upper().strip()
    resto_chaves = lista_chaves[1:]

    # Caso 1: Dados atuais são um Dicionário
    if isinstance(dados_atuais, dict):
        # Tenta encontrar a chave exata (ignorando espaços da API)
        for k, v in dados_atuais.items():
            # AQUI ESTÁ A CORREÇÃO: k.upper().strip()
            if k.upper().strip() == chave_atual:
                return _navegar_recursivamente(v, resto_chaves)
        
        # Tentativa de combinação (ex: TELEFONES_FIXO)
        if resto_chaves:
            chave_combinada = f"{chave_atual}_{resto_chaves[0].upper().strip()}"
            for k, v in dados_atuais.items():
                if k.upper().strip() == chave_combinada:
                    return _navegar_recursivamente(v, resto_chaves[1:])

    # Caso 2: Dados atuais são uma Lista (1:N)
    elif isinstance(dados_atuais, list):
        resultados_coletados = []
        for item in dados_atuais:
            res = _navegar_recursivamente(item, lista_chaves)
            if res is not None:
                if isinstance(res, list):
                    resultados_coletados.extend(res)
                else:
                    resultados_coletados.append(res)
        return resultados_coletados if resultados_coletados else None

    return None

def extrair_valor_por_caminho_complexo(dados_api, string_mapeamento):
    """
    Interpreta strings como: "TELEFONES_FIXO;_TELEFONES_MOVÍVEIS_;_TELEFONE_;NÚMERO"
    """
    if not string_mapeamento: return None
    
    caminhos_alternativos = str(string_mapeamento).split(';')
    
    for caminho_bruto in caminhos_alternativos:
        # Limpa o caminho do MAPA
        caminho_limpo = caminho_bruto.strip().replace(' ', '_')
        partes = [p for p in caminho_limpo.split('_') if p]
        
        valor_encontrado = _navegar_recursivamente(dados_api, partes)
        valor_sanitizado = sanitizar_valor_api(valor_encontrado)
        
        if valor_sanitizado is not None:
            if isinstance(valor_sanitizado, list) and len(valor_sanitizado) == 0:
                continue 
            return valor_sanitizado

    return None

# =============================================================================
# 4. SALVAMENTO PADRÃO (LEGADO)
# =============================================================================

def verificar_coluna_cpf(cur, tabela):
    try:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'banco_pf' AND table_name = '{tabela}' AND column_name IN ('cpf', 'cpf_ref')")
        res = cur.fetchone()
        if res: return res[0]
    except: pass
    return 'cpf_ref' 

def salvar_dados_fator_no_banco(dados_api):
    raw_cpf = extrair_valor_por_caminho_complexo(dados_api, "CPF;_CADASTRAIS_CPF")
    
    conn = get_conn()
    if not conn: return False, "Erro BD"
    
    if not raw_cpf: return False, "CPF não localizado."
    cpf_limpo = mv.ValidadorDocumentos.cpf_para_sql(raw_cpf)
    if not cpf_limpo: return False, f"CPF Inválido: {raw_cpf}"

    try:
        cur = conn.cursor()
        
        campos = {
            'nome': extrair_valor_por_caminho_complexo(dados_api, "NOME;_CADASTRAIS_NOME"),
            'rg': extrair_valor_por_caminho_complexo(dados_api, "RG;_CADASTRAIS_RG"),
            'data_nascimento': mv.ValidadorData.para_sql(extrair_valor_por_caminho_complexo(dados_api, "NASCTO;NASCIMENTO;_CADASTRAIS_NASCTO")),
            'nome_mae': extrair_valor_por_caminho_complexo(dados_api, "NOME_MAE;MAE;_CADASTRAIS_NOME_MAE"),
            'nome_pai': extrair_valor_por_caminho_complexo(dados_api, "NOME_PAI;PAI;_CADASTRAIS_NOME_PAI"),
            'uf_rg': extrair_valor_por_caminho_complexo(dados_api, "UF_RG;UF_EMISSAO"),
            'sexo': extrair_valor_por_caminho_complexo(dados_api, "SEXO;_CADASTRAIS_SEXO")
        }

        sql_pf = """
            INSERT INTO banco_pf.pf_dados (cpf, nome, rg, data_nascimento, nome_mae, nome_pai, uf_rg, data_criacao) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (cpf) DO UPDATE SET 
            nome=EXCLUDED.nome, data_nascimento=EXCLUDED.data_nascimento, nome_mae=EXCLUDED.nome_mae
        """
        cur.execute(sql_pf, (cpf_limpo, campos['nome'], campos['rg'], campos['data_nascimento'], campos['nome_mae'], campos['nome_pai'], campos['uf_rg']))
        
        tels = extrair_valor_por_caminho_complexo(dados_api, "TELEFONES_MOVEL_TELEFONE_NUMERO;TELEFONES_FIXO_TELEFONE_NUMERO;TELEFONES_NUMERO")
        if tels:
            if not isinstance(tels, list): tels = [tels]
            col_tel = verificar_coluna_cpf(cur, 'pf_telefones')
            for t in tels:
                tv = mv.ValidadorContato.telefone_para_sql(t)
                if tv:
                    cur.execute(f"INSERT INTO banco_pf.pf_telefones ({col_tel}, numero) VALUES (%s, %s) ON CONFLICT DO NOTHING", (cpf_limpo, tv))

        conn.commit(); conn.close()
        return True, "Dados básicos salvos."
    except Exception as e:
        return False, str(e)

# =============================================================================
# 5. DISTRIBUIÇÃO DINÂMICA (MAPA DE DADOS)
# =============================================================================

def executar_distribuicao_dinamica(dados_api):
    conn = get_conn()
    if not conn: return [], ["Erro de conexão DB"]

    sucessos = []
    erros = []

    try:
        df_map = pd.read_sql("SELECT tabela_referencia, tabela_referencia_coluna, jason_api_fatorconferi_coluna FROM conexoes.fatorconferi_conexao_tabelas", conn)
        tabelas = df_map['tabela_referencia'].unique()
        cur = conn.cursor()

        # 1. Identifica listas conhecidas no JSON (Normalizado com .strip)
        chaves_listas_detectadas = {} 
        for k, v in dados_api.items():
            k_limpo = k.upper().strip()
            if isinstance(v, list) and len(v) > 0:
                chaves_listas_detectadas[k_limpo] = v
            if isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    sub_k_limpo = sub_k.upper().strip()
                    if isinstance(sub_v, list) and len(sub_v) > 0:
                        chaves_listas_detectadas[sub_k_limpo] = sub_v

        GRUPOS_LISTA = {
            'TELEFONES': ['NUMERO', 'DDD', 'TIPO_TEL', 'OPERADORA', 'TELEFONE'],
            'ENDERECOS': ['RUA', 'LOGRADOURO', 'BAIRRO', 'CIDADE', 'CEP', 'UF', 'ESTADO', 'ENDERECO'],
            'EMAILS': ['EMAIL']
        }

        for tabela in tabelas:
            try:
                regras = df_map[df_map['tabela_referencia'] == tabela]
                
                lista_para_iterar = None
                chaves_mapeadas = [str(r['jason_api_fatorconferi_coluna']).strip().upper() for _, r in regras.iterrows()]
                
                for nome_grupo, campos_tipicos in GRUPOS_LISTA.items():
                    if any(c in chaves_mapeadas for c in campos_tipicos):
                        for chave_api_lista, conteudo_lista in chaves_listas_detectadas.items():
                            if nome_grupo in chave_api_lista or chave_api_lista in [nome_grupo]:
                                lista_para_iterar = conteudo_lista
                                break
                    if lista_para_iterar: break
                
                if not lista_para_iterar:
                    lista_para_iterar = [None]

                for item_lista in lista_para_iterar:
                    colunas_sql = []
                    valores_insert = []
                    tem_dado = False
                    
                    for _, row in regras.iterrows():
                        col_sql = str(row['tabela_referencia_coluna']).strip()
                        chave_xml = str(row['jason_api_fatorconferi_coluna']).strip()
                        
                        valor = None
                        
                        # 1. Tenta no Item da Lista (Ignorando espaços na chave do item)
                        if item_lista and isinstance(item_lista, dict):
                            for k_item, v_item in item_lista.items():
                                # Checa se a chave do mapa está contida na chave do item (ex: NÚMERO em " NÚMERO ")
                                # Ou busca exata com strip
                                k_limpo = k_item.upper().strip()
                                # Limpa a chave do mapa para comparação (remove _TELEFONE_ etc)
                                chave_xml_limpa = chave_xml.upper().replace('_', '').strip()
                                
                                if k_limpo == chave_xml_limpa or k_limpo in chave_xml.upper():
                                    valor = v_item
                                    break
                                    
                        elif item_lista and isinstance(item_lista, str):
                            if 'EMAIL' in chave_xml.upper():
                                valor = item_lista

                        # 2. Tenta na Raiz
                        if valor is None:
                            valor = extrair_valor_por_caminho_complexo(dados_api, chave_xml)

                        if valor:
                            if isinstance(valor, (list, dict)): valor = str(valor)
                            if ('cpf' in col_sql.lower() or 'cpf' in chave_xml.lower()):
                                cpf_ajustado = mv.ValidadorDocumentos.cpf_para_sql(valor)
                                if cpf_ajustado: valor = cpf_ajustado
                            tem_dado = True
                        
                        colunas_sql.append(col_sql)
                        valores_insert.append(valor)

                    if tem_dado:
                        placeholders = ", ".join(["%s"] * len(valores_insert))
                        cols = ", ".join(colunas_sql)
                        sql = f"INSERT INTO {tabela} ({cols}) VALUES ({placeholders})"
                        cur.execute(sql, tuple(valores_insert))

                sucessos.append(tabela)
            
            except Exception as e:
                conn.rollback()
                erros.append(f"❌ Erro '{tabela}': {e}")

        conn.commit(); cur.close(); conn.close()
        return sucessos, erros

    except Exception as e:
        if conn: conn.close()
        return [], [str(e)]

# =============================================================================
# 6. FUNÇÕES API / CONSULTA
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
        
        # --- PARSER ATUALIZADO XML PURO ---
        dados = parse_xml_to_dict(resp.text)
        
        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        
        try:
            with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4, ensure_ascii=False)
        except Exception as e:
            st.error(f"Erro ao salvar arquivo JSON de log: {e}")

        # Validação simples se veio algo (buscando NOME em qualquer lugar)
        if not extrair_valor_por_caminho_complexo(dados, "NOME;CADASTRAIS_NOME"): 
            conn.close()
            return {
                "sucesso": False, 
                "msg": f"Sem dados retornados ou erro de estrutura. (Log: {nome_arq})", 
                "dados": dados
            }
        
        # Garante CPF no retorno se não veio
        if not extrair_valor_por_caminho_complexo(dados, "CPF;CADASTRAIS_CPF"): 
            dados['CPF_CONSULTADO_INSERIDO'] = cpf_padrao

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
# APP PRINCIPAL E INTERFACE
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

def salvar_alteracoes_mapa_completo(df_original, df_editado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        ids_orig = set(df_original['id'].dropna().astype(int).tolist())
        ids_novos = set(df_editado['id'].dropna().astype(int).tolist())
        
        ids_para_deletar = ids_orig - ids_novos
        
        if ids_para_deletar:
            if len(ids_para_deletar) == 1:
                cur.execute(f"DELETE FROM conexoes.fatorconferi_conexao_tabelas WHERE id = {list(ids_para_deletar)[0]}")
            else:
                cur.execute(f"DELETE FROM conexoes.fatorconferi_conexao_tabelas WHERE id IN {tuple(ids_para_deletar)}")

        for index, row in df_editado.iterrows():
            rid = row.get('id')
            tab_ref = row.get('tabela_referencia')
            col_ref = row.get('tabela_referencia_coluna')
            json_key = row.get('jason_api_fatorconferi_coluna')
            
            if pd.isna(rid) or rid == '':
                cur.execute("""
                    INSERT INTO conexoes.fatorconferi_conexao_tabelas 
                    (tabela_referencia, tabela_referencia_coluna, jason_api_fatorconferi_coluna)
                    VALUES (%s, %s, %s)
                """, (tab_ref, col_ref, json_key))
            elif int(rid) in ids_orig:
                cur.execute("""
                    UPDATE conexoes.fatorconferi_conexao_tabelas 
                    SET tabela_referencia=%s, tabela_referencia_coluna=%s, jason_api_fatorconferi_coluna=%s
                    WHERE id=%s
                """, (tab_ref, col_ref, json_key, int(rid)))
        
        conn.commit(); conn.close()
        return True
    except Exception as e:
        if conn: conn.close()
        st.error(f"Erro ao salvar mapa: {e}")
        return False

def listar_clientes_carteira():
    conn = get_conn()
    if conn:
        try: df = pd.read_sql("SELECT * FROM conexoes.fator_cliente_carteira ORDER BY id", conn); conn.close(); return df
        except: conn.close()
    return pd.DataFrame()

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
                with st.expander("Ver Dados Retornados (Estrutura Fiel)", expanded=True): st.json(res['dados'])
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
                    num_rows="fixed",
                    key=f"editor_mapa_{tabela_sel}"
                )
                
                if st.button("💾 Salvar Mapeamento", type="primary"):
                    if salvar_mapeamento_grade(tabela_sel, df_editado):
                        st.success(f"Mapeamento salvo com sucesso para a tabela **{tabela_sel}**!")
                        time.sleep(1.5)
                        st.rerun()
        
        st.divider()
        st.markdown("### 📋 Tabela Geral de Conexões (Editável)")
        df_geral = listar_todos_mapeamentos()
        
        df_editado_geral = st.data_editor(
            df_geral, 
            key="editor_geral_mapeamentos", 
            num_rows="dynamic", 
            use_container_width=True
        )
        
        if st.button("💾 Salvar Alterações Gerais", type="primary"):
            if salvar_alteracoes_mapa_completo(df_geral, df_editado_geral):
                st.success("Tabela geral atualizada com sucesso!")
                time.sleep(1.5)
                st.rerun()