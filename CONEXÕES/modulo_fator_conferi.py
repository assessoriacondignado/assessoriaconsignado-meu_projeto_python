import streamlit as st
import pandas as pd
import psycopg2
import requests
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

import conexao
import modulo_validadores as mv

# --- CONFIGURAÇÕES ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PASTA_JSON = os.path.join(BASE_DIR, "JSON")

if not os.path.exists(PASTA_JSON):
    try: os.makedirs(PASTA_JSON, exist_ok=True)
    except: pass

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

# =============================================================================
# 1. SANITIZAÇÃO E FORMATAÇÃO (REGRA 3: MAIÚSCULO, DATA, NULL)
# =============================================================================

def sanitizar_e_formatar(valor):
    """
    Aplica todas as regras de limpeza de uma vez:
    1. Converte NULO/Empty -> None
    2. Converte Data dd/mm/yyyy -> yyyy-mm-dd
    3. Converte Tudo para MAIÚSCULO
    """
    if valor is None: return None
    
    # Se for lista, não sanitiza aqui (será tratado no loop de inserção)
    if isinstance(valor, list): return valor
    
    # Converte para string e remove espaços
    v_str = str(valor).strip()
    
    # 1. Regra de Nulos
    if not v_str or v_str.upper() in ["NULO", "NULL", "NONE", "[]", "{}"]:
        return None
    
    # 2. Regra de Data (DD/MM/YYYY -> YYYY-MM-DD)
    # Regex simples para identificar dd/mm/yyyy
    if re.match(r'^\d{2}/\d{2}/\d{4}$', v_str):
        try:
            dt_obj = datetime.strptime(v_str, '%d/%m/%Y')
            return dt_obj.strftime('%Y-%m-%d')
        except: 
            pass # Se der erro, retorna o original
            
    # 3. Regra de Maiúsculo
    return v_str.upper()

# =============================================================================
# 2. PARSER SIMPLIFICADO (XML -> JSON)
# =============================================================================

def _xml_to_dict_simple(element):
    # Pega o texto do elemento
    text = element.text.strip() if element.text else None
    
    # Se não tem filhos, retorna o texto
    if len(element) == 0:
        return text

    result = {}
    for child in element:
        tag = child.tag.replace('{', '').split('}')[-1].upper() # Remove namespace e põe Upper
        child_data = _xml_to_dict_simple(child)
        
        if tag in result:
            if isinstance(result[tag], list):
                result[tag].append(child_data)
            else:
                result[tag] = [result[tag], child_data]
        else:
            result[tag] = child_data
    return result

def parse_xml_to_dict(texto_raw):
    """Lê XML ou JSON e retorna Dicionário Python Puro"""
    try:
        # Tenta decodificar se for bytes
        if isinstance(texto_raw, bytes):
            texto_raw = texto_raw.decode('utf-8', errors='ignore')
        
        # Limpa encoding do cabeçalho se existir
        texto_raw = texto_raw.replace('ISO-8859-1', 'UTF-8')

        # Tenta ler como JSON direto
        try:
            return json.loads(texto_raw)
        except:
            pass
            
        # Se falhar, lê como XML
        root = ET.fromstring(texto_raw)
        return _xml_to_dict_simple(root)
    except Exception as e:
        return {}

# =============================================================================
# 3. NOVA EXTRAÇÃO POR SINTAXE (REGRA 2: ; [] {})
# =============================================================================

def extrair_valor_novo_padrao(dados, caminho_str):
    """
    Navega no JSON usando APENAS o separador ';'
    Detecta '[]' para identificar listas e '{}' para chaves finais.
    Ex: TELEFONES_MOVEL;TELEFONE;[]{NUMERO}
    """
    if not caminho_str: return None
    
    # Remove aspas de exemplo (ex: "0000") que o usuário possa ter deixado
    caminho_limpo = re.sub(r'".*?"', '', caminho_str).strip()
    
    # Divide pelo separador mandatório ;
    passos = [p.strip() for p in caminho_limpo.split(';') if p.strip()]
    
    cursor = dados # Começa na raiz
    
    for i, passo in enumerate(passos):
        if cursor is None: return None
        
        # Detecta marcadores
        is_list_iter = '[]' in passo
        # Limpa marcadores para pegar o nome da chave real
        chave = passo.replace('[]', '').replace('{', '').replace('}', '').upper()
        
        # Lógica de Navegação
        if isinstance(cursor, dict):
            # Busca a chave no dicionário (case insensitive para garantir)
            # A chave no JSON já foi convertida para Upper no Parser, mas garantimos aqui
            encontrou = False
            for k, v in cursor.items():
                if k.upper() == chave:
                    cursor = v
                    encontrou = True
                    break
            if not encontrou: return None
            
        elif isinstance(cursor, list) and is_list_iter:
            # ESTAMOS NUMA LISTA (LOOP)
            # Precisamos extrair a chave de TODOS os itens
            lista_valores = []
            for item in cursor:
                if isinstance(item, dict):
                    # Tenta pegar o valor da chave dentro do item
                    for k, v in item.items():
                        if k.upper() == chave:
                            lista_valores.append(v)
                            break
                elif isinstance(item, str) and chave == "": 
                    # Caso onde a lista é de strings simples
                    lista_valores.append(item)
            
            # Se for o último passo, retorna a lista encontrada
            # Se não for, teríamos que lidar com lista de listas (complexo), 
            # mas pela regra 1:N simples, assumimos que [] é o passo final ou penúltimo.
            cursor = lista_valores
            
        else:
            return None # Caminho inválido ou estrutura não bate

    return cursor

# =============================================================================
# 4. INSERÇÃO NO BANCO (REGRA 3: PASSO A e PASSO B)
# =============================================================================

def executar_distribuicao_dinamica(dados_api):
    conn = get_conn()
    if not conn: return [], ["Erro conexão DB"]
    
    sucessos = []
    erros = []
    
    try:
        # Lê o mapa do banco
        df_map = pd.read_sql("SELECT tabela_referencia, tabela_referencia_coluna, jason_api_fatorconferi_coluna FROM conexoes.fatorconferi_conexao_tabelas", conn)
        tabelas = df_map['tabela_referencia'].unique()
        cur = conn.cursor()
        
        for tabela in tabelas:
            try:
                regras = df_map[df_map['tabela_referencia'] == tabela]
                
                # Dicionário para guardar os dados extraídos: { 'coluna_sql': valor_ou_lista }
                dados_extraidos = {}
                max_linhas = 1 # Para controlar se vamos inserir 1 linha ou N linhas (loop)
                
                # --- PASSO A: EXTRAÇÃO ---
                for _, row in regras.iterrows():
                    col_sql = str(row['tabela_referencia_coluna']).strip()
                    caminho = str(row['jason_api_fatorconferi_coluna']).strip()
                    
                    # 1. Extrai usando a nova lógica (;)
                    valor_raw = extrair_valor_novo_padrao(dados_api, caminho)
                    
                    # 2. Sanitiza (Maiúsculo, Data, Null)
                    if isinstance(valor_raw, list):
                        # Sanitiza cada item da lista
                        valor_final = [sanitizar_e_formatar(v) for v in valor_raw]
                        if len(valor_final) > max_linhas:
                            max_linhas = len(valor_final)
                    else:
                        valor_final = sanitizar_e_formatar(valor_raw)
                        
                    # 3. Validação Específica de CPF (Remove pontuação para SQL)
                    if 'CPF' in col_sql.upper() or 'CPF' in caminho.upper():
                        if isinstance(valor_final, list):
                            valor_final = [mv.ValidadorDocumentos.cpf_para_sql(v) for v in valor_final]
                        else:
                            valor_final = mv.ValidadorDocumentos.cpf_para_sql(valor_final)

                    dados_extraidos[col_sql] = valor_final

                # --- PASSO B: INSERÇÃO (LOOP) ---
                if not dados_extraidos: continue

                cols = list(dados_extraidos.keys())
                placeholders = ", ".join(["%s"] * len(cols))
                sql_base = f"INSERT INTO {tabela} ({', '.join(cols)}) VALUES ({placeholders})"
                
                # Adiciona regra de conflito (UPSERT)
                cols_lower = [c.lower() for c in cols]
                if 'cpf' in cols_lower:
                    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c.lower() != 'cpf'])
                    sql_base += f" ON CONFLICT (cpf) DO UPDATE SET {update_set}" if update_set else " ON CONFLICT (cpf) DO NOTHING"
                elif 'id' in cols_lower:
                    sql_base += " ON CONFLICT (id) DO NOTHING"

                count_ins = 0
                for i in range(max_linhas):
                    linha_vals = []
                    tem_dado = False
                    
                    for col in cols:
                        val = dados_extraidos[col]
                        
                        # Se é lista, pega o item 'i'. Se é valor único (ex: CPF do titular), repete.
                        if isinstance(val, list):
                            item = val[i] if i < len(val) else None
                        else:
                            item = val
                            
                        if item: tem_dado = True
                        linha_vals.append(item)
                    
                    # Só insere se a linha tiver algum conteúdo útil
                    if tem_dado:
                        cur.execute(sql_base, tuple(linha_vals))
                        count_ins += 1
                
                sucessos.append(f"{tabela} ({count_ins})")

            except Exception as e:
                conn.rollback()
                erros.append(f"Erro em {tabela}: {e}")
        
        conn.commit(); cur.close(); conn.close()
        return sucessos, erros

    except Exception as e:
        if conn: conn.close()
        return [], [str(e)]

# =============================================================================
# 5. FUNÇÕES DE API E INTERFACE (MANTIDAS PADRÃO)
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
        if not res_custo: return False, "Custo não definido."
            
        valor_debitar = float(res_custo[0])
        id_prod_vinc = res_custo[1] 
        nome_prod_vinc = res_custo[2]
        
        if valor_debitar <= 0: return True, "Gratuito."

        sql_saldo = "SELECT saldo_novo FROM cliente.extrato_carteira_por_produto WHERE id_cliente = %s ORDER BY id DESC LIMIT 1"
        cur.execute(sql_saldo, (id_cli,))
        res_saldo = cur.fetchone()
        saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
        saldo_novo = saldo_anterior - valor_debitar
        
        sql_insert = """
            INSERT INTO cliente.extrato_carteira_por_produto (
                produto_vinculado, id_cliente, nome_cliente, id_usuario, nome_usuario,
                origem_lancamento, data_lancamento, tipo_lancamento, valor_lancado, saldo_anterior, saldo_novo, id_produto
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'DEBITO', %s, %s, %s, %s)
        """
        cur.execute(sql_insert, (
            nome_prod_vinc, id_cli, dados_cliente['nome'], str(dados_cliente.get('id_usuario', '0')), dados_cliente.get('nome_usuario', 'Sistema'),
            origem_custo_chave, valor_debitar, saldo_anterior, saldo_novo, id_prod_vinc
        ))
        return True, f"Débito R$ {valor_debitar:.2f}"
    except Exception as e: return False, f"Erro: {str(e)}"

def realizar_consulta_cpf(cpf, ambiente, forcar_nova=False, id_cliente_pagador_manual=None):
    cpf_padrao = mv.ValidadorDocumentos.cpf_para_sql(cpf)
    if not cpf_padrao: return {"sucesso": False, "msg": "CPF Inválido"}

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
            if res: dados_pagador["id"] = res[0]; dados_pagador["nome"] = res[1]
        except: pass
    else:
        d = buscar_cliente_vinculado_ao_usuario(id_usuario)
        dados_pagador["id"] = d['id']; dados_pagador["nome"] = d['nome']

    origem_real = buscar_origem_por_ambiente(ambiente)

    try:
        cur = conn.cursor()
        
        # Cache Check
        if not forcar_nova:
            cur.execute("SELECT caminho_json FROM conexoes.fatorconferi_registo_consulta WHERE cpf_consultado=%s AND status_api='SUCESSO' ORDER BY id DESC LIMIT 1", (cpf_padrao,))
            res = cur.fetchone()
            if res and res[0] and os.path.exists(res[0]):
                with open(res[0], 'r', encoding='utf-8') as f: dados = json.load(f)
                conn.close()
                return {"sucesso": True, "dados": dados, "msg": "Cache recuperado."}

        # Saldo Check
        custo_previsto = 0.0
        if dados_pagador['id']:
             sql_custo = "SELECT valor_custo FROM cliente.valor_custo_carteira_cliente WHERE id_cliente = %s AND origem_custo = %s LIMIT 1"
             cur.execute(sql_custo, (str(dados_pagador['id']), origem_real))
             res_custo = cur.fetchone()
             custo_previsto = float(res_custo[0]) if res_custo else buscar_valor_consulta_atual()

             sql_saldo = "SELECT saldo_novo FROM cliente.extrato_carteira_por_produto WHERE id_cliente = %s ORDER BY id DESC LIMIT 1"
             cur.execute(sql_saldo, (str(dados_pagador['id']),))
             res_s = cur.fetchone()
             saldo_atual = float(res_s[0]) if res_s else 0.0

             if saldo_atual < custo_previsto:
                 conn.close()
                 return {"sucesso": False, "msg": "Saldo insuficiente."}

        # API Call
        cred = buscar_credenciais()
        if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token API ausente."}
        
        resp = requests.get(f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}", timeout=30)
        dados = parse_xml_to_dict(resp.text)
        
        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        try:
            with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4, ensure_ascii=False)
        except: pass

        cur.execute("INSERT INTO conexoes.fatorconferi_registo_consulta (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, origem_consulta, data_hora, id_cliente, nome_cliente, ambiente) VALUES ('CPF SIMPLES', %s, %s, %s, %s, %s, 'SUCESSO', %s, NOW(), %s, %s, %s)", 
                    (cpf_padrao, id_usuario, nome_usuario, custo_previsto, path, origem_real, dados_pagador['id'], dados_pagador['nome'], ambiente))
        
        msg_fin = ""
        if dados_pagador['id']:
            ok_fin, txt_fin = processar_cobranca_novo_fluxo(conn, dados_pagador, origem_real)
            msg_fin = f" | {txt_fin}"
        
        conn.commit(); conn.close()
        return {"sucesso": True, "dados": dados, "msg": "Consulta OK." + msg_fin}
    except Exception as e:
        if conn: conn.close()
        return {"sucesso": False, "msg": str(e)}

# =============================================================================
# APP INTERFACE
# =============================================================================

def carregar_dados_genericos(nome_tabela):
    conn = get_conn()
    if conn:
        try: df = pd.read_sql(f"SELECT * FROM {nome_tabela} ORDER BY id DESC", conn); conn.close(); return df
        except: conn.close(); return None
    return None

def salvar_alteracoes_mapa_completo(df_original, df_editado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        ids_orig = set(df_original['id'].dropna().astype(int).tolist())
        ids_novos = set(df_editado['id'].dropna().astype(int).tolist())
        ids_del = ids_orig - ids_novos
        
        if ids_del:
            if len(ids_del) == 1: cur.execute(f"DELETE FROM conexoes.fatorconferi_conexao_tabelas WHERE id = {list(ids_del)[0]}")
            else: cur.execute(f"DELETE FROM conexoes.fatorconferi_conexao_tabelas WHERE id IN {tuple(ids_del)}")

        for index, row in df_editado.iterrows():
            rid = row.get('id')
            tab = row.get('tabela_referencia')
            col = row.get('tabela_referencia_coluna')
            js = row.get('jason_api_fatorconferi_coluna')
            
            if pd.isna(rid) or rid == '':
                cur.execute("INSERT INTO conexoes.fatorconferi_conexao_tabelas (tabela_referencia, tabela_referencia_coluna, jason_api_fatorconferi_coluna) VALUES (%s, %s, %s)", (tab, col, js))
            elif int(rid) in ids_orig:
                cur.execute("UPDATE conexoes.fatorconferi_conexao_tabelas SET tabela_referencia=%s, tabela_referencia_coluna=%s, jason_api_fatorconferi_coluna=%s WHERE id=%s", (tab, col, js, int(rid)))
        
        conn.commit(); conn.close()
        return True
    except: return False

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    tabs = st.tabs(["🔍 Consulta", "⚙️ Mapa de Dados", "📋 Histórico", "💰 Saldo"])

    with tabs[0]:
        col_cli, col_cpf = st.columns([2, 2])
        id_cli_teste = None
        conn = get_conn()
        if conn:
            try:
                df = pd.read_sql("SELECT id, nome FROM admin.clientes ORDER BY nome", conn)
                opcoes = {row['id']: row['nome'] for _, row in df.iterrows()}
                id_cli_teste = col_cli.selectbox("Cliente Pagador", [None] + list(opcoes.keys()), format_func=lambda x: opcoes[x] if x else "Automático")
            except: pass
            conn.close()
            
        cpf_in = col_cpf.text_input("CPF")
        if st.button("Consultar"):
            if cpf_in:
                res = realizar_consulta_cpf(cpf_in, "teste_de_consulta_fatorconferi.cpf", False, id_cli_teste)
                st.session_state['res_fator'] = res
                
                if res['sucesso']:
                    # CHAMA SOMENTE A NOVA FUNÇÃO DE INSERÇÃO
                    logs_ok, logs_erro = executar_distribuicao_dinamica(res['dados'])
                    if logs_ok: st.success(f"Dados inseridos: {', '.join(logs_ok)}")
                    if logs_erro: st.error(f"Erros: {', '.join(logs_erro)}")
        
        if 'res_fator' in st.session_state:
            r = st.session_state['res_fator']
            if r['sucesso']:
                st.success(r['msg'])
                with st.expander("JSON"): st.json(r['dados'])
            else: st.error(r['msg'])

    with tabs[1]:
        st.info("Sintaxe: SEÇÃO;SUBCAMPO;[]{LISTA}")
        conn = get_conn()
        if conn:
            df_map = pd.read_sql("SELECT * FROM conexoes.fatorconferi_conexao_tabelas ORDER BY id DESC", conn)
            df_edit = st.data_editor(df_map, num_rows="dynamic", use_container_width=True, key="editor_mapa_geral")
            if st.button("Salvar Mapa"):
                if salvar_alteracoes_mapa_completo(df_map, df_edit): st.success("Salvo!")
            conn.close()

    with tabs[2]:
        conn = get_conn()
        if conn:
            df = pd.read_sql("SELECT * FROM conexoes.fatorconferi_registo_consulta ORDER BY id DESC LIMIT 10", conn)
            st.dataframe(df, use_container_width=True)
            conn.close()

    with tabs[3]:
        if st.button("Ver Saldo API"):
            ok, val = consultar_saldo_api()
            if ok: st.metric("Saldo", f"R$ {val:.2f}")