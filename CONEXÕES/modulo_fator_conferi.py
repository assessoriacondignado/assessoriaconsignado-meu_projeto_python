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

# --- CONFIGURAÇÃO DE CAMINHOS (PATH FIX) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# --- IMPORTAÇÕES ---
try:
    import conexao
except ImportError:
    conexao = None

try:
    import modulo_validadores as mv
except ImportError as e:
    st.error(f"Erro crítico: Não foi possível importar 'modulo_validadores'. Detalhe: {e}")
    st.stop()

# --- DIRETÓRIOS ---
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
# 1. SANITIZAÇÃO E FORMATAÇÃO
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

def sanitizar_e_formatar(valor):
    """
    Aplica regras de limpeza: Nulos, Maiúsculas e Data ISO.
    """
    if valor is None: return None
    
    if isinstance(valor, list): return valor
    
    v_str = str(valor).strip()
    
    if not v_str or v_str.upper() in ["NULO", "NULL", "NONE", "[]", "{}"]:
        return None
    
    # Data BR (dd/mm/yyyy) -> ISO (yyyy-mm-dd)
    if re.match(r'^\d{2}/\d{2}/\d{4}$', v_str):
        try:
            dt_obj = datetime.strptime(v_str, '%d/%m/%Y')
            return dt_obj.strftime('%Y-%m-%d')
        except: pass 
            
    return v_str.upper()

# =============================================================================
# 2. PARSER SIMPLIFICADO
# =============================================================================

def _xml_to_dict_simple(element):
    text = element.text.strip() if element.text else None
    if len(element) == 0:
        return text

    result = {}
    for child in element:
        tag = child.tag.replace('{', '').split('}')[-1].upper()
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
    try:
        if isinstance(texto_raw, bytes):
            texto_raw = texto_raw.decode('utf-8', errors='ignore')
        texto_raw = texto_raw.replace('ISO-8859-1', 'UTF-8')

        try:
            return json.loads(texto_raw)
        except:
            pass
            
        root = ET.fromstring(texto_raw)
        return _xml_to_dict_simple(root)
    except Exception as e:
        return {}

# =============================================================================
# 3. EXTRAÇÃO INTELIGENTE (IGNORA ESPAÇOS NO JSON)
# =============================================================================

def extrair_valor_novo_padrao(dados, caminho_str):
    """
    Navega no JSON usando ';' como separador.
    Ignora espaços nas chaves do JSON (ex: " TELEFONE " vira "TELEFONE").
    """
    if not caminho_str: return None
    
    caminho_limpo = re.sub(r'".*?"', '', caminho_str).strip()
    passos = [p.strip() for p in caminho_limpo.split(';') if p.strip()]
    
    cursor = dados 
    
    for i, passo in enumerate(passos):
        if cursor is None: return None
        
        is_list_iter = '[]' in passo
        chave = passo.replace('[]', '').replace('{', '').replace('}', '').upper()
        
        if isinstance(cursor, dict):
            encontrou = False
            for k, v in cursor.items():
                if k.upper().strip() == chave:
                    cursor = v
                    encontrou = True
                    break
            if not encontrou: return None
            
        elif isinstance(cursor, list) and is_list_iter:
            lista_valores = []
            for item in cursor:
                if isinstance(item, dict):
                    for k, v in item.items():
                        if k.upper().strip() == chave:
                            lista_valores.append(v)
                            break
                elif isinstance(item, str) and chave == "": 
                    lista_valores.append(item)
            cursor = lista_valores
            
        else:
            return None

    return cursor

# =============================================================================
# 4. DISTRIBUIÇÃO DINÂMICA
# =============================================================================

def executar_distribuicao_dinamica(dados_api):
    conn = get_conn()
    if not conn: return [], ["Erro conexão DB"]
    
    sucessos = []
    erros = []
    
    try:
        df_map = pd.read_sql("SELECT tabela_referencia, tabela_referencia_coluna, jason_api_fatorconferi_coluna FROM conexoes.fatorconferi_conexao_tabelas", conn)
        tabelas = df_map['tabela_referencia'].unique()
        cur = conn.cursor()
        
        for tabela in tabelas:
            try:
                regras = df_map[df_map['tabela_referencia'] == tabela]
                
                dados_extraidos = {}
                max_linhas = 1 
                
                # --- PASSO A: EXTRAÇÃO ---
                for _, row in regras.iterrows():
                    col_sql = str(row['tabela_referencia_coluna']).strip()
                    caminho = str(row['jason_api_fatorconferi_coluna']).strip()
                    
                    valor_raw = extrair_valor_novo_padrao(dados_api, caminho)
                    
                    # Sanitiza e Formata
                    if isinstance(valor_raw, list):
                        valor_final = [sanitizar_e_formatar(v) for v in valor_raw]
                        if len(valor_final) > max_linhas:
                            max_linhas = len(valor_final)
                    else:
                        valor_final = sanitizar_e_formatar(valor_raw)
                        
                    # Limpeza extra para CPF (banco geralmente pede apenas números)
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
                
                # Lógica para evitar erros de duplicidade (UPSERT)
                cols_lower = [c.lower() for c in cols]
                
                # Se for tabela de lista (telefones/enderecos), NÃO usa ON CONFLICT (CPF) pois CPF repete.
                is_lista_1_n = any(x in tabela.lower() for x in ['telefone', 'endereco', 'email', 'socio', 'veiculo'])
                
                if not is_lista_1_n and 'cpf' in cols_lower:
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
                        # Se for lista, pega o item da vez. Se for valor fixo (CPF), repete.
                        if isinstance(val, list):
                            item = val[i] if i < len(val) else None
                        else:
                            item = val
                            
                        if item: tem_dado = True
                        linha_vals.append(item)
                    
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
# 5. FUNÇÕES DE SUPORTE
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
    # Padroniza CPF para string limpa
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
        
        # Verifica Cache
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
             custo_previsto = float(res_custo[0]) if res_custo else buscar_valor_consulta_atual()

             sql_saldo = "SELECT saldo_novo FROM cliente.extrato_carteira_por_produto WHERE id_cliente = %s ORDER BY id DESC LIMIT 1"
             cur.execute(sql_saldo, (str(dados_pagador['id']),))
             res_s = cur.fetchone()
             saldo_atual = float(res_s[0]) if res_s else 0.0

             if saldo_atual < custo_previsto:
                 conn.close()
                 return {"sucesso": False, "msg": "Saldo insuficiente."}

        cred = buscar_credenciais()
        if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token API ausente."}
        
        resp = requests.get(f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}", timeout=30)
        dados = parse_xml_to_dict(resp.text)
        
        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        try:
            with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4, ensure_ascii=False)
        except: pass

        # --- ATUALIZAÇÃO PARA BIGINT ---
        # Converte CPF limpo para Inteiro para gravar na nova coluna
        cpf_num = int(re.sub(r'\D', '', str(cpf_padrao)))

        cur.execute("""
            INSERT INTO conexoes.fatorconferi_registo_consulta 
            (tipo_consulta, cpf_consultado, cpf_consultado_num, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, origem_consulta, data_hora, id_cliente, nome_cliente, ambiente) 
            VALUES ('CPF SIMPLES', %s, %s, %s, %s, %s, %s, 'SUCESSO', %s, NOW(), %s, %s, %s)
            """, 
            (cpf_padrao, cpf_num, id_usuario, nome_usuario, custo_previsto, path, origem_real, dados_pagador['id'], dados_pagador['nome'], ambiente)
        )
        
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
# 6. INTERFACE E UTILITÁRIOS
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
        
        conn.commit(); conn.close(); return True
    except: return False

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
        except: return False
        finally: conn.close()

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
    except: return []
    finally: conn.close()

def listar_colunas_geral(nome_tabela_completo):
    conn = get_conn()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            parts = nome_tabela_completo.split('.')
            schema = parts[0] if len(parts) > 1 else 'public'
            tabela = parts[1] if len(parts) > 1 else parts[0]
            cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position
            """, (schema, tabela))
            return [r[0] for r in cur.fetchall()]
    except: return []
    finally: conn.close()

def listar_mapeamento_tabela(nome_tabela):
    conn = get_conn()
    if not conn: return {}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT tabela_referencia_coluna, jason_api_fatorconferi_coluna FROM conexoes.fatorconferi_conexao_tabelas WHERE tabela_referencia = %s", (nome_tabela,))
            return {row[0]: row[1] for row in cur.fetchall()}
    except: return {}
    finally: conn.close()

def listar_todos_mapeamentos():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try: return pd.read_sql("SELECT * FROM conexoes.fatorconferi_conexao_tabelas ORDER BY id DESC", conn)
    except: return pd.DataFrame()
    finally: conn.close()

def salvar_mapeamento_grade(nome_tabela, df_mapeamento):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        for index, row in df_mapeamento.iterrows():
            col_sql = row['Coluna SQL']
            chave_json = str(row['Chave JSON API']).strip()
            cur.execute("DELETE FROM conexoes.fatorconferi_conexao_tabelas WHERE tabela_referencia = %s AND tabela_referencia_coluna = %s", (nome_tabela, col_sql))
            if chave_json:
                cur.execute("INSERT INTO conexoes.fatorconferi_conexao_tabelas (tabela_referencia, tabela_referencia_coluna, jason_api_fatorconferi_coluna) VALUES (%s, %s, %s)", (nome_tabela, col_sql, chave_json))
        conn.commit(); return True
    except: 
        if conn: conn.rollback()
        return False
    finally: conn.close()

def app_fator_conferi():
    criar_tabela_conexao_tabelas()
    st.markdown("### ⚡ Painel Fator Conferi")
    tabs = st.tabs(["👥 Clientes", "🔍 Teste de Consulta", "💰 Saldo API", "📋 Histórico", "⚙️ Parâmetros", "🗺️ Mapa de Dados"])

    with tabs[0]: 
        st.info("Gestão de Carteiras")
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
                        lista_sucessos, lista_erros = executar_distribuicao_dinamica(res['dados'])
                        if lista_sucessos: st.success(f"✅ Dados distribuídos para: {', '.join(lista_sucessos)}")
                        if lista_erros: st.error(f"⚠️ Relatório de Importação:\n{chr(10).join(lista_erros)}")
                            
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
        st.markdown("<p style='color: lightblue; font-size: 12px;'>Tabela: conexoes.fatorconferi_registo_consulta</p>", unsafe_allow_html=True)
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
        st.info("Sintaxe: SEÇÃO;SUBCAMPO;[]{LISTA}")
        lista_tabelas = listar_tabelas_disponiveis()
        tabela_sel = st.selectbox("1. Selecione a Tabela Destino:", ["(Selecione)"] + lista_tabelas)
        
        if tabela_sel != "(Selecione)":
            colunas_db = listar_colunas_geral(tabela_sel)
            mapa_existente = listar_mapeamento_tabela(tabela_sel)
            colunas_pre_selecionadas = [c for c in mapa_existente.keys() if c in colunas_db]
            colunas_sel = st.multiselect("2. Escolha as colunas para mapear:", options=colunas_db, default=colunas_pre_selecionadas)
            
            if colunas_sel:
                st.divider()
                st.markdown("#### 3. Editar Mapeamento")
                dados_grade = []
                for col in colunas_sel:
                    val_atual = mapa_existente.get(col, "")
                    dados_grade.append({"Tabela Destino": tabela_sel, "Coluna SQL": col, "Chave JSON API": val_atual})
                
                df_grade = pd.DataFrame(dados_grade)
                df_editado = st.data_editor(
                    df_grade,
                    column_config={"Tabela Destino": st.column_config.TextColumn(disabled=True), "Coluna SQL": st.column_config.TextColumn(disabled=True)},
                    hide_index=True, use_container_width=True, num_rows="fixed", key=f"editor_mapa_{tabela_sel}"
                )
                
                if st.button("💾 Salvar Mapeamento", type="primary"):
                    if salvar_mapeamento_grade(tabela_sel, df_editado):
                        st.success(f"Mapeamento salvo!"); time.sleep(1.5); st.rerun()
        
        st.divider()
        st.markdown("### 📋 Tabela Geral de Conexões (Editável)")
        df_geral = listar_todos_mapeamentos()
        df_editado_geral = st.data_editor(df_geral, key="editor_geral_mapeamentos", num_rows="dynamic", use_container_width=True)
        if st.button("💾 Salvar Alterações Gerais", type="primary"):
            if salvar_alteracoes_mapa_completo(df_geral, df_editado_geral):
                st.success("Tabela geral atualizada!"); time.sleep(1.5); st.rerun()