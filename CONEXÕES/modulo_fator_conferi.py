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
# 1. FUNÇÕES AUXILIARES E VALIDAÇÕES
# =============================================================================

def limpar_apenas_numeros(valor):
    if not valor: return ""
    return re.sub(r'\D', '', str(valor))

def limpar_normalizar_cpf(cpf_raw):
    if not cpf_raw: return ""
    s = str(cpf_raw).strip()
    apenas_nums = re.sub(r'\D', '', s)
    return apenas_nums.zfill(11)

def validar_email(email):
    if not email: return False
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(regex, email))

def validar_formatar_telefone(tel_raw):
    s = str(tel_raw).strip()
    if re.search(r'[a-zA-Z]', s): return None 
    numeros = re.sub(r'\D', '', s)
    if len(numeros) < 10 or len(numeros) > 11: return None
    return numeros

def validar_formatar_cep(cep_raw):
    numeros = limpar_apenas_numeros(cep_raw)
    if len(numeros) != 8: return None
    return numeros 

def formatar_data_iso(data_str):
    if not data_str: return None
    try:
        return datetime.strptime(data_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        return None

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

# =============================================================================
# 2. PARSING DE XML/JSON
# =============================================================================

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
        
        cad = find_tag(root, 'cadastrais')
        if cad is not None:
            dados['nome'] = get_tag_text(cad, 'nome')
            dados['cpf'] = get_tag_text(cad, 'cpf')
            dados['nascimento'] = get_tag_text(cad, 'nascto')
            dados['mae'] = get_tag_text(cad, 'nome_mae')
            dados['rg'] = get_tag_text(cad, 'rg')
            dados['pai'] = get_tag_text(cad, 'nome_pai')
            dados['uf_rg'] = get_tag_text(cad, 'uf_rg')
            dados['sexo'] = get_tag_text(cad, 'sexo')
        
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

        emails = []
        em_root = find_tag(root, 'emails')
        if em_root is not None:
            for em in em_root:
                if 'email' in em.tag.lower() and em.text: 
                    emails.append(em.text)
        dados['emails'] = emails

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
    cpf_limpo = limpar_normalizar_cpf(raw_cpf)
    
    if not cpf_limpo or len(cpf_limpo) != 11:
        return False, f"CPF inválido para importação: '{raw_cpf}'"

    try:
        cur = conn.cursor()
        
        campos = {
            'nome': dados_api.get('nome') or "CLIENTE IMPORTADO",
            'rg': dados_api.get('rg'),
            'data_nascimento': formatar_data_iso(dados_api.get('nascimento')),
            'nome_mae': dados_api.get('mae'),
            'nome_pai': dados_api.get('pai'),
            'uf_rg': dados_api.get('uf_rg'),
            'pis': dados_api.get('pis'),
            'cnh': dados_api.get('cnh'),
            'serie_ctps': dados_api.get('serie_ctps'),
            'nome_procurador': dados_api.get('nome_procurador'),
            'cpf_procurador': limpar_normalizar_cpf(dados_api.get('cpf_procurador'))
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

        raw_telefones = dados_api.get('telefones', []) or []
        count_tel = 0
        col_tel = verificar_coluna_cpf(cur, 'pf_telefones')
        
        for t in raw_telefones:
            val_bruto = str(t.get('numero', '')) if isinstance(t, dict) else str(t)
            val_limpo = limpar_apenas_numeros(val_bruto)
            tel_validado = validar_formatar_telefone(val_limpo)
            
            if tel_validado:
                cur.execute(f"SELECT 1 FROM banco_pf.pf_telefones WHERE {col_tel}=%s AND numero=%s", (cpf_limpo, tel_validado))
                if not cur.fetchone():
                    cur.execute(f"INSERT INTO banco_pf.pf_telefones ({col_tel}, numero, data_atualizacao) VALUES (%s, %s, CURRENT_DATE)", (cpf_limpo, tel_validado))
                    count_tel += 1

        raw_emails = dados_api.get('emails', []) or []
        count_email = 0
        col_email = verificar_coluna_cpf(cur, 'pf_emails')

        for e in raw_emails:
            val_bruto = str(e.get('email', '')) if isinstance(e, dict) else str(e)
            val_limpo = val_bruto.strip().lower()
            
            if validar_email(val_limpo):
                cur.execute(f"SELECT 1 FROM banco_pf.pf_emails WHERE {col_email}=%s AND email=%s", (cpf_limpo, val_limpo))
                if not cur.fetchone():
                    cur.execute(f"INSERT INTO banco_pf.pf_emails ({col_email}, email) VALUES (%s, %s)", (cpf_limpo, val_limpo))
                    count_email += 1

        raw_ends = dados_api.get('enderecos', []) or []
        count_end = 0
        col_end = verificar_coluna_cpf(cur, 'pf_enderecos')

        for d in raw_ends:
            if isinstance(d, dict):
                cep_val = validar_formatar_cep(d.get('cep'))
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

# --- FUNÇÃO DE VERIFICAÇÃO DE SALDO ---
def verificar_saldo_suficiente(id_cliente, ambiente, valor_consulta):
    if not id_cliente: 
        return True, "Cliente não identificado (Teste ou Admin)." 
    
    conn = get_conn()
    if not conn: return False, "Erro conexão DB"
    
    try:
        cur = conn.cursor()
        
        cur.execute("SELECT origem FROM conexoes.fatorconferi_ambiente_consulta WHERE ambiente = %s LIMIT 1", (ambiente,))
        res_amb = cur.fetchone()
        if not res_amb:
            conn.close()
            return False, f"Ambiente '{ambiente}' não configurado nas origens."
        origem_ambiente = res_amb[0]
        
        cur.execute("SELECT cpf FROM admin.clientes WHERE id = %s", (id_cliente,))
        res_cli = cur.fetchone()
        if not res_cli:
            conn.close()
            return False, "Cliente não encontrado na base admin."
        cpf_cliente = res_cli[0]
        cpf_limpo = re.sub(r'\D', '', str(cpf_cliente))

        query_cart = """
            SELECT c.nome_tabela_transacoes, c.nome_carteira
            FROM cliente.cliente_carteira_lista l
            JOIN cliente.carteiras_config c ON l.nome_carteira = c.nome_carteira
            WHERE l.origem_custo = %s 
            AND regexp_replace(l.cpf_cliente, '[^0-9]', '', 'g') = %s
            LIMIT 1
        """
        cur.execute(query_cart, (origem_ambiente, cpf_limpo))
        res_tab = cur.fetchone()
        
        if not res_tab:
            conn.close()
            return False, f"Cliente sem carteira ativa para origem '{origem_ambiente}'."
            
        tabela_transacoes = res_tab[0]
        
        query_saldo = f"SELECT saldo_novo FROM {tabela_transacoes} WHERE regexp_replace(cpf_cliente, '[^0-9]', '', 'g') = %s ORDER BY id DESC LIMIT 1"
        cur.execute(query_saldo, (cpf_limpo,))
        res_saldo = cur.fetchone()
        
        saldo_atual = float(res_saldo[0]) if res_saldo else 0.0
        
        conn.close()
        
        if saldo_atual >= valor_consulta:
            return True, f"Saldo OK (R$ {saldo_atual:.2f})"
        else:
            return False, f"Saldo insuficiente. Disponível: R$ {saldo_atual:.2f} | Custo: R$ {valor_consulta:.2f}"

    except Exception as e:
        if conn: conn.close()
        return False, f"Erro ao verificar saldo: {str(e)}"

def processar_debito_automatico(id_cliente_pagador, nome_ambiente_origem):
    if not id_cliente_pagador: return False, "ID Cliente ausente."
    conn = get_conn()
    if not conn: return False, "Erro DB."
    try:
        cur = conn.cursor()
        
        cur.execute("SELECT origem FROM conexoes.fatorconferi_ambiente_consulta WHERE ambiente = %s LIMIT 1", (nome_ambiente_origem,))
        res_amb = cur.fetchone()
        if not res_amb:
            conn.close(); return False, "Ambiente não cadastrado."
        origem = res_amb[0]

        cur.execute("SELECT cpf FROM admin.clientes WHERE id = %s", (id_cliente_pagador,))
        res_cpf = cur.fetchone()
        if not res_cpf:
            conn.close(); return False, "Cliente admin não encontrado."
        cpf_limpo = re.sub(r'\D', '', str(res_cpf[0]))

        query = """SELECT l.cpf_cliente, l.nome_cliente, l.custo_carteira, l.origem_custo, c.nome_tabela_transacoes 
                   FROM cliente.cliente_carteira_lista l JOIN cliente.carteiras_config c ON l.nome_carteira = c.nome_carteira
                   WHERE regexp_replace(l.cpf_cliente, '[^0-9]', '', 'g') = %s AND l.origem_custo = %s LIMIT 1"""
        cur.execute(query, (cpf_limpo, origem))
        dados = cur.fetchone()
        if not dados: conn.close(); return False, "Carteira não encontrada para débito."

        cpf_cli_banco, nome_cli, custo, orig_custo, tabela = dados
        val = float(custo) if custo else 0.0
        
        cur.execute(f"SELECT saldo_novo FROM {tabela} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_cli_banco,))
        res_saldo = cur.fetchone()
        saldo_ant = float(res_saldo[0]) if res_saldo else 0.0
        
        cur.execute(f"INSERT INTO {tabela} (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao) VALUES (%s, %s, %s, %s, 'DEBITO', %s, %s, %s, NOW())",
                    (cpf_cli_banco, nome_cli, orig_custo, nome_ambiente_origem, val, saldo_ant, saldo_ant - val))
        conn.commit(); conn.close()
        return True, f"Débito R$ {val:.2f} OK."
    except Exception as e:
        if conn: conn.close()
        return False, str(e)

def realizar_consulta_cpf(cpf, ambiente, forcar_nova=False, id_cliente_pagador_manual=None):
    cpf_padrao = ''.join(filter(str.isdigit, str(cpf))).zfill(11)
    conn = get_conn()
    if not conn: return {"sucesso": False, "msg": "Erro DB."}
    
    id_usuario = st.session_state.get('usuario_id', 0)
    nome_usuario = st.session_state.get('usuario_nome', 'Sistema')
    
    id_cliente_final = None
    nome_cliente_final = None
    if id_cliente_pagador_manual:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome FROM admin.clientes WHERE id=%s", (id_cliente_pagador_manual,))
            res = cur.fetchone()
            if res: id_cliente_final=id_cliente_pagador_manual; nome_cliente_final=res[0]
        except: pass
    else:
        d = buscar_cliente_vinculado_ao_usuario(id_usuario)
        id_cliente_final=d['id']; nome_cliente_final=d['nome']

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

        custo = buscar_valor_consulta_atual()
        if id_cliente_final:
            saldo_ok, msg_saldo = verificar_saldo_suficiente(id_cliente_final, ambiente, custo)
            if not saldo_ok:
                conn.close()
                return {"sucesso": False, "msg": f"BLOQUEIO: {msg_saldo}"}

        cred = buscar_credenciais()
        if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token ausente."}
        
        resp = requests.get(f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}", timeout=30)
        resp.encoding = 'ISO-8859-1'
        dados = parse_xml_to_dict(resp.text)
        
        if not dados.get('nome'): conn.close(); return {"sucesso": False, "msg": "Sem dados.", "dados": dados}
        if not dados.get('cpf'): dados['cpf'] = cpf_padrao

        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4)
        
        cur.execute("INSERT INTO conexoes.fatorconferi_registo_consulta (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, origem_consulta, data_hora, id_cliente, nome_cliente, ambiente) VALUES ('CPF SIMPLES', %s, %s, %s, %s, %s, 'SUCESSO', %s, NOW(), %s, %s, %s)", 
                    (cpf_padrao, id_usuario, nome_usuario, custo, path, origem_real, id_cliente_final, nome_cliente_final, ambiente))
        conn.commit()
        
        msg_fin = ""
        if id_cliente_final:
            ok_fin, txt_fin = processar_debito_automatico(id_cliente_final, ambiente)
            msg_fin = f" | {txt_fin}"
        
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

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    tabs = st.tabs(["👥 Clientes", "🔍 Teste de Consulta", "💰 Saldo API", "📋 Histórico", "⚙️ Parâmetros"])

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
                # --- CORREÇÃO AQUI: BUSCA DE TODOS OS CLIENTES DE ADMIN.CLIENTES ---
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