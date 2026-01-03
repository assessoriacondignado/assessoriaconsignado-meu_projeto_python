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
    if element is None: return None
    res = element.find(tag_name)
    if res is not None: return res.text
    res = element.find(tag_name.upper())
    if res is not None: return res.text
    res = element.find(tag_name.lower())
    if res is not None: return res.text
    return None

def find_tag(element, tag_name):
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
        
        cad = find_tag(root, 'cadastrais')
        if cad is not None:
            dados['nome'] = get_tag_text(cad, 'nome')
            dados['cpf'] = get_tag_text(cad, 'cpf')
            dados['nascimento'] = get_tag_text(cad, 'nascto')
            dados['mae'] = get_tag_text(cad, 'nome_mae')
            dados['rg'] = get_tag_text(cad, 'rg')
            dados['titulo'] = get_tag_text(cad, 'titulo_eleitor')
            dados['sexo'] = get_tag_text(cad, 'sexo')
        
        telefones = []
        tm = find_tag(root, 'telefones_movel')
        if tm is not None:
            for child in tm:
                if 'telefone' in child.tag.lower():
                    telefones.append({'numero': get_tag_text(child, 'numero'), 'tipo': 'MOVEL', 'prioridade': get_tag_text(child, 'prioridade')})
        
        tf = find_tag(root, 'telefones_fixo')
        if tf is not None:
            for child in tf:
                if 'telefone' in child.tag.lower():
                    telefones.append({'numero': get_tag_text(child, 'numero'), 'tipo': 'FIXO', 'prioridade': get_tag_text(child, 'prioridade')})
        dados['telefones'] = telefones

        emails = []
        em_root = find_tag(root, 'emails')
        if em_root is not None:
            for em in em_root:
                if 'email' in em.tag.lower() and em.text: emails.append(em.text)
        dados['emails'] = emails

        enderecos = []
        end_root = find_tag(root, 'enderecos')
        if end_root is not None:
            for end in end_root:
                if 'endereco' in end.tag.lower():
                    logr = get_tag_text(end, 'logradouro') or ""
                    num = get_tag_text(end, 'numero') or ""
                    comp = get_tag_text(end, 'complemento') or ""
                    rua_full = f"{logr}, {num} {comp}".strip().strip(',')
                    enderecos.append({'rua': rua_full, 'bairro': get_tag_text(end, 'bairro'), 'cidade': get_tag_text(end, 'cidade'), 'uf': get_tag_text(end, 'estado'), 'cep': get_tag_text(end, 'cep')})
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

def obter_origem_padronizada(nome_origem):
    conn = get_conn()
    origem_final = nome_origem 
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT origem FROM conexoes.fatorconferi_origem_consulta_fator WHERE origem = %s", (nome_origem,))
            res = cur.fetchone()
            if res: origem_final = res[0]
            conn.close()
        except:
            if conn: conn.close()
    return origem_final

# [NOVA FUNÇÃO] Busca origem baseada no ambiente
def buscar_origem_por_ambiente(nome_ambiente):
    conn = get_conn()
    origem_padrao = "WEB USUÁRIO" # Fallback
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

# =============================================================================
# 2. FUNÇÃO DE DÉBITO FINANCEIRO (ATUALIZADA)
# =============================================================================

def processar_debito_automatico(origem_da_consulta, dados_consulta):
    """
    Nova Regra de Débito:
    1. Identifica o USUÁRIO logado.
    2. Cruza USUÁRIO + ORIGEM na lista de carteiras (cliente.cliente_carteira_lista).
    3. Identifica CLIENTE PAGADOR, CARTEIRA e VALOR.
    4. Busca TABELA SQL na config.
    5. Lança o débito.
    """
    nome_usuario_logado = st.session_state.get('usuario_nome') # Usa nome para bater com a lista
    # Se preferir usar CPF do usuário, mude para: st.session_state.get('usuario_cpf') e ajuste a query
    
    if not nome_usuario_logado: return False, "Usuário não identificado na sessão."

    conn = get_conn()
    if not conn: return False, "Erro conexão DB."
    
    try:
        cur = conn.cursor()
        
        # 1. Busca na Lista de Carteiras conciliando USUÁRIO + ORIGEM
        # Isso localiza quem é o cliente (pagador), qual a carteira e o valor
        cur.execute("""
            SELECT cpf_cliente, nome_cliente, nome_carteira, custo_carteira, nome_produto
            FROM cliente.cliente_carteira_lista 
            WHERE nome_usuario = %s AND origem_custo = %s 
            LIMIT 1
        """, (nome_usuario_logado, origem_da_consulta))
        
        res_lista = cur.fetchone()

        if not res_lista:
            conn.close()
            return False, f"Usuário '{nome_usuario_logado}' não tem carteira vinculada para origem '{origem_da_consulta}'."
        
        cpf_pagador = res_lista[0]
        nome_pagador = res_lista[1]
        nome_carteira_vinculada = res_lista[2]
        valor_cobranca = float(res_lista[3])
        # nome_produto = res_lista[4] (Se precisar usar no histórico)

        # 2. Busca a TABELA SQL na 'cliente.carteiras_config' usando o nome da carteira
        cur.execute("""
            SELECT nome_tabela_transacoes 
            FROM cliente.carteiras_config 
            WHERE nome_carteira = %s AND status = 'ATIVO' 
            LIMIT 1
        """, (nome_carteira_vinculada,))
        res_config = cur.fetchone()

        if not res_config:
            conn.close()
            return False, f"Configuração da tabela para '{nome_carteira_vinculada}' não encontrada."
            
        tabela_sql = res_config[0]

        # 3. Realizar o Lançamento de Débito
        cur.execute(f"SELECT saldo_novo FROM {tabela_sql} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_pagador,))
        res_saldo = cur.fetchone()
        saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
        
        novo_saldo = saldo_anterior - valor_cobranca
        cpf_consultado = dados_consulta.get('cpf', 'Desconhecido')
        motivo = f"Consulta Fator ({origem_da_consulta}): {cpf_consultado}"
        
        sql_insert = f"INSERT INTO {tabela_sql} (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao) VALUES (%s, %s, %s, %s, 'DEBITO', %s, %s, %s, NOW())"
        cur.execute(sql_insert, (cpf_pagador, nome_pagador, motivo, origem_da_consulta, valor_cobranca, saldo_anterior, novo_saldo))
        
        conn.commit()
        conn.close()
        return True, f"Débito de R$ {valor_cobranca:.2f} na tabela {tabela_sql} (Pagador: {nome_pagador})."

    except Exception as e:
        if conn: conn.close()
        return False, f"Erro financeiro: {str(e)}"

# =============================================================================
# 3. FUNÇÕES GESTÃO DE PARÂMETROS
# =============================================================================

def carregar_dados_genericos(nome_tabela):
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql(f"SELECT * FROM {nome_tabela} ORDER BY id DESC", conn)
            conn.close(); return df
        except: 
            if conn: conn.close()
            return None
    return None

def criar_tabela_ambiente():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS conexoes.fatorconferi_ambiente_consulta (
                    id SERIAL PRIMARY KEY,
                    ambiente VARCHAR(255),
                    origem VARCHAR(255)
                );
            """)
            conn.commit(); conn.close()
            return True
        except:
            conn.close()
            return False
    return False

def salvar_alteracoes_genericas(nome_tabela, df_original, df_editado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        
        ids_orig = set(df_original['id'].dropna().astype(int).tolist())
        
        ids_editados_atuais = set()
        for _, row in df_editado.iterrows():
            if pd.notna(row.get('id')) and row.get('id') != '':
                try: ids_editados_atuais.add(int(row['id']))
                except: pass

        ids_del = ids_orig - ids_editados_atuais
        if ids_del:
            ids_str = ",".join(map(str, ids_del))
            cur.execute(f"DELETE FROM {nome_tabela} WHERE id IN ({ids_str})")

        for index, row in df_editado.iterrows():
            cols_db = [c for c in row.index if c not in ['id', 'data_hora', 'data_criacao', 'data_registro']]
            vals = [row[c] for c in cols_db]
            row_id = row.get('id')
            eh_novo = pd.isna(row_id) or row_id == '' or row_id is None
            
            if eh_novo:
                placeholders = ", ".join(["%s"] * len(cols_db))
                col_names = ", ".join(cols_db)
                cur.execute(f"INSERT INTO {nome_tabela} ({col_names}) VALUES ({placeholders})", vals)
            elif int(row_id) in ids_orig:
                set_clause = ", ".join([f"{c} = %s" for c in cols_db])
                vals_update = vals + [int(row_id)]
                cur.execute(f"UPDATE {nome_tabela} SET {set_clause} WHERE id = %s", vals_update)
                
        conn.commit(); conn.close(); return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}"); 
        if conn: conn.close()
        return False

# =============================================================================
# 4. SALVAR BASE PF E CONSULTA
# =============================================================================

def salvar_dados_fator_no_banco(dados_api):
    conn = get_conn()
    if not conn: return False, "Erro de conexão."
    try:
        cur = conn.cursor()
        cpf_limpo = re.sub(r'\D', '', str(dados_api.get('cpf', '')))
        if not cpf_limpo or len(cpf_limpo) != 11: return False, "CPF inválido."

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
        
        for t in dados_api.get('telefones', []):
            n = re.sub(r'\D', '', str(t['numero']))
            if n: cur.execute("INSERT INTO banco_pf.pf_telefones (cpf, numero, tag_qualificacao, data_atualizacao) VALUES (%s, %s, %s, CURRENT_DATE) ON CONFLICT DO NOTHING", (cpf_limpo, n, t.get('prioridade', '')))
        
        for e in dados_api.get('emails', []):
            if e: cur.execute("INSERT INTO banco_pf.pf_emails (cpf, email) VALUES (%s, %s) ON CONFLICT DO NOTHING", (cpf_limpo, str(e).lower()))
            
        for d in dados_api.get('enderecos', []):
            cp = re.sub(r'\D', '', str(d['cep']))
            if cp: cur.execute("INSERT INTO banco_pf.pf_enderecos (cpf, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (cpf_limpo, d['rua'], d['bairro'], d['cidade'], d['uf'], cp))

        conn.commit(); conn.close()
        return True, "Dados salvos na Base PF."
    except Exception as e:
        if conn: conn.close()
        return False, f"Erro DB: {e}"

def realizar_consulta_cpf(cpf, origem, forcar_nova=False):
    cpf_padrao = ''.join(filter(str.isdigit, str(cpf))).zfill(11)
    conn = get_conn()
    if not conn: return {"sucesso": False, "msg": "Erro DB."}
    try:
        cur = conn.cursor()
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
                            cur.execute("INSERT INTO conexoes.fatorconferi_registo_consulta (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora) VALUES (%s, %s, %s, %s, 0, %s, 'SUCESSO', %s, %s, 'CACHE', NOW())", ("CPF SIMPLES", cpf_padrao, id_usr, usr, res[0], res[0], origem))
                            conn.commit(); conn.close()
                            return {"sucesso": True, "dados": dados, "msg": "Cache recuperado."}
                except: pass
        
        cred = buscar_credenciais()
        if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token ausente."}
        resp = requests.get(f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}", timeout=30)
        resp.encoding = 'ISO-8859-1'
        
        dados = parse_xml_to_dict(resp.text)
        if not dados.get('nome') and not dados.get('cpf'): conn.close(); return {"sucesso": False, "msg": "Retorno vazio ou erro.", "raw": resp.text, "dados": dados}

        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4)
        
        # --- DÉBITO FINANCEIRO COM NOVA REGRA ---
        msg_financeira = ""
        ok_fin, txt_fin = processar_debito_automatico(origem, dados)
        if ok_fin: msg_financeira = f" | {txt_fin}"
        else: msg_financeira = f" | ⚠️ Falha Financeira: {txt_fin}"
        
        custo = buscar_valor_consulta_atual()
        usr = st.session_state.get('usuario_nome', 'Sistema')
        id_usr = st.session_state.get('usuario_id', 0)
        cur.execute("INSERT INTO conexoes.fatorconferi_registo_consulta (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora) VALUES (%s, %s, %s, %s, %s, %s, 'SUCESSO', %s, %s, 'PAGO', NOW())", ("CPF SIMPLES", cpf_padrao, id_usr, usr, custo, path, path, origem))
        conn.commit(); conn.close()
        return {"sucesso": True, "dados": dados, "msg": "Consulta realizada." + msg_financeira}
        
    except Exception as e:
        if conn: conn.close()
        return {"sucesso": False, "msg": str(e)}

def listar_clientes_carteira():
    conn = get_conn()
    if conn:
        try: df = pd.read_sql("SELECT * FROM conexoes.fator_cliente_carteira ORDER BY id", conn); conn.close(); return df
        except: conn.close()
    return pd.DataFrame()

# =============================================================================
# 5. INTERFACE PRINCIPAL
# =============================================================================

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    tabs = st.tabs(["👥 Clientes", "🔍 Teste de Consulta", "💰 Saldo API", "📋 Histórico", "⚙️ Parâmetros"])

    with tabs[0]: 
        st.info("Gestão de Carteiras (Use o Módulo Clientes para criar novas)")
        df = listar_clientes_carteira()
        if not df.empty: st.dataframe(df, use_container_width=True)

    with tabs[1]:
        st.markdown("#### 1.1 Consulta e Importação")
        c1, c2, c3 = st.columns([3, 1.5, 1.5])
        cpf_in = c1.text_input("CPF")
        forcar = c2.checkbox("Ignorar Histórico", value=False)
        
        if c3.button("🔍 Consultar", type="primary"):
            if cpf_in:
                with st.spinner("Buscando..."):
                    # 1. Identifica ORIGEM pelo AMBIENTE
                    nome_ambiente = "Painel Fator Conferi / Teste de Consulta" # Nome fixo deste ambiente
                    origem_padrao = buscar_origem_por_ambiente(nome_ambiente)
                    
                    st.toast(f"Ambiente: {nome_ambiente} -> Origem: {origem_padrao}")
                    
                    res = realizar_consulta_cpf(cpf_in, origem_padrao, forcar)
                    st.session_state['resultado_fator'] = res
        
        if 'resultado_fator' in st.session_state:
            res = st.session_state['resultado_fator']
            if res['sucesso']:
                if "msg" in res: st.success(res['msg'])
                st.divider()
                if st.button("💾 Salvar na Base PF", type="primary"):
                    ok_s, msg_s = salvar_dados_fator_no_banco(res['dados'])
                    if ok_s: st.success(msg_s)
                    else: st.error(msg_s)
                
                dados = res['dados']
                with st.expander("Dados", expanded=True):
                    st.json(dados)
            else: st.error(res.get('msg', 'Erro'))

    with tabs[2]: 
        if st.button("🔄 Atualizar"): 
            ok, v = consultar_saldo_api()
            if ok: st.metric("Saldo Atual", f"R$ {v:.2f}")
            else: st.error("Erro ao consultar saldo.")
        
    with tabs[3]: 
        # --- Alteração Solicitada: Nome da tabela exibido acima da planilha ---
        st.markdown("<p style='color: lightblue; font-size: 12px; margin-bottom: 0px;'>Tabela: conexoes.fatorconferi_registo_consulta</p>", unsafe_allow_html=True)
        
        conn = get_conn()
        if conn: 
            st.dataframe(pd.read_sql("SELECT * FROM conexoes.fatorconferi_registo_consulta ORDER BY id DESC LIMIT 20", conn))
            conn.close()
    
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
                    if st.button("🛠️ Criar Tabela Ambiente Agora", type="primary"):
                        if criar_tabela_ambiente(): st.success("Criada!"); st.rerun()
            else:
                st.info(f"Editando: `{nome_sql}`")
                cols_travadas = ["id", "data_hora", "data_criacao", "data_registro"]
                df_editado = st.data_editor(df_param, key=f"editor_{nome_sql}", num_rows="dynamic", use_container_width=True, disabled=cols_travadas)
                if st.button("💾 Salvar Alterações", type="primary"):
                    if salvar_alteracoes_genericas(nome_sql, df_param, df_editado): st.success("Salvo!"); time.sleep(1); st.rerun()