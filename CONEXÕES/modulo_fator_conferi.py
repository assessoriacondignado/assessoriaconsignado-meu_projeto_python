import streamlit as st
import pandas as pd
import psycopg2
import requests
import json
import os
import time
import re
import base64
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta

# --- IMPORTAÇÃO DE MÓDULOS EXTERNOS (MODULO PF CADASTRO) ---
try:
    # Ajuste o caminho relativo conforme a estrutura real das suas pastas
    # Tenta localizar o modulo_pf_cadastro na pasta OPERACIONAL
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
# 1. FUNÇÕES AUXILIARES (PARSING E DADOS)
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

# =============================================================================
# 2. FUNÇÃO DE DÉBITO FINANCEIRO
# =============================================================================

def processar_debito_automatico(id_cliente_pagador, nome_ambiente_origem):
    if not id_cliente_pagador:
        return False, "ID do Cliente não informado para cobrança."

    conn = get_conn()
    if not conn: return False, "Erro de conexão ao processar débito."

    try:
        cur = conn.cursor()

        cur.execute("SELECT origem FROM conexoes.fatorconferi_ambiente_consulta WHERE ambiente = %s LIMIT 1", (nome_ambiente_origem,))
        res_amb = cur.fetchone()
        if not res_amb:
            conn.close()
            return False, f"Ambiente '{nome_ambiente_origem}' não cadastrado em fatorconferi_ambiente_consulta."
        
        origem_identificada = res_amb[0]

        query_wallet = """
            SELECT 
                l.cpf_cliente, 
                l.nome_cliente, 
                l.custo_carteira, 
                l.origem_custo, 
                c.nome_tabela_transacoes 
            FROM cliente.cliente_carteira_lista l
            JOIN cliente.carteiras_config c ON l.nome_carteira = c.nome_carteira
            WHERE l.id_cliente = %s AND l.origem_custo = %s
            LIMIT 1
        """
        cur.execute(query_wallet, (id_cliente_pagador, origem_identificada))
        dados_wallet = cur.fetchone()

        if not dados_wallet:
            conn.close()
            return False, f"Nenhuma carteira encontrada para Cliente ID {id_cliente_pagador} com Origem '{origem_identificada}'."

        cpf_cli_db, nome_cli_db, custo_db, origem_custo_db, tabela_sql = dados_wallet
        valor_debito = float(custo_db) if custo_db else 0.0

        cur.execute(f"SELECT saldo_novo FROM {tabela_sql} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_cli_db,))
        res_saldo = cur.fetchone()
        saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
        
        novo_saldo = saldo_anterior - valor_debito
        motivo_lancamento = origem_custo_db 
        origem_lancamento_db = nome_ambiente_origem

        sql_insert = f"""
            INSERT INTO {tabela_sql} 
            (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao) 
            VALUES (%s, %s, %s, %s, 'DEBITO', %s, %s, %s, NOW())
        """
        cur.execute(sql_insert, (cpf_cli_db, nome_cli_db, motivo_lancamento, origem_lancamento_db, valor_debito, saldo_anterior, novo_saldo))
        
        conn.commit()
        conn.close()
        return True, f"Débito de R$ {valor_debito:.2f} realizado com sucesso (Tab: {tabela_sql})."

    except Exception as e:
        if conn: conn.close()
        return False, f"Erro crítico no débito: {str(e)}"

# =============================================================================
# 3. SALVAR BASE PF (INTEGRAÇÃO ESTRITA E CORRIGIDA)
# =============================================================================

def salvar_dados_fator_no_banco(dados_api):
    """
    Função atualizada para utilizar as regras do modulo_pf_cadastro.
    Corrige o parsing de listas do JSON (Telefones e Emails).
    """
    conn = get_conn()
    if not conn: return False, "Erro de conexão."
    
    try:
        # 1. Tratamento Inicial do CPF
        raw_cpf = str(dados_api.get('cpf', '')).strip()
        cpf_limpo = modulo_pf_cadastro.limpar_normalizar_cpf(raw_cpf)
        
        if not cpf_limpo or len(cpf_limpo) != 11:
            conn.close()
            return False, f"CPF inválido para gravação: '{raw_cpf}'"

        nome_cliente = dados_api.get('nome') or "Cliente Fator"

        # --- ETAPA 1: GARANTIR O CADASTRO (MODO NOVO) ---
        dados_novo = {'nome': nome_cliente, 'cpf': cpf_limpo}
        
        # Tenta criar o cadastro básico.
        modulo_pf_cadastro.salvar_pf(
            dados_gerais=dados_novo,
            df_tel=pd.DataFrame(),
            df_email=pd.DataFrame(),
            df_end=pd.DataFrame(),
            df_emp=pd.DataFrame(),
            df_contr=pd.DataFrame(),
            modo="novo"
        )
        
        # --- ETAPA 2: ENRIQUECIMENTO (MODO EDITAR) ---
        
        # A. Preparar Dados Gerais
        dados_editar = {
            'nome': nome_cliente,
            'cpf': cpf_limpo,
            'rg': dados_api.get('rg'),
            'nome_mae': dados_api.get('mae'),
            'data_nascimento': modulo_pf_cadastro.converter_data_br_iso(dados_api.get('nascimento'))
        }
        
        # B. Preparar Telefones (CORREÇÃO DA LEITURA DE OBJETOS E LIMPEZA PRÉVIA)
        lista_tels = []
        raw_telefones = dados_api.get('telefones', [])
        
        if raw_telefones is None: raw_telefones = []
        
        for t in raw_telefones:
            # Verifica se é um dicionário (padrão do JSON da Fator) ou string
            numero_bruto = ""
            if isinstance(t, dict):
                numero_bruto = str(t.get('numero', ''))
            else:
                numero_bruto = str(t)
                
            # Limpeza preventiva ANTES da validação: remove caracteres não numéricos
            numero_limpo = re.sub(r'\D', '', numero_bruto)
            
            # Validação usando a regra do cadastro
            num_val, erro = modulo_pf_cadastro.validar_formatar_telefone(numero_limpo)
            if num_val and not erro:
                lista_tels.append({'numero': num_val})
        
        df_tels = pd.DataFrame(lista_tels)

        # C. Preparar Emails (CORREÇÃO DA LEITURA MISTA)
        lista_emails = []
        raw_emails = dados_api.get('emails', [])
        
        if raw_emails is None: raw_emails = []
        
        for e in raw_emails:
            # Se for string simples ["email1", "email2"] (Padrão observado)
            val_email = ""
            if isinstance(e, str):
                val_email = e
            # Se for objeto {"email": "..."}
            elif isinstance(e, dict):
                 val_email = str(e.get('email', ''))
            
            if val_email:
                email_limpo = val_email.strip().lower()
                if modulo_pf_cadastro.validar_email(email_limpo):
                    lista_emails.append({'email': email_limpo})

        df_emails = pd.DataFrame(lista_emails)

        # D. Preparar Endereços
        lista_ends = []
        raw_enderecos = dados_api.get('enderecos', [])
        
        if raw_enderecos is None: raw_enderecos = []

        for d in raw_enderecos:
            if isinstance(d, dict):
                cep_num, _, erro_cep = modulo_pf_cadastro.validar_formatar_cep(d.get('cep'))
                if (cep_num and not erro_cep) or d.get('rua'):
                    uf_val = str(d.get('uf', '')).upper()
                    if modulo_pf_cadastro.validar_uf(uf_val):
                         lista_ends.append({
                             'cep': cep_num,
                             'rua': d.get('rua'),
                             'bairro': d.get('bairro'),
                             'cidade': d.get('cidade'),
                             'uf': uf_val
                         })
        df_ends = pd.DataFrame(lista_ends)

        # Envia tudo para o modulo de cadastro salvar no modo EDITAR
        ok_edit, msg_edit = modulo_pf_cadastro.salvar_pf(
            dados_gerais=dados_editar,
            df_tel=df_tels,
            df_email=df_emails,
            df_end=df_ends,
            df_emp=pd.DataFrame(),
            df_contr=pd.DataFrame(),
            modo="editar",
            cpf_original=cpf_limpo
        )
        
        conn.close()
        
        if ok_edit:
            qtd_tel = len(df_tels)
            qtd_email = len(df_emails)
            return True, f"Consulta realizada. | ✅ Atualizado: {qtd_tel} tels, {qtd_email} emails."
        else:
            return False, f"Erro na atualização dos dados: {msg_edit}"

    except Exception as e:
        if conn: conn.close()
        return False, f"Erro crítico na integração PF: {str(e)}"

# =============================================================================
# 4. FUNÇÕES DE CONSULTA
# =============================================================================

def realizar_consulta_cpf(cpf, ambiente, forcar_nova=False, id_cliente_pagador_manual=None):
    cpf_padrao = ''.join(filter(str.isdigit, str(cpf))).zfill(11)
    conn = get_conn()
    if not conn: return {"sucesso": False, "msg": "Erro DB."}
    
    # Identificação do Usuário e Cliente
    id_usuario_logado = st.session_state.get('usuario_id', 0)
    nome_usuario_logado = st.session_state.get('usuario_nome', 'Sistema')
    
    id_cliente_final = None
    nome_cliente_final = None
    
    if id_cliente_pagador_manual:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome FROM admin.clientes WHERE id = %s", (id_cliente_pagador_manual,))
            res_m = cur.fetchone()
            if res_m:
                id_cliente_final = id_cliente_pagador_manual
                nome_cliente_final = res_m[0]
        except: pass
    else:
        dados_vinculo = buscar_cliente_vinculado_ao_usuario(id_usuario_logado)
        id_cliente_final = dados_vinculo['id']
        nome_cliente_final = dados_vinculo['nome']

    origem_real = buscar_origem_por_ambiente(ambiente)

    try:
        cur = conn.cursor()
        
        # CACHE
        if not forcar_nova:
            cur.execute("SELECT caminho_json FROM conexoes.fatorconferi_registo_consulta WHERE cpf_consultado = %s AND status_api = 'SUCESSO' ORDER BY id DESC LIMIT 1", (cpf_padrao,))
            res = cur.fetchone()
            if res and res[0] and os.path.exists(res[0]):
                try:
                    with open(res[0], 'r', encoding='utf-8') as f: 
                        dados = json.load(f)
                        if not dados.get('cpf'): dados['cpf'] = cpf_padrao
                        
                        if dados.get('nome') or dados.get('cpf'):
                            cur.execute("""
                                INSERT INTO conexoes.fatorconferi_registo_consulta 
                                (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora, id_cliente, nome_cliente, ambiente) 
                                VALUES (%s, %s, %s, %s, 0, %s, 'SUCESSO', 'BAIXAR', %s, 'CACHE', NOW(), %s, %s, %s)
                            """, ("CPF SIMPLES", cpf_padrao, id_usuario_logado, nome_usuario_logado, res[0], origem_real, id_cliente_final, nome_cliente_final, ambiente))
                            conn.commit(); conn.close()
                            return {"sucesso": True, "dados": dados, "msg": "Cache recuperado (Sem Débito)."}
                except: pass
        
        # NOVA CONSULTA API
        cred = buscar_credenciais()
        if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token ausente."}
        
        resp = requests.get(f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}", timeout=30)
        resp.encoding = 'ISO-8859-1'
        
        dados = parse_xml_to_dict(resp.text)
        
        if not dados.get('nome'):
            conn.close()
            return {"sucesso": False, "msg": "Retorno vazio ou erro na API.", "raw": resp.text, "dados": dados}
            
        if not dados.get('cpf'):
            dados['cpf'] = cpf_padrao

        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4)
        
        custo_ref = buscar_valor_consulta_atual()
        cur.execute("""
            INSERT INTO conexoes.fatorconferi_registo_consulta 
            (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora, id_cliente, nome_cliente, ambiente) 
            VALUES (%s, %s, %s, %s, %s, %s, 'SUCESSO', 'BAIXAR', %s, 'PAGO', NOW(), %s, %s, %s)
        """, ("CPF SIMPLES", cpf_padrao, id_usuario_logado, nome_usuario_logado, custo_ref, path, origem_real, id_cliente_final, nome_cliente_final, ambiente))
        conn.commit()
        
        msg_financeira = ""
        if id_cliente_final:
            ok_fin, txt_fin = processar_debito_automatico(id_cliente_final, ambiente)
            if ok_fin: 
                msg_financeira = f" | ✅ {txt_fin}"
            else: 
                msg_financeira = f" | ⚠️ Falha Financeira: {txt_fin}"
        else:
            msg_financeira = " | ⚠️ Sem débito (Cliente não identificado)."

        conn.close()
        return {"sucesso": True, "dados": dados, "msg": "Consulta realizada." + msg_financeira}
        
    except Exception as e:
        if conn: conn.close()
        return {"sucesso": False, "msg": str(e)}

# =============================================================================
# 5. GESTÃO DE PARÂMETROS E INTERFACE
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
                df_clis = pd.read_sql("SELECT DISTINCT l.id_cliente, l.nome_cliente FROM cliente.cliente_carteira_lista l ORDER BY l.nome_cliente", conn)
                opcoes_cli = {row['id_cliente']: row['nome_cliente'] for _, row in df_clis.iterrows()}
                id_cliente_teste = col_cli.selectbox("Cliente Pagador (Teste Manual)", options=[None] + list(opcoes_cli.keys()), format_func=lambda x: opcoes_cli[x] if x else "Usar Vínculo Automático")
            except: pass
            finally: conn.close()
            
        cpf_in = col_cpf.text_input("CPF Consultado")
        forcar = st.checkbox("Ignorar Histórico (Forçar Cobrança)", value=False)
        
        if st.button("🔍 Consultar", type="primary"):
            if cpf_in:
                with st.spinner("Buscando..."):
                    nome_ambiente = "teste_de_consulta_fatorconferi.cpf" 
                    st.toast(f"Ambiente: {nome_ambiente}")
                    res = realizar_consulta_cpf(cpf_in, nome_ambiente, forcar, id_cliente_teste)
                    st.session_state['resultado_fator'] = res

                    # AUTOMAÇÃO: Salva automaticamente
                    if res['sucesso']:
                        ok_s, msg_s = salvar_dados_fator_no_banco(res['dados'])
                        if ok_s: st.toast(f"{msg_s}", icon="💾")
                        else: st.error(f"Erro ao salvar na base PF: {msg_s}")
        
        if 'resultado_fator' in st.session_state:
            res = st.session_state['resultado_fator']
            if res['sucesso']:
                if "msg" in res: st.success(res['msg'])
                
                dados = res['dados']
                with st.expander("Ver Dados Retornados", expanded=True):
                    st.json(dados)
            else: st.error(res.get('msg', 'Erro'))

    with tabs[2]: 
        if st.button("🔄 Atualizar"): 
            ok, v = consultar_saldo_api()
            if ok: st.metric("Saldo Atual", f"R$ {v:.2f}")
            else: st.error("Erro ao consultar saldo.")
        
    with tabs[3]: 
        st.markdown("<p style='color: lightblue; font-size: 12px; margin-bottom: 0px;'>Tabela: conexoes.fatorconferi_registo_consulta</p>", unsafe_allow_html=True)
        conn = get_conn()
        if conn: 
            df_hist = pd.read_sql("SELECT * FROM conexoes.fatorconferi_registo_consulta ORDER BY id DESC LIMIT 20", conn)
            conn.close()
            
            event = st.dataframe(
                df_hist,
                on_select="rerun",
                selection_mode="single-row",
                use_container_width=True,
                hide_index=True
            )
            
            if len(event.selection.rows) > 0:
                idx = event.selection.rows[0]
                linha_selecionada = df_hist.iloc[idx]
                caminho_arq = linha_selecionada.get("caminho_json")
                
                if caminho_arq and os.path.exists(caminho_arq):
                    try:
                        with open(caminho_arq, "r", encoding="utf-8") as f:
                            conteudo_json = f.read()
                        
                        nome_download = os.path.basename(caminho_arq)
                        st.download_button(
                            label=f"⬇️ Baixar JSON ({nome_download})",
                            data=conteudo_json,
                            file_name=nome_download,
                            mime="application/json"
                        )
                    except Exception as e:
                        st.error(f"Erro ao ler arquivo: {e}")
                else:
                    st.warning("⚠️ Arquivo não encontrado no servidor.")
    
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