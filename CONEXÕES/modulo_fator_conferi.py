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
# 1. FUNÇÕES AUXILIARES E INTEGRAÇÃO
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
    # Apenas informativo ou fallback, pois o valor real vem da carteira do cliente
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

def converter_data_banco(data_str):
    if not data_str: return None
    try:
        return datetime.strptime(data_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except: return None

# --- PARSERS XML ---
def get_tag_text(element, tag_name):
    if element is None: return None
    res = element.find(tag_name) or element.find(tag_name.upper()) or element.find(tag_name.lower())
    return res.text if res is not None else None

def find_tag(element, tag_name):
    if element is None: return None
    return element.find(tag_name) or element.find(tag_name.upper()) or element.find(tag_name.lower())

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
        if tm:
            for child in tm:
                if 'telefone' in child.tag.lower():
                    telefones.append({'numero': get_tag_text(child, 'numero'), 'tipo': 'MOVEL', 'prioridade': get_tag_text(child, 'prioridade')})
        tf = find_tag(root, 'telefones_fixo')
        if tf:
            for child in tf:
                if 'telefone' in child.tag.lower():
                    telefones.append({'numero': get_tag_text(child, 'numero'), 'tipo': 'FIXO', 'prioridade': get_tag_text(child, 'prioridade')})
        dados['telefones'] = telefones

        emails = []
        em_root = find_tag(root, 'emails')
        if em_root:
            for em in em_root:
                if 'email' in em.tag.lower() and em.text: emails.append(em.text)
        dados['emails'] = emails

        enderecos = []
        end_root = find_tag(root, 'enderecos')
        if end_root:
            for end in end_root:
                if 'endereco' in end.tag.lower():
                    dados_end = {
                        'rua': f"{get_tag_text(end, 'logradouro') or ''}, {get_tag_text(end, 'numero') or ''} {get_tag_text(end, 'complemento') or ''}".strip().strip(','),
                        'bairro': get_tag_text(end, 'bairro'),
                        'cidade': get_tag_text(end, 'cidade'),
                        'uf': get_tag_text(end, 'estado'),
                        'cep': get_tag_text(end, 'cep')
                    }
                    enderecos.append(dados_end)
        dados['enderecos'] = enderecos
        return dados
    except Exception as e:
        return {"erro": f"Falha ao processar XML: {e}", "raw": xml_string}

def consultar_saldo_api():
    cred = buscar_credenciais()
    if not cred['token']: return False, 0.0
    try:
        response = requests.get(f"{cred['url']}?acao=VER_SALDO&TK={cred['token']}", timeout=10)
        valor_texto = response.text.strip()
        if '<' in valor_texto:
            try: valor_texto = ET.fromstring(valor_texto).text 
            except: pass
        saldo = float(valor_texto.replace(',', '.')) if valor_texto else 0.0
        
        conn = get_conn()
        if conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO conexoes.fatorconferi_registro_de_saldo (valor_saldo) VALUES (%s)", (saldo,))
            conn.commit(); conn.close()
        return True, saldo
    except: return False, 0.0

# --- LÓGICA DE IDENTIFICAÇÃO DE AMBIENTE ---

def buscar_origem_por_ambiente(nome_ambiente):
    """
    Busca na tabela 'fatorconferi_ambiente_consulta' qual a Origem configurada 
    para o ambiente informado (ex: 'teste de consulta').
    """
    conn = get_conn()
    origem_encontrada = None
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT origem FROM conexoes.fatorconferi_ambiente_consulta WHERE ambiente = %s LIMIT 1", (nome_ambiente,))
            res = cur.fetchone()
            if res: origem_encontrada = res[0]
            conn.close()
        except:
            if conn: conn.close()
    
    # Retorna None se não achar, para o sistema alertar
    return origem_encontrada

# =============================================================================
# 2. LÓGICA DE DÉBITO FINANCEIRO (REGRA NOVA)
# =============================================================================

def processar_debito_automatico(origem_da_consulta, dados_consulta):
    """
    REGRA:
    1. Identifica USUÁRIO logado.
    2. Usa USUÁRIO + ORIGEM para buscar a regra na Lista de Carteiras do Cliente.
    3. Descobre o Cliente, a Carteira (Tabela) e o Custo.
    4. Lança o Débito.
    """
    # 1. Identificar Usuário
    nome_usuario_logado = st.session_state.get('usuario_nome')
    if not nome_usuario_logado: 
        return False, "Usuário não identificado na sessão."

    conn = get_conn()
    if not conn: return False, "Erro conexão DB."
    
    try:
        cur = conn.cursor()
        
        # 2. Localizar na Lista de Carteiras (Conciliação: Usuário + Origem)
        # A tabela cliente.cliente_carteira_lista contém quem é o dono da carteira (cliente), o usuário vinculado e o custo.
        cur.execute("""
            SELECT cpf_cliente, nome_cliente, nome_carteira, custo_carteira 
            FROM cliente.cliente_carteira_lista 
            WHERE nome_usuario = %s AND origem_custo = %s 
            LIMIT 1
        """, (nome_usuario_logado, origem_da_consulta))
        
        res_lista = cur.fetchone()

        if not res_lista:
            conn.close()
            return False, f"Nenhuma carteira vinculada encontrada para o usuário '{nome_usuario_logado}' com origem '{origem_da_consulta}'."
        
        cpf_pagador = res_lista[0]
        nome_pagador = res_lista[1]
        nome_carteira_vinculada = res_lista[2]
        valor_cobranca = float(res_lista[3])

        # 3. Descobrir qual tabela SQL usar (baseado no nome da carteira encontrada)
        cur.execute("""
            SELECT nome_tabela_transacoes 
            FROM cliente.carteiras_config 
            WHERE nome_carteira = %s AND status = 'ATIVO' 
            LIMIT 1
        """, (nome_carteira_vinculada,))
        res_config = cur.fetchone()

        if not res_config:
            conn.close()
            return False, f"Tabela de transações não configurada para a carteira '{nome_carteira_vinculada}'."
            
        tabela_sql = res_config[0]

        # 4. Lançamento do Débito
        # Busca saldo anterior
        cur.execute(f"SELECT saldo_novo FROM {tabela_sql} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_pagador,))
        res_saldo = cur.fetchone()
        saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
        
        # Calcula novo saldo
        novo_saldo = saldo_anterior - valor_cobranca
        
        # Monta motivo
        cpf_consultado = dados_consulta.get('cpf', 'Desconhecido')
        motivo = f"Consulta Fator ({origem_da_consulta}): {cpf_consultado}"
        
        # Insere Registro
        sql_insert = f"""
            INSERT INTO {tabela_sql} 
            (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao)
            VALUES (%s, %s, %s, %s, 'DEBITO', %s, %s, %s, NOW())
        """
        cur.execute(sql_insert, (cpf_pagador, nome_pagador, motivo, origem_da_consulta, valor_cobranca, saldo_anterior, novo_saldo))
        
        conn.commit()
        conn.close()
        return True, f"Débito de R$ {valor_cobranca:.2f} aplicado com sucesso na carteira '{nome_carteira_vinculada}'."

    except Exception as e:
        if conn: conn.close()
        return False, f"Erro processamento financeiro: {str(e)}"

# =============================================================================
# 3. GESTÃO DE PARÂMETROS (TABELAS)
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
                -- Tenta inserir o registro padrão se estiver vazia
                INSERT INTO conexoes.fatorconferi_ambiente_consulta (ambiente, origem)
                SELECT 'teste de consulta', 'WEB USUÁRIO'
                WHERE NOT EXISTS (SELECT 1 FROM conexoes.fatorconferi_ambiente_consulta);
            """)
            conn.commit(); conn.close()
            return True
        except:
            conn.close(); return False
    return False

def salvar_alteracoes_genericas(nome_tabela, df_original, df_editado):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        ids_orig = set(df_original['id'].dropna().astype(int).tolist())
        
        # Delete
        ids_edit = set()
        for _, r in df_editado.iterrows():
            if pd.notna(r.get('id')) and r.get('id') != '':
                try: ids_edit.add(int(r['id']))
                except: pass
        
        ids_del = ids_orig - ids_edit
        if ids_del: cur.execute(f"DELETE FROM {nome_tabela} WHERE id IN ({','.join(map(str, ids_del))})")

        # Upsert
        for _, row in df_editado.iterrows():
            cols = [c for c in row.index if c not in ['id', 'data_hora', 'data_criacao', 'data_registro']]
            vals = [row[c] for c in cols]
            rid = row.get('id')
            
            if pd.isna(rid) or rid == '':
                pl = ", ".join(["%s"]*len(cols)); cn = ", ".join(cols)
                cur.execute(f"INSERT INTO {nome_tabela} ({cn}) VALUES ({pl})", vals)
            elif int(rid) in ids_orig:
                sc = ", ".join([f"{c} = %s" for c in cols])
                cur.execute(f"UPDATE {nome_tabela} SET {sc} WHERE id = %s", vals + [int(rid)])
        
        conn.commit(); conn.close(); return True
    except Exception as e:
        st.error(f"Erro: {e}"); 
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
        if not cpf_limpo: return False, "CPF inválido."

        campos = {
            'nome': dados_api.get('nome'), 'data_nascimento': converter_data_banco(dados_api.get('nascimento')),
            'rg': dados_api.get('rg'), 'nome_mae': dados_api.get('mae')
        }
        
        cur.execute("""
            INSERT INTO banco_pf.pf_dados (cpf, nome, data_nascimento, rg, nome_mae, data_criacao)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (cpf) DO UPDATE SET
                nome = COALESCE(EXCLUDED.nome, banco_pf.pf_dados.nome),
                data_nascimento = COALESCE(EXCLUDED.data_nascimento, banco_pf.pf_dados.data_nascimento),
                rg = COALESCE(EXCLUDED.rg, banco_pf.pf_dados.rg),
                nome_mae = COALESCE(EXCLUDED.nome_mae, banco_pf.pf_dados.nome_mae);
        """, (cpf_limpo, campos['nome'], campos['data_nascimento'], campos['rg'], campos['nome_mae']))
        
        # Telefones, emails, etc (simplificado)
        conn.commit(); conn.close()
        return True, "Dados salvos na Base PF."
    except Exception as e:
        if conn: conn.close()
        return False, f"Erro DB: {e}"

def realizar_consulta_cpf(cpf, origem_definida, forcar_nova=False):
    if not origem_definida:
        return {"sucesso": False, "msg": "Origem de custo não definida para este ambiente. Verifique os parâmetros."}

    cpf_padrao = ''.join(filter(str.isdigit, str(cpf))).zfill(11)
    conn = get_conn()
    if not conn: return {"sucesso": False, "msg": "Erro DB."}
    
    try:
        cur = conn.cursor()
        
        # 1. Cache
        if not forcar_nova:
            cur.execute("SELECT caminho_json FROM conexoes.fatorconferi_registo_consulta WHERE cpf_consultado = %s AND status_api = 'SUCESSO' ORDER BY id DESC LIMIT 1", (cpf_padrao,))
            res = cur.fetchone()
            if res and res[0] and os.path.exists(res[0]):
                try:
                    with open(res[0], 'r', encoding='utf-8') as f: dados = json.load(f)
                    if dados.get('nome'):
                        usr = st.session_state.get('usuario_nome', 'Sistema'); id_usr = st.session_state.get('usuario_id', 0)
                        # Loga uso do cache (sem custo)
                        cur.execute("INSERT INTO conexoes.fatorconferi_registo_consulta (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora) VALUES (%s, %s, %s, %s, 0, %s, 'SUCESSO', %s, %s, 'CACHE', NOW())", ("CPF SIMPLES", cpf_padrao, id_usr, usr, res[0], res[0], origem_definida))
                        conn.commit(); conn.close()
                        return {"sucesso": True, "dados": dados, "msg": "Recuperado do Cache."}
                except: pass
        
        # 2. Nova Consulta API
        cred = buscar_credenciais()
        if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token ausente."}
        
        resp = requests.get(f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}", timeout=30)
        dados = parse_xml_to_dict(resp.text)
        
        if not dados.get('nome'): 
            conn.close()
            return {"sucesso": False, "msg": "CPF não localizado ou erro na API.", "dados": dados}

        # 3. Salva JSON
        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4)
        
        # 4. PROCESSA O DÉBITO (Nova Regra)
        ok_fin, txt_fin = processar_debito_automatico(origem_definida, dados)
        msg_fin = f" | {txt_fin}" if ok_fin else f" | ⚠️ {txt_fin}"
        
        # 5. Registra Log
        usr = st.session_state.get('usuario_nome', 'Sistema'); id_usr = st.session_state.get('usuario_id', 0)
        # Valor pago aqui é simbólico, o real foi descontado na carteira
        cur.execute("INSERT INTO conexoes.fatorconferi_registo_consulta (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora) VALUES (%s, %s, %s, %s, %s, %s, 'SUCESSO', %s, %s, 'PAGO', NOW())", ("CPF SIMPLES", cpf_padrao, id_usr, usr, 0.0, path, path, origem_definida))
        conn.commit(); conn.close()
        
        return {"sucesso": True, "dados": dados, "msg": "Consulta realizada." + msg_fin}

    except Exception as e:
        if conn: conn.close()
        return {"sucesso": False, "msg": str(e)}

# =============================================================================
# 5. INTERFACE PRINCIPAL
# =============================================================================

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    tabs = st.tabs(["👥 Clientes", "🔍 Teste de Consulta", "💰 Saldo API", "📋 Histórico", "⚙️ Parâmetros"])

    # Tab Clientes e Histórico mantidos simples...
    with tabs[0]: st.info("Use o Módulo Clientes para gerenciar carteiras."); st.dataframe(listar_clientes_carteira(), use_container_width=True)
    with tabs[3]: 
        conn = get_conn()
        if conn: st.dataframe(pd.read_sql("SELECT * FROM conexoes.fatorconferi_registo_consulta ORDER BY id DESC LIMIT 20", conn)); conn.close()

    # --- ABA TESTE DE CONSULTA ---
    with tabs[1]:
        st.markdown("#### 1.1 Consulta e Importação")
        
        # [NOVO] Identificação do Ambiente Fixo
        ambiente_atual = "teste de consulta"
        
        c1, c2, c3 = st.columns([3, 1.5, 1.5])
        cpf_in = c1.text_input("CPF")
        forcar = c2.checkbox("Ignorar Histórico", value=False)
        
        if c3.button("🔍 Consultar", type="primary"):
            if cpf_in:
                with st.spinner(f"Identificando regra para ambiente: '{ambiente_atual}'..."):
                    # 1. Busca Origem pelo Ambiente
                    origem_definida = obter_origem_padronizada(buscar_origem_por_ambiente(ambiente_atual))
                    
                    if not origem_definida:
                        st.error(f"Erro: Nenhuma origem configurada para o ambiente '{ambiente_atual}'. Verifique a aba Parâmetros.")
                    else:
                        # 2. Executa Consulta passando a Origem correta
                        res = realizar_consulta_cpf(cpf_in, origem_definida, forcar)
                        st.session_state['resultado_fator'] = res
        
        if 'resultado_fator' in st.session_state:
            res = st.session_state['resultado_fator']
            if res['sucesso']:
                if "msg" in res: 
                    if "⚠️" in res['msg']: st.warning(res['msg'])
                    else: st.success(res['msg'])
                
                st.divider()
                if st.button("💾 Salvar na Base PF", type="primary"):
                    ok_s, msg_s = salvar_dados_fator_no_banco(res['dados'])
                    if ok_s: st.success(msg_s)
                    else: st.error(msg_s)
                
                with st.expander("Visualizar Dados Retornados", expanded=True):
                    st.json(res['dados'])
            else: st.error(res.get('msg', 'Erro'))

    # --- ABA PARÂMETROS ---
    with tabs[4]: 
        st.markdown("### 🛠️ Gestão de Tabelas do Sistema")
        opcoes = {
            "1. Carteiras de Clientes": "conexoes.fator_cliente_carteira",
            "2. Origens de Consulta": "conexoes.fatorconferi_origem_consulta_fator",
            "3. Ambiente de Consulta": "conexoes.fatorconferi_ambiente_consulta",
            "4. Parâmetros Gerais": "conexoes.fatorconferi_parametros",
            "5. Registros de Consulta": "conexoes.fatorconferi_registo_consulta",
            "6. Tipos de Consulta": "conexoes.fatorconferi_tipo_consulta_fator",
            "7. Valores da Consulta": "conexoes.fatorconferi_valor_da_consulta",
            "8. Relação de Conexões": "conexoes.relacao"
        }
        sel = st.selectbox("Tabela:", list(opcoes.keys()))
        sql_tab = opcoes[sel]
        
        df = carregar_dados_genericos(sql_tab)
        if df is None:
            st.warning(f"Tabela `{sql_tab}` não encontrada.")
            if sql_tab == "conexoes.fatorconferi_ambiente_consulta":
                if st.button("🛠️ Criar Tabela Agora"): 
                    criar_tabela_ambiente(); st.rerun()
        else:
            st.info(f"Editando: `{sql_tab}`")
            df_ed = st.data_editor(df, key=f"ed_{sql_tab}", num_rows="dynamic", use_container_width=True, disabled=["id", "data_hora", "data_criacao"])
            if st.button("💾 Salvar Alterações"):
                if salvar_alteracoes_genericas(sql_tab, df, df_ed): st.success("Salvo!"); time.sleep(1); st.rerun()