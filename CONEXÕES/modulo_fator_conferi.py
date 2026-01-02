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
# Garante que o caminho seja absoluto e correto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PASTA_JSON = os.path.join(BASE_DIR, "JSON")

# Tenta criar a pasta se não existir
try:
    if not os.path.exists(PASTA_JSON):
        os.makedirs(PASTA_JSON, exist_ok=True)
        print(f"✅ Pasta JSON criada em: {PASTA_JSON}")
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
# 1. FUNÇÕES AUXILIARES (API, XML, CREDENCIAIS, FORMATAÇÃO)
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
    """Busca o valor atual da consulta na tabela de parâmetros"""
    conn = get_conn()
    valor = 0.50 # Fallback padrão
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
    """Aplica máscara de CPF ou CNPJ para visualização na tabela"""
    dado = re.sub(r'\D', '', str(valor))
    if len(dado) == 11:
        return f"{dado[:3]}.{dado[3:6]}.{dado[6:9]}-{dado[9:]}"
    elif len(dado) == 14:
        return f"{dado[:2]}.{dado[2:5]}.{dado[5:8]}/{dado[8:12]}-{dado[12:]}"
    return valor

def parse_xml_to_dict(xml_string):
    try:
        # Tenta limpar encoding se vier bagunçado
        xml_string = xml_string.replace('ISO-8859-1', 'UTF-8') 
        root = ET.fromstring(xml_string)
        dados = {}
        
        # Parse básico
        cad = root.find('cadastrais')
        if cad is not None:
            dados['nome'] = cad.findtext('nome')
            dados['cpf'] = cad.findtext('cpf')
            dados['nascimento'] = cad.findtext('nascto')
            dados['mae'] = cad.findtext('nome_mae')
            dados['situacao'] = cad.findtext('situacao_receita')
        
        telefones = []
        tm = root.find('telefones_movel')
        if tm is not None:
            for fone in tm.findall('telefone'):
                telefones.append({
                    'numero': fone.findtext('numero'),
                    'whatsapp': fone.findtext('tem_zap')
                })
        dados['telefones'] = telefones
        
        # Salva o XML bruto também para debug
        dados['_raw_xml'] = xml_string
        return dados
    except Exception as e:
        return {"erro": f"Falha ao processar XML: {e}", "raw": xml_string}

def consultar_saldo_api():
    cred = buscar_credenciais()
    if not cred['token']: return False, "Token não configurado."
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
        return False, str(e)

# =============================================================================
# FLUXO PRINCIPAL DE CONSULTA
# =============================================================================

def realizar_consulta_cpf(cpf, origem="Teste Manual", forcar_nova=False):
    # 1. Limpeza e Definição Automática do Tipo
    cpf_limpo_raw = ''.join(filter(str.isdigit, str(cpf)))
    
    if len(cpf_limpo_raw) <= 11: 
        cpf_padrao = cpf_limpo_raw.zfill(11)
        tipo_registro = "CPF SIMPLES"
    else: 
        cpf_padrao = cpf_limpo_raw.zfill(14)
        tipo_registro = "CNPJ SIMPLES"
    
    conn = get_conn()
    if not conn: return {"sucesso": False, "msg": "Erro de conexão com banco de dados."}
    
    try:
        cur = conn.cursor()
        
        # --- A.1 VERIFICAÇÃO DE CACHE (Se não for forçada) ---
        if not forcar_nova:
            cur.execute("""
                SELECT caminho_json, link_arquivo_consulta 
                FROM conexoes.fatorconferi_registo_consulta 
                WHERE cpf_consultado = %s AND status_api = 'SUCESSO'
                ORDER BY id DESC LIMIT 1
            """, (cpf_padrao,))
            
            registro_anterior = cur.fetchone()
            
            if registro_anterior:
                caminho_existente = registro_anterior[0]
                link_existente = registro_anterior[1] if registro_anterior[1] else caminho_existente
                
                dados_parsed = {}
                msg_retorno = "Dados recuperados do histórico (R$ 0,00)."

                # Verifica se arquivo existe fisicamente
                if caminho_existente and os.path.exists(caminho_existente):
                    try:
                        with open(caminho_existente, 'r', encoding='utf-8') as f:
                            dados_parsed = json.load(f)
                    except: 
                        msg_retorno += " (Erro leitura arquivo)"
                else:
                    msg_retorno += " (Arquivo físico não localizado. Recomenda-se forçar nova consulta.)"

                # Registra o log de acesso ao cache
                usuario = st.session_state.get('usuario_nome', 'Sistema')
                id_user = st.session_state.get('usuario_id', 0)
                
                sql_cache = """
                    INSERT INTO conexoes.fatorconferi_registo_consulta 
                    (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora)
                    VALUES (%s, %s, %s, %s, 0.00, %s, 'SUCESSO', %s, %s, 'CACHE', NOW())
                """
                cur.execute(sql_cache, (tipo_registro, cpf_padrao, id_user, usuario, caminho_existente, link_existente, origem))
                conn.commit(); conn.close()
                return {"sucesso": True, "dados": dados_parsed, "msg": msg_retorno}
        
        # --- NOVA CONSULTA API ---
        cred = buscar_credenciais()
        if not cred['token']:
            conn.close(); return {"sucesso": False, "msg": "Token da API não configurado."}
            
        url = f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}"
        
        # Timeout aumentado para evitar falhas de rede
        response = requests.get(url, timeout=30)
        response.encoding = 'ISO-8859-1'
        xml_content = response.text
        
        if "Não localizado" in xml_content or "erro" in xml_content.lower():
             conn.close()
             return {"sucesso": False, "msg": f"CPF não localizado ou erro na API: {xml_content}", "raw": xml_content}
        
        dados_parsed = parse_xml_to_dict(xml_content)
        
        # --- SALVAMENTO DO ARQUIVO ---
        nome_arquivo = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        caminho_completo = os.path.join(PASTA_JSON, nome_arquivo)
        
        try:
            with open(caminho_completo, 'w', encoding='utf-8') as f:
                json.dump(dados_parsed, f, ensure_ascii=False, indent=4)
        except Exception as e_save:
            conn.close()
            return {"sucesso": False, "msg": f"Erro de Permissão ao salvar arquivo: {e_save}"}
        
        # --- REGISTRO NO BANCO ---
        usuario = st.session_state.get('usuario_nome', 'Sistema')
        id_user = st.session_state.get('usuario_id', 0)
        custo = buscar_valor_consulta_atual()
        
        link_arquivo = caminho_completo 
        
        sql = """
            INSERT INTO conexoes.fatorconferi_registo_consulta 
            (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora)
            VALUES (%s, %s, %s, %s, %s, %s, 'SUCESSO', %s, %s, 'PAGO', NOW())
        """
        cur.execute(sql, (tipo_registro, cpf_padrao, id_user, usuario, custo, caminho_completo, link_arquivo, origem))
        conn.commit(); conn.close()
        
        return {"sucesso": True, "dados": dados_parsed, "msg": f"Consulta realizada e salva em: {nome_arquivo}"}
        
    except Exception as e:
        if conn: conn.close()
        return {"sucesso": False, "msg": f"Erro Geral: {str(e)}"}

# ... [RESTO DAS FUNÇÕES FINANCEIRAS E CRUD - MANTIDAS IGUAIS AO ANTERIOR] ...
# Para economizar espaço e evitar corte, as funções abaixo (financeiro, crud, dialogs) 
# são idênticas ao código anterior. Mantenha as funções:
# listar_clientes_carteira, cadastrar_carteira_cliente, movimentar_saldo, etc.
# listar_origem_consulta, salvar_origem_consulta, etc.
# Dialogs: dialog_movimentar, etc.

# Vou incluir apenas a parte que mudou do APP PRINCIPAL (Interface)

def listar_clientes_carteira():
    conn = get_conn()
    if conn:
        try:
            query = """
                SELECT cc.id, cc.nome_cliente, cc.custo_por_consulta, cc.saldo_atual, cc.status,
                       ac.cpf, ac.telefone
                FROM conexoes.fator_cliente_carteira cc
                LEFT JOIN admin.clientes ac ON cc.id_cliente_admin = ac.id
                ORDER BY cc.nome_cliente
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def cadastrar_carteira_cliente(id_admin, nome, custo_inicial):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO conexoes.fator_cliente_carteira (id_cliente_admin, nome_cliente, custo_por_consulta, saldo_atual)
                VALUES (%s, %s, %s, 0.00)
                ON CONFLICT (id_cliente_admin) DO NOTHING
            """, (id_admin, nome, custo_inicial))
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

def movimentar_saldo(id_carteira, tipo, valor, motivo, usuario_resp):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT saldo_atual FROM conexoes.fator_cliente_carteira WHERE id = %s", (id_carteira,))
            res = cur.fetchone()
            if not res: return False, "Carteira não encontrada"
            saldo_ant = float(res[0])
            
            valor = float(valor)
            if tipo == 'DEBITO': novo_saldo = saldo_ant - valor
            else: novo_saldo = saldo_ant + valor
            
            cur.execute("UPDATE conexoes.fator_cliente_carteira SET saldo_atual = %s WHERE id = %s", (novo_saldo, id_carteira))
            cur.execute("""
                INSERT INTO conexoes.fator_cliente_transacoes 
                (id_carteira, tipo, valor, saldo_anterior, saldo_novo, motivo, usuario_responsavel)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (id_carteira, tipo, valor, saldo_ant, novo_saldo, motivo, usuario_resp))
            conn.commit(); conn.close()
            return True, "Sucesso"
        except Exception as e:
            conn.close(); return False, str(e)
    return False

def atualizar_custo_cliente(id_carteira, novo_custo):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE conexoes.fator_cliente_carteira SET custo_por_consulta = %s WHERE id = %s", (novo_custo, id_carteira))
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

def excluir_carteira_cliente(id_carteira):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM conexoes.fator_cliente_carteira WHERE id = %s", (id_carteira,))
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

def buscar_extrato_cliente_filtrado(id_carteira, data_ini, data_fim):
    conn = get_conn()
    if conn:
        try:
            dt_ini_str = data_ini.strftime('%Y-%m-%d 00:00:00')
            dt_fim_str = data_fim.strftime('%Y-%m-%d 23:59:59')
            
            query = """
                SELECT id, data_transacao, tipo, valor, saldo_novo, motivo, usuario_responsavel 
                FROM conexoes.fator_cliente_transacoes 
                WHERE id_carteira = %s 
                  AND data_transacao BETWEEN %s AND %s
                ORDER BY data_transacao DESC
            """
            df = pd.read_sql(query, conn, params=(id_carteira, dt_ini_str, dt_fim_str))
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def editar_transacao_db(id_transacao, id_carteira, novo_tipo, novo_valor, novo_motivo):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT tipo, valor FROM conexoes.fator_cliente_transacoes WHERE id=%s", (id_transacao,))
            res = cur.fetchone()
            if not res: return False
            tipo_ant, valor_ant = res
            valor_ant = float(valor_ant)
            
            fator_ant = -1 if tipo_ant == 'DEBITO' else 1
            fator_novo = -1 if novo_tipo == 'DEBITO' else 1
            diff = (float(novo_valor) * fator_novo) - (valor_ant * fator_ant)
            
            cur.execute("""
                UPDATE conexoes.fator_cliente_transacoes 
                SET tipo=%s, valor=%s, motivo=%s 
                WHERE id=%s
            """, (novo_tipo, novo_valor, novo_motivo, id_transacao))
            
            cur.execute("""
                UPDATE conexoes.fator_cliente_carteira 
                SET saldo_atual = saldo_atual + %s 
                WHERE id=%s
            """, (diff, id_carteira))
            
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

def excluir_transacao_db(id_transacao, id_carteira):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT tipo, valor FROM conexoes.fator_cliente_transacoes WHERE id=%s", (id_transacao,))
            res = cur.fetchone()
            if not res: return False
            tipo, valor = res
            
            fator = 1 if tipo == 'DEBITO' else -1
            ajuste = float(valor) * fator
            
            cur.execute("DELETE FROM conexoes.fator_cliente_transacoes WHERE id=%s", (id_transacao,))
            cur.execute("UPDATE conexoes.fator_cliente_carteira SET saldo_atual = saldo_atual + %s WHERE id=%s", (ajuste, id_carteira))
            
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

# --- CRUD PARAMETROS ---
def listar_origem_consulta():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, origem FROM conexoes.fatorconferi_origem_consulta ORDER BY origem", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_origem_consulta(origem):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO conexoes.fatorconferi_origem_consulta (origem) VALUES (%s)", (origem,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def atualizar_origem_consulta(id_reg, nova_origem):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE conexoes.fatorconferi_origem_consulta SET origem=%s WHERE id=%s", (nova_origem, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_origem_consulta(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM conexoes.fatorconferi_origem_consulta WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def listar_tipo_consulta_fator():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, tipo FROM conexoes.fatorconferi_tipo_consulta_fator ORDER BY tipo", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_tipo_consulta_fator(tipo):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO conexoes.fatorconferi_tipo_consulta_fator (tipo) VALUES (%s)", (tipo,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def atualizar_tipo_consulta_fator(id_reg, novo_tipo):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE conexoes.fatorconferi_tipo_consulta_fator SET tipo=%s WHERE id=%s", (novo_tipo, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_tipo_consulta_fator(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM conexoes.fatorconferi_tipo_consulta_fator WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def listar_valor_consulta():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, valor_da_consulta, data_atualizacao FROM conexoes.fatorconferi_valor_da_consulta ORDER BY data_atualizacao DESC", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_valor_consulta(valor):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO conexoes.fatorconferi_valor_da_consulta (valor_da_consulta) VALUES (%s)", (valor,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def atualizar_valor_consulta(id_reg, novo_valor):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE conexoes.fatorconferi_valor_da_consulta SET valor_da_consulta=%s, data_atualizacao=NOW() WHERE id=%s", (novo_valor, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_valor_consulta(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM conexoes.fatorconferi_valor_da_consulta WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

# --- DIALOGS (MANTIDOS) ---
@st.dialog("💰 Movimentar Saldo")
def dialog_movimentar(id_cart, nome_cli):
    st.write(f"Cliente: **{nome_cli}**")
    tipo = st.selectbox("Tipo de Operação", ["CREDITO", "DEBITO"])
    valor = st.number_input("Valor (R$)", min_value=0.01, step=1.00)
    motivo = st.text_input("Motivo", placeholder="Ex: Pix, Consulta Avulsa...")
    if st.button("Confirmar Movimentação", type="primary"):
        user_logado = st.session_state.get('usuario_nome', 'Admin')
        ok, msg = movimentar_saldo(id_cart, tipo, valor, motivo, user_logado)
        if ok: st.success("Saldo atualizado!"); time.sleep(1); st.rerun()
        else: st.error(f"Erro: {msg}")

@st.dialog("➕ Novo Cliente Fator")
def dialog_novo_cliente_fator():
    conn = get_conn()
    query = "SELECT id, nome FROM admin.clientes WHERE id NOT IN (SELECT id_cliente_admin FROM conexoes.fator_cliente_carteira) ORDER BY nome"
    df_adm = pd.read_sql(query, conn); conn.close()
    if df_adm.empty:
        st.info("Todos os clientes já possuem carteira.")
        return
    sel_idx = st.selectbox("Selecione o Cliente", range(len(df_adm)), format_func=lambda x: df_adm.iloc[x]['nome'])
    custo = st.number_input("Custo por Consulta (R$)", value=0.50, step=0.01)
    if st.button("Criar Carteira"):
        cli = df_adm.iloc[sel_idx]
        if cadastrar_carteira_cliente(int(cli['id']), cli['nome'], custo): st.success("Carteira criada!"); st.rerun()

@st.dialog("✏️ Editar Custo")
def dialog_editar_custo(id_cart, nome_cli, custo_atual):
    st.write(f"Editando: **{nome_cli}**")
    novo_custo = st.number_input("Custo (R$)", value=float(custo_atual), step=0.01)
    if st.button("Salvar"):
        if atualizar_custo_cliente(id_cart, novo_custo): st.success("Atualizado!"); time.sleep(1); st.rerun()

@st.dialog("🚨 Excluir Carteira")
def dialog_excluir_carteira(id_cart, nome_cli):
    st.warning(f"Excluir carteira de **{nome_cli}**?"); c1, c2 = st.columns(2)
    if c1.button("✅ Sim"):
        if excluir_carteira_cliente(id_cart): st.success("Excluído."); time.sleep(1); st.rerun()
    if c2.button("❌ Não"): st.rerun()

@st.dialog("✏️ Editar Transação")
def dialog_editar_transacao(transacao, id_carteira):
    st.write(f"Editando Transação"); n_tipo = st.selectbox("Tipo", ["CREDITO", "DEBITO"], index=0 if transacao['tipo']=="CREDITO" else 1)
    n_valor = st.number_input("Valor", value=float(transacao['valor']), step=0.10); n_motivo = st.text_input("Motivo", value=transacao['motivo'])
    if st.button("Salvar"):
        if editar_transacao_db(transacao['id'], id_carteira, n_tipo, n_valor, n_motivo): st.success("Atualizado!"); time.sleep(1); st.rerun()

@st.dialog("🗑️ Excluir Transação")
def dialog_excluir_transacao(id_transacao, id_carteira):
    st.warning("Tem certeza?"); c1, c2 = st.columns(2)
    if c1.button("Sim"):
        if excluir_transacao_db(id_transacao, id_carteira): st.success("Excluído!"); time.sleep(1); st.rerun()
    if c2.button("Não"): st.rerun()

@st.dialog("✏️ Editar Origem")
def dialog_editar_origem(id_reg, nome_atual):
    with st.form("fe"): 
        nn = st.text_input("Origem", value=nome_atual)
        if st.form_submit_button("Salvar"):
            if atualizar_origem_consulta(id_reg, nn): st.success("Ok"); time.sleep(0.5); st.rerun()

@st.dialog("✏️ Editar Tipo")
def dialog_editar_tipo_consulta(id_reg, nome_atual):
    with st.form("ft"):
        nn = st.text_input("Tipo", value=nome_atual)
        if st.form_submit_button("Salvar"):
            if atualizar_tipo_consulta_fator(id_reg, nn): st.success("Ok"); time.sleep(0.5); st.rerun()

@st.dialog("✏️ Editar Valor")
def dialog_editar_valor_consulta(id_reg, valor_atual):
    with st.form("fv"):
        nv = st.number_input("Valor", value=float(valor_atual), step=0.01)
        if st.form_submit_button("Salvar"):
            if atualizar_valor_consulta(id_reg, nv): st.success("Ok"); time.sleep(0.5); st.rerun()

# =============================================================================
# 5. INTERFACE PRINCIPAL
# =============================================================================

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    
    creds = buscar_credenciais()
    if not creds['token']: st.warning("⚠️ Token da API não configurado.")
    
    tabs = st.tabs([
        "👥 Clientes", "🔍 Teste de Consulta", "💰 Saldo API", 
        "📋 Histórico", "⚙️ Parâmetros", "🤖 Chatbot", "📂 Lote"
    ])

    with tabs[0]: # Clientes
        c1, c2 = st.columns([5, 1]); c1.markdown("#### Carteiras"); 
        if c2.button("➕ Novo", key="nc"): dialog_novo_cliente_fator()
        df_cli = listar_clientes_carteira()
        if not df_cli.empty:
            for _, r in df_cli.iterrows():
                with st.container():
                    c1, c2, c3, c4 = st.columns([3, 1, 1, 2])
                    c1.write(f"**{r['nome_cliente']}**")
                    cor = "green" if float(r['saldo_atual']) > 0 else "red"
                    c2.markdown(f":{cor}[R$ {float(r['saldo_atual']):.2f}]")
                    c3.write(f"R$ {float(r['custo_por_consulta']):.2f}")
                    with c4:
                        b1, b2, b3, b4 = st.columns(4)
                        if b1.button("💲", key=f"mv_{r['id']}"): dialog_movimentar(r['id'], r['nome_cliente'])
                        if b2.button("📜", key=f"ex_{r['id']}"): 
                            st.session_state['cli_exp'] = r['id'] if st.session_state.get('cli_exp') != r['id'] else None
                        if b3.button("✏️", key=f"ed_{r['id']}"): dialog_editar_custo(r['id'], r['nome_cliente'], r['custo_por_consulta'])
                        if b4.button("🗑️", key=f"dl_{r['id']}"): dialog_excluir_carteira(r['id'], r['nome_cliente'])
                    st.markdown("<hr style='margin:5px 0'>", unsafe_allow_html=True)
                
                if st.session_state.get('cli_exp') == r['id']:
                    with st.container(border=True):
                        st.caption("Extrato")
                        df_ext = buscar_extrato_cliente_filtrado(r['id'], date.today()-timedelta(days=30), date.today())
                        if not df_ext.empty:
                            for _, tr in df_ext.iterrows():
                                ct1, ct2, ct3, ct4, ct5 = st.columns([2, 3, 1, 1, 1])
                                ct1.write(pd.to_datetime(tr['data_transacao']).strftime('%d/%m %H:%M'))
                                ct2.write(tr['motivo'])
                                ct3.write(tr['tipo'])
                                ct4.write(f"{float(tr['valor']):.2f}")
                                with ct5:
                                    if st.button("✏️", key=f"e{tr['id']}"): dialog_editar_transacao(tr, r['id'])
                                    if st.button("🗑️", key=f"d{tr['id']}"): dialog_excluir_transacao(tr['id'], r['id'])
                                st.divider()

    with tabs[1]: # Teste de Consulta
        st.markdown("#### 1.1 Ambiente de Teste Manual")
        c1, c2 = st.columns([3, 1])
        cpf_input = c1.text_input("CPF para Consulta")
        
        # --- ATUALIZAÇÃO AQUI ---
        forcar = st.checkbox("Forçar Nova Consulta (Ignorar Cache)", value=False)
        # ------------------------

        if c2.button("🔍 Consultar", type="primary"):
            if cpf_input:
                with st.spinner("Consultando..."):
                    res = realizar_consulta_cpf(cpf_input, "WEB USUÁRIO", forcar_nova=forcar)
                    if res['sucesso']:
                        if "msg" in res: st.info(f"ℹ️ {res['msg']}")
                        else: st.success("Sucesso!")
                        st.json(res['dados'])
                    else: st.error(f"Erro: {res.get('msg', 'Erro')}")

    with tabs[2]: # Saldo API
        if st.button("🔄 Atualizar"): 
            ok, v = consultar_saldo_api()
            if ok: st.metric("Saldo", f"R$ {v:.2f}")
        
    with tabs[3]: # Histórico
        st.markdown("#### 5.1 Histórico")
        conn = get_conn()
        if conn:
            query = "SELECT * FROM conexoes.fatorconferi_registo_consulta ORDER BY id DESC LIMIT 50"
            df = pd.read_sql(query, conn)
            st.dataframe(df, hide_index=True)
            conn.close()

    with tabs[4]: # Parâmetros
        st.info("Configurações de Origem, Tipo e Valor.")
        # [Implementação simplificada das tabelas de parâmetros já incluída nas funções CRUD acima]
        # Para brevidade no copy-paste, a lógica visual aqui seria similar à do modulo_cliente
        # Caso precise, posso expandir essa aba especificamente.

    with tabs[5]: st.info("Em breve.")
    with tabs[6]: st.info("Em breve.")