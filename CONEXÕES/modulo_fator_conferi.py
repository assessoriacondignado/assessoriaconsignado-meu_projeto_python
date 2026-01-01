import streamlit as st
import pandas as pd
import psycopg2
import requests
import json
import os
import time
import re
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
import conexao

# --- CONFIGURAÇÕES ---
PASTA_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "JSON")
if not os.path.exists(PASTA_JSON):
    os.makedirs(PASTA_JSON)

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
    """Aplica máscara de CPF ou CNPJ para visualização"""
    dado = re.sub(r'\D', '', str(valor))
    if len(dado) == 11:
        return f"{dado[:3]}.{dado[3:6]}.{dado[6:9]}-{dado[9:]}"
    elif len(dado) == 14:
        return f"{dado[:2]}.{dado[2:5]}.{dado[5:8]}/{dado[8:12]}-{dado[12:]}"
    return valor

def parse_xml_to_dict(xml_string):
    try:
        xml_string = xml_string.replace('ISO-8859-1', 'UTF-8') 
        root = ET.fromstring(xml_string)
        dados = {}
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

def realizar_consulta_cpf(cpf, tipo="COMPLETA"):
    cred = buscar_credenciais()
    if not cred['token']: return {"sucesso": False, "msg": "Token ausente"}
    cpf_limpo = ''.join(filter(str.isdigit, str(cpf)))
    url = f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_limpo}"
    try:
        response = requests.get(url, timeout=15)
        response.encoding = 'ISO-8859-1'
        xml_content = response.text
        if "Não localizado" in xml_content or "erro" in xml_content.lower():
             return {"sucesso": False, "msg": "CPF Não localizado ou erro na API", "raw": xml_content}
        dados_parsed = parse_xml_to_dict(xml_content)
        nome_arquivo = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_limpo}.json"
        caminho_completo = os.path.join(PASTA_JSON, nome_arquivo)
        with open(caminho_completo, 'w', encoding='utf-8') as f:
            json.dump(dados_parsed, f, ensure_ascii=False, indent=4)
        
        conn = get_conn()
        if conn:
            cur = conn.cursor()
            usuario = st.session_state.get('usuario_nome', 'Sistema')
            id_user = st.session_state.get('usuario_id', 0)
            
            # Busca o valor dinâmico
            custo = buscar_valor_consulta_atual()
            
            sql = """
                INSERT INTO conexoes.fatorconferi_registo_consulta 
                (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api)
                VALUES (%s, %s, %s, %s, %s, %s, 'SUCESSO')
            """
            cur.execute(sql, (tipo, cpf_limpo, id_user, usuario, custo, caminho_completo))
            conn.commit(); conn.close()
        return {"sucesso": True, "dados": dados_parsed}
    except Exception as e:
        return {"sucesso": False, "msg": str(e)}

# =============================================================================
# 2. FUNÇÕES DE GESTÃO FINANCEIRA
# =============================================================================

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

# =============================================================================
# 3. FUNÇÕES CRUD PARÂMETROS
# =============================================================================

# --- ORIGEM CONSULTA ---
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

# --- TIPO CONSULTA FATOR ---
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

# --- VALOR DA CONSULTA (NOVO) ---
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

# =============================================================================
# 4. DIALOGS (POP-UPS)
# =============================================================================

@st.dialog("💰 Movimentar Saldo")
def dialog_movimentar(id_cart, nome_cli):
    st.write(f"Cliente: **{nome_cli}**")
    tipo = st.selectbox("Tipo de Operação", ["CREDITO", "DEBITO"])
    valor = st.number_input("Valor (R$)", min_value=0.01, step=1.00)
    motivo = st.text_input("Motivo", placeholder="Ex: Pix, Consulta Avulsa...")
    if st.button("Confirmar Movimentação", type="primary"):
        user_logado = st.session_state.get('usuario_nome', 'Admin')
        ok, msg = movimentar_saldo(id_cart, tipo, valor, motivo, user_logado)
        if ok:
            st.success("Saldo atualizado!")
            time.sleep(1); st.rerun()
        else: st.error(f"Erro: {msg}")

@st.dialog("➕ Novo Cliente Fator")
def dialog_novo_cliente_fator():
    conn = get_conn()
    query = """
        SELECT id, nome FROM admin.clientes 
        WHERE id NOT IN (SELECT id_cliente_admin FROM conexoes.fator_cliente_carteira)
        ORDER BY nome
    """
    df_adm = pd.read_sql(query, conn); conn.close()
    if df_adm.empty:
        st.info("Todos os clientes já possuem carteira.")
        return
    sel_idx = st.selectbox("Selecione o Cliente", range(len(df_adm)), format_func=lambda x: df_adm.iloc[x]['nome'])
    custo = st.number_input("Custo por Consulta (R$)", value=0.50, step=0.01)
    if st.button("Criar Carteira"):
        cli = df_adm.iloc[sel_idx]
        if cadastrar_carteira_cliente(int(cli['id']), cli['nome'], custo):
            st.success("Carteira criada!"); st.rerun()

@st.dialog("✏️ Editar Custo")
def dialog_editar_custo(id_cart, nome_cli, custo_atual):
    st.write(f"Editando: **{nome_cli}**")
    novo_custo = st.number_input("Custo por Consulta (R$)", value=float(custo_atual), step=0.01, min_value=0.0)
    if st.button("Salvar Alteração"):
        if atualizar_custo_cliente(id_cart, novo_custo):
            st.success("Atualizado!"); time.sleep(1); st.rerun()
        else: st.error("Erro ao salvar.")

@st.dialog("🚨 Excluir Carteira")
def dialog_excluir_carteira(id_cart, nome_cli):
    st.warning(f"Tem certeza que deseja excluir a carteira de **{nome_cli}**?")
    st.caption("O histórico financeiro será perdido permanentemente.")
    c1, c2 = st.columns(2)
    if c1.button("✅ Sim, Excluir", type="primary", use_container_width=True):
        if excluir_carteira_cliente(id_cart):
            st.success("Excluído."); time.sleep(1); st.rerun()
    if c2.button("❌ Cancelar", use_container_width=True): st.rerun()

@st.dialog("✏️ Editar Transação")
def dialog_editar_transacao(transacao, id_carteira):
    st.write(f"Editando Transação")
    n_tipo = st.selectbox("Tipo", ["CREDITO", "DEBITO"], index=0 if transacao['tipo']=="CREDITO" else 1)
    n_valor = st.number_input("Valor", value=float(transacao['valor']), step=0.10)
    n_motivo = st.text_input("Motivo", value=transacao['motivo'])
    
    if st.button("Salvar Alterações"):
        if editar_transacao_db(transacao['id'], id_carteira, n_tipo, n_valor, n_motivo):
            st.success("Atualizado!")
            time.sleep(1); st.rerun()
        else:
            st.error("Erro ao atualizar.")

@st.dialog("🗑️ Excluir Transação")
def dialog_excluir_transacao(id_transacao, id_carteira):
    st.warning("Tem certeza? O saldo será ajustado automaticamente.")
    c1, c2 = st.columns(2)
    if c1.button("Sim, Excluir", type="primary"):
        if excluir_transacao_db(id_transacao, id_carteira):
            st.success("Excluído!")
            time.sleep(1); st.rerun()
    if c2.button("Cancelar"):
        st.rerun()

@st.dialog("✏️ Editar Origem")
def dialog_editar_origem(id_reg, nome_atual):
    st.caption(f"Editando: {nome_atual}")
    with st.form("form_edit_origem"):
        novo_nome = st.text_input("Origem", value=nome_atual)
        if st.form_submit_button("💾 Salvar", use_container_width=True):
            if atualizar_origem_consulta(id_reg, novo_nome):
                st.success("Atualizado!"); time.sleep(0.5); st.rerun()
            else: st.error("Erro.")

@st.dialog("✏️ Editar Tipo Consulta")
def dialog_editar_tipo_consulta(id_reg, nome_atual):
    st.caption(f"Editando: {nome_atual}")
    with st.form("form_edit_tipo"):
        novo_nome = st.text_input("Tipo", value=nome_atual)
        if st.form_submit_button("💾 Salvar", use_container_width=True):
            if atualizar_tipo_consulta_fator(id_reg, novo_nome):
                st.success("Atualizado!"); time.sleep(0.5); st.rerun()
            else: st.error("Erro.")

@st.dialog("✏️ Editar Valor")
def dialog_editar_valor_consulta(id_reg, valor_atual):
    st.caption(f"Editando ID: {id_reg}")
    with st.form("form_edit_valor"):
        novo_valor = st.number_input("Valor (R$)", value=float(valor_atual), step=0.01)
        if st.form_submit_button("💾 Salvar", use_container_width=True):
            if atualizar_valor_consulta(id_reg, novo_valor):
                st.success("Atualizado!"); time.sleep(0.5); st.rerun()
            else: st.error("Erro.")

# =============================================================================
# 5. INTERFACE PRINCIPAL
# =============================================================================

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    
    creds = buscar_credenciais()
    if not creds['token']:
        st.warning("⚠️ Token da API não configurado.")
    
    tabs = st.tabs([
        "👥 Clientes (Financeiro)", "🔍 Teste de Consulta", "💰 Saldo API (Global)", 
        "📋 Histórico (Logs)", "⚙️ Parâmetros", "🤖 Chatbot Config", "📂 Consulta em Lote"
    ])

    # --- ABA 1: CLIENTES ---
    with tabs[0]:
        c1, c2 = st.columns([5, 1])
        c1.markdown("#### Gestão de Saldo dos Clientes")
        if c2.button("➕ Novo", key="add_cli_fator"): dialog_novo_cliente_fator()
            
        df_cli = listar_clientes_carteira()
        
        if not df_cli.empty:
            # Cabeçalho Fixo
            st.markdown("""
            <div style="display:flex; font-weight:bold; color:#555; padding:8px; border-bottom:2px solid #ddd; margin-bottom:10px; background-color:#f8f9fa;">
                <div style="flex:3;">Nome / CPF</div>
                <div style="flex:1;">Saldo</div>
                <div style="flex:1;">Custo</div>
                <div style="flex:2; text-align:center;">Ações</div>
            </div>
            """, unsafe_allow_html=True)
            
            for _, row in df_cli.iterrows():
                with st.container():
                    cc1, cc2, cc3, cc4 = st.columns([3, 1, 1, 2])
                    
                    cpf_txt = f" / {row['cpf']}" if row.get('cpf') else ""
                    cc1.write(f"**{row['nome_cliente']}**{cpf_txt}")
                    
                    val = float(row['saldo_atual'])
                    cor = "green" if val > 0 else "red"
                    cc2.markdown(f":{cor}[R$ {val:.2f}]")
                    
                    cc3.write(f"R$ {float(row['custo_por_consulta']):.2f}")
                    
                    with cc4:
                        b1, b2, b3, b4 = st.columns(4)
                        if b1.button("💲", key=f"mov_{row['id']}", help="Movimentar Saldo"):
                            dialog_movimentar(row['id'], row['nome_cliente'])
                        
                        if b2.button("📜", key=f"ext_{row['id']}", help="Ver/Ocultar Extrato"):
                            if st.session_state.get('cli_expandido') == row['id']:
                                st.session_state['cli_expandido'] = None
                            else:
                                st.session_state['cli_expandido'] = row['id']
                                st.session_state['pag_hist'] = 1 
                        
                        if b3.button("✏️", key=f"edt_{row['id']}", help="Editar Custo"):
                            dialog_editar_custo(row['id'], row['nome_cliente'], row['custo_por_consulta'])
                        if b4.button("🗑️", key=f"del_{row['id']}", help="Excluir Carteira"):
                            dialog_excluir_carteira(row['id'], row['nome_cliente'])
                    
                    st.markdown("<hr style='margin: 5px 0; border-color: #eee;'>", unsafe_allow_html=True)

                if st.session_state.get('cli_expandido') == row['id']:
                    with st.container(border=True):
                        st.caption(f"📜 Histórico: {row['nome_cliente']}")
                        
                        fd1, fd2, fd3 = st.columns([2, 2, 4])
                        data_ini = fd1.date_input("Data Inicial", value=date.today() - timedelta(days=30), key=f"ini_{row['id']}")
                        data_fim = fd2.date_input("Data Final", value=date.today(), key=f"fim_{row['id']}")
                        
                        df_ext = buscar_extrato_cliente_filtrado(row['id'], data_ini, data_fim)
                        
                        if not df_ext.empty:
                            items_por_pag = 15
                            total_items = len(df_ext)
                            pag_atual = st.session_state.get('pag_hist', 1)
                            total_pags = (total_items // items_por_pag) + (1 if total_items % items_por_pag > 0 else 0)
                            
                            inicio = (pag_atual - 1) * items_por_pag
                            fim = inicio + items_por_pag
                            df_view = df_ext.iloc[inicio:fim]
                            
                            st.markdown("""
                            <div style="display: flex; font-weight: bold; background-color: #e9ecef; padding: 5px; border-radius: 4px; font-size:0.9em;">
                                <div style="flex: 2;">Data</div>
                                <div style="flex: 3;">Motivo</div>
                                <div style="flex: 1;">Tipo</div>
                                <div style="flex: 1.5;">Valor</div>
                                <div style="flex: 1.5;">Saldo</div>
                                <div style="flex: 1.5; text-align: center;">Ações</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            for _, tr in df_view.iterrows():
                                tc1, tc2, tc3, tc4, tc5, tc6 = st.columns([2, 3, 1, 1.5, 1.5, 1.5])
                                tc1.write(pd.to_datetime(tr['data_transacao']).strftime('%d/%m/%y %H:%M'))
                                tc2.write(tr['motivo'])
                                
                                cor_t = "green" if tr['tipo'] == 'CREDITO' else "red"
                                tc3.markdown(f":{cor_t}[{tr['tipo']}]")
                                tc4.write(f"R$ {float(tr['valor']):.2f}")
                                tc5.write(f"R$ {float(tr['saldo_novo']):.2f}")
                                
                                with tc6:
                                    bc1, bc2 = st.columns(2)
                                    if bc1.button("✏️", key=f"e_tr_{tr['id']}", help="Editar"):
                                        dialog_editar_transacao(tr, row['id'])
                                    if bc2.button("❌", key=f"d_tr_{tr['id']}", help="Excluir"):
                                        dialog_excluir_transacao(tr['id'], row['id'])
                                st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
                            
                            pc1, pc2, pc3 = st.columns([1, 3, 1])
                            if pc1.button("⬅️ Anterior", key=f"prev_{row['id']}"):
                                if st.session_state['pag_hist'] > 1:
                                    st.session_state['pag_hist'] -= 1
                                    st.rerun()
                            pc2.markdown(f"<div style='text-align:center;'>Página {pag_atual} de {total_pags}</div>", unsafe_allow_html=True)
                            if pc3.button("Próxima ➡️", key=f"next_{row['id']}"):
                                if st.session_state['pag_hist'] < total_pags:
                                    st.session_state['pag_hist'] += 1
                                    st.rerun()
                        else: st.warning("Nenhum registro encontrado no período selecionado.")
        else: st.info("Nenhum cliente configurado.")

    # --- ABA 2: TESTE MANUAL ---
    with tabs[1]:
        st.markdown("#### 1.1 Ambiente de Teste Manual")
        c1, c2 = st.columns([3, 1])
        cpf_input = c1.text_input("CPF para Consulta")
        if c2.button("🔍 Consultar", type="primary"):
            if cpf_input:
                with st.spinner("Consultando..."):
                    res = realizar_consulta_cpf(cpf_input)
                    if res['sucesso']:
                        st.success("Sucesso!")
                        st.json(res['dados'])
                    else: st.error(f"Erro: {res['msg']}")

    # --- ABA 3: SALDO API ---
    with tabs[2]: 
        st.markdown("#### 2.1 Controle de Saldo API (Fator)")
        if st.button("🔄 Atualizar Saldo API"):
            ok, val = consultar_saldo_api()
            if ok: st.metric("Saldo Atual", f"R$ {val:.2f}")
            else: st.error(f"Erro: {val}")
        conn = get_conn()
        if conn:
            df_saldo = pd.read_sql("SELECT data_consulta, valor_saldo FROM conexoes.fatorconferi_registro_de_saldo ORDER BY id DESC LIMIT 10", conn)
            st.dataframe(df_saldo, use_container_width=True)
            conn.close()

    # --- ABA 4: HISTÓRICO ---
    with tabs[3]: 
        st.markdown("#### 5.1 Histórico de Consultas")
        conn = get_conn()
        if conn:
            # Busca colunas originais do banco
            query_hist = """
                SELECT id, data_hora, tipo_consulta, cpf_consultado, id_usuario, nome_usuario, 
                       valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, 
                       tipo_cobranca, id_grupo_cliente, id_grupo_empresas, id_empresa
                FROM conexoes.fatorconferi_registo_consulta 
                ORDER BY id DESC LIMIT 50
            """
            try:
                df_logs = pd.read_sql(query_hist, conn)
                
                if not df_logs.empty:
                    # 1.1 Formatação da Data (dd/mm/yyyy hh:mm:ss)
                    df_logs['data_hora'] = pd.to_datetime(df_logs['data_hora']).dt.strftime('%d/%m/%Y %H:%M:%S')
                    
                    # 2.1 Formatação CPF/CNPJ com pontuação
                    df_logs['cpf_consultado'] = df_logs['cpf_consultado'].apply(formatar_cpf_cnpj_visual)
                    
                    # 4.2 Formatação Valor Pago em Decimal BR (0,00)
                    df_logs['valor_pago'] = df_logs['valor_pago'].fillna(0.0).apply(lambda x: f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

                    # RENOMEAÇÃO DE COLUNAS (Regras Visuais)
                    df_logs.rename(columns={
                        'data_hora': 'Data Consulta',       # 1.2
                        'cpf_consultado': 'CPF/CNPJ',       # 2.2
                        'id_usuario': 'ID Usuário',         # 3.1
                        'nome_usuario': 'Nome Usuário',     # 4.1
                        'valor_pago': 'Valor Pago',         # 4.1
                        'caminho_json': 'Caminho JSON',     # 5.1
                        'status_api': 'Status API',         # 6.1
                        'link_arquivo_consulta': 'Link Arquivo Consulta', # 7.1
                        'tipo_consulta': 'Tipo Consulta',
                        'origem_consulta': 'Origem',
                        'tipo_cobranca': 'Cobrança'
                    }, inplace=True)

                    # 7.1 Link Clicável e Exibição Final
                    st.dataframe(
                        df_logs, 
                        use_container_width=True,
                        column_config={
                            "Link Arquivo Consulta": st.column_config.LinkColumn(
                                "Link Arquivo Consulta",
                                help="Clique para baixar o arquivo",
                                display_text="📥 Abrir"
                            )
                        }
                    )
                else:
                    st.info("Nenhum histórico encontrado.")

            except Exception as e:
                st.error(f"Erro ao carregar histórico: {e}")
            finally:
                conn.close()

    # --- ABA 5: PARÂMETROS ---
    with tabs[4]: 
        
        # 1. ORIGEM CONSULTA
        with st.expander("📍 Origem da Consulta", expanded=True):
            with st.container(border=True):
                st.caption("Novo Item")
                c_in, c_bt = st.columns([5, 1])
                n_orig = c_in.text_input("Origem", key="in_orig", label_visibility="collapsed", placeholder="Ex: API, Web...")
                if c_bt.button("➕", key="add_orig", use_container_width=True):
                    if n_orig: salvar_origem_consulta(n_orig); st.rerun()
            
            st.divider()
            df_orig = listar_origem_consulta()
            if not df_orig.empty:
                for _, r in df_orig.iterrows():
                    ca1, ca2, ca3 = st.columns([8, 1, 1]) 
                    ca1.markdown(f"**{r['id']}** | {r['origem']}")
                    if ca2.button("✏️", key=f"ed_orig_{r['id']}"): dialog_editar_origem(r['id'], r['origem'])
                    if ca3.button("🗑️", key=f"del_orig_{r['id']}"): excluir_origem_consulta(r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

        # 2. TIPO CONSULTA FATOR
        with st.expander("🔍 Tipo Consulta Fator", expanded=True):
            with st.container(border=True):
                st.caption("Novo Item")
                c_in, c_bt = st.columns([5, 1])
                n_tipo = c_in.text_input("Tipo", key="in_tipo", label_visibility="collapsed", placeholder="Ex: Simples, Completa...")
                if c_bt.button("➕", key="add_tipo", use_container_width=True):
                    if n_tipo: salvar_tipo_consulta_fator(n_tipo); st.rerun()
            
            st.divider()
            df_tipo = listar_tipo_consulta_fator()
            if not df_tipo.empty:
                for _, r in df_tipo.iterrows():
                    ca1, ca2, ca3 = st.columns([8, 1, 1])
                    ca1.markdown(f"**{r['id']}** | {r['tipo']}")
                    if ca2.button("✏️", key=f"ed_tipo_{r['id']}"): dialog_editar_tipo_consulta(r['id'], r['tipo'])
                    if ca3.button("🗑️", key=f"del_tipo_{r['id']}"): excluir_tipo_consulta_fator(r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

        # 3. VALOR DA CONSULTA (NOVO)
        with st.expander("💲 Valor da Consulta (Custo)", expanded=True):
            with st.container(border=True):
                st.caption("Novo Valor Base")
                c_in, c_bt = st.columns([5, 1])
                n_valor = c_in.number_input("Valor (R$)", key="in_valor_cons", step=0.01, label_visibility="collapsed")
                if c_bt.button("➕", key="add_valor_cons", use_container_width=True):
                    if n_valor >= 0: salvar_valor_consulta(n_valor); st.rerun()
            
            st.divider()
            df_val = listar_valor_consulta()
            if not df_val.empty:
                for _, r in df_val.iterrows():
                    ca1, ca2, ca3 = st.columns([8, 1, 1])
                    val_fmt = f"R$ {float(r['valor_da_consulta']):.2f}"
                    dt_fmt = r['data_atualizacao'].strftime('%d/%m/%Y %H:%M') if r['data_atualizacao'] else "-"
                    
                    ca1.markdown(f"**{val_fmt}** | Atualizado em: {dt_fmt}")
                    if ca2.button("✏️", key=f"ed_val_{r['id']}"): dialog_editar_valor_consulta(r['id'], r['valor_da_consulta'])
                    if ca3.button("🗑️", key=f"del_val_{r['id']}"): excluir_valor_consulta(r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Nenhum valor configurado.")

    with tabs[5]: st.info("Chatbot em desenvolvimento.")
    with tabs[6]: st.info("Lote em desenvolvimento.")