import streamlit as st
import pandas as pd
import psycopg2
import requests
import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime
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
# 1. FUNÇÕES AUXILIARES (API, XML, CREDENCIAIS)
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
            sql = """
                INSERT INTO conexoes.fatorconferi_registo_consulta 
                (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api)
                VALUES (%s, %s, %s, %s, %s, %s, 'SUCESSO')
            """
            custo = 0.50 
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
            # Query ajustada para trazer CPF do cliente admin
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
    return False, "Erro Conexão"

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

def buscar_extrato_cliente(id_carteira):
    conn = get_conn()
    if conn:
        try:
            # Inclui o ID da transação para poder editar/excluir
            query = """
                SELECT id, data_transacao, tipo, valor, saldo_novo, motivo, usuario_responsavel 
                FROM conexoes.fator_cliente_transacoes 
                WHERE id_carteira = %s 
                ORDER BY id DESC LIMIT 50
            """
            df = pd.read_sql(query, conn, params=(id_carteira,))
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

# Novas funções para editar/excluir transações do extrato
def editar_transacao_db(id_transacao, id_carteira, novo_tipo, novo_valor, novo_motivo):
    # ATENÇÃO: Editar transação antiga altera o saldo atual? 
    # Simplificação: Apenas atualiza o registro visual. Recalcular saldo retroativo é complexo.
    # Vamos assumir que ajusta o registro e faz um ajuste de saldo compensatório se necessário, 
    # ou apenas edita o texto/valor visualmente.
    # Para ser seguro e simples: Editamos o registro e atualizamos o saldo da carteira com a DIFERENÇA.
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            # Pega valor antigo para calcular diferença
            cur.execute("SELECT tipo, valor FROM conexoes.fator_cliente_transacoes WHERE id=%s", (id_transacao,))
            res = cur.fetchone()
            if not res: return False
            tipo_ant, valor_ant = res
            valor_ant = float(valor_ant)
            
            # Calcula impacto no saldo
            fator_ant = -1 if tipo_ant == 'DEBITO' else 1
            fator_novo = -1 if novo_tipo == 'DEBITO' else 1
            
            diff = (float(novo_valor) * fator_novo) - (valor_ant * fator_ant)
            
            # Atualiza transação
            cur.execute("""
                UPDATE conexoes.fator_cliente_transacoes 
                SET tipo=%s, valor=%s, motivo=%s 
                WHERE id=%s
            """, (novo_tipo, novo_valor, novo_motivo, id_transacao))
            
            # Atualiza saldo da carteira com a diferença
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
            
            # Reverte o saldo (se era debito, devolve; se era credito, tira)
            fator = 1 if tipo == 'DEBITO' else -1
            ajuste = float(valor) * fator
            
            cur.execute("DELETE FROM conexoes.fator_cliente_transacoes WHERE id=%s", (id_transacao,))
            cur.execute("UPDATE conexoes.fator_cliente_carteira SET saldo_atual = saldo_atual + %s WHERE id=%s", (ajuste, id_carteira))
            
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

# =============================================================================
# 3. DIALOGS (POP-UPS)
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

# --- NOVOS DIALOGS DE EDIÇÃO/EXCLUSÃO DO EXTRATO ---

@st.dialog("✏️ Editar Transação")
def dialog_editar_transacao(transacao, id_carteira):
    st.write(f"Editando ID: {transacao['id']}")
    n_tipo = st.selectbox("Tipo", ["CREDITO", "DEBITO"], index=0 if transacao['tipo']=="CREDITO" else 1)
    n_valor = st.number_input("Valor", value=float(transacao['valor']), step=0.10)
    n_motivo = st.text_input("Motivo", value=transacao['motivo'])
    
    if st.button("Salvar Alterações"):
        if editar_transacao_db(transacao['id'], id_carteira, n_tipo, n_valor, n_motivo):
            st.success("Atualizado!"); time.sleep(1); st.rerun()
        else:
            st.error("Erro ao atualizar.")

@st.dialog("🗑️ Excluir Transação")
def dialog_excluir_transacao(id_transacao, id_carteira):
    st.warning("Tem certeza? O saldo será ajustado automaticamente.")
    if st.button("Sim, Excluir"):
        if excluir_transacao_db(id_transacao, id_carteira):
            st.success("Excluído!"); time.sleep(1); st.rerun()

@st.dialog("📜 Extrato Financeiro", width="large")
def dialog_extrato(id_cart, nome_cli):
    st.markdown(f"### Extrato: {nome_cli}")
    df = buscar_extrato_cliente(id_cart)
    
    if not df.empty:
        # Cabeçalho Fixo Visual
        st.markdown("""
        <div style="display: flex; font-weight: bold; background-color: #f0f0f0; padding: 8px; border-radius: 5px;">
            <div style="flex: 2;">Data</div>
            <div style="flex: 2;">Motivo</div>
            <div style="flex: 1;">Tipo</div>
            <div style="flex: 1;">Valor</div>
            <div style="flex: 1;">Saldo</div>
            <div style="flex: 1; text-align: center;">Ações</div>
        </div>
        """, unsafe_allow_html=True)
        
        for _, row in df.iterrows():
            with st.container():
                c1, c2, c3, c4, c5, c6 = st.columns([2, 2, 1, 1, 1, 1])
                
                dt = pd.to_datetime(row['data_transacao']).strftime('%d/%m/%y %H:%M')
                c1.write(dt)
                c2.write(row['motivo'])
                
                cor_tipo = "green" if row['tipo'] == 'CREDITO' else "red"
                c3.markdown(f":{cor_tipo}[{row['tipo']}]")
                
                c4.write(f"R$ {float(row['valor']):.2f}")
                c5.write(f"R$ {float(row['saldo_novo']):.2f}")
                
                # Botões de Ação na Linha
                with c6:
                    bc1, bc2 = st.columns(2)
                    if bc1.button("✏️", key=f"edt_tr_{row['id']}", help="Editar"):
                        dialog_editar_transacao(row, id_cart)
                    if bc2.button("❌", key=f"del_tr_{row['id']}", help="Excluir"):
                        dialog_excluir_transacao(row['id'], id_cart)
                
                st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
    else:
        st.info("Sem movimentações recentes.")

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

# =============================================================================
# 4. INTERFACE PRINCIPAL
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
                    # Layout Colunas
                    cc1, cc2, cc3, cc4 = st.columns([3, 1, 1, 2])
                    
                    # Nome + CPF (ajustado)
                    cpf_txt = f" / {row['cpf']}" if row['cpf'] else ""
                    cc1.write(f"**{row['nome_cliente']}**{cpf_txt}")
                    
                    val = float(row['saldo_atual'])
                    cor = "green" if val > 0 else "red"
                    cc2.markdown(f":{cor}[R$ {val:.2f}]")
                    
                    cc3.write(f"R$ {float(row['custo_por_consulta']):.2f}")
                    
                    # Botões
                    with cc4:
                        b1, b2, b3, b4 = st.columns(4)
                        if b1.button("💲", key=f"mov_{row['id']}", help="Movimentar Saldo"):
                            dialog_movimentar(row['id'], row['nome_cliente'])
                        if b2.button("📜", key=f"ext_{row['id']}", help="Ver Extrato"):
                            dialog_extrato(row['id'], row['nome_cliente'])
                        if b3.button("✏️", key=f"edt_{row['id']}", help="Editar Custo"):
                            dialog_editar_custo(row['id'], row['nome_cliente'], row['custo_por_consulta'])
                        if b4.button("🗑️", key=f"del_{row['id']}", help="Excluir Carteira"):
                            dialog_excluir_carteira(row['id'], row['nome_cliente'])
                    
                    st.markdown("<hr style='margin: 5px 0; border-color: #eee;'>", unsafe_allow_html=True)
        else: st.info("Nenhum cliente configurado.")

    # ... (MANTÉM AS OUTRAS ABAS IGUAIS AO CÓDIGO ANTERIOR) ...
    # --- ABA 2: TESTE DE CONSULTA ---
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

    with tabs[2]: # Saldo Global
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

    with tabs[3]: # Logs
        st.markdown("#### 5.1 Histórico de Consultas")
        conn = get_conn()
        if conn:
            df_logs = pd.read_sql("SELECT id, data_hora, cpf_consultado, nome_usuario, status_api FROM conexoes.fatorconferi_registo_consulta ORDER BY id DESC LIMIT 50", conn)
            st.dataframe(df_logs, use_container_width=True)
            conn.close()

    with tabs[4]: st.info("Parâmetros em desenvolvimento.")
    with tabs[5]: st.info("Chatbot em desenvolvimento.")
    with tabs[6]: st.info("Lote em desenvolvimento.")