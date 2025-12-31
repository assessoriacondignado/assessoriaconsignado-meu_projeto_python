import streamlit as st
import pandas as pd
import psycopg2
import requests
import json
import os
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
    """Busca a chave da API salva no modulo_conexoes (tabela conexoes.relacao)"""
    conn = get_conn()
    cred = {"url": "https://fator.confere.link/api/", "token": ""}
    if conn:
        try:
            cur = conn.cursor()
            # Busca pela conexão com nome 'FATOR CONFERI' ou similar
            cur.execute("SELECT key_conexao FROM conexoes.relacao WHERE nome_conexao ILIKE '%FATOR%' LIMIT 1")
            res = cur.fetchone()
            if res: cred["token"] = res[0]
        except: pass
        finally: conn.close()
    return cred

def parse_xml_to_dict(xml_string):
    """Converte o retorno XML da Fator para Dicionário Python"""
    try:
        # Tratamento básico para encoding incorreto vindo da API antiga
        xml_string = xml_string.replace('ISO-8859-1', 'UTF-8') 
        root = ET.fromstring(xml_string)
        
        dados = {}
        
        # Exemplo de extração baseado no App Script
        cad = root.find('cadastrais')
        if cad is not None:
            dados['nome'] = cad.findtext('nome')
            dados['cpf'] = cad.findtext('cpf')
            dados['nascimento'] = cad.findtext('nascto')
            dados['mae'] = cad.findtext('nome_mae')
            dados['situacao'] = cad.findtext('situacao_receita')
        
        # Extração de Telefones
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
        
        # Tenta limpar caracteres não numéricos se vier XML
        if '<' in valor_texto:
            try:
                root = ET.fromstring(valor_texto)
                valor_texto = root.text 
            except: pass
            
        saldo = float(valor_texto.replace(',', '.')) if valor_texto else 0.0
        
        # Salva no histórico global
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
        
        # Salva JSON
        nome_arquivo = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_limpo}.json"
        caminho_completo = os.path.join(PASTA_JSON, nome_arquivo)
        with open(caminho_completo, 'w', encoding='utf-8') as f:
            json.dump(dados_parsed, f, ensure_ascii=False, indent=4)
            
        # Registra no Banco (Log Geral)
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
            custo = 0.50 # Valor padrão, idealmente viria da config
            cur.execute(sql, (tipo, cpf_limpo, id_user, usuario, custo, caminho_completo))
            conn.commit(); conn.close()
            
        return {"sucesso": True, "dados": dados_parsed}
        
    except Exception as e:
        return {"sucesso": False, "msg": str(e)}

# =============================================================================
# 2. FUNÇÕES DE GESTÃO FINANCEIRA (CLIENTES)
# =============================================================================

def listar_clientes_carteira():
    conn = get_conn()
    if conn:
        try:
            query = """
                SELECT id, nome_cliente, custo_por_consulta, saldo_atual, status 
                FROM conexoes.fator_cliente_carteira 
                ORDER BY nome_cliente
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
    """
    Registra crédito ou débito e atualiza o saldo do cliente.
    """
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            
            # 1. Busca saldo atual
            cur.execute("SELECT saldo_atual FROM conexoes.fator_cliente_carteira WHERE id = %s", (id_carteira,))
            res = cur.fetchone()
            if not res: return False, "Carteira não encontrada"
            saldo_ant = float(res[0])
            
            valor = float(valor)
            if tipo == 'DEBITO':
                novo_saldo = saldo_ant - valor
            else: # CREDITO
                novo_saldo = saldo_ant + valor
            
            # 2. Atualiza Saldo
            cur.execute("UPDATE conexoes.fator_cliente_carteira SET saldo_atual = %s WHERE id = %s", (novo_saldo, id_carteira))
            
            # 3. Registra Extrato
            cur.execute("""
                INSERT INTO conexoes.fator_cliente_transacoes 
                (id_carteira, tipo, valor, saldo_anterior, saldo_novo, motivo, usuario_responsavel)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (id_carteira, tipo, valor, saldo_ant, novo_saldo, motivo, usuario_resp))
            
            conn.commit(); conn.close()
            return True, "Sucesso"
        except Exception as e:
            conn.close()
            return False, str(e)
    return False, "Erro Conexão"

def buscar_extrato_cliente(id_carteira):
    conn = get_conn()
    if conn:
        try:
            query = """
                SELECT data_transacao, tipo, valor, saldo_novo, motivo, usuario_responsavel 
                FROM conexoes.fator_cliente_transacoes 
                WHERE id_carteira = %s 
                ORDER BY id DESC LIMIT 50
            """
            df = pd.read_sql(query, conn, params=(id_carteira,))
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

# =============================================================================
# 3. DIALOGS E COMPONENTES VISUAIS
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
        else:
            st.error(f"Erro: {msg}")

@st.dialog("➕ Novo Cliente Fator")
def dialog_novo_cliente_fator():
    conn = get_conn()
    # Busca clientes do admin que AINDA NÃO têm carteira
    query = """
        SELECT id, nome FROM admin.clientes 
        WHERE id NOT IN (SELECT id_cliente_admin FROM conexoes.fator_cliente_carteira)
        ORDER BY nome
    """
    df_adm = pd.read_sql(query, conn)
    conn.close()
    
    if df_adm.empty:
        st.info("Todos os clientes já possuem carteira.")
        return

    sel_idx = st.selectbox("Selecione o Cliente", range(len(df_adm)), format_func=lambda x: df_adm.iloc[x]['nome'])
    custo = st.number_input("Custo por Consulta (R$)", value=0.50, step=0.01)
    
    if st.button("Criar Carteira"):
        cli = df_adm.iloc[sel_idx]
        if cadastrar_carteira_cliente(int(cli['id']), cli['nome'], custo):
            st.success("Carteira criada!"); st.rerun()

@st.dialog("📜 Extrato")
def dialog_extrato(id_cart):
    df = buscar_extrato_cliente(id_cart)
    if not df.empty:
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        st.info("Sem movimentações.")

# =============================================================================
# 4. INTERFACE PRINCIPAL
# =============================================================================

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    
    # Valida credenciais
    creds = buscar_credenciais()
    if not creds['token']:
        st.warning("⚠️ Token da API não configurado. Vá em 'Conexões' e configure uma entrada com nome 'FATOR CONFERI'.")
    
    tabs = st.tabs([
        "👥 Clientes (Financeiro)", 
        "🔍 Teste de Consulta", 
        "💰 Saldo API (Global)", 
        "📋 Histórico (Logs)", 
        "⚙️ Parâmetros", 
        "🤖 Chatbot Config", 
        "📂 Consulta em Lote"
    ])

    # --- ABA 1: CLIENTES ---
    with tabs[0]:
        c1, c2 = st.columns([5, 1])
        c1.markdown("#### Gestão de Saldo dos Clientes")
        if c2.button("➕ Novo", key="add_cli_fator"):
            dialog_novo_cliente_fator()
            
        df_cli = listar_clientes_carteira()
        
        if not df_cli.empty:
            st.markdown("""
            <div style="display:flex; font-weight:bold; color:#555; padding:5px; border-bottom:1px solid #ddd; margin-bottom:5px;">
                <div style="flex:3;">Nome</div>
                <div style="flex:1;">Saldo</div>
                <div style="flex:1;">Custo</div>
                <div style="flex:1.5; text-align:right;">Ações</div>
            </div>
            """, unsafe_allow_html=True)
            
            for _, row in df_cli.iterrows():
                with st.container():
                    cc1, cc2, cc3, cc4 = st.columns([3, 1, 1, 1.5])
                    cc1.write(f"**{row['nome_cliente']}**")
                    
                    val = float(row['saldo_atual'])
                    cor = "green" if val > 0 else "red"
                    cc2.markdown(f":{cor}[R$ {val:.2f}]")
                    
                    cc3.write(f"R$ {row['custo_por_consulta']}")
                    
                    with cc4:
                        b1, b2, b3 = st.columns([1,1,1])
                        if b1.button("💲", key=f"mov_{row['id']}", help="Movimentar Saldo"):
                            dialog_movimentar(row['id'], row['nome_cliente'])
                        if b2.button("📜", key=f"ext_{row['id']}", help="Ver Extrato"):
                            dialog_extrato(row['id'])
                        if b3.button("✏️", key=f"edt_{row['id']}", help="Editar (Em breve)"):
                            pass # Implementar edição se necessário
                    st.divider()
        else:
            st.info("Nenhum cliente configurado.")

    # --- ABA 2: TESTE DE CONSULTA ---
    with tabs[1]:
        st.markdown("#### 1.1 Ambiente de Teste Manual")
        c1, c2 = st.columns([3, 1])
        cpf_input = c1.text_input("CPF para Consulta")
        btn_cons = c2.button("🔍 Consultar Agora", type="primary", use_container_width=True)
        
        if btn_cons and cpf_input:
            with st.spinner("Consultando API Fator..."):
                res = realizar_consulta_cpf(cpf_input)
                
                if res['sucesso']:
                    dados = res['dados']
                    st.success("Consulta Realizada!")
                    with st.container(border=True):
                        st.markdown(f"**Nome:** {dados.get('nome')}")
                        st.markdown(f"**Mãe:** {dados.get('mae')}")
                        st.markdown(f"**Nascimento:** {dados.get('nascimento')}")
                        st.divider()
                        st.markdown("**Telefones Encontrados:**")
                        if dados.get('telefones'):
                            for t in dados['telefones']:
                                st.code(f"{t['numero']} (WhatsApp: {t['whatsapp']})")
                        else:
                            st.info("Sem telefones.")
                    with st.expander("Ver JSON Completo"):
                        st.json(dados)
                else:
                    st.error(f"Erro: {res['msg']}")
                    if 'raw' in res:
                        with st.expander("Ver Resposta Bruta"):
                            st.code(res['raw'])

    # --- ABA 3: SALDO GLOBAL ---
    with tabs[2]:
        st.markdown("#### 2.1 Controle de Saldo API (Fator)")
        if st.button("🔄 Atualizar Saldo API"):
            ok, val = consultar_saldo_api()
            if ok:
                st.metric("Saldo Atual", f"R$ {val:.2f}")
                if val < 5.0: st.error("⚠️ Saldo Baixo!")
            else:
                st.error(f"Erro ao verificar saldo: {val}")
        
        st.divider()
        st.markdown("##### Histórico de Verificações")
        conn = get_conn()
        if conn:
            df_saldo = pd.read_sql("SELECT data_consulta, valor_saldo, observacao FROM conexoes.fatorconferi_registro_de_saldo ORDER BY id DESC LIMIT 20", conn)
            st.dataframe(df_saldo, use_container_width=True)
            conn.close()

    # --- ABA 4: HISTÓRICO LOGS ---
    with tabs[3]:
        st.markdown("#### 5.1 Histórico de Consultas")
        conn = get_conn()
        if conn:
            query = """
                SELECT id, data_hora, tipo_consulta, cpf_consultado, nome_usuario, valor_pago, status_api, caminho_json 
                FROM conexoes.fatorconferi_registo_consulta 
                ORDER BY id DESC LIMIT 50
            """
            df_logs = pd.read_sql(query, conn)
            st.dataframe(df_logs.drop(columns=['caminho_json']), use_container_width=True)
            
            st.markdown("---")
            col_id = st.selectbox("Selecione ID para ver o JSON", df_logs['id'].tolist())
            if col_id:
                row = df_logs[df_logs['id'] == col_id].iloc[0]
                path = row['caminho_json']
                if path and os.path.exists(path):
                    with open(path, 'r', encoding='utf-8') as f:
                        st.json(json.load(f))
                else:
                    st.warning("Arquivo JSON não encontrado no servidor.")
            conn.close()

    # --- ABA 5: PARÂMETROS ---
    with tabs[4]:
        st.markdown("#### 6. Parâmetros do Módulo")
        conn = get_conn()
        if conn:
            df_params = pd.read_sql("SELECT id, nome_parametro, valor_parametro, observacao, status FROM conexoes.fatorconferi_parametros ORDER BY id", conn)
            for index, row in df_params.iterrows():
                c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
                c1.text(row['nome_parametro'])
                valor = c2.text_input("Valor", value=row['valor_parametro'], key=f"p_val_{row['id']}", help=row['observacao'])
                status = c3.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if row['status']=='ATIVO' else 1, key=f"p_st_{row['id']}")
                if c4.button("Salvar", key=f"btn_p_{row['id']}"):
                    cur = conn.cursor()
                    cur.execute("UPDATE conexoes.fatorconferi_parametros SET valor_parametro=%s, status=%s WHERE id=%s", (valor, status, row['id']))
                    conn.commit()
                    st.toast("Parâmetro atualizado!")
                    time.sleep(1); st.rerun()
            conn.close()

    # --- ABA 6: CHATBOT ---
    with tabs[5]:
        st.markdown("#### 8. Configuração Chatbot (Layout)")
        st.info("Integração com W-API para comandos via WhatsApp.")
        c_cmd1, c_cmd2 = st.columns(2)
        c_cmd1.text_input("Comando Consulta Simples", value="#CPF:")
        c_cmd2.text_input("Comando Consulta Completa", value="#CPF/COMPLETO:")
        st.text_area("Mensagem de Resposta (Template)", value="✅ CONSULTA REALIZADA\nNome: {nome}\nMãe: {mae}\n...", height=150)
        st.button("Salvar Configuração Bot")

    # --- ABA 7: LOTE ---
    with tabs[6]:
        st.markdown("#### 9. Consulta em Lote")
        st.caption("Cole uma lista de CPFs (um por linha) para processamento em massa.")
        txt_cpfs = st.text_area("Lista de CPFs", height=200)
        if st.button("🚀 Processar Lote"):
            if not txt_cpfs:
                st.warning("Lista vazia.")
            else:
                cpfs = [line.strip() for line in txt_cpfs.split('\n') if len(line.strip()) >= 11]
                st.info(f"Iniciando processamento de {len(cpfs)} CPFs...")
                progresso = st.progress(0)
                resultados_lote = []
                for i, cpf in enumerate(cpfs):
                    res = realizar_consulta_cpf(cpf)
                    status = "SUCESSO" if res['sucesso'] else "ERRO"
                    nome = res.get('dados', {}).get('nome', '-') if res['sucesso'] else '-'
                    resultados_lote.append({"CPF": cpf, "Status": status, "Nome": nome, "Msg": res.get('msg', '')})
                    progresso.progress((i + 1) / len(cpfs))
                st.success("Processamento concluído!")
                df_res_lote = pd.DataFrame(resultados_lote)
                st.dataframe(df_res_lote)