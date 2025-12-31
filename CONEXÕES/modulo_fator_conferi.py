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

# --- FUNÇÕES AUXILIARES (API & XML) ---

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
        # A API retorna texto puro ou XML simples para saldo? 
        # Baseado no doc, parece ser texto direto ou XML. Vamos assumir que retorna um número no corpo.
        valor_texto = response.text.strip()
        
        # Tenta limpar caracteres não numéricos se vier XML
        if '<' in valor_texto:
            root = ET.fromstring(valor_texto)
            valor_texto = root.text # Ajustar conforme retorno real
            
        saldo = float(valor_texto.replace(',', '.')) if valor_texto else 0.0
        
        # Salva no histórico
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
        # O App script diz que o retorno é ISO-8859-1
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
            
        # Registra no Banco
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
            # Valor fixo por enquanto, depois pode vir da tabela de parametros
            custo = 0.50 
            cur.execute(sql, (tipo, cpf_limpo, id_user, usuario, custo, caminho_completo))
            conn.commit(); conn.close()
            
        return {"sucesso": True, "dados": dados_parsed}
        
    except Exception as e:
        return {"sucesso": False, "msg": str(e)}

# --- INTERFACE DO MÓDULO ---

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    
    # Busca credenciais para mostrar status
    creds = buscar_credenciais()
    if not creds['token']:
        st.warning("⚠️ Token da API não configurado. Vá em 'Conexões' e configure uma entrada com nome 'FATOR CONFERI'.")
    
    tabs = st.tabs([
        "🔍 Teste de Consulta", 
        "💰 Saldo & Limites", 
        "📋 Histórico (Logs)", 
        "⚙️ Parâmetros", 
        "🤖 Chatbot Config", 
        "📂 Consulta em Lote"
    ])

    # 1. ABA TESTE DE CONSULTA
    with tabs[0]:
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
                    
                    # Exibição Visual (Card)
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

    # 2. ABA SALDO
    with tabs[1]:
        st.markdown("#### 2.1 Controle de Saldo")
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

    # 3. ABA HISTÓRICO LOGS
    with tabs[2]:
        st.markdown("#### 5.1 Histórico de Consultas")
        conn = get_conn()
        if conn:
            query = """
                SELECT id, data_hora, tipo_consulta, cpf_consultado, nome_usuario, valor_pago, status_api, caminho_json 
                FROM conexoes.fatorconferi_registo_consulta 
                ORDER BY id DESC LIMIT 50
            """
            df_logs = pd.read_sql(query, conn)
            
            # Exibe tabela
            st.dataframe(df_logs.drop(columns=['caminho_json']), use_container_width=True)
            
            # Recuperar JSON
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

    # 4. ABA PARÂMETROS
    with tabs[3]:
        st.markdown("#### 6. Parâmetros do Módulo")
        conn = get_conn()
        if conn:
            df_params = pd.read_sql("SELECT id, nome_parametro, valor_parametro, observacao, status FROM conexoes.fatorconferi_parametros ORDER BY id", conn)
            
            for index, row in df_params.iterrows():
                c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
                c1.text(row['nome_parametro'])
                # Tooltip no mouse over (help)
                valor = c2.text_input("Valor", value=row['valor_parametro'], key=f"p_val_{row['id']}", help=row['observacao'])
                
                status = c3.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if row['status']=='ATIVO' else 1, key=f"p_st_{row['id']}")
                
                if c4.button("Salvar", key=f"btn_p_{row['id']}"):
                    cur = conn.cursor()
                    cur.execute("UPDATE conexoes.fatorconferi_parametros SET valor_parametro=%s, status=%s WHERE id=%s", (valor, status, row['id']))
                    conn.commit()
                    st.toast("Parâmetro atualizado!")
                    time.sleep(1)
                    st.rerun()
            conn.close()

    # 5. ABA CHATBOT (Layout)
    with tabs[4]:
        st.markdown("#### 8. Configuração Chatbot (Layout)")
        st.info("Integração com W-API para comandos via WhatsApp.")
        
        c_cmd1, c_cmd2 = st.columns(2)
        c_cmd1.text_input("Comando Consulta Simples", value="#CPF:")
        c_cmd2.text_input("Comando Consulta Completa", value="#CPF/COMPLETO:")
        
        st.text_area("Mensagem de Resposta (Template)", value="✅ CONSULTA REALIZADA\nNome: {nome}\nMãe: {mae}\n...", height=150)
        st.button("Salvar Configuração Bot")

    # 6. ABA LOTE
    with tabs[5]:
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
                    # Simulação de delay para não bloquear a API
                    res = realizar_consulta_cpf(cpf)
                    status = "SUCESSO" if res['sucesso'] else "ERRO"
                    nome = res.get('dados', {}).get('nome', '-') if res['sucesso'] else '-'
                    
                    resultados_lote.append({"CPF": cpf, "Status": status, "Nome": nome, "Msg": res.get('msg', '')})
                    progresso.progress((i + 1) / len(cpfs))
                
                st.success("Processamento concluído!")
                df_res_lote = pd.DataFrame(resultados_lote)
                st.dataframe(df_res_lote)