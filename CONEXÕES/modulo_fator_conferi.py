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

BASE_DIR = os.path.dirname(os.path.abspath(__file__)); PASTA_JSON = os.path.join(BASE_DIR, "JSON")
if not os.path.exists(PASTA_JSON): os.makedirs(PASTA_JSON, exist_ok=True)

def get_conn():
    try: return psycopg2.connect(host=conexao.host, port=conexao.port, database=conexao.database, user=conexao.user, password=conexao.password)
    except: return None

def buscar_credenciais():
    conn = get_conn(); cred = {"url": "https://fator.confere.link/api/", "token": ""}
    if conn:
        try: cur = conn.cursor(); cur.execute("SELECT key_conexao FROM conexoes.relacao WHERE nome_conexao ILIKE '%FATOR%' LIMIT 1"); res = cur.fetchone(); cred["token"] = res[0] if res else ""
        except: pass
        finally: conn.close()
    return cred

def obter_origem_padronizada(nome_origem):
    conn = get_conn(); origem_final = nome_origem
    if conn:
        try: cur = conn.cursor(); cur.execute("SELECT origem FROM conexoes.fatorconferi_origem_consulta_fator WHERE origem = %s", (nome_origem,)); res = cur.fetchone(); origem_final = res[0] if res else nome_origem
        except: pass
        finally: conn.close()
    return origem_final

def processar_debito_automatico(origem_da_consulta, dados_consulta):
    id_usuario_logado = st.session_state.get('usuario_id')
    if not id_usuario_logado: return False, "Usuário não logado."
    conn = get_conn()
    if not conn: return False, "Erro conexão DB."
    try:
        cur = conn.cursor()
        cur.execute("SELECT cpf, nome FROM admin.clientes WHERE id_usuario_vinculo = %s LIMIT 1", (id_usuario_logado,))
        res_pagador = cur.fetchone()
        if not res_pagador: conn.close(); return False, "Usuário sem cliente vinculado."
        cpf_pagador, nome_pagador = res_pagador[0], res_pagador[1]

        cur.execute("SELECT nome_carteira, custo_carteira FROM cliente.cliente_carteira_lista WHERE cpf_cliente = %s AND origem_custo = %s LIMIT 1", (cpf_pagador, origem_da_consulta))
        res_lista = cur.fetchone()
        if not res_lista: conn.close(); return False, f"Cliente não possui a carteira '{origem_da_consulta}' na lista."
        nome_carteira_vinculada, valor_cobranca = res_lista[0], float(res_lista[1])

        cur.execute("SELECT nome_tabela_transacoes FROM cliente.carteiras_config WHERE nome_carteira = %s AND status = 'ATIVO' LIMIT 1", (nome_carteira_vinculada,))
        res_config = cur.fetchone()
        if not res_config: conn.close(); return False, f"Tabela não encontrada para '{nome_carteira_vinculada}'."
        tabela_sql = res_config[0]

        cur.execute(f"SELECT saldo_novo FROM {tabela_sql} WHERE cpf_cliente = %s ORDER BY id DESC LIMIT 1", (cpf_pagador,))
        res_saldo = cur.fetchone(); saldo_anterior = float(res_saldo[0]) if res_saldo else 0.0
        novo_saldo = saldo_anterior - valor_cobranca
        cpf_consultado = dados_consulta.get('cpf', 'Desconhecido')
        motivo = f"Consulta Fator ({origem_da_consulta}): {cpf_consultado}"
        
        sql_insert = f"INSERT INTO {tabela_sql} (cpf_cliente, nome_cliente, motivo, origem_lancamento, tipo_lancamento, valor, saldo_anterior, saldo_novo, data_transacao) VALUES (%s, %s, %s, %s, 'DEBITO', %s, %s, %s, NOW())"
        cur.execute(sql_insert, (cpf_pagador, nome_pagador, motivo, origem_da_consulta, valor_cobranca, saldo_anterior, novo_saldo))
        conn.commit(); conn.close(); return True, f"Débito de R$ {valor_cobranca:.2f} na tabela {tabela_sql}."
    except Exception as e:
        if conn: conn.close()
        return False, f"Erro financeiro: {str(e)}"

def realizar_consulta_cpf(cpf, origem="Teste Manual", forcar_nova=False):
    cpf_padrao = ''.join(filter(str.isdigit, str(cpf))).zfill(11)
    conn = get_conn()
    if not conn: return {"sucesso": False, "msg": "Erro DB."}
    try:
        cur = conn.cursor()
        if not forcar_nova:
            cur.execute("SELECT caminho_json FROM conexoes.fatorconferi_registo_consulta WHERE cpf_consultado = %s AND status_api = 'SUCESSO' ORDER BY id DESC LIMIT 1", (cpf_padrao,))
            res = cur.fetchone()
            if res and res[0] and os.path.exists(res[0]):
                with open(res[0], 'r', encoding='utf-8') as f: dados = json.load(f)
                if dados.get('nome'):
                    usr = st.session_state.get('usuario_nome', 'Sistema'); id_usr = st.session_state.get('usuario_id', 0)
                    cur.execute("INSERT INTO conexoes.fatorconferi_registo_consulta (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora) VALUES (%s, %s, %s, %s, 0, %s, 'SUCESSO', %s, %s, 'CACHE', NOW())", ("CPF SIMPLES", cpf_padrao, id_usr, usr, res[0], res[0], origem))
                    conn.commit(); conn.close()
                    return {"sucesso": True, "dados": dados, "msg": "Cache recuperado."}
        
        cred = buscar_credenciais()
        if not cred['token']: conn.close(); return {"sucesso": False, "msg": "Token ausente."}
        resp = requests.get(f"{cred['url']}?acao=CONS_CPF&TK={cred['token']}&DADO={cpf_padrao}", timeout=30)
        
        # Simulação de Parser (Substitua pela sua função parse_xml_to_dict real)
        dados = {"nome": "NOME SIMULADO", "cpf": cpf_padrao} # Placeholder
        
        nome_arq = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{cpf_padrao}.json"
        path = os.path.join(PASTA_JSON, nome_arq)
        with open(path, 'w', encoding='utf-8') as f: json.dump(dados, f, indent=4)
        
        ok_fin, txt_fin = processar_debito_automatico(origem, dados)
        msg_fin = f" | {txt_fin}" if ok_fin else f" | ⚠️ {txt_fin}"
        
        usr = st.session_state.get('usuario_nome', 'Sistema'); id_usr = st.session_state.get('usuario_id', 0)
        cur.execute("INSERT INTO conexoes.fatorconferi_registo_consulta (tipo_consulta, cpf_consultado, id_usuario, nome_usuario, valor_pago, caminho_json, status_api, link_arquivo_consulta, origem_consulta, tipo_cobranca, data_hora) VALUES (%s, %s, %s, %s, %s, %s, 'SUCESSO', %s, %s, 'PAGO', NOW())", ("CPF SIMPLES", cpf_padrao, id_usr, usr, 0.50, path, path, origem))
        conn.commit(); conn.close()
        return {"sucesso": True, "dados": dados, "msg": "Consulta realizada." + msg_fin}
    except Exception as e:
        if conn: conn.close()
        return {"sucesso": False, "msg": str(e)}

def app_fator_conferi():
    st.markdown("### ⚡ Painel Fator Conferi")
    tabs = st.tabs(["Consulta", "Histórico"])
    with tabs[0]:
        c1, c2, c3 = st.columns([3, 1.5, 1.5])
        cpf = c1.text_input("CPF"); forcar = c2.checkbox("Ignorar Cache")
        if c3.button("Consultar"):
            origem = obter_origem_padronizada("WEB USUÁRIO")
            res = realizar_consulta_cpf(cpf, origem, forcar)
            if res['sucesso']: st.success(res['msg']); st.json(res['dados'])
            else: st.error(res['msg'])
    with tabs[1]:
        conn = get_conn(); 
        if conn: st.dataframe(pd.read_sql("SELECT * FROM conexoes.fatorconferi_registo_consulta ORDER BY id DESC LIMIT 50", conn)); conn.close()