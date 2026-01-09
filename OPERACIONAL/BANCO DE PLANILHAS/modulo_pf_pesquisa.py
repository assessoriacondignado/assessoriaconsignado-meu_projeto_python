import streamlit as st
import pandas as pd
from datetime import date, datetime
import re
import time
import json
import modulo_pf_cadastro as pf_core
import modulo_pf_config_exportacao as pf_export

# --- NOVA FUNÇÃO PRINCIPAL: GERENCIADOR DE TELAS ---
def app_gestao_pesquisa():
    """
    Função principal que gerencia a navegação interna do módulo de Gestão.
    Alterna entre Lista, Edição e Visualização sem depender do menu principal.
    """
    if 'pf_view' not in st.session_state:
        st.session_state['pf_view'] = 'lista'

    if st.session_state['pf_view'] == 'lista':
        interface_pesquisa_ampla()
    
    elif st.session_state['pf_view'] == 'visualizar':
        interface_visualizar_cliente()

    elif st.session_state['pf_view'] == 'editar':
        interface_cadastro_pf()

# --- FUNÇÕES DE NAVEGAÇÃO ---
def ir_para_visualizar(cpf):
    st.session_state['pf_view'] = 'visualizar'
    st.session_state['pf_cpf_selecionado'] = str(cpf)

def ir_para_editar(cpf):
    st.session_state['pf_view'] = 'editar'
    st.session_state['pf_cpf_selecionado'] = str(cpf)
    st.session_state['form_loaded'] = False

# --- CONFIGURAÇÃO DE CADASTRO ---
CONFIG_CADASTRO = {
    "Dados Pessoais": [
        {"label": "Nome Completo", "key": "nome", "tabela": "geral", "tipo": "texto", "obrigatorio": True},
        {"label": "CPF", "key": "cpf", "tabela": "geral", "tipo": "cpf", "obrigatorio": True},
        {"label": "RG", "key": "rg", "tabela": "geral", "tipo": "texto"},
        {"label": "Data Nascimento", "key": "data_nascimento", "tabela": "geral", "tipo": "data"},
        {"label": "Nome da Mãe", "key": "nome_mae", "tabela": "geral", "tipo": "texto"},
        {"label": "Nome do Pai", "key": "nome_pai", "tabela": "geral", "tipo": "texto"},
        {"label": "UF do RG", "key": "uf_rg", "tabela": "geral", "tipo": "texto"},
        {"label": "Dados Exp. RG", "key": "dados_exp_rg", "tabela": "geral", "tipo": "texto"},
        {"label": "PIS", "key": "pis", "tabela": "geral", "tipo": "texto"},
        {"label": "CNH", "key": "cnh", "tabela": "geral", "tipo": "texto"},
        {"label": "Série CTPS", "key": "serie_ctps", "tabela": "geral", "tipo": "texto"},
        {"label": "Nome Procurador", "key": "nome_procurador", "tabela": "geral", "tipo": "texto"},
        {"label": "CPF Procurador", "key": "cpf_procurador", "tabela": "geral", "tipo": "cpf"}, 
    ],
    "Contatos": [
        {"label": "Telefone", "key": "numero", "tabela": "telefones", "tipo": "telefone", "multiplo": True},
        {"label": "E-mail", "key": "email", "tabela": "emails", "tipo": "email", "multiplo": True},
    ],
    "Endereços": [
        {"label": "CEP", "key": "cep", "tabela": "enderecos", "tipo": "cep", "multiplo": True, "agrupado": True}, 
        {"label": "Logradouro", "key": "rua", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "Bairro", "key": "bairro", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "Cidade", "key": "cidade", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "UF", "key": "uf", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
    ]
}

# --- FUNÇÕES AUXILIARES DE INTERFACE ---
def inserir_dado_staging(campo_config, valor, extras=None):
    tabela = campo_config['tabela']
    chave = campo_config['key']
    if tabela not in st.session_state['dados_staging']:
        if campo_config.get('multiplo'): st.session_state['dados_staging'][tabela] = []
        else: st.session_state['dados_staging'][tabela] = {}

    erro = None
    valor_final = valor
    if campo_config['tipo'] == 'cpf':
        val, erro = pf_core.validar_formatar_cpf(valor)
        if not erro: valor_final = pf_core.limpar_normalizar_cpf(val)
    elif campo_config['tipo'] == 'telefone':
        val, erro = pf_core.validar_formatar_telefone(valor)
        if not erro: valor_final = val
    elif campo_config['tipo'] == 'email':
        if not pf_core.validar_email(valor): erro = "E-mail inválido."
    
    if erro: st.error(erro); return
    if not valor_final and campo_config.get('obrigatorio'): st.toast(f"❌ O campo {campo_config['label']} é obrigatório."); return

    if campo_config.get('multiplo'):
        novo_item = {chave: valor_final}
        if extras: novo_item.update(extras)
        if isinstance(valor, dict): st.session_state['dados_staging'][tabela].append(valor)
        else: st.session_state['dados_staging'][tabela].append(novo_item)
        st.toast(f"✅ {campo_config['label']} adicionado!")
    else:
        st.session_state['dados_staging'][tabela][chave] = valor_final
        st.toast(f"✅ {campo_config['label']} atualizado!")

# --- TELAS: CADASTRO E VISUALIZAÇÃO ---
def interface_cadastro_pf():
    pf_core.init_db_structures()
    is_edit = st.session_state['pf_view'] == 'editar'
    
    cpf_formatado_titulo = ""
    if is_edit:
        raw_cpf = st.session_state.get('pf_cpf_selecionado', '')
        cpf_formatado_titulo = pf_core.formatar_cpf_visual(raw_cpf)
    
    titulo = f"✏️ Editar: {cpf_formatado_titulo}" if is_edit else "➕ Novo Cadastro"
    st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'form_loaded': False}))
    st.markdown(f"### {titulo}")

    if 'dados_staging' not in st.session_state:
        st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': [], 'dados_clt': []}

    if is_edit and not st.session_state.get('form_loaded'):
        dados_db = pf_core.carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
        st.session_state['dados_staging'] = dados_db
        st.session_state['form_loaded'] = True
    elif not is_edit and not st.session_state.get('form_loaded'):
        st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': [], 'dados_clt': []}
        st.session_state['form_loaded'] = True

    c_builder, c_preview = st.columns([3, 2])

    with c_builder:
        st.markdown("#### 🏗️ Inserir Dados")
        with st.expander("Dados Pessoais", expanded=True):
            if not is_edit: st.info("ℹ️ Para cadastrar dados complementares, salve o Nome e CPF primeiro.")
            for campo in CONFIG_CADASTRO["Dados Pessoais"]:
                if not is_edit and campo['key'] not in ['nome', 'cpf']: continue
                if is_edit and campo['key'] == 'cpf':
                    c_lab, c_inp = st.columns([1.2, 3.5])
                    c_lab.markdown(f"**{campo['label']}:**")
                    val_atual = st.session_state['dados_staging']['geral'].get('cpf', '')
                    c_inp.text_input("CPF Display", value=pf_core.formatar_cpf_visual(val_atual), disabled=True, label_visibility="collapsed")
                    continue
                
                c_lbl, c_inp, c_btn = st.columns([1.2, 2.5, 1.0])
                c_lbl.markdown(f"**{campo['label']}:**")
                with c_inp:
                    if campo['tipo'] == 'data':
                        val_pre = st.session_state['dados_staging']['geral'].get(campo['key'])
                        if isinstance(val_pre, str):
                            try: val_pre = datetime.strptime(val_pre, '%Y-%m-%d').date()
                            except: val_pre = None
                        val = st.date_input("Data", value=val_pre, min_value=date(1900, 1, 1), max_value=date(2050, 12, 31), format="DD/MM/YYYY", key=f"in_{campo['key']}", label_visibility="collapsed")
                    else:
                        val_pre = st.session_state['dados_staging']['geral'].get(campo['key'], '')
                        val = st.text_input("Texto", value=val_pre, label_visibility="collapsed", key=f"in_{campo['key']}")
                with c_btn:
                    if st.button("Inserir", key=f"btn_{campo['key']}", type="primary", use_container_width=True): 
                        inserir_dado_staging(campo, val)
        
        with st.expander("Contatos"):
            if not is_edit: st.info("🚫 Disponível apenas no modo 'Editar'.")
            else:
                c_tel_in, c_tel_btn = st.columns([4, 2])
                with c_tel_in: tel = st.text_input("Número", key="in_tel_num", placeholder="Ex: (82)999025155")
                with c_tel_btn:
                    st.write(""); st.write("") 
                    if st.button("Inserir Telefone", type="primary", use_container_width=True):
                        cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'numero'][0]
                        inserir_dado_staging(cfg, tel, None)
                st.divider()
                st.markdown("##### 📧 Cadastro de E-mail")
                c_mail_in, c_mail_btn = st.columns([5, 2])
                with c_mail_in: mail = st.text_input("E-mail", key="in_mail", placeholder="exemplo@email.com")
                with c_mail_btn:
                    st.write(""); st.write("")
                    if st.button("Inserir E-mail", type="primary", use_container_width=True):
                        if pf_core.validar_email(mail):
                            cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'email'][0]
                            inserir_dado_staging(cfg, mail)
                        else: st.error("⚠️ E-mail inválido.")

        with st.expander("Endereço"):
            if not is_edit: st.info("🚫 Disponível apenas no modo 'Editar'.")
            else:
                st.markdown("##### 📍 Cadastro de Endereço")
                c_cep, c_rua = st.columns([1.5, 3.5])
                with c_cep: cep = st.text_input("CEP", key="in_end_cep")
                with c_rua: rua = st.text_input("Logradouro", key="in_end_rua")
                c_bai, c_cid, c_uf = st.columns([2, 2, 1])
                with c_bai: bairro = st.text_input("Bairro", key="in_end_bairro")
                with c_cid: cidade = st.text_input("Cidade", key="in_end_cid")
                with c_uf: uf_digitada = st.text_input("UF", key="in_end_uf", max_chars=2).upper()
                if st.button("Inserir Endereço", type="primary", use_container_width=True):
                    cep_num, cep_vis, erro_cep = pf_core.validar_formatar_cep(cep)
                    if erro_cep: st.error(erro_cep)
                    elif not rua: st.warning("Logradouro obrigatório.")
                    else:
                        obj_end = {'cep': cep_num, 'rua': rua, 'bairro': bairro, 'cidade': cidade, 'uf': uf_digitada}
                        if 'enderecos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['enderecos'] = []
                        st.session_state['dados_staging']['enderecos'].append(obj_end)
                        st.toast("✅ Endereço adicionado!")

        with st.expander("Emprego e Renda (Vínculo)"):
            c_conv, c_matr, c_btn_emp = st.columns([3, 3, 2])
            with c_conv: conv = st.text_input("Convênio", key="in_emp_conv")
            with c_matr: matr = st.text_input("Matrícula", key="in_emp_matr")
            with c_btn_emp:
                st.write(""); st.write("")
                if st.button("Inserir Vínculo", type="primary", use_container_width=True):
                    if conv and matr:
                        obj_emp = {'convenio': conv.upper(), 'matricula': matr, 'dados_extras': ''}
                        if 'empregos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['empregos'] = []
                        st.session_state['dados_staging']['empregos'].append(obj_emp)
                        st.toast("✅ Vínculo adicionado!")
                        st.rerun()
                    else: st.warning("Campos obrigatórios.")

        with st.expander("Contratos / Planilhas"):
            lista_empregos = st.session_state['dados_staging'].get('empregos', [])
            if not lista_empregos: st.info("Insira um vínculo primeiro.")
            else:
                opcoes_matr = [f"{e['matricula']} - {e['convenio']}" for e in lista_empregos]
                sel_vinculo = st.selectbox("Vincular à Matrícula:", opcoes_matr, key="sel_vinc_contr")
                idx_vinc = opcoes_matr.index(sel_vinculo)
                dados_vinc = lista_empregos[idx_vinc]
                tabelas_destino = pf_core.listar_tabelas_por_convenio(dados_vinc['convenio'])
                
                for nome_tabela, tipo_tabela in tabelas_destino:
                    st.markdown("---")
                    st.markdown(f"###### 📝 {tipo_tabela or 'Dados'} ({nome_tabela})")
                    sufixo = f"{nome_tabela}_{idx_vinc}"
                    colunas_banco = pf_core.get_colunas_tabela(nome_tabela)
                    inputs_gerados = {}
                    cols_ui = st.columns(2)
                    for idx_col, (col_nome, col_tipo) in enumerate(colunas_banco):
                        if col_nome in ['id', 'matricula_ref', 'matricula', 'convenio', 'tipo_planilha', 'importacao_id', 'data_criacao', 'data_atualizacao', 'cpf_ref']: continue
                        with cols_ui[idx_col % 2]:
                            key_input = f"inp_{col_nome}_{sufixo}"
                            if 'date' in col_tipo.lower(): val = st.date_input(col_nome, value=None, key=key_input)
                            else: val = st.text_input(col_nome, key=key_input)
                            inputs_gerados[col_nome] = val
                    
                    if st.button(f"Inserir em {tipo_tabela}", key=f"btn_save_{sufixo}", type="primary"):
                        inputs_gerados.update({'matricula_ref': dados_vinc['matricula'], 'convenio': dados_vinc['convenio'], 'origem_tabela': nome_tabela, 'tipo_origem': tipo_tabela})
                        if 'contratos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['contratos'] = []
                        st.session_state['dados_staging']['contratos'].append(inputs_gerados)
                        st.toast(f"✅ {tipo_tabela} adicionado!")

    with c_preview:
        st.markdown("### 📋 Resumo")
        geral = st.session_state['dados_staging'].get('geral', {})
        if geral:
            cols = st.columns(2)
            for i, (k, v) in enumerate(geral.items()):
                if v: cols[i%2].text_input(k.upper(), value=str(v), disabled=True, key=f"v_g_{k}")
        
        # Listagens simplificadas para preview
        for k in ['telefones', 'emails', 'enderecos', 'empregos', 'contratos']:
            itens = st.session_state['dados_staging'].get(k, [])
            if itens:
                st.warning(f"{k.upper()} ({len(itens)})")
                for i, item in enumerate(itens):
                    c1, c2 = st.columns([5, 1])
                    c1.write(str(item.values())[:50] + "...")
                    if c2.button("🗑️", key=f"rm_{k}_{i}"):
                        st.session_state['dados_staging'][k].pop(i); st.rerun()

        st.divider()
        if st.button("💾 CONFIRMAR E SALVAR", type="primary", use_container_width=True):
            staging = st.session_state['dados_staging']
            if not staging['geral'].get('nome') or not staging['geral'].get('cpf'): st.error("Nome e CPF são obrigatórios.")
            else:
                modo_salvar = "editar" if is_edit else "novo"
                cpf_orig = pf_core.limpar_normalizar_cpf(st.session_state.get('pf_cpf_selecionado')) if is_edit else None
                sucesso, msg = pf_core.salvar_pf(staging['geral'], pd.DataFrame(staging['telefones']), pd.DataFrame(staging['emails']), pd.DataFrame(staging['enderecos']), pd.DataFrame(staging['empregos']), pd.DataFrame(staging['contratos']), modo_salvar, cpf_orig)
                if sucesso:
                    st.success(msg); time.sleep(1.5)
                    st.session_state['pf_view'] = 'lista'; st.session_state['form_loaded'] = False; st.rerun()
                else: st.error(msg)

def interface_visualizar_cliente():
    cpf_cliente = st.session_state.get('pf_cpf_selecionado')
    if st.button("⬅️ Voltar"): st.session_state['pf_view'] = 'lista'; st.rerun()
    if not cpf_cliente: st.error("Erro: Nenhum cliente selecionado."); return

    dados = pf_core.carregar_dados_completos(cpf_cliente)
    g = dados.get('geral', {})
    if not g: st.error("Cliente não encontrado."); return
    
    st.markdown(f"### 👤 {g.get('nome')} - {pf_core.formatar_cpf_visual(g.get('cpf'))}")
    t1, t2, t3 = st.tabs(["Cadastro", "Financeiro", "Contatos"])
    with t1:
        st.write(g)
        st.divider()
        st.markdown("##### Vínculos")
        st.dataframe(pd.DataFrame(dados.get('empregos', [])), use_container_width=True)
    with t2:
        for v in dados.get('empregos', []):
            with st.expander(f"Contratos: {v['convenio']} ({v['matricula']})"):
                st.dataframe(pd.DataFrame(v.get('contratos', [])), use_container_width=True)
    with t3:
        st.write("Telefones:"); st.dataframe(pd.DataFrame(dados.get('telefones', [])), use_container_width=True)
        st.write("Endereços:"); st.dataframe(pd.DataFrame(dados.get('enderecos', [])), use_container_width=True)

# --- CONFIGURAÇÕES E SQL (PESQUISA) ---
CAMPOS_CONFIG = {
    "Dados Pessoais": [
        {"label": "Nome", "coluna": "d.nome", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CPF", "coluna": "d.cpf", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Nascimento", "coluna": "d.data_nascimento", "tipo": "data", "tabela": "banco_pf.pf_dados"},
        {"label": "Idade", "coluna": "virtual_idade", "tipo": "numero", "tabela": "banco_pf.pf_dados"},
        {"label": "RG", "coluna": "d.rg", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome da Mãe", "coluna": "d.nome_mae", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "ID Importação", "coluna": "d.importacao_id", "tipo": "texto", "tabela": "banco_pf.pf_dados"}
    ],
    "Contatos": [
        {"label": "DDD", "coluna": "virtual_ddd", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Telefone", "coluna": "tel.numero", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "E-mail", "coluna": "em.email", "tipo": "texto", "tabela": "banco_pf.pf_emails"}
    ],
    "Endereços": [
        {"label": "Cidade", "coluna": "ende.cidade", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "UF", "coluna": "ende.uf", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "CEP", "coluna": "ende.cep", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"}
    ],
    "Profissional": [
        {"label": "Matrícula", "coluna": "emp.matricula", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Convênio", "coluna": "emp.convenio", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"}
    ]
}

def buscar_pf_simples(termo, pagina=1, itens_por_pagina=50):
    """Busca rápida por Nome, CPF ou Telefone"""
    conn = pf_core.get_conn()
    if conn:
        try:
            termo_limpo = pf_core.limpar_normalizar_cpf(termo)
            # Se parecer CPF ou número de telefone (apenas dígitos e > 6 caracteres)
            if termo_limpo and len(termo_limpo) > 6:
                sql_base = "SELECT DISTINCT d.id, d.nome, d.cpf, d.data_nascimento FROM banco_pf.pf_dados d LEFT JOIN banco_pf.pf_telefones t ON d.cpf = t.cpf WHERE d.cpf LIKE %s OR t.numero LIKE %s"
                params = [f"%{termo_limpo}%", f"%{termo_limpo}%"]
            else:
                # Busca por nome
                sql_base = "SELECT DISTINCT d.id, d.nome, d.cpf, d.data_nascimento FROM banco_pf.pf_dados d WHERE d.nome ILIKE %s"
                params = [f"%{termo}%"]
            
            # Contagem
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM ({sql_base}) as sub", tuple(params))
            total = cur.fetchone()[0]
            
            # Dados paginados
            offset = (pagina-1)*itens_por_pagina
            df = pd.read_sql(f"{sql_base} ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}", conn, params=tuple(params))
            conn.close()
            return df, total
        except Exception as e:
            st.error(f"Erro na busca simples: {e}") 
            if conn: conn.close()
    return pd.DataFrame(), 0

def executar_pesquisa_ampla(regras_ativas, pagina=1, itens_por_pagina=50):
    conn = pf_core.get_conn()
    if conn:
        try:
            sql_select = "SELECT DISTINCT d.id, d.nome, d.cpf, d.data_nascimento "
            sql_from = "FROM banco_pf.pf_dados d "
            joins_map = {
                'banco_pf.pf_telefones': "JOIN banco_pf.pf_telefones tel ON d.cpf = tel.cpf",
                'banco_pf.pf_emails': "JOIN banco_pf.pf_emails em ON d.cpf = em.cpf",
                'banco_pf.pf_enderecos': "JOIN banco_pf.pf_enderecos ende ON d.cpf = ende.cpf",
                'banco_pf.pf_emprego_renda': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf",
                'banco_pf.pf_contratos': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf JOIN banco_pf.pf_contratos ctr ON emp.matricula = ctr.matricula_ref",
                'banco_pf.pf_matricula_dados_clt': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf LEFT JOIN banco_pf.pf_matricula_dados_clt clt ON emp.matricula = clt.matricula"
            }
            active_joins = []; conditions = []; params = []

            for regra in regras_ativas:
                tabela = regra['tabela']; coluna = regra['coluna']; op = regra['operador']
                val_raw = regra['valor']; tipo = regra['tipo']
                if tabela in joins_map and joins_map[tabela] not in active_joins: active_joins.append(joins_map[tabela])
                col_sql = f"{coluna}"
                if coluna == 'virtual_idade': col_sql = "EXTRACT(YEAR FROM AGE(d.data_nascimento))"
                if coluna == 'virtual_ddd': col_sql = "SUBSTRING(tel.numero, 1, 2)"

                if op == "∅" or op == "Vazio": 
                    conditions.append(f"({col_sql} IS NULL OR {col_sql}::TEXT = '')"); continue
                if val_raw is None or str(val_raw).strip() == "": continue
                
                conds_or = []
                # Adaptação simplificada dos operadores
                if op == "=": conds_or.append(f"{col_sql} = %s"); params.append(val_raw)
                elif "Contém" in str(op) or "ILIKE" in str(op): conds_or.append(f"{col_sql} ILIKE %s"); params.append(f"%{val_raw}%")
                elif ">" in str(op): conds_or.append(f"{col_sql} > %s"); params.append(val_raw)
                elif "<" in str(op): conds_or.append(f"{col_sql} < %s"); params.append(val_raw)
                else: conds_or.append(f"{col_sql} ILIKE %s"); params.append(f"%{val_raw}%") # Default
                
                if conds_or: conditions.append(f"({' OR '.join(conds_or)})")

            full_joins = " ".join(active_joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            # Count
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(DISTINCT d.id) {sql_from} {full_joins} {sql_where}", tuple(params))
            total = cur.fetchone()[0]
            
            # Select
            offset = (pagina - 1) * itens_por_pagina
            query = f"{sql_select} {sql_from} {full_joins} {sql_where} ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}"
            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close()
            return df.fillna(""), total
        except Exception as e: 
            st.error(f"Erro SQL: {e}"); return pd.DataFrame(), 0
    return pd.DataFrame(), 0

# --- INTERFACE PRINCIPAL DE PESQUISA ---

def interface_pesquisa_ampla():
    c_voltar, c_tipos, c_limpar, c_spacer = st.columns([1, 1.5, 1.5, 5])
    if c_voltar.button("⬅️ Voltar"): st.session_state['pf_view'] = 'lista'; st.rerun()
    if c_limpar.button("🗑️ Limpar Filtros"): 
        st.session_state['regras_pesquisa'] = []; st.session_state['executar_busca'] = False; st.session_state['termo_rapido_cache'] = ""; st.rerun()
    
    st.divider()
    
    # 1. BUSCA RÁPIDA (Solução do problema)
    termo = st.text_input("🚀 Busca Rápida (Nome, CPF ou Telefone)", key="busca_rapida", placeholder="Digite e pressione Enter...")
    
    df_res = pd.DataFrame()
    total = 0
    modo_exibicao = False

    if termo:
        st.session_state['executar_busca'] = False # Desativa busca avançada
        df_res, total = buscar_pf_simples(termo, pagina=st.session_state.get('pf_pagina_atual', 1))
        modo_exibicao = True
        st.info(f"Mostrando resultados para: **{termo}**")
    
    else:
        # 2. BUSCA AVANÇADA (Layout original)
        c_menu, c_regras = st.columns([4, 2]) 
        with c_menu:
            st.markdown("### 🗂️ Filtros Avançados")
            termo_filtro = st.text_input("🔍 Buscar critério...", key="filtro_campos_ampla")
            for grupo, campos in CAMPOS_CONFIG.items():
                campos_filtrados = [c for c in campos if termo_filtro.lower() in c['label'].lower()]
                if campos_filtrados:
                    with st.expander(grupo, expanded=bool(termo_filtro)):
                        cols = st.columns(3)
                        for i, campo in enumerate(campos_filtrados):
                            if cols[i%3].button(f"➕ {campo['label']}", key=f"add_{campo['coluna']}"):
                                st.session_state['regras_pesquisa'].append({'label': campo['label'], 'coluna': campo['coluna'], 'tabela': campo['tabela'], 'tipo': campo['tipo'], 'operador': 'Contém', 'valor': ''})
                                st.rerun()

        with c_regras:
            st.markdown("### 🎯 Regras Ativas")
            if not st.session_state.get('regras_pesquisa'): st.caption("Nenhum filtro selecionado.")
            regras_rem = []
            for i, r in enumerate(st.session_state.get('regras_pesquisa', [])):
                with st.container(border=True):
                    st.caption(f"**{r['label']}**")
                    c_op, c_val, c_del = st.columns([2, 3, 1])
                    novo_op = c_op.selectbox("Op", ["=", "Contém", "Vazio"], key=f"op_{i}", label_visibility="collapsed")
                    novo_val = c_val.text_input("Valor", value=r['valor'], key=f"val_{i}", label_visibility="collapsed")
                    st.session_state['regras_pesquisa'][i].update({'operador': novo_op, 'valor': novo_val})
                    if c_del.button("X", key=f"del_{i}"): regras_rem.append(i)
            
            if regras_rem:
                for i in sorted(regras_rem, reverse=True): st.session_state['regras_pesquisa'].pop(i)
                st.rerun()
            
            st.divider()
            if st.button("🔎 FILTRAR AGORA", type="primary", use_container_width=True): st.session_state['executar_busca'] = True

        if st.session_state.get('executar_busca'):
            df_res, total = executar_pesquisa_ampla(st.session_state['regras_pesquisa'], st.session_state.get('pf_pagina_atual', 1))
            modo_exibicao = True

    # 3. EXIBIÇÃO DE RESULTADOS (Comum)
    if modo_exibicao:
        st.divider()
        st.write(f"**Resultados Encontrados:** {total}")
        if not df_res.empty:
            st.markdown("""<div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex;"><div style="flex: 2;">Ações</div><div style="flex: 1;">ID</div><div style="flex: 2;">CPF</div><div style="flex: 4;">Nome</div></div>""", unsafe_allow_html=True)
            for _, row in df_res.iterrows():
                c1, c2, c3, c4 = st.columns([2, 1, 2, 4])
                with c1:
                    b1, b2, b3 = st.columns(3)
                    b1.button("👁️", key=f"v_{row['id']}", on_click=ir_para_visualizar, args=(row['cpf'],))
                    b2.button("✏️", key=f"e_{row['id']}", on_click=ir_para_editar, args=(row['cpf'],))
                    if b3.button("🗑️", key=f"d_{row['id']}"): pf_core.dialog_excluir_pf(str(row['cpf']), row['nome'])
                c2.write(str(row['id'])); c3.write(pf_core.formatar_cpf_visual(row['cpf'])); c4.write(row['nome'])
                st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
            
            # Paginação
            cp1, cp2, cp3 = st.columns([1, 3, 1])
            if cp1.button("⬅️ Ant.") and st.session_state.get('pf_pagina_atual', 1) > 1:
                st.session_state['pf_pagina_atual'] -= 1; st.rerun()
            if cp3.button("Próx. ➡️"):
                st.session_state['pf_pagina_atual'] = st.session_state.get('pf_pagina_atual', 1) + 1; st.rerun()
        else:
            st.warning("Nenhum registro encontrado para os critérios informados.")