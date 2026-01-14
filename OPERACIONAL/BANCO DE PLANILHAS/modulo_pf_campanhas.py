import streamlit as st
import pandas as pd
import json
import time
import re
from datetime import date
# Importações dos módulos ativos
import modulo_pf_cadastro as pf_core

# =============================================================================
# CONFIGURAÇÃO DE CAMPOS PARA FILTRO (Recriado localmente)
# =============================================================================
CAMPOS_CONFIG = {
    "Dados Pessoais": [
        {"label": "Nome", "coluna": "d.nome", "tabela": "banco_pf.pf_dados", "tipo": "texto"},
        {"label": "CPF", "coluna": "d.cpf", "tabela": "banco_pf.pf_dados", "tipo": "texto"},
        {"label": "Data Nascimento", "coluna": "d.data_nascimento", "tabela": "banco_pf.pf_dados", "tipo": "data"},
        {"label": "Idade (Virtual)", "coluna": "virtual_idade", "tabela": "banco_pf.pf_dados", "tipo": "numero"},
        {"label": "RG", "coluna": "d.rg", "tabela": "banco_pf.pf_dados", "tipo": "texto"},
        {"label": "Nome da Mãe", "coluna": "d.nome_mae", "tabela": "banco_pf.pf_dados", "tipo": "texto"},
        {"label": "Sexo", "coluna": "d.sexo", "tabela": "banco_pf.pf_dados", "tipo": "texto"},
    ],
    "Endereço": [
        {"label": "Cidade", "coluna": "ende.cidade", "tabela": "banco_pf.pf_enderecos", "tipo": "texto"},
        {"label": "UF", "coluna": "ende.uf", "tabela": "banco_pf.pf_enderecos", "tipo": "texto"},
        {"label": "Bairro", "coluna": "ende.bairro", "tabela": "banco_pf.pf_enderecos", "tipo": "texto"},
        {"label": "CEP", "coluna": "ende.cep", "tabela": "banco_pf.pf_enderecos", "tipo": "texto"},
    ],
    "Vínculos (Emprego)": [
        {"label": "Convênio", "coluna": "emp.convenio", "tabela": "banco_pf.pf_emprego_renda", "tipo": "texto"},
        {"label": "Matrícula", "coluna": "emp.matricula", "tabela": "banco_pf.pf_emprego_renda", "tipo": "texto"},
    ],
    "Contatos": [
        {"label": "Telefone (Número)", "coluna": "tel.numero", "tabela": "banco_pf.pf_telefones", "tipo": "texto"},
        {"label": "E-mail", "coluna": "em.email", "tabela": "banco_pf.pf_emails", "tipo": "texto"},
    ]
}

# =============================================================================
# 1. MOTOR DE BUSCA INTERNO
# =============================================================================

def executar_pesquisa_campanha_interna(regras_ativas, pagina=1, itens_por_pagina=50):
    conn = pf_core.get_conn()
    if conn:
        try:
            sql_select = "SELECT DISTINCT d.id, d.nome, d.cpf, d.data_nascimento "
            sql_from = "FROM banco_pf.pf_dados d "
            
            # --- MAPA DE JOINS ---
            joins_map = {
                'banco_pf.pf_telefones': "JOIN banco_pf.pf_telefones tel ON d.cpf = tel.cpf",
                'banco_pf.pf_emails': "JOIN banco_pf.pf_emails em ON d.cpf = em.cpf",
                'banco_pf.pf_enderecos': "JOIN banco_pf.pf_enderecos ende ON d.cpf = ende.cpf",
                'banco_pf.pf_emprego_renda': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf",
                'banco_pf.pf_contratos': "JOIN banco_pf.pf_contratos ctr ON emp.matricula = ctr.matricula",
                'banco_pf.pf_matricula_dados_clt': "LEFT JOIN banco_pf.pf_matricula_dados_clt clt ON emp.matricula = clt.matricula"
            }
            
            tabelas_necessarias = set()
            
            for regra in regras_ativas:
                tabela = regra['tabela']
                if tabela == 'banco_pf.pf_contratos_clt': tabela = 'banco_pf.pf_matricula_dados_clt'
                tabelas_necessarias.add(tabela)

            if 'banco_pf.pf_matricula_dados_clt' in tabelas_necessarias or 'banco_pf.pf_contratos' in tabelas_necessarias:
                tabelas_necessarias.add('banco_pf.pf_emprego_renda')

            active_joins = []
            
            if 'banco_pf.pf_emprego_renda' in tabelas_necessarias:
                active_joins.append(joins_map['banco_pf.pf_emprego_renda'])
            
            for tbl in tabelas_necessarias:
                if tbl == 'banco_pf.pf_emprego_renda': continue 
                if tbl in joins_map:
                    active_joins.append(joins_map[tbl])

            # --- CONSTRUÇÃO DO WHERE ---
            conditions = []
            params = []

            for regra in regras_ativas:
                tabela = regra['tabela']
                if tabela == 'banco_pf.pf_contratos_clt': tabela = 'banco_pf.pf_matricula_dados_clt'
                
                coluna = regra['coluna']
                op = regra['operador']
                val_raw = regra['valor']
                tipo = regra.get('tipo', 'texto')
                
                col_sql = f"{coluna}"
                if coluna == 'virtual_idade':
                    col_sql = "EXTRACT(YEAR FROM AGE(d.data_nascimento))"

                if op == "∅" or op == "Vazio": 
                    conditions.append(f"({col_sql} IS NULL OR {col_sql}::TEXT = '')"); continue
                if val_raw is None or str(val_raw).strip() == "": continue
                
                valores = [v.strip() for v in str(val_raw).split(',') if v.strip()]
                conds_or = []
                for val in valores:
                    if 'cpf' in coluna or 'cnpj' in coluna: val = pf_core.limpar_normalizar_cpf(val)
                    if tipo == 'numero': val = re.sub(r'\D', '', val)

                    if tipo == 'data':
                        if op == "=": conds_or.append(f"{col_sql} = %s"); params.append(val)
                        elif op == "≥" or op == "A Partir": conds_or.append(f"{col_sql} >= %s"); params.append(val)
                        elif op == "≤" or op == "Até": conds_or.append(f"{col_sql} <= %s"); params.append(val)
                        elif op == "≠": conds_or.append(f"{col_sql} <> %s"); params.append(val)
                        continue 

                    if op == "=>" or op == "Começa com": conds_or.append(f"{col_sql} ILIKE %s"); params.append(f"{val}%")
                    elif op == "<=>" or op == "Contém": conds_or.append(f"{col_sql} ILIKE %s"); params.append(f"%{val}%")
                    elif op == "=" or op == "Igual": 
                        if tipo == 'numero': conds_or.append(f"{col_sql} = %s"); params.append(val)
                        else: conds_or.append(f"{col_sql} ILIKE %s"); params.append(val)
                    elif op == "≠" or op == "Diferente": conds_or.append(f"{col_sql} <> %s"); params.append(val)
                    elif op == "<≠>" or op == "Não Contém": conds_or.append(f"{col_sql} NOT ILIKE %s"); params.append(f"%{val}%")
                    elif op in [">", "<", "≥", "≤"]:
                        sym = {">":">", "<":"<", "≥":">=", "≤":"<="}[op]
                        conds_or.append(f"{col_sql} {sym} %s"); params.append(val)
                
                if conds_or: conditions.append(f"({' OR '.join(conds_or)})")

            full_joins = " ".join(active_joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            count_sql = f"SELECT COUNT(DISTINCT d.id) {sql_from} {full_joins} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total = cur.fetchone()[0]
            
            offset = (pagina - 1) * itens_por_pagina
            query = f"{sql_select} {sql_from} {full_joins} {sql_where} ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}"
            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close()
            return df.fillna(""), total
        except Exception as e: 
            st.error(f"Erro SQL Interno: {e}")
            if conn: conn.close()
            return pd.DataFrame(), 0
            
    return pd.DataFrame(), 0

# =============================================================================
# 2. FUNÇÕES DE BANCO DE DADOS (CRUD CAMPANHA)
# =============================================================================

def salvar_campanha(nome, objetivo, status, filtros_lista):
    conn = pf_core.get_conn()
    if conn:
        try:
            filtros_json = json.dumps(filtros_lista, default=str)
            txt_visual = "; ".join([f.get('descricao_visual', '') for f in filtros_lista])
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO banco_pf.pf_campanhas (nome_campanha, objetivo, status, filtros_config, filtros_aplicaveis, data_criacao)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (nome, objetivo, status, filtros_json, txt_visual, date.today()))
            conn.commit(); conn.close()
            return True
        except Exception as e: st.error(f"Erro ao salvar: {e}"); conn.close()
    return False

def atualizar_campanha_db(id_campanha, nome, objetivo, status, filtros_lista):
    conn = pf_core.get_conn()
    if conn:
        try:
            id_campanha = int(id_campanha)
            filtros_json = json.dumps(filtros_lista, default=str)
            txt_visual = "; ".join([f.get('descricao_visual', '') for f in filtros_lista])
            cur = conn.cursor()
            cur.execute("""
                UPDATE banco_pf.pf_campanhas SET nome_campanha=%s, objetivo=%s, status=%s, filtros_config=%s, filtros_aplicaveis=%s WHERE id=%s
            """, (nome, objetivo, status, filtros_json, txt_visual, id_campanha))
            conn.commit(); conn.close()
            return True
        except Exception as e: st.error(f"Erro ao atualizar: {e}"); conn.close()
    return False

def excluir_campanha_db(id_campanha):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM banco_pf.pf_campanhas WHERE id = %s", (int(id_campanha),))
            conn.commit(); conn.close()
            return True
        except Exception as e: st.error(f"Erro ao excluir: {e}"); conn.close()
    return False

def listar_campanhas_ativas():
    conn = pf_core.get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT id, nome_campanha, filtros_config, filtros_aplicaveis, objetivo, data_criacao, status FROM banco_pf.pf_campanhas ORDER BY id DESC", conn)
            conn.close(); return df
        except: conn.close()
    return pd.DataFrame()

def vincular_campanha_aos_clientes(id_campanha, nome_campanha, lista_ids_clientes):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            if not lista_ids_clientes: return 0
            ids_tuple = tuple(int(x) for x in lista_ids_clientes)
            query = f"UPDATE banco_pf.pf_dados SET id_campanha = %s WHERE id IN %s"
            cur.execute(query, (str(id_campanha), ids_tuple))
            afetados = cur.rowcount
            conn.commit(); conn.close()
            return afetados
        except Exception as e: st.error(f"Erro ao vincular: {e}"); conn.close()
    return 0

# =============================================================================
# 3. DIALOGS (POP-UPS)
# =============================================================================

@st.dialog("✏️ Editar Campanha", width="large")
def dialog_editar_campanha(dados_atuais):
    if 'edit_filtros' not in st.session_state:
        try: st.session_state['edit_filtros'] = json.loads(dados_atuais['filtros_config'])
        except: st.session_state['edit_filtros'] = []

    c1, c2 = st.columns([3, 1])
    novo_nome = c1.text_input("Nome", value=dados_atuais['nome_campanha'])
    status_opts = ["ATIVO", "INATIVO"]
    idx_st = status_opts.index(dados_atuais['status']) if dados_atuais['status'] in status_opts else 0
    novo_status = c2.selectbox("Status", status_opts, index=idx_st)
    novo_obj = st.text_area("Objetivo", value=dados_atuais['objetivo'])
    
    st.divider()
    st.markdown("#### 🛠️ Reconfigurar Filtros")
    
    with st.container(border=True):
        st.caption("Adicionar nova regra:")
        opcoes_campos = []
        mapa_campos = {}
        
        # USA CAMPOS_CONFIG LOCAL
        for grupo, lista in CAMPOS_CONFIG.items():
            for item in lista:
                chave = f"{grupo} -> {item['label']}"
                opcoes_campos.append(chave)
                mapa_campos[chave] = item

        ec1, ec2, ec3, ec4 = st.columns([2, 1.5, 2, 1])
        cp_sel = ec1.selectbox("Campo", opcoes_campos, key="ed_cp")
        op_sel = ec2.selectbox("Op.", ["=", ">", "<", "≥", "≤", "≠", "Contém", "Começa com"], key="ed_op")
        val_sel = ec3.text_input("Valor", key="ed_val")
        
        if ec4.button("➕ Add", key="btn_add_edit"):
            if cp_sel in mapa_campos:
                dado = mapa_campos[cp_sel]
                st.session_state['edit_filtros'].append({
                    'label': dado['label'], 'coluna': dado['coluna'], 'tabela': dado['tabela'],
                    'tipo': dado.get('tipo', 'texto'), 'operador': op_sel, 'valor': val_sel,
                    'descricao_visual': f"({dado['tabela']}, {dado['label']}, {op_sel}, {val_sel})"
                })
                st.rerun()

    if st.session_state['edit_filtros']:
        st.write("📋 **Filtros Ativos:**")
        for idx, f in enumerate(st.session_state['edit_filtros']):
            cols = st.columns([0.1, 0.8, 0.1])
            cols[0].write(f"{idx+1}.")
            cols[1].code(f.get('descricao_visual', 'Regra'), language="sql")
            if cols[2].button("🗑️", key=f"del_f_edit_{idx}"):
                st.session_state['edit_filtros'].pop(idx)
                st.rerun()

    st.markdown("---")
    col_salvar, col_fechar = st.columns([2, 1])
    
    if col_salvar.button("💾 SALVAR ALTERAÇÕES", type="primary", use_container_width=True):
        if atualizar_campanha_db(dados_atuais['id'], novo_nome, novo_obj, novo_status, st.session_state['edit_filtros']):
            st.success("Campanha atualizada!")
            if 'edit_filtros' in st.session_state: del st.session_state['edit_filtros']
            if 'id_campanha_em_edicao' in st.session_state: del st.session_state['id_campanha_em_edicao']
            time.sleep(1); st.rerun()

    if col_fechar.button("Fechar"):
        if 'edit_filtros' in st.session_state: del st.session_state['edit_filtros']
        if 'id_campanha_em_edicao' in st.session_state: del st.session_state['id_campanha_em_edicao']
        st.rerun()

@st.dialog("⚠️ Excluir Campanha")
def dialog_excluir_campanha(id_campanha, nome):
    st.error(f"Excluir: **{nome}**?")
    if st.button("🚨 SIM, EXCLUIR", use_container_width=True):
        if excluir_campanha_db(id_campanha):
            st.success("Removido."); time.sleep(1); st.rerun()
    if st.button("Cancelar", use_container_width=True): st.rerun()

# =============================================================================
# 4. INTERFACE PRINCIPAL
# =============================================================================

def app_campanhas(key_sufix="default"):
    st.markdown("## 📢 Gestão de Campanhas e Perfilamento")
    if 'pag_campanha' not in st.session_state: st.session_state['pag_campanha'] = 1

    tab_config, tab_aplicar = st.tabs(["⚙️ Configurar Campanha", "🚀 Executar Campanha"])

    # --- ABA 1: CONFIGURAÇÃO ---
    with tab_config:
        st.markdown("### 📝 Nova Campanha")
        with st.form(f"form_create_campanha_{key_sufix}"):
            c1, c2, c3 = st.columns([3, 1.5, 1.5])
            nome = c1.text_input("Nome da Campanha")
            data_criacao = c2.date_input("Data Criação", value=date.today(), disabled=True)
            status = c3.selectbox("Status", ["ATIVO", "INATIVO"])
            objetivo = st.text_area("Objetivo da Campanha")
            
            st.divider()
            st.markdown("#### 🎯 Configuração de Filtros Padrão")
            
            if 'campanha_filtros_temp' not in st.session_state:
                st.session_state['campanha_filtros_temp'] = []

            opcoes_campos = []
            mapa_campos = {}
            
            # USA CAMPOS_CONFIG LOCAL
            for grupo, lista in CAMPOS_CONFIG.items():
                for item in lista:
                    chave = f"{grupo} -> {item['label']}"
                    opcoes_campos.append(chave)
                    mapa_campos[chave] = item

            rc1, rc2, rc3, rc4 = st.columns([2, 1.5, 2, 1])
            campo_sel = rc1.selectbox("Campo", opcoes_campos, key=f"cp_new_camp_{key_sufix}")
            op_sel = rc2.selectbox("Operador", ["=", ">", "<", "≥", "≤", "≠", "Contém", "Começa com"], key=f"op_new_camp_{key_sufix}")
            valor_sel = rc3.text_input("Valor", key=f"val_new_camp_{key_sufix}")
            
            if rc4.form_submit_button("➕ Incluir"):
                if valor_sel and campo_sel in mapa_campos:
                    dado = mapa_campos[campo_sel]
                    st.session_state['campanha_filtros_temp'].append({
                        'label': dado['label'], 'coluna': dado['coluna'], 'tabela': dado['tabela'],
                        'tipo': dado.get('tipo', 'texto'), 'operador': op_sel, 'valor': valor_sel,
                        'descricao_visual': f"({dado['tabela']}, {dado['label']}, {op_sel}, {valor_sel})"
                    })
                    st.rerun()

            if st.session_state['campanha_filtros_temp']:
                st.write("Filtros:")
                for f in st.session_state['campanha_filtros_temp']:
                    st.code(f['descricao_visual'])
                if st.form_submit_button("Limpar"):
                    st.session_state['campanha_filtros_temp'] = []; st.rerun()

            st.markdown("---")
            if st.form_submit_button("💾 SALVAR CAMPANHA"):
                if nome and st.session_state['campanha_filtros_temp']:
                    if salvar_campanha(nome, objetivo, status, st.session_state['campanha_filtros_temp']):
                        st.success("Criada com sucesso!"); st.session_state['campanha_filtros_temp'] = []; time.sleep(1); st.rerun()
                else: st.warning("Preencha nome e filtros.")

    # --- ABA 2: EXECUTAR ---
    with tab_aplicar:
        st.markdown("### 🚀 Executar Campanha")
        
        df_todas = listar_campanhas_ativas()
        
        if df_todas.empty:
            st.info("Nenhuma campanha cadastrada.")
        else:
            if 'id_campanha_em_edicao' in st.session_state and st.session_state['id_campanha_em_edicao']:
                id_edit = st.session_state['id_campanha_em_edicao']
                camp_edit = df_todas[df_todas['id'] == id_edit]
                if not camp_edit.empty: dialog_editar_campanha(camp_edit.iloc[0])
                else: del st.session_state['id_campanha_em_edicao']; st.rerun()

            sel_camp_fmt = df_todas.apply(lambda x: f"#{x['id']} - {x['nome_campanha']} ({x['status']})", axis=1)
            idx = st.selectbox("Selecione a Campanha", range(len(df_todas)), format_func=lambda x: sel_camp_fmt[x], key=f"sel_camp_main_{key_sufix}")
            
            campanha = df_todas.iloc[idx]
            filtros_db = json.loads(campanha['filtros_config']) if campanha['filtros_config'] else []
            
            with st.container(border=True):
                c_info, c_acts = st.columns([3.5, 1.5])
                with c_info:
                    st.markdown(f"**Campanha:** {campanha['nome_campanha']}")
                    st.caption(f"Objetivo: {campanha['objetivo']}")
                    st.markdown("**Filtros Padrão:**")
                    st.info(campanha['filtros_aplicaveis']) 
                with c_acts:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("✏️ Editar", key=f"btn_ed_{key_sufix}", use_container_width=True):
                        if 'edit_filtros' in st.session_state: del st.session_state['edit_filtros']
                        st.session_state['id_campanha_em_edicao'] = campanha['id']
                        st.rerun()
                    if st.button("🗑️ Excluir", key=f"btn_del_{key_sufix}", type="primary", use_container_width=True):
                        dialog_excluir_campanha(campanha['id'], campanha['nome_campanha'])

            st.markdown("#### 🔎 Filtros Adicionais (Opcional)")
            if 'filtros_extras' not in st.session_state: st.session_state['filtros_extras'] = []
            
            opcoes_campos = []
            mapa_campos = {}
            # USA CAMPOS_CONFIG LOCAL
            for grupo, lista in CAMPOS_CONFIG.items():
                for item in lista:
                    chave = f"{grupo} -> {item['label']}"
                    opcoes_campos.append(chave)
                    mapa_campos[chave] = item

            fe1, fe2, fe3, fe4 = st.columns([2, 1.5, 2, 1])
            ex_campo = fe1.selectbox("Campo Extra", opcoes_campos, key=f"cp_ex_{key_sufix}")
            ex_op = fe2.selectbox("Operador", ["=", ">", "<", "Contém"], key=f"op_ex_{key_sufix}")
            ex_val = fe3.text_input("Valor", key=f"val_ex_{key_sufix}")
            
            if fe4.button("➕ Add", key=f"add_ex_{key_sufix}"):
                if ex_campo in mapa_campos:
                    dado_ex = mapa_campos[ex_campo]
                    st.session_state['filtros_extras'].append({
                        'label': dado_ex['label'], 'coluna': dado_ex['coluna'], 'tabela': dado_ex['tabela'],
                        'tipo': dado_ex.get('tipo', 'texto'), 'operador': ex_op, 'valor': ex_val
                    })
            
            if st.session_state['filtros_extras']:
                st.write("Extras:")
                for fx in st.session_state['filtros_extras']:
                    st.caption(f"{fx['label']} {fx['operador']} {fx['valor']}")
                if st.button("Limpar Extras", key=f"cls_ex_{key_sufix}"): st.session_state['filtros_extras'] = []; st.rerun()

            st.divider()
            
            cd1, cd2, cd3 = st.columns([1.5, 1.5, 3])
            data_ref = cd2.date_input("Data Referência", value=date.today(), min_value=date(1900, 1, 1), max_value=date(2050, 12, 31), format="DD/MM/YYYY", key=f"dt_ref_{key_sufix}")

            if st.button("🔎 VISUALIZAR PÚBLICO ALVO", type="primary", use_container_width=True, key=f"btn_see_target_{key_sufix}"):
                todos_filtros = filtros_db + st.session_state['filtros_extras']
                with st.spinner("Analisando base de dados..."):
                    df_res, total = executar_pesquisa_campanha_interna(todos_filtros, pagina=st.session_state['pag_campanha'], itens_por_pagina=50)
                st.session_state['resultado_campanha_df'] = df_res
                st.session_state['resultado_campanha_total'] = total

            if 'resultado_campanha_df' in st.session_state and st.session_state['resultado_campanha_df'] is not None:
                df_r = st.session_state['resultado_campanha_df']
                tot = st.session_state['resultado_campanha_total']
                
                st.markdown(f"### Resultados: {tot} encontrados")
                csv = df_r.to_csv(index=False, sep=';', encoding='utf-8-sig')
                st.download_button("⬇️ Exportar CSV", data=csv, file_name="campanha_resultado.csv", mime="text/csv", key=f"dl_csv_{key_sufix}")
                
                if not df_r.empty:
                    st.markdown("""<div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex;"><div style="flex: 1;">Ações</div><div style="flex: 1;">ID</div><div style="flex: 2;">CPF</div><div style="flex: 4;">Nome</div></div>""", unsafe_allow_html=True)
                    for _, row in df_r.iterrows():
                        rc1, rc2, rc3, rc4 = st.columns([1, 1, 2, 4])
                        with rc1:
                            if st.button("👁️", key=f"v_camp_{row['id']}_{key_sufix}", help="Visualizar Cliente"):
                                st.session_state.update({'pf_view_ativa': 'visualizar', 'pf_cpf_selecionado': str(row['cpf'])})
                                # Redireciona para o módulo de cadastro
                                pf_core.app_cadastro_unificado()
                                st.rerun()
                        rc2.write(str(row['id']))
                        rc3.write(pf_core.formatar_cpf_visual(row['cpf']))
                        rc4.write(row['nome'])
                        st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
                    
                    cp1, cp2, cp3 = st.columns([1, 3, 1])
                    if cp1.button("⬅️ Ant.", key=f"ant_pag_{key_sufix}") and st.session_state['pag_campanha'] > 1:
                        st.session_state['pag_campanha'] -= 1; st.rerun()
                    if cp3.button("Próx. ➡️", key=f"prox_pag_{key_sufix}"):
                        st.session_state['pag_campanha'] += 1; st.rerun()

                st.divider()
                st.info(f"Ao confirmar abaixo, o ID da campanha **{campanha['id']}** será aplicado no cadastro desses clientes.")
                if st.button(f"✅ CONFIRMAR VÍNCULO ({tot} CLIENTES)", key=f"conf_vinc_{key_sufix}"):
                    ids = df_r['id'].tolist()
                    if ids:
                        qtd = vincular_campanha_aos_clientes(campanha['id'], campanha['nome_campanha'], ids)
                        st.balloons()
                        st.success(f"{qtd} clientes atualizados com a campanha '{campanha['nome_campanha']}'.")
                        st.session_state['resultado_campanha_df'] = None
                    else:
                        st.error("Lista vazia.")