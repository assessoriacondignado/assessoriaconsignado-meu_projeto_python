import streamlit as st
import pandas as pd
from datetime import date
import re
import modulo_pf_cadastro as pf_core

# --- CONFIGURAÇÕES DE CAMPOS ---
CAMPOS_CONFIG = {
    "Dados Pessoais": [
        {"label": "Nome", "coluna": "d.nome", "tipo": "texto", "tabela": "pf_dados"},
        {"label": "CPF", "coluna": "d.cpf", "tipo": "texto", "tabela": "pf_dados"},
        {"label": "RG", "coluna": "d.rg", "tipo": "texto", "tabela": "pf_dados"},
        {"label": "Data Nascimento", "coluna": "d.data_nascimento", "tipo": "data", "tabela": "pf_dados"},
        {"label": "Nome da Mãe", "coluna": "d.nome_mae", "tipo": "texto", "tabela": "pf_dados"}
    ],
    "Endereços": [
        {"label": "Logradouro", "coluna": "end.rua", "tipo": "texto", "tabela": "pf_enderecos"},
        {"label": "Bairro", "coluna": "end.bairro", "tipo": "texto", "tabela": "pf_enderecos"},
        {"label": "Cidade", "coluna": "end.cidade", "tipo": "texto", "tabela": "pf_enderecos"},
        {"label": "UF", "coluna": "end.uf", "tipo": "texto", "tabela": "pf_enderecos"},
        {"label": "CEP", "coluna": "end.cep", "tipo": "texto", "tabela": "pf_enderecos"}
    ],
    "Contatos": [
        {"label": "Telefone", "coluna": "tel.numero", "tipo": "texto", "tabela": "pf_telefones"},
        {"label": "E-mail", "coluna": "em.email", "tipo": "texto", "tabela": "pf_emails"}
    ],
    "Profissional (Geral)": [
        {"label": "Matrícula", "coluna": "emp.matricula", "tipo": "texto", "tabela": "pf_emprego_renda"},
        {"label": "Convênio", "coluna": "emp.convenio", "tipo": "texto", "tabela": "pf_emprego_renda"},
        {"label": "Contrato Empréstimo", "coluna": "ctr.contrato", "tipo": "texto", "tabela": "pf_contratos"}
    ],
    "Contratos CLT / CAGED": [
        {"label": "Nome Empresa", "coluna": "clt.cnpj_nome", "tipo": "texto", "tabela": "admin.pf_contratos_clt"},
        {"label": "CNPJ", "coluna": "clt.cnpj_numero", "tipo": "texto", "tabela": "admin.pf_contratos_clt"},
        {"label": "CBO (Cargo)", "coluna": "clt.cbo_nome", "tipo": "texto", "tabela": "admin.pf_contratos_clt"},
        {"label": "CNAE (Atividade)", "coluna": "clt.cnae_nome", "tipo": "texto", "tabela": "admin.pf_contratos_clt"},
        {"label": "Data Admissão", "coluna": "clt.data_admissao", "tipo": "data", "tabela": "admin.pf_contratos_clt"},
        {"label": "Qtd Funcionários", "coluna": "clt.qtd_funcionarios", "tipo": "numero", "tabela": "admin.pf_contratos_clt"}
    ]
}

# --- FUNÇÕES SQL ---
def buscar_pf_simples(termo, filtro_importacao_id=None, pagina=1, itens_por_pagina=50):
    conn = pf_core.get_conn()
    if conn:
        try:
            termo_limpo = re.sub(r'\D', '', termo).lstrip('0')
            param_nome = f"%{termo}%"
            sql_base = "SELECT d.id, d.nome, d.cpf, d.data_nascimento FROM pf_dados d "
            conds = ["d.nome ILIKE %s"]
            params = [param_nome]
            if termo_limpo: 
                sql_base += " LEFT JOIN pf_telefones t ON d.cpf=t.cpf_ref"
                conds.append("d.cpf ILIKE %s"); conds.append("t.numero ILIKE %s")
                params.append(f"%{termo_limpo}%"); params.append(f"%{termo_limpo}%")
            
            where = " WHERE " + " OR ".join(conds)
            
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(DISTINCT d.id) FROM pf_dados d {sql_base.split('pf_dados d')[1]} {where}", tuple(params))
            total = cur.fetchone()[0]
            
            offset = (pagina-1)*itens_por_pagina
            df = pd.read_sql(f"{sql_base} {where} GROUP BY d.id ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}", conn, params=tuple(params))
            conn.close()
            return df, total
        except: conn.close()
    return pd.DataFrame(), 0

def executar_pesquisa_ampla(regras_ativas, pagina=1, itens_por_pagina=50):
    conn = pf_core.get_conn()
    if conn:
        try:
            sql_select = "SELECT DISTINCT d.id, d.nome, d.cpf, d.data_nascimento "
            sql_from = "FROM pf_dados d "
            joins_map = {
                'pf_telefones': "JOIN pf_telefones tel ON d.cpf = tel.cpf_ref",
                'pf_emails': "JOIN pf_emails em ON d.cpf = em.cpf_ref",
                'pf_enderecos': "JOIN pf_enderecos end ON d.cpf = end.cpf_ref",
                'pf_emprego_renda': "JOIN pf_emprego_renda emp ON d.cpf = emp.cpf_ref",
                'pf_contratos': "JOIN pf_emprego_renda emp ON d.cpf = emp.cpf_ref JOIN pf_contratos ctr ON emp.matricula = ctr.matricula_ref",
                'admin.pf_contratos_clt': "JOIN pf_emprego_renda emp ON d.cpf = emp.cpf_ref LEFT JOIN admin.pf_contratos_clt clt ON emp.matricula = clt.matricula_ref"
            }
            active_joins = []; conditions = []; params = []

            for regra in regras_ativas:
                tabela = regra['tabela']; coluna = regra['coluna']; op = regra['operador']
                val_raw = regra['valor']; tipo = regra['tipo']
                
                if tabela in joins_map and joins_map[tabela] not in active_joins:
                    active_joins.append(joins_map[tabela])
                
                col_sql = f"{coluna}"
                if op == "∅": 
                    conditions.append(f"({col_sql} IS NULL OR {col_sql}::TEXT = '')"); continue
                if val_raw is None or str(val_raw).strip() == "": continue
                
                valores = [v.strip() for v in str(val_raw).split(',') if v.strip()]
                conds_or = []
                for val in valores:
                    if 'cpf' in coluna or 'cnpj' in coluna: val = pf_core.limpar_normalizar_cpf(val)
                    if tipo == 'numero': val = re.sub(r'\D', '', val)

                    if tipo == 'data':
                        if op == "=": conds_or.append(f"{col_sql} = %s"); params.append(val)
                        elif op == "≥": conds_or.append(f"{col_sql} >= %s"); params.append(val)
                        elif op == "≤": conds_or.append(f"{col_sql} <= %s"); params.append(val)
                        elif op == "≠": conds_or.append(f"{col_sql} <> %s"); params.append(val)
                        continue 

                    if op == "=>": conds_or.append(f"{col_sql} ILIKE %s"); params.append(f"{val}%")
                    elif op == "<=>": conds_or.append(f"{col_sql} ILIKE %s"); params.append(f"%{val}%")
                    elif op == "=": 
                        if tipo == 'numero': conds_or.append(f"{col_sql} = %s"); params.append(val)
                        else: conds_or.append(f"{col_sql} ILIKE %s"); params.append(val)
                    elif op == "≠": conds_or.append(f"{col_sql} <> %s"); params.append(val)
                    elif op == "<≠>": conds_or.append(f"{col_sql} NOT ILIKE %s"); params.append(f"%{val}%")
                    elif op == "o": pass 
                    elif op in [">", "<", "≥", "≤"]:
                        sym = {">":">", "<":"<", "≥":">=", "≤":"<="}[op]
                        conds_or.append(f"{col_sql} {sym} %s"); params.append(val)
                
                if op == "o":
                    placeholders = ','.join(['%s'] * len(valores))
                    conditions.append(f"{col_sql} IN ({placeholders})"); params.extend(valores)
                elif conds_or: conditions.append(f"({' OR '.join(conds_or)})")

            full_joins = " ".join(active_joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            count_sql = f"SELECT COUNT(DISTINCT d.id) {sql_from} {full_joins} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total = cur.fetchone()[0]
            
            offset = (pagina - 1) * itens_por_pagina
            query = f"{sql_select} {sql_from} {full_joins} {sql_where} ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}"
            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close(); return df.fillna(""), total
        except Exception as e: st.error(f"Erro SQL: {e}"); return pd.DataFrame(), 0
    return pd.DataFrame(), 0

# --- INTERFACE DE PESQUISA ---
def interface_pesquisa_ampla():
    c_nav_esq, c_nav_dir = st.columns([1, 6])
    if c_nav_esq.button("⬅️ Voltar"):
        st.session_state.update({'pf_view': 'lista'})
        st.rerun()
    if c_nav_dir.button("🗑️ Limpar Filtros"):
        st.session_state['regras_pesquisa'] = []
        st.session_state['executar_busca'] = False
        st.session_state['pagina_atual'] = 1
        st.rerun()
    st.divider()

    conn = pf_core.get_conn()
    ops_cache = {'texto': [], 'numero': [], 'data': []}
    if conn:
        df_ops = pd.read_sql("SELECT tipo, simbolo, descricao FROM pf_operadores_de_filtro", conn)
        conn.close()
        for _, r in df_ops.iterrows():
            ops_cache[r['tipo']].append(f"{r['simbolo']} : {r['descricao']}") 

    c_menu, c_regras = st.columns([1.5, 3.5])
    with c_menu:
        st.markdown("### 🗂️ Campos Disponíveis")
        for grupo, campos in CAMPOS_CONFIG.items():
            with st.expander(grupo):
                for campo in campos:
                    if st.button(f"➕ {campo['label']}", key=f"add_{campo['coluna']}"):
                        st.session_state['regras_pesquisa'].append({
                            'label': campo['label'], 'coluna': campo['coluna'], 'tabela': campo['tabela'],
                            'tipo': campo['tipo'], 'operador': None, 'valor': ''
                        })
                        st.rerun()

    with c_regras:
        st.markdown("### 🎯 Regras Ativas")
        if not st.session_state['regras_pesquisa']: st.info("Nenhuma regra selecionada. Clique nos itens à esquerda.")
        regras_rem = []
        for i, regra in enumerate(st.session_state['regras_pesquisa']):
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([2, 2, 3, 0.5])
                c1.markdown(f"**{regra['label']}**")
                opcoes = ops_cache.get(regra['tipo'], [])
                idx_sel = opcoes.index(regra['operador']) if regra['operador'] in opcoes else 0
                novo_op_full = c2.selectbox("Op.", opcoes, index=idx_sel, key=f"op_{i}", label_visibility="collapsed")
                novo_op_simbolo = novo_op_full.split(' : ')[0] if novo_op_full else "="
                
                if novo_op_simbolo == '∅':
                    c3.text_input("Valor", value="[Vazio]", disabled=True, key=f"val_{i}", label_visibility="collapsed")
                    novo_valor = None
                elif regra['tipo'] == 'data':
                    novo_valor = c3.date_input("Data", value=None, min_value=date(1900,1,1), max_value=date(2025,12,31), key=f"val_{i}", format="DD/MM/YYYY", label_visibility="collapsed")
                else:
                    novo_valor = c3.text_input("Valor", value=regra['valor'], key=f"val_{i}", label_visibility="collapsed")

                st.session_state['regras_pesquisa'][i]['operador'] = novo_op_full
                st.session_state['regras_pesquisa'][i]['valor'] = novo_valor
                if c4.button("🗑️", key=f"del_{i}"): regras_rem.append(i)

        if regras_rem:
            for idx in sorted(regras_rem, reverse=True): st.session_state['regras_pesquisa'].pop(idx)
            st.rerun()

        st.divider()
        if st.button("🔎 FILTRAR AGORA", type="primary", use_container_width=True):
            st.session_state['executar_busca'] = True

    if st.session_state.get('executar_busca'):
        regras_limpas = []
        for r in st.session_state['regras_pesquisa']:
            r_copy = r.copy()
            if r_copy['operador']: r_copy['operador'] = r_copy['operador'].split(' : ')[0]
            regras_limpas.append(r_copy)

        df_res, total = executar_pesquisa_ampla(regras_limpas, st.session_state['pagina_atual'])
        st.write(f"**Resultados:** {total}")
        
        if not df_res.empty:
            st.markdown("""<div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex;"><div style="flex: 1;">Ações</div><div style="flex: 1;">ID</div><div style="flex: 2;">CPF</div><div style="flex: 4;">Nome</div></div>""", unsafe_allow_html=True)
            for _, row in df_res.iterrows():
                c1, c2, c3, c4 = st.columns([1, 1, 2, 4])
                with c1:
                    if st.button("👁️", key=f"v_{row['id']}"): pf_core.dialog_visualizar_cliente(str(row['cpf']))
                c2.write(str(row['id'])); c3.write(pf_core.formatar_cpf_visual(row['cpf'])); c4.write(row['nome'])
                st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
            
            cp1, cp2, cp3 = st.columns([1, 3, 1])
            if cp1.button("⬅️ Ant.") and st.session_state['pagina_atual'] > 1: st.session_state['pagina_atual'] -= 1; st.rerun()
            if cp3.button("Próx. ➡️"): st.session_state['pagina_atual'] += 1; st.rerun()
        else: st.warning("Nenhum registro encontrado.")