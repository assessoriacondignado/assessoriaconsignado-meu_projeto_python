import streamlit as st
import pandas as pd
from datetime import date
import re
import time
import modulo_pf_cadastro as pf_core
import modulo_pf_exportacao as pf_export

# --- CONFIGURAÇÕES DE CAMPOS (MANTIDA IGUAL AO ÚLTIMO UPDATE) ---
CAMPOS_CONFIG = {
    "Dados Pessoais": [
        {"label": "Nome", "coluna": "d.nome", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CPF", "coluna": "d.cpf", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "RG", "coluna": "d.rg", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "UF RG", "coluna": "d.uf_rg", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Exp. RG", "coluna": "d.data_exp_rg", "tipo": "data", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Nascimento", "coluna": "d.data_nascimento", "tipo": "data", "tabela": "banco_pf.pf_dados"},
        {"label": "Idade (Cálculo)", "coluna": "virtual_idade", "tipo": "numero", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome da Mãe", "coluna": "d.nome_mae", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome do Pai", "coluna": "d.nome_pai", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CNH", "coluna": "d.cnh", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "PIS", "coluna": "d.pis", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CTPS/Série", "coluna": "d.ctps_serie", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome Procurador", "coluna": "d.nome_procurador", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CPF Procurador", "coluna": "d.cpf_procurador", "tipo": "texto", "tabela": "banco_pf.pf_dados"}
    ],
    "Endereços": [
        {"label": "Logradouro", "coluna": "ende.rua", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "Bairro", "coluna": "ende.bairro", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "Cidade", "coluna": "ende.cidade", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "UF", "coluna": "ende.uf", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "CEP", "coluna": "ende.cep", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"}
    ],
    "Contatos": [
        {"label": "Telefone (Número)", "coluna": "tel.numero", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Tag WhatsApp", "coluna": "tel.tag_whats", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Tag Qualificação", "coluna": "tel.tag_qualificacao", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Data Atualização (Tel)", "coluna": "tel.data_atualizacao", "tipo": "data", "tabela": "banco_pf.pf_telefones"},
        {"label": "E-mail", "coluna": "em.email", "tipo": "texto", "tabela": "banco_pf.pf_emails"}
    ],
    "Profissional (Geral)": [
        {"label": "Matrícula", "coluna": "emp.matricula", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Convênio", "coluna": "emp.convenio", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Data Atualização (Emp)", "coluna": "emp.data_atualizacao", "tipo": "data", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "ID Importação (Emp)", "coluna": "emp.importacao_id", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Contrato Empréstimo", "coluna": "ctr.contrato", "tipo": "texto", "tabela": "banco_pf.pf_contratos"}
    ],
    "Contratos CLT / CAGED": [
        {"label": "Nome Empresa", "coluna": "clt.cnpj_nome", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CNPJ", "coluna": "clt.cnpj_numero", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "TAG (Destaque)", "coluna": "clt.tag", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CBO (Nome)", "coluna": "clt.cbo_nome", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CBO (Código)", "coluna": "clt.cbo_codigo", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CNAE (Nome)", "coluna": "clt.cnae_nome", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CNAE (Código)", "coluna": "clt.cnae_codigo", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Admissão", "coluna": "clt.data_admissao", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Tempo Admissão (Anos)", "coluna": "clt.tempo_admissao_anos", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Início Emprego", "coluna": "clt.data_inicio_emprego", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Tempo Início (Anos)", "coluna": "clt.tempo_inicio_emprego_anos", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Abertura Empresa", "coluna": "clt.data_abertura_empresa", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Tempo Abertura (Anos)", "coluna": "clt.tempo_abertura_anos", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Qtd Funcionários", "coluna": "clt.qtd_funcionarios", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"}
    ],
    "Controle e Sistema": [
        {"label": "ID da Importação (Geral)", "coluna": "d.importacao_id", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "ID da Campanha", "coluna": "d.id_campanha", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Criação (Cadastro)", "coluna": "d.data_criacao", "tipo": "data", "tabela": "banco_pf.pf_dados"}
    ]
}

# --- FUNÇÕES SQL ---

def buscar_pf_simples(termo, filtro_importacao_id=None, pagina=1, itens_por_pagina=50):
    conn = pf_core.get_conn()
    if conn:
        try:
            termo_limpo = re.sub(r'\D', '', termo).lstrip('0')
            param_nome = f"%{termo}%"
            # Schema banco_pf
            sql_base = "SELECT d.id, d.nome, d.cpf, d.data_nascimento FROM banco_pf.pf_dados d "
            conds = ["d.nome ILIKE %s"]
            params = [param_nome]
            if termo_limpo: 
                # Ajuste: Assumindo que você também vai renomear cpf_ref para cpf nas tabelas de telefone
                # Se ainda for cpf_ref, mude t.cpf para t.cpf_ref abaixo
                sql_base += " LEFT JOIN banco_pf.pf_telefones t ON d.cpf=t.cpf_ref" 
                conds.append("d.cpf ILIKE %s"); conds.append("t.numero ILIKE %s")
                params.append(f"%{termo_limpo}%"); params.append(f"%{termo_limpo}%")
            
            where = " WHERE " + " OR ".join(conds)
            
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(DISTINCT d.id) FROM banco_pf.pf_dados d {sql_base.split('banco_pf.pf_dados d')[1]} {where}", tuple(params))
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
            sql_from = "FROM banco_pf.pf_dados d "
            
            joins_map = {
                'banco_pf.pf_telefones': "JOIN banco_pf.pf_telefones tel ON d.cpf = tel.cpf_ref",
                'banco_pf.pf_emails': "JOIN banco_pf.pf_emails em ON d.cpf = em.cpf_ref",
                'banco_pf.pf_enderecos': "JOIN banco_pf.pf_enderecos ende ON d.cpf = ende.cpf_ref",
                'banco_pf.pf_emprego_renda': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf",
                'banco_pf.pf_contratos': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf JOIN banco_pf.pf_contratos ctr ON emp.matricula = ctr.matricula_ref",
                'banco_pf.pf_matricula_dados_clt': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf LEFT JOIN banco_pf.pf_matricula_dados_clt clt ON emp.matricula = clt.matricula"
            }
            active_joins = []; conditions = []; params = []

            for regra in regras_ativas:
                tabela = regra['tabela']; coluna = regra['coluna']; op = regra['operador']
                val_raw = regra['valor']; tipo = regra['tipo']
                
                # Tratamento para garantir nome correto da tabela
                if tabela == 'banco_pf.pf_contratos_clt': tabela = 'banco_pf.pf_matricula_dados_clt'

                if tabela in joins_map and joins_map[tabela] not in active_joins:
                    active_joins.append(joins_map[tabela])
                
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
                
                if op == "o":
                    placeholders = ','.join(['%s'] * len(valores))
                    conditions.append(f"{col_sql} IN ({placeholders})"); params.extend(valores)
                elif conds_or: conditions.append(f"({' OR '.join(conds_or)})")

            full_joins = " ".join(active_joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            # Contagem para paginação
            count_sql = f"SELECT COUNT(DISTINCT d.id) {sql_from} {full_joins} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total = cur.fetchone()[0]
            
            # Busca paginada
            offset = (pagina - 1) * itens_por_pagina
            limit_clause = f"LIMIT {itens_por_pagina} OFFSET {offset}" if itens_por_pagina < 9999999 else ""
            
            query = f"{sql_select} {sql_from} {full_joins} {sql_where} ORDER BY d.nome {limit_clause}"
            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close()
            return df.fillna(""), total
        except Exception as e: 
            st.error(f"Erro SQL: {e}"); 
            return pd.DataFrame(), 0
    return pd.DataFrame(), 0

# --- FUNÇÃO DE EXCLUSÃO EM LOTE ---
def executar_exclusao_lote(tipo, cpfs_alvo, convenio=None, sub_opcao=None):
    conn = pf_core.get_conn()
    if not conn: return False, "Erro de conexão."
    try:
        cur = conn.cursor()
        cpfs_tuple = tuple(str(c) for c in cpfs_alvo)
        if not cpfs_tuple: return False, "Nenhum CPF na lista."

        if tipo == "Cadastro Completo":
            query = "DELETE FROM banco_pf.pf_dados WHERE cpf IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "Telefones":
            query = "DELETE FROM banco_pf.pf_telefones WHERE cpf_ref IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "E-mails":
            query = "DELETE FROM banco_pf.pf_emails WHERE cpf_ref IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "Endereços":
            query = "DELETE FROM banco_pf.pf_enderecos WHERE cpf_ref IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "Emprego e Renda":
            if not convenio: return False, "Convênio não selecionado."
            if sub_opcao == "Excluir Vínculo Completo (Matrícula + Contratos)":
                query = "DELETE FROM banco_pf.pf_emprego_renda WHERE cpf IN %s AND convenio = %s"
                cur.execute(query, (cpfs_tuple, convenio))
            elif sub_opcao == "Excluir Apenas Contratos":
                query = """
                    DELETE FROM banco_pf.pf_contratos 
                    WHERE matricula_ref IN (
                        SELECT matricula FROM banco_pf.pf_emprego_renda 
                        WHERE cpf IN %s AND convenio = %s
                    )
                """
                cur.execute(query, (cpfs_tuple, convenio))

        registros = cur.rowcount
        conn.commit()
        conn.close()
        return True, f"Operação realizada com sucesso! {registros} registros afetados."

    except Exception as e:
        if conn: conn.close()
        return False, f"Erro na execução: {e}"

# --- INTERFACES VISUAIS ---

def interface_pesquisa_rapida():
    c1, c2 = st.columns([2, 2])
    busca = c2.text_input("🔎 Pesquisa Rápida (Nome/CPF)", key="pf_busca")
    col_b1, col_b2, col_b3 = st.columns([1, 1, 1])
    if col_b1.button("➕ Novo"): st.session_state.update({'pf_view': 'novo', 'form_loaded': False}); st.rerun()
    if col_b2.button("🔍 Pesquisa Ampla"): st.session_state.update({'pf_view': 'pesquisa_ampla'}); st.rerun()
    if col_b3.button("📥 Importar"): st.session_state.update({'pf_view': 'importacao', 'import_step': 1}); st.rerun()
    
    if busca:
        df_lista, total = buscar_pf_simples(busca, pagina=st.session_state.get('pagina_atual', 1))
        
        if not df_lista.empty:
            st.markdown(f"**Resultados encontrados: {total}**")
            
            st.markdown("""
            <div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex;">
                <div style="flex: 2;">Ações</div>
                <div style="flex: 1;">ID</div>
                <div style="flex: 2;">CPF</div>
                <div style="flex: 4;">Nome</div>
            </div>
            """, unsafe_allow_html=True)

            for _, row in df_lista.iterrows():
                c1, c2, c3, c4 = st.columns([2, 1, 2, 4])
                
                with c1:
                    b1, b2, b3 = st.columns(3)
                    with b1:
                        if st.button("👁️", key=f"v_fast_{row['id']}", help="Visualizar"): 
                            pf_core.dialog_visualizar_cliente(str(row['cpf']))
                    with b2:
                        if st.button("✏️", key=f"e_fast_{row['id']}", help="Editar"): 
                            st.session_state.update({'pf_view': 'editar', 'pf_cpf_selecionado': str(row['cpf']), 'form_loaded': False}); st.rerun()
                    with b3:
                        if st.button("🗑️", key=f"d_fast_{row['id']}", help="Excluir"): 
                            pf_core.dialog_excluir_pf(str(row['cpf']), row['nome'])
                
                c2.write(str(row['id']))
                c3.write(pf_core.formatar_cpf_visual(row['cpf']))
                c4.write(row['nome'])
                st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
            
            cp1, cp2, cp3 = st.columns([1, 3, 1])
            if cp1.button("⬅️ Ant.", key="prev_fast") and st.session_state.get('pagina_atual', 1) > 1: 
                st.session_state['pagina_atual'] -= 1
                st.rerun()
            if cp3.button("Próx. ➡️", key="next_fast"): 
                st.session_state['pagina_atual'] = st.session_state.get('pagina_atual', 1) + 1
                st.rerun()
        else: 
            st.warning("Nenhum registro encontrado.")
    else:
        st.info("Utilize a busca para listar clientes.")

def interface_pesquisa_ampla():
    c_nav_esq, c_nav_dir = st.columns([1, 6])
    if c_nav_esq.button("⬅️ Voltar"): st.session_state.update({'pf_view': 'lista'}); st.rerun()
    if c_nav_dir.button("🗑️ Limpar Filtros"): st.session_state['regras_pesquisa'] = []; st.session_state['executar_busca'] = False; st.session_state['pagina_atual'] = 1; st.rerun()
    st.divider()

    conn = pf_core.get_conn()
    ops_cache = {'texto': [], 'numero': [], 'data': []}
    lista_convenios = []
    if conn:
        try:
            df_ops = pd.read_sql("SELECT tipo, simbolo, descricao FROM banco_pf.pf_operadores_de_filtro", conn)
            for _, r in df_ops.iterrows(): ops_cache[r['tipo']].append(f"{r['simbolo']} : {r['descricao']}")
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT convenio FROM banco_pf.pf_emprego_renda WHERE convenio IS NOT NULL ORDER BY convenio")
            lista_convenios = [r[0] for r in cur.fetchall()]
            cur.close()
        except: pass
        conn.close()

    # --- ALTERAÇÃO DE LAYOUT: Colunas para Botões ---
    # c_menu (Campos) = 4, c_regras (Regras) = 1.5 (proporção 4:1.5 para dar mais espaço aos botões)
    c_menu, c_regras = st.columns([4, 2]) 
    
    with c_menu:
        st.markdown("### 🗂️ Campos Disponíveis")
        for grupo, campos in CAMPOS_CONFIG.items():
            with st.expander(grupo, expanded=False):
                # Cria 4 colunas dentro do expander para os botões ficarem lado a lado
                colunas_botoes = st.columns(4)
                for idx, campo in enumerate(campos):
                    with colunas_botoes[idx % 4]: # Distribui em carrossel nas 4 colunas
                        if st.button(f"➕ {campo['label']}", key=f"add_{campo['coluna']}", use_container_width=True):
                            st.session_state['regras_pesquisa'].append({
                                'label': campo['label'], 'coluna': campo['coluna'], 'tabela': campo['tabela'],
                                'tipo': campo['tipo'], 'operador': None, 'valor': ''
                            })
                            st.rerun()

    with c_regras:
        st.markdown("### 🎯 Regras Ativas")
        if not st.session_state['regras_pesquisa']: 
            st.info("Nenhuma regra. Selecione ao lado.")
        
        regras_rem = []
        for i, regra in enumerate(st.session_state['regras_pesquisa']):
            with st.container(border=True):
                # Layout mais compacto para a regra
                st.caption(f"**{regra['label']}**")
                
                c_op, c_val, c_del = st.columns([2, 3, 1])
                
                opcoes = ops_cache.get(regra['tipo'], [])
                idx_sel = opcoes.index(regra['operador']) if regra['operador'] in opcoes else 0
                novo_op_full = c_op.selectbox("Op.", opcoes, index=idx_sel, key=f"op_{i}", label_visibility="collapsed")
                novo_op_simbolo = novo_op_full.split(' : ')[0] if novo_op_full else "="
                
                if novo_op_simbolo == '∅':
                    c_val.text_input("Valor", value="[Vazio]", disabled=True, key=f"val_{i}", label_visibility="collapsed")
                    novo_valor = None
                elif regra['tipo'] == 'data':
                    novo_valor = c_val.date_input("Data", value=None, min_value=date(1900,1,1), max_value=date(2050,12,31), key=f"val_{i}", format="DD/MM/YYYY", label_visibility="collapsed")
                else:
                    novo_valor = c_val.text_input("Valor", value=regra['valor'], key=f"val_{i}", label_visibility="collapsed")

                st.session_state['regras_pesquisa'][i]['operador'] = novo_op_full
                st.session_state['regras_pesquisa'][i]['valor'] = novo_valor
                
                if c_del.button("🗑️", key=f"del_{i}"): regras_rem.append(i)

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
            st.divider()
            
            with st.expander("📂 Exportar Dados", expanded=False):
                df_modelos = pf_export.listar_modelos_ativos()
                
                if not df_modelos.empty:
                    c_sel, c_btn = st.columns([3, 1])
                    opcoes_mods = df_modelos.apply(lambda x: f"{x['id']} - {x['nome_modelo']}", axis=1)
                    idx_mod = c_sel.selectbox("Selecione o Modelo de Exportação:", range(len(df_modelos)), format_func=lambda x: opcoes_mods[x])
                    modelo_selecionado = df_modelos.iloc[idx_mod]
                    
                    st.caption(f"📝 {modelo_selecionado['descricao']}")
                    
                    if c_btn.button("⬇️ Gerar Arquivo"):
                        with st.spinner("Processando exportação..."):
                            df_total, _ = executar_pesquisa_ampla(regras_limpas, 1, 999999)
                            lista_cpfs = df_total['cpf'].unique().tolist()
                            df_final = pf_export.gerar_dataframe_por_modelo(modelo_selecionado['id'], lista_cpfs)
                            
                            if not df_final.empty:
                                csv = df_final.to_csv(sep=';', index=False, encoding='utf-8-sig')
                                st.download_button(label="💾 Baixar CSV", data=csv, file_name=f"export_{modelo_selecionado['tipo_processamento'].lower()}.csv", mime="text/csv")
                                st.success(f"Arquivo gerado com {len(df_final)} linhas!")
                            else:
                                st.warning("A exportação retornou vazio.")
                else:
                    st.warning("Nenhum modelo de exportação configurado.")

            with st.expander("🗑️ Zona de Perigo: Exclusão em Lote", expanded=False):
                st.error(f"Atenção: A exclusão será aplicada aos {total} clientes filtrados na pesquisa atual.")
                modulos_exclusao = ["Selecione...", "Cadastro Completo", "Telefones", "E-mails", "Endereços", "Emprego e Renda"]
                tipo_exc = st.selectbox("O que deseja excluir?", modulos_exclusao)
                convenio_sel = None
                sub_opcao_sel = None
                
                if tipo_exc == "Emprego e Renda":
                    c_emp1, c_emp2 = st.columns(2)
                    convenio_sel = c_emp1.selectbox("Qual Convênio?", lista_convenios)
                    sub_opcao_sel = c_emp2.radio("Nível de Exclusão", ["Excluir Vínculo Completo (Matrícula + Contratos)", "Excluir Apenas Contratos"])
                    
                if tipo_exc != "Selecione...":
                    if st.button("Preparar Exclusão", key="btn_prep_exc"):
                        st.session_state['confirm_delete_lote'] = True
                        st.rerun()
                    if st.session_state.get('confirm_delete_lote'):
                        st.warning(f"Você está prestes a excluir **{tipo_exc}** de **{total}** clientes.")
                        c_sim, c_nao = st.columns(2)
                        if c_sim.button("🚨 SIM, EXCLUIR DEFINITIVAMENTE", type="primary", key="btn_conf_exc"):
                            df_total, _ = executar_pesquisa_ampla(regras_limpas, 1, 999999) 
                            lista_cpfs = df_total['cpf'].tolist()
                            ok, msg = executar_exclusao_lote(tipo_exc, lista_cpfs, convenio_sel, sub_opcao_sel)
                            if ok:
                                st.success(msg)
                                st.session_state['confirm_delete_lote'] = False
                                time.sleep(2); st.rerun()
                            else: st.error(f"Erro: {msg}")
                        if c_nao.button("Cancelar", key="btn_canc_exc"):
                            st.session_state['confirm_delete_lote'] = False; st.rerun()
            st.divider()

            st.markdown("""<div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex;"><div style="flex: 2;">Ações</div><div style="flex: 1;">ID</div><div style="flex: 2;">CPF</div><div style="flex: 4;">Nome</div></div>""", unsafe_allow_html=True)
            for _, row in df_res.iterrows():
                c1, c2, c3, c4 = st.columns([2, 1, 2, 4])
                with c1:
                    b1, b2, b3 = st.columns(3)
                    with b1:
                        if st.button("👁️", key=f"v_{row['id']}"): pf_core.dialog_visualizar_cliente(str(row['cpf']))
                    with b2:
                        if st.button("✏️", key=f"e_{row['id']}"): 
                            st.session_state.update({'pf_view': 'editar', 'pf_cpf_selecionado': str(row['cpf']), 'form_loaded': False}); st.rerun()
                    with b3:
                        if st.button("🗑️", key=f"d_{row['id']}"): pf_core.dialog_excluir_pf(str(row['cpf']), row['nome'])
                c2.write(str(row['id'])); c3.write(pf_core.formatar_cpf_visual(row['cpf'])); c4.write(row['nome'])
                st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
            
            cp1, cp2, cp3 = st.columns([1, 3, 1])
            if cp1.button("⬅️ Ant.") and st.session_state['pagina_atual'] > 1: st.session_state['pagina_atual'] -= 1; st.rerun()
            if cp3.button("Próx. ➡️"): st.session_state['pagina_atual'] += 1; st.rerun()
        else: st.warning("Nenhum registro encontrado.")