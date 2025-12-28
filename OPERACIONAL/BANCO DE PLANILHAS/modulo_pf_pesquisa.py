import streamlit as st
import pandas as pd
from datetime import date
import re
import time
import modulo_pf_cadastro as pf_core

# --- CONFIGURAÇÕES DE CAMPOS ---
CAMPOS_CONFIG = {
    "Dados Pessoais": [
        {"label": "Nome", "coluna": "d.nome", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CPF", "coluna": "d.cpf", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "RG", "coluna": "d.rg", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Nascimento", "coluna": "d.data_nascimento", "tipo": "data", "tabela": "banco_pf.pf_dados"},
        {"label": "Idade", "coluna": "virtual_idade", "tipo": "numero", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome da Mãe", "coluna": "d.nome_mae", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "ID Campanha", "coluna": "d.id_campanha", "tipo": "texto", "tabela": "banco_pf.pf_dados"}
    ],
    "Endereços": [
        {"label": "Logradouro", "coluna": "ende.rua", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "Bairro", "coluna": "ende.bairro", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "Cidade", "coluna": "ende.cidade", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "UF", "coluna": "ende.uf", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "CEP", "coluna": "ende.cep", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"}
    ],
    "Contatos": [
        {"label": "Telefone", "coluna": "tel.numero", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "E-mail", "coluna": "em.email", "tipo": "texto", "tabela": "banco_pf.pf_emails"}
    ],
    "Profissional (Geral)": [
        {"label": "Matrícula", "coluna": "emp.matricula", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Convênio", "coluna": "emp.convenio", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Contrato Empréstimo", "coluna": "ctr.contrato", "tipo": "texto", "tabela": "banco_pf.pf_contratos"}
    ],
    "Contratos CLT / CAGED": [
        {"label": "Nome Empresa", "coluna": "clt.cnpj_nome", "tipo": "texto", "tabela": "banco_pf.pf_contratos_clt"},
        {"label": "CNPJ", "coluna": "clt.cnpj_numero", "tipo": "texto", "tabela": "banco_pf.pf_contratos_clt"},
        {"label": "CBO (Cargo)", "coluna": "clt.cbo_nome", "tipo": "texto", "tabela": "banco_pf.pf_contratos_clt"},
        {"label": "CNAE (Atividade)", "coluna": "clt.cnae_nome", "tipo": "texto", "tabela": "banco_pf.pf_contratos_clt"},
        {"label": "Data Admissão", "coluna": "clt.data_admissao", "tipo": "data", "tabela": "banco_pf.pf_contratos_clt"},
        {"label": "Qtd Funcionários", "coluna": "clt.qtd_funcionarios", "tipo": "numero", "tabela": "banco_pf.pf_contratos_clt"}
    ],
    "Controle de Importação": [
        {"label": "ID da Importação", "coluna": "d.importacao_id", "tipo": "numero", "tabela": "banco_pf.pf_dados"}
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
                'banco_pf.pf_emprego_renda': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf_ref",
                'banco_pf.pf_contratos': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf_ref JOIN banco_pf.pf_contratos ctr ON emp.matricula = ctr.matricula_ref",
                'banco_pf.pf_contratos_clt': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf_ref LEFT JOIN banco_pf.pf_contratos_clt clt ON emp.matricula = clt.matricula_ref"
            }
            active_joins = []; conditions = []; params = []

            for regra in regras_ativas:
                tabela = regra['tabela']; coluna = regra['coluna']; op = regra['operador']
                val_raw = regra['valor']; tipo = regra['tipo']
                
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
            # Se for exportação total (limite muito alto), ajusta
            limit_clause = f"LIMIT {itens_por_pagina} OFFSET {offset}" if itens_por_pagina < 9999999 else ""
            
            query = f"{sql_select} {sql_from} {full_joins} {sql_where} ORDER BY d.nome {limit_clause}"
            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close()
            return df.fillna(""), total
        except Exception as e: 
            st.error(f"Erro SQL: {e}") 
            return pd.DataFrame(), 0
    return pd.DataFrame(), 0

# --- GERADOR DE RELATÓRIO DETALHADO (MODELO CONTRATOS) ---
def gerar_relatorio_contratos_completo(cpfs_alvo):
    """
    Gera um DataFrame complexo:
    1. Baseado nos CPFs filtrados.
    2. Busca todas as tabelas de contratos dinamicamente.
    3. Agrega telefones, emails e endereços para não duplicar linhas excessivamente.
    4. Garante 1 linha por Contrato (referência principal).
    """
    conn = pf_core.get_conn()
    if not conn or not cpfs_alvo: return pd.DataFrame()
    
    try:
        # Prepara a lista de CPFs para a query IN
        placeholders = ",".join(["%s"] * len(cpfs_alvo))
        params_cpfs = tuple(cpfs_alvo)

        # 1. Busca Dados Pessoais (Base)
        query_dados = f"SELECT * FROM banco_pf.pf_dados WHERE cpf IN ({placeholders})"
        df_dados = pd.read_sql(query_dados, conn, params=params_cpfs)
        
        # 2. Busca Dados Agregados (Telefones, Emails, Endereços)
        # Agrupa múltiplos registros em uma única célula para manter a linha única por contrato
        query_tel = f"SELECT cpf_ref, STRING_AGG(numero || ' (' || COALESCE(tag_qualificacao,'') || ')', ' | ') as telefones FROM banco_pf.pf_telefones WHERE cpf_ref IN ({placeholders}) GROUP BY cpf_ref"
        df_tel = pd.read_sql(query_tel, conn, params=params_cpfs)
        
        query_email = f"SELECT cpf_ref, STRING_AGG(email, ' | ') as emails FROM banco_pf.pf_emails WHERE cpf_ref IN ({placeholders}) GROUP BY cpf_ref"
        df_email = pd.read_sql(query_email, conn, params=params_cpfs)
        
        query_end = f"SELECT cpf_ref, STRING_AGG(rua || ', ' || bairro || ' - ' || cidade || '/' || uf || ' (' || cep || ')', ' | ') as enderecos FROM banco_pf.pf_enderecos WHERE cpf_ref IN ({placeholders}) GROUP BY cpf_ref"
        df_end = pd.read_sql(query_end, conn, params=params_cpfs)

        # Merge dos dados cadastrais
        df_full = df_dados.merge(df_tel, left_on='cpf', right_on='cpf_ref', how='left')\
                          .merge(df_email, left_on='cpf', right_on='cpf_ref', how='left')\
                          .merge(df_end, left_on='cpf', right_on='cpf_ref', how='left')
        
        # Limpa colunas de join duplicadas
        cols_drop = [c for c in df_full.columns if '_ref' in c]
        df_full.drop(columns=cols_drop, inplace=True, errors='ignore')

        # 3. Busca Empregos (Necessário para vincular contrato -> matricula -> cpf)
        query_emp = f"SELECT cpf_ref, convenio, matricula, dados_extras as emprego_extras FROM banco_pf.pf_emprego_renda WHERE cpf_ref IN ({placeholders})"
        df_emp = pd.read_sql(query_emp, conn, params=params_cpfs)
        
        # 4. Busca Dinâmica de Contratos (Todas as tabelas pf_contratos%)
        cur = conn.cursor()
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'banco_pf' AND table_name LIKE 'pf_contratos%'
        """)
        tabelas_contratos = cur.fetchall()
        
        df_contratos_final = pd.DataFrame()
        
        if not df_emp.empty:
            matriculas = df_emp['matricula'].dropna().unique().tolist()
            if matriculas:
                placeholders_mat = ",".join(["%s"] * len(matriculas))
                params_mat = tuple(matriculas)
                
                for schema, tabela in tabelas_contratos:
                    try:
                        # Tenta ler a tabela de contratos
                        query_ctr = f"SELECT *, '{tabela}' as origem_contrato FROM {schema}.{tabela} WHERE matricula_ref IN ({placeholders_mat})"
                        df_temp = pd.read_sql(query_ctr, conn, params=params_mat)
                        
                        # Padronização de colunas (caso variem)
                        if not df_temp.empty:
                            # Remove colunas técnicas que não interessam no relatório
                            df_temp = df_temp.drop(columns=['id', 'data_criacao', 'data_atualizacao', 'importacao_id'], errors='ignore')
                            df_contratos_final = pd.concat([df_contratos_final, df_temp], ignore_index=True)
                    except: continue

        # 5. Consolidação Final (Vínculo: Dados -> Emprego -> Contrato)
        # Se o cliente não tiver contrato, ele deve aparecer? O modelo diz "usar contrato como referência". 
        # Se não tem contrato, não tem linha. (Inner Join implícito pela lógica)
        
        # Join Dados + Emprego
        df_base = df_full.merge(df_emp, left_on='cpf', right_on='cpf_ref', how='inner')
        
        # Join (Dados+Emprego) + Contratos
        if not df_contratos_final.empty:
            df_export = df_base.merge(df_contratos_final, left_on='matricula', right_on='matricula_ref', how='inner')
        else:
            # Se não achou nenhum contrato, retorna vazio
            df_export = pd.DataFrame()

        # Limpeza final e Ordenação
        if not df_export.empty:
            cols_drop = [c for c in df_export.columns if '_ref' in c or c == 'id']
            df_export.drop(columns=cols_drop, inplace=True, errors='ignore')
            
            # Ordenação Solicitada: 1º CPF, 2º Convênio, 3º Matrícula
            cols_sort = []
            if 'cpf' in df_export.columns: cols_sort.append('cpf')
            if 'convenio' in df_export.columns: cols_sort.append('convenio')
            if 'matricula' in df_export.columns: cols_sort.append('matricula')
            
            if cols_sort:
                df_export.sort_values(by=cols_sort, inplace=True)
                
            # Organização de Colunas (Layout solicitado: Dados -> Tel -> Email -> End -> Emprego -> Contrato)
            # Reordenamos movendo as colunas conhecidas para o início
            cols = list(df_export.columns)
            prioridade = ['nome', 'cpf', 'rg', 'data_nascimento', 'telefones', 'emails', 'enderecos', 'convenio', 'matricula']
            
            ordered_cols = [c for c in prioridade if c in cols] + [c for c in cols if c not in prioridade]
            df_export = df_export[ordered_cols]

        conn.close()
        return df_export

    except Exception as e:
        st.error(f"Erro ao gerar relatório detalhado: {e}")
        conn.close()
        return pd.DataFrame()

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
                query = "DELETE FROM banco_pf.pf_emprego_renda WHERE cpf_ref IN %s AND convenio = %s"
                cur.execute(query, (cpfs_tuple, convenio))
            elif sub_opcao == "Excluir Apenas Contratos":
                query = """
                    DELETE FROM banco_pf.pf_contratos 
                    WHERE matricula_ref IN (
                        SELECT matricula FROM banco_pf.pf_emprego_renda 
                        WHERE cpf_ref IN %s AND convenio = %s
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
            
            # --- LAYOUT ATUALIZADO (IGUAL PESQUISA AMPLA) ---
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
                            st.session_state.update({'pf_view': 'editar', 'pf_cpf_selecionado': str(row['cpf']), 'form_loaded': False})
                            st.rerun()
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
                    novo_valor = c3.date_input("Data", value=None, min_value=date(1900,1,1), max_value=date(2050,12,31), key=f"val_{i}", format="DD/MM/YYYY", label_visibility="collapsed")
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
            st.divider()
            # --- ÁREA DE AÇÕES EM MASSA ---
            
            with st.expander("📂 Exportar Dados", expanded=False):
                # Seletor de Tipo de Exportação
                tipo_export = st.radio("Escolha o Modelo de Exportação:", ["📄 Lista Simples (Tela)", "📑 Modelo Contratos (Detalhado)"])
                
                c_btn_exp, c_info = st.columns([1, 3])
                
                if tipo_export == "📄 Lista Simples (Tela)":
                    csv_data = df_res.to_csv(sep=';', index=False, encoding='utf-8-sig')
                    c_btn_exp.download_button(label="⬇️ Baixar Lista", data=csv_data, file_name="resultado_simples.csv", mime="text/csv")
                    c_info.caption("Exporta apenas as colunas visíveis na tabela abaixo.")
                
                else:
                    if c_btn_exp.button("⚡ Gerar Relatório Contratos"):
                        # Busca todos os IDs (sem paginação) para o relatório completo
                        with st.spinner("Compilando dados de contratos... Isso pode levar alguns segundos."):
                            # Reexecuta query sem limite para pegar todos os IDs
                            df_total, _ = executar_pesquisa_ampla(regras_limpas, 1, 999999)
                            cpfs_alvo = df_total['cpf'].unique().tolist()
                            
                            df_contratos = gerar_relatorio_contratos_completo(cpfs_alvo)
                            
                            if not df_contratos.empty:
                                csv_contr = df_contratos.to_csv(sep=';', index=False, encoding='utf-8-sig')
                                st.success(f"Relatório gerado! {len(df_contratos)} registros.")
                                st.download_button(label="⬇️ Baixar Contratos", data=csv_contr, file_name="relatorio_contratos_detalhado.csv", mime="text/csv")
                            else:
                                st.warning("Nenhum contrato encontrado para os clientes filtrados.")

            # ... Restante do código de exclusão ...
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
                        if tipo_exc == "Emprego e Renda":
                            st.warning(f"Convênio: {convenio_sel} | Ação: {sub_opcao_sel}")
                            
                        c_sim, c_nao = st.columns(2)
                        
                        if c_sim.button("🚨 SIM, EXCLUIR DEFINITIVAMENTE", type="primary", key="btn_conf_exc"):
                            # Recriando a query sem limit para pegar todos os CPFs
                            df_total, _ = executar_pesquisa_ampla(regras_limpas, 1, 999999) 
                            lista_cpfs = df_total['cpf'].tolist()
                            
                            ok, msg = executar_exclusao_lote(tipo_exc, lista_cpfs, convenio_sel, sub_opcao_sel)
                            if ok:
                                st.success(msg)
                                st.session_state['confirm_delete_lote'] = False
                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error(f"Erro: {msg}")
                                
                        if c_nao.button("Cancelar", key="btn_canc_exc"):
                            st.session_state['confirm_delete_lote'] = False
                            st.rerun()
            st.divider()
            
            # Tabela de Resultados Visual
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