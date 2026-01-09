import streamlit as st
import pandas as pd
from datetime import date
import re
import time
import json
import modulo_pf_cadastro as pf_core
import modulo_pf_config_exportacao as pf_export

# =============================================================================
# 1. FUNÇÕES DE CONTROLE (O GERENTE)
# =============================================================================

def navegar_visualizar(cpf):
    """Callback para ir para tela de visualização"""
    st.session_state['pf_view'] = 'visualizar'
    st.session_state['pf_cpf_selecionado'] = str(cpf)

def navegar_editar(cpf):
    """Callback para ir para tela de edição"""
    st.session_state['pf_view'] = 'editar'
    st.session_state['pf_cpf_selecionado'] = str(cpf)
    st.session_state['form_loaded'] = False

def voltar_para_lista():
    """Callback para voltar para a lista"""
    st.session_state['pf_view'] = 'lista'

def app_gestao_pesquisa():
    """
    Função principal que gerencia a navegação interna deste módulo.
    Torna o módulo de pesquisa 'independente' para gerenciar suas telas.
    """
    # Define estado inicial se não existir
    if 'pf_view' not in st.session_state:
        st.session_state['pf_view'] = 'lista'
        
    # ROTEAMENTO INTERNO
    view_atual = st.session_state['pf_view']

    if view_atual == 'lista':
        interface_pesquisa_ampla()
    
    elif view_atual == 'visualizar':
        # Chama a visualização do módulo de cadastro
        if hasattr(pf_core, 'interface_visualizar_cliente'):
            pf_core.interface_visualizar_cliente()
        else:
            st.error("Função de visualização não encontrada no módulo de cadastro.")
            if st.button("Voltar"): voltar_para_lista(); st.rerun()

    elif view_atual == 'editar':
        # Chama a edição do módulo de cadastro
        pf_core.interface_cadastro_pf()
    
    elif view_atual == 'novo':
         pf_core.interface_cadastro_pf()

# =============================================================================
# 2. CONFIGURAÇÕES E BANCO DE DADOS
# =============================================================================

CAMPOS_CONFIG = {
    "Dados Pessoais": [
        {"label": "Nome", "coluna": "d.nome", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CPF", "coluna": "d.cpf", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Nascimento", "coluna": "d.data_nascimento", "tipo": "data", "tabela": "banco_pf.pf_dados"},
        {"label": "Idade (Cálculo)", "coluna": "virtual_idade", "tipo": "numero", "tabela": "banco_pf.pf_dados"},
        {"label": "RG", "coluna": "d.rg", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "UF RG", "coluna": "d.uf_rg", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Exp. RG", "coluna": "d.data_exp_rg", "tipo": "data", "tabela": "banco_pf.pf_dados"},
        {"label": "CNH", "coluna": "d.cnh", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "PIS", "coluna": "d.pis", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CTPS/Série", "coluna": "d.ctps_serie", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome da Mãe", "coluna": "d.nome_mae", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome do Pai", "coluna": "d.nome_pai", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome Procurador", "coluna": "d.nome_procurador", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CPF Procurador", "coluna": "d.cpf_procurador", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "ID Importação", "coluna": "d.importacao_id", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "ID Campanha", "coluna": "d.id_campanha", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Criação (Reg)", "coluna": "d.data_criacao", "tipo": "data", "tabela": "banco_pf.pf_dados"}
    ],
    "Endereços": [
        {"label": "Logradouro", "coluna": "ende.rua", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "Bairro", "coluna": "ende.bairro", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "Cidade", "coluna": "ende.cidade", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "UF", "coluna": "ende.uf", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "CEP", "coluna": "ende.cep", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"}
    ],
    "Contatos": [
        {"label": "DDD (Telefone)", "coluna": "virtual_ddd", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Telefone (Número)", "coluna": "tel.numero", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Tag WhatsApp", "coluna": "tel.tag_whats", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Tag Qualificação", "coluna": "tel.tag_qualificacao", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Data Atualização (Tel)", "coluna": "tel.data_atualizacao", "tipo": "data", "tabela": "banco_pf.pf_telefones"},
        {"label": "E-mail", "coluna": "em.email", "tipo": "texto", "tabela": "banco_pf.pf_emails"}
    ],
    "Profissional (Geral)": [
        {"label": "Matrícula (Geral)", "coluna": "emp.matricula", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Convênio (Geral)", "coluna": "emp.convenio", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Contrato Empréstimo", "coluna": "ctr.contrato", "tipo": "texto", "tabela": "banco_pf.pf_contratos"}
    ],
    "Contratos CLT / CAGED": [
        {"label": "Matrícula", "coluna": "clt.matricula", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Convênio", "coluna": "clt.convenio", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Nome Empresa", "coluna": "clt.cnpj_nome", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CNPJ", "coluna": "clt.cnpj_numero", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "TAG (Destaque)", "coluna": "clt.tag", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Qtd Funcionários", "coluna": "clt.qtd_funcionarios", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Abertura Empresa", "coluna": "clt.data_abertura_empresa", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Tempo Abertura (Anos)", "coluna": "clt.tempo_abertura_anos", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Admissão", "coluna": "clt.data_admissao", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Tempo Admissão (Anos)", "coluna": "clt.tempo_admissao_anos", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Início Emprego", "coluna": "clt.data_inicio_emprego", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Tempo Início (Anos)", "coluna": "clt.tempo_inicio_emprego_anos", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CNAE Nome", "coluna": "clt.cnae_nome", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CNAE Código", "coluna": "clt.cnae_codigo", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CBO Nome", "coluna": "clt.cbo_nome", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CBO Código", "coluna": "clt.cbo_codigo", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "ID Importação (CLT)", "coluna": "clt.importacao_id", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Criação (CLT)", "coluna": "clt.data_criacao", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Atualização (CLT)", "coluna": "clt.data_atualizacao", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"}
    ]
}

def buscar_pf_simples(termo, filtro_importacao_id=None, pagina=1, itens_por_pagina=50):
    conn = pf_core.get_conn()
    if conn:
        try:
            termo_limpo = pf_core.limpar_normalizar_cpf(termo)
            param_nome = f"%{termo}%"
            sql_base = "SELECT d.id, d.nome, d.cpf, d.data_nascimento FROM banco_pf.pf_dados d "
            conds = ["d.nome ILIKE %s"]
            params = [param_nome]
            if termo_limpo: 
                sql_base += " LEFT JOIN banco_pf.pf_telefones t ON d.cpf = t.cpf" 
                conds.append("d.cpf ILIKE %s")
                conds.append("t.numero ILIKE %s")
                params.append(f"%{termo_limpo}%")
                params.append(f"%{termo_limpo}%")
            
            where = " WHERE " + " OR ".join(conds)
            cur = conn.cursor()
            part_from = sql_base.split('FROM', 1)[1]
            cur.execute(f"SELECT COUNT(DISTINCT d.id) FROM {part_from} {where}", tuple(params))
            total = cur.fetchone()[0]
            offset = (pagina-1)*itens_por_pagina
            df = pd.read_sql(f"{sql_base} {where} GROUP BY d.id ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}", conn, params=tuple(params))
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
                if tabela == 'banco_pf.pf_contratos_clt': tabela = 'banco_pf.pf_matricula_dados_clt'
                if tabela in joins_map and joins_map[tabela] not in active_joins: active_joins.append(joins_map[tabela])
                col_sql = f"{coluna}"
                if coluna == 'virtual_idade': col_sql = "EXTRACT(YEAR FROM AGE(d.data_nascimento))"
                if coluna == 'virtual_ddd': col_sql = "SUBSTRING(tel.numero, 1, 2)"

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
            count_sql = f"SELECT COUNT(DISTINCT d.id) {sql_from} {full_joins} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total = cur.fetchone()[0]
            offset = (pagina - 1) * itens_por_pagina
            limit_clause = f"LIMIT {itens_por_pagina} OFFSET {offset}" if itens_por_pagina < 9999999 else ""
            query = f"{sql_select} {sql_from} {full_joins} {sql_where} ORDER BY d.nome {limit_clause}"
            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close()
            return df.fillna(""), total
        except Exception as e: 
            st.error(f"Erro SQL: {e}")
            return pd.DataFrame(), 0
    return pd.DataFrame(), 0

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
            query = "DELETE FROM banco_pf.pf_telefones WHERE cpf IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "E-mails":
            query = "DELETE FROM banco_pf.pf_emails WHERE cpf IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "Endereços":
            query = "DELETE FROM banco_pf.pf_enderecos WHERE cpf IN %s"
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
        conn.commit(); conn.close()
        return True, f"Operação realizada com sucesso! {registros} registros afetados."
    except Exception as e:
        if conn: conn.close()
        return False, f"Erro na execução: {e}"

# --- FUNÇÕES PARA "TIPOS DE FILTRO" (MODELO FIXO) ---
def listar_tabelas_pf(conn):
    try:
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'banco_pf' AND table_name LIKE 'pf_%' ORDER BY table_name"
        cur = conn.cursor(); cur.execute(query); res = [r[0] for r in cur.fetchall()]; return res
    except: return []

def listar_colunas_tabela(conn, tabela):
    try:
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'banco_pf' AND table_name = %s ORDER BY ordinal_position"
        cur = conn.cursor(); cur.execute(query, (tabela,)); res = [r[0] for r in cur.fetchall()]; return res
    except: return []

def salvar_modelo_fixo(nome, tabela, lista_colunas, resumo):
    conn = pf_core.get_conn()
    if conn:
        try:
            colunas_json = json.dumps(lista_colunas); cur = conn.cursor()
            cur.execute("INSERT INTO banco_pf.pf_modelos_filtro_fixo (nome_modelo, tabela_alvo, coluna_alvo, resumo) VALUES (%s, %s, %s, %s)", (nome, tabela, colunas_json, resumo))
            conn.commit(); conn.close(); return True
        except Exception as e: st.error(f"Erro ao salvar: {e}"); conn.close()
    return False

def listar_modelos_fixos():
    conn = pf_core.get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT * FROM banco_pf.pf_modelos_filtro_fixo ORDER BY id DESC", conn); conn.close(); return df
        except: conn.close()
    return pd.DataFrame()

def executar_filtro_fixo(tabela, colunas_alvo):
    conn = pf_core.get_conn()
    if conn:
        try:
            try:
                lista_cols = json.loads(colunas_alvo)
                if not isinstance(lista_cols, list): lista_cols = [str(lista_cols)]
            except: lista_cols = [str(colunas_alvo)]
            cols_str = ", ".join(lista_cols); query = f"SELECT DISTINCT {cols_str} FROM banco_pf.{tabela} ORDER BY {lista_cols[0]}"
            df = pd.read_sql(query, conn); conn.close(); return df
        except Exception as e: st.error(f"Erro ao executar filtro: {e}"); conn.close()
    return pd.DataFrame()

@st.dialog("📂 Tipos de Filtro (Modelos Fixos)", width="large")
def dialog_tipos_filtro():
    t1, t2 = st.tabs(["CRIAR MODELO", "CONSULTAR FILTRO"])
    with t1:
        st.markdown("#### 🆕 Novo Modelo de Filtro")
        conn = pf_core.get_conn()
        if conn:
            tabelas = listar_tabelas_pf(conn)
            sel_tabela = st.selectbox("1. Selecione a Lista (Tabela SQL)", options=tabelas)
            cols = []
            if sel_tabela: cols = listar_colunas_tabela(conn, sel_tabela)
            sel_colunas = st.multiselect("2. Selecione os Itens (Cabeçalhos)", options=cols); conn.close(); st.divider()
            nome_modelo = st.text_input("3. Nome do Modelo de Filtro"); resumo_modelo = st.text_area("4. Resumo / Descrição")
            if st.button("💾 Salvar Modelo"):
                if nome_modelo and sel_tabela and sel_colunas:
                    if salvar_modelo_fixo(nome_modelo, sel_tabela, sel_colunas, resumo_modelo): st.success("Modelo salvo com sucesso!"); time.sleep(1); st.rerun()
                else: st.warning("Preencha todos os campos e selecione ao menos uma coluna.")
        else: st.error("Erro de conexão.")
    with t2:
        st.markdown("#### 🔎 Consultar Dados por Modelo")
        df_modelos = listar_modelos_fixos()
        if not df_modelos.empty:
            opcoes = df_modelos.apply(lambda x: f"{x['id']} - {x['nome_modelo']}", axis=1); sel_modelo = st.selectbox("Selecione o Modelo", options=opcoes)
            if sel_modelo:
                id_sel = int(sel_modelo.split(' - ')[0]); modelo = df_modelos[df_modelos['id'] == id_sel].iloc[0]; st.info(f"**Resumo:** {modelo['resumo']}")
                try: cols_list = json.loads(modelo['coluna_alvo']); cols_str = ", ".join(cols_list)
                except: cols_str = str(modelo['coluna_alvo'])
                st.caption(f"Fonte: Tabela `{modelo['tabela_alvo']}` | Colunas: `{cols_str}`")
                if st.button("👁️ Ver Resultado (Dados Únicos)"):
                    with st.spinner("Buscando dados únicos..."): df_res = executar_filtro_fixo(modelo['tabela_alvo'], modelo['coluna_alvo'])
                    if not df_res.empty: st.dataframe(df_res, use_container_width=True, hide_index=True); st.write(f"Total de itens únicos: {len(df_res)}")
                    else: st.warning("Nenhum dado encontrado.")
        else: st.info("Nenhum modelo cadastrado ainda.")

# =============================================================================
# 3. INTERFACES VISUAIS (A LISTA E A PESQUISA AMPLA)
# =============================================================================

def interface_pesquisa_ampla():
    c_voltar, c_tipos, c_limpar, c_spacer = st.columns([1, 1.5, 1.5, 5])
    
    # Botão voltar agora apenas zera a visualização
    # (Pois já estamos dentro do app_gestao_pesquisa)
    if c_voltar.button("⬅️ Voltar"): 
        st.session_state['regras_pesquisa'] = []
        st.session_state['executar_busca'] = False
        st.session_state['pf_view'] = 'lista'
        st.rerun()
        
    if c_tipos.button("📂 Tipos de Filtro"): dialog_tipos_filtro()
    if c_limpar.button("🗑️ Limpar Filtros"): 
        st.session_state['regras_pesquisa'] = []
        st.session_state['executar_busca'] = False
        st.session_state['pf_pagina_atual'] = 1
        if 'cache_export_ampla' in st.session_state: del st.session_state['cache_export_ampla']
        st.rerun()
    
    st.divider()
    conn = pf_core.get_conn(); ops_cache = {'texto': [], 'numero': [], 'data': []}; lista_convenios = []
    if conn:
        try:
            df_ops = pd.read_sql("SELECT tipo, simbolo, descricao FROM banco_pf.pf_operadores_de_filtro", conn)
            for _, r in df_ops.iterrows(): ops_cache[r['tipo']].append(f"{r['simbolo']} : {r['descricao']}")
            cur = conn.cursor(); cur.execute("SELECT DISTINCT convenio FROM banco_pf.pf_emprego_renda WHERE convenio IS NOT NULL ORDER BY convenio"); lista_convenios = [r[0] for r in cur.fetchall()]; cur.close()
        except: pass
        conn.close()
    
    # --- ÁREA DE SELEÇÃO DE FILTROS ---
    c_menu, c_regras = st.columns([4, 2]) 
    with c_menu:
        st.markdown("### 🗂️ Campos Disponíveis")
        termo_filtro = st.text_input("🔍 Filtrar campos (Digite para buscar...)", key="filtro_campos_ampla")
        for grupo, campos in CAMPOS_CONFIG.items():
            campos_filtrados = [c for c in campos if termo_filtro.lower() in c['label'].lower()]
            if campos_filtrados:
                with st.expander(grupo, expanded=bool(termo_filtro)):
                    colunas_botoes = st.columns(4)
                    for idx, campo in enumerate(campos_filtrados):
                        with colunas_botoes[idx % 4]: 
                            if st.button(f"➕ {campo['label']}", key=f"add_{campo['coluna']}", use_container_width=True):
                                st.session_state['regras_pesquisa'].append({'label': campo['label'], 'coluna': campo['coluna'], 'tabela': campo['tabela'], 'tipo': campo['tipo'], 'operador': None, 'valor': ''}); st.rerun()

    with c_regras:
        st.markdown("### 🎯 Regras Ativas")
        if not st.session_state['regras_pesquisa']: st.info("Nenhuma regra. Selecione ao lado.")
        regras_rem = []
        for i, regra in enumerate(st.session_state['regras_pesquisa']):
            with st.container(border=True):
                st.caption(f"**{regra['label']}**"); c_op, c_val, c_del = st.columns([2, 3, 1])
                opcoes = ops_cache.get(regra['tipo'], []); idx_sel = opcoes.index(regra['operador']) if regra['operador'] in opcoes else 0
                novo_op_full = c_op.selectbox("Op.", opcoes, index=idx_sel, key=f"op_{i}", label_visibility="collapsed")
                novo_op_simbolo = novo_op_full.split(' : ')[0] if novo_op_full else "="
                if novo_op_simbolo == '∅': c_val.text_input("Valor", value="[Vazio]", disabled=True, key=f"val_{i}", label_visibility="collapsed"); novo_valor = None
                elif regra['tipo'] == 'data': 
                    novo_valor = c_val.date_input("Data", value=None, min_value=date(1900,1,1), max_value=date(2050,12,31), key=f"val_{i}", format="DD/MM/YYYY", label_visibility="collapsed")
                else: novo_valor = c_val.text_input("Valor", value=regra['valor'], key=f"val_{i}", label_visibility="collapsed")
                st.session_state['regras_pesquisa'][i]['operador'] = novo_op_full; st.session_state['regras_pesquisa'][i]['valor'] = novo_valor
                if c_del.button("🗑️", key=f"del_{i}"): regras_rem.append(i)
        if regras_rem:
            for idx in sorted(regras_rem, reverse=True): st.session_state['regras_pesquisa'].pop(idx)
            st.rerun()
        st.divider()
        if st.button("🔎 FILTRAR AGORA", type="primary", use_container_width=True): st.session_state['executar_busca'] = True

    # --- RESULTADOS DA BUSCA ---
    if st.session_state.get('executar_busca'):
        regras_limpas = []
        for r in st.session_state['regras_pesquisa']:
            r_copy = r.copy()
            if r_copy['operador']: r_copy['operador'] = r_copy['operador'].split(' : ')[0]
            regras_limpas.append(r_copy)
        
        df_res, total = executar_pesquisa_ampla(regras_limpas, st.session_state.get('pf_pagina_atual', 1))
        st.write(f"**Resultados:** {total}")
        
        if not df_res.empty:
            st.divider()

            # --- ÁREA DE EXPORTAÇÃO ---
            with st.expander("📂 Exportar Dados", expanded=False):
                df_modelos = pf_export.listar_modelos_ativos()
                if not df_modelos.empty:
                    c_sel, c_btn = st.columns([3, 1])
                    opcoes_mods = df_modelos.apply(lambda x: f"{x['id']} - {x['nome_modelo']}", axis=1)
                    idx_mod = c_sel.selectbox("Modelo:", range(len(df_modelos)), format_func=lambda x: opcoes_mods[x], key="mod_ampla")
                    if c_btn.button("⬇️ Gerar Arquivo", key="btn_ampla_exp"):
                        # ... Lógica simplificada de exportação para brevidade ...
                        pass

            st.divider()
            st.markdown("""<div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex;"><div style="flex: 2;">Ações</div><div style="flex: 1;">ID</div><div style="flex: 2;">CPF</div><div style="flex: 4;">Nome</div></div>""", unsafe_allow_html=True)
            
            for _, row in df_res.iterrows():
                c1, c2, c3, c4 = st.columns([2, 1, 2, 4])
                with c1:
                    b1, b2, b3 = st.columns(3)
                    
                    # --- BOTÕES COM CALLBACK (CORRIGIDOS) ---
                    # Usa on_click para garantir que o estado mude ANTES do rerun
                    b1.button("👁️", key=f"v_{row['id']}", on_click=navegar_visualizar, args=(row['cpf'],))
                    b2.button("✏️", key=f"e_{row['id']}", on_click=navegar_editar, args=(row['cpf'],))
                    
                    if b3.button("🗑️", key=f"d_{row['id']}"): 
                        pf_core.dialog_excluir_pf(str(row['cpf']), row['nome'])
                
                c2.write(str(row['id']))
                c3.write(pf_core.formatar_cpf_visual(row['cpf']))
                c4.write(row['nome'])
                st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
            
            cp1, cp2, cp3 = st.columns([1, 3, 1])
            if cp1.button("⬅️ Ant.") and st.session_state.get('pf_pagina_atual', 1) > 1: st.session_state['pf_pagina_atual'] -= 1; st.rerun()
            if cp3.button("Próx. ➡️"): st.session_state['pf_pagina_atual'] = st.session_state.get('pf_pagina_atual', 1) + 1; st.rerun()
        else: st.warning("Nenhum registro encontrado.")