import streamlit as st
import pandas as pd
import json
import time
from datetime import date
import modulo_pf_cadastro as pf_core

# =============================================================================
# 1. FUNÇÕES DE BANCO (CRUD MODELOS)
# =============================================================================

def listar_modelos_ativos():
    conn = pf_core.get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT id, nome_modelo, tipo_processamento, descricao, data_criacao FROM banco_pf.pf_modelos_exportacao WHERE status='ATIVO' ORDER BY id", conn)
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def salvar_modelo(nome, tipo, desc):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO banco_pf.pf_modelos_exportacao (nome_modelo, tipo_processamento, descricao, status) VALUES (%s, %s, %s, 'ATIVO')", (nome, tipo, desc))
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

def atualizar_modelo(id_mod, nome, tipo, desc):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE banco_pf.pf_modelos_exportacao SET nome_modelo=%s, tipo_processamento=%s, descricao=%s WHERE id=%s", (nome, tipo, desc, id_mod))
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

def excluir_modelo(id_mod):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM banco_pf.pf_modelos_exportacao WHERE id=%s", (id_mod,))
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

# =============================================================================
# 2. MOTOR DE EXPORTAÇÃO (LÓGICA)
# =============================================================================

def gerar_dataframe_por_modelo(id_modelo, lista_cpfs):
    conn = pf_core.get_conn()
    if not conn: return pd.DataFrame()
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT tipo_processamento FROM banco_pf.pf_modelos_exportacao WHERE id=%s", (int(id_modelo),))
        res = cur.fetchone()
        tipo_proc = res[0] if res else 'SIMPLES'
    except:
        tipo_proc = 'SIMPLES'
    
    if tipo_proc == 'CONTRATOS_DETALHADO':
        # Agora o modelo detalhado TAMBÉM usará a lógica pivotada para contatos, 
        # mas mantendo a linha por contrato.
        return _motor_contratos_detalhado(conn, lista_cpfs)
    else:
        return _motor_simples(conn, lista_cpfs)

# --- FUNÇÃO AUXILIAR DE PIVOTAGEM ---

def _pivotar_dados(df, index_col, value_cols, prefixo_base):
    """
    Transforma linhas em colunas agrupadas sequencialmente.
    Ex: Telefone 1, Whats 1, Qualif 1, Telefone 2, Whats 2...
    """
    if df.empty: return pd.DataFrame()
    
    # 1. Numera a sequência de cada item por CPF (1, 2, 3...)
    # Garante que a ordem original do banco seja respeitada
    df['seq'] = df.groupby(index_col).cumcount() + 1
    
    # 2. Pivota a tabela
    # index=CPF, columns=Sequencia, values=[Campos]
    df_pivot = df.pivot(index=index_col, columns='seq', values=value_cols)
    
    # 3. Achata e Renomeia as colunas Hierárquicas
    # O Pandas cria um MultiIndex (Ex: ('numero', 1), ('tag_whats', 1))
    # Precisamos transformar em: "Telefone 1", "Whats 1"
    
    new_columns = []
    
    # Identifica o número máximo de itens (colunas geradas)
    max_seq = df['seq'].max()
    
    # Reorganiza para ficar agrupado: Todos do 1, depois Todos do 2...
    for i in range(1, max_seq + 1):
        for col_name in value_cols:
            # Mapeamento de nomes amigáveis
            nome_final = col_name.capitalize()
            if col_name == 'numero': nome_final = "Telefone"
            elif col_name == 'tag_whats': nome_final = "Whats"
            elif col_name == 'tag_qualificacao': nome_final = "Qualificacao"
            elif col_name == 'rua': nome_final = "Logradouro"
            
            # Tenta acessar a coluna pivotada se ela existir para aquele índice
            if (col_name, i) in df_pivot.columns:
                # Renomeia para algo plano
                # Ex: "Telefone 1"
                new_col_name = f"{nome_final} {i}"
                
                # Adiciona no dataframe final achatado (flat)
                # Mas espera... o pivot já está feito, só precisamos renomear e reordenar.
                pass 

    # Maneira mais fácil de achatar e depois reordenar:
    df_pivot.columns = [f"{c[0]}_{c[1]}" for c in df_pivot.columns]
    df_pivot = df_pivot.reset_index()
    
    # Agora reordenamos as colunas para ficar (Tel_1, Whats_1, Qualif_1, Tel_2...)
    # Em vez de (Tel_1, Tel_2... Whats_1, Whats_2...)
    cols_ordenadas = [index_col]
    
    for i in range(1, max_seq + 1):
        for col_name in value_cols:
            col_chave = f"{col_name}_{i}"
            if col_chave in df_pivot.columns:
                # Renomeia para ficar bonito
                nome_amigavel = col_name.capitalize()
                if col_name == 'numero': nome_amigavel = "Telefone"
                elif col_name == 'tag_whats': nome_amigavel = "Whats"
                elif col_name == 'tag_qualificacao': nome_amigavel = "Qualif."
                
                novo_nome = f"{nome_amigavel} {i}"
                df_pivot.rename(columns={col_chave: novo_nome}, inplace=True)
                cols_ordenadas.append(novo_nome)
                
    # Retorna apenas as colunas ordenadas e existentes
    return df_pivot[cols_ordenadas]

# --- MOTORES INTERNOS ---

def _motor_simples(conn, lista_cpfs):
    """
    Exportação Simples: 1 Linha por CPF.
    Contatos expandidos em colunas sequenciais.
    """
    if not lista_cpfs: conn.close(); return pd.DataFrame()
    
    try:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        params_cpfs = tuple(lista_cpfs)

        # 1. Dados Pessoais
        query_dados = f"SELECT id, nome, cpf, rg, data_nascimento, nome_mae FROM banco_pf.pf_dados WHERE cpf IN ({placeholders})"
        df_dados = pd.read_sql(query_dados, conn, params=params_cpfs)
        
        # 2. Telefones (Busca tudo para pivotar)
        query_tel = f"SELECT cpf_ref, numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf_ref IN ({placeholders}) ORDER BY cpf_ref, id"
        df_tel_raw = pd.read_sql(query_tel, conn, params=params_cpfs)
        df_tel_pivot = _pivotar_dados(df_tel_raw, 'cpf_ref', ['numero', 'tag_whats', 'tag_qualificacao'], 'Tel')

        # 3. Emails
        query_email = f"SELECT cpf_ref, email FROM banco_pf.pf_emails WHERE cpf_ref IN ({placeholders}) ORDER BY cpf_ref, id"
        df_email_raw = pd.read_sql(query_email, conn, params=params_cpfs)
        df_email_pivot = _pivotar_dados(df_email_raw, 'cpf_ref', ['email'], 'Email')

        # 4. Endereços
        query_end = f"SELECT cpf_ref, rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf_ref IN ({placeholders}) ORDER BY cpf_ref, id"
        df_end_raw = pd.read_sql(query_end, conn, params=params_cpfs)
        df_end_pivot = _pivotar_dados(df_end_raw, 'cpf_ref', ['rua', 'bairro', 'cidade', 'uf', 'cep'], 'End')

        # 5. Merge Geral
        df_final = df_dados.merge(df_tel_pivot, left_on='cpf', right_on='cpf_ref', how='left')\
                           .merge(df_email_pivot, left_on='cpf', right_on='cpf_ref', how='left')\
                           .merge(df_end_pivot, left_on='cpf', right_on='cpf_ref', how='left')

        cols_drop = [c for c in df_final.columns if '_ref' in c]
        df_final.drop(columns=cols_drop, inplace=True, errors='ignore')
        
        conn.close()
        return df_final
        
    except Exception as e:
        st.error(f"Erro exportação simples: {e}")
        conn.close()
        return pd.DataFrame()

def _motor_contratos_detalhado(conn, lista_cpfs):
    """
    Exportação Detalhada: 1 Linha por Contrato + Dados Pessoais + Contatos Pivotados
    """
    if not lista_cpfs: conn.close(); return pd.DataFrame()
    
    try:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        params_cpfs = tuple(lista_cpfs)

        # 1. Dados Pessoais
        df_dados = pd.read_sql(f"SELECT * FROM banco_pf.pf_dados WHERE cpf IN ({placeholders})", conn, params=params_cpfs)
        
        # 2. Contatos Pivotados (Mesma lógica do simples, mas aplicado aqui)
        # Telefones
        query_tel = f"SELECT cpf_ref, numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf_ref IN ({placeholders}) ORDER BY cpf_ref, id"
        df_tel = _pivotar_dados(pd.read_sql(query_tel, conn, params=params_cpfs), 'cpf_ref', ['numero', 'tag_whats', 'tag_qualificacao'], 'Tel')
        
        # Emails
        query_email = f"SELECT cpf_ref, email FROM banco_pf.pf_emails WHERE cpf_ref IN ({placeholders}) ORDER BY cpf_ref, id"
        df_email = _pivotar_dados(pd.read_sql(query_email, conn, params=params_cpfs), 'cpf_ref', ['email'], 'Email')
        
        # Endereços
        query_end = f"SELECT cpf_ref, rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf_ref IN ({placeholders}) ORDER BY cpf_ref, id"
        df_end = _pivotar_dados(pd.read_sql(query_end, conn, params=params_cpfs), 'cpf_ref', ['rua', 'bairro', 'cidade', 'uf', 'cep'], 'End')

        # Merge dos dados cadastrais com contatos
        df_full = df_dados.merge(df_tel, left_on='cpf', right_on='cpf_ref', how='left')\
                          .merge(df_email, left_on='cpf', right_on='cpf_ref', how='left')\
                          .merge(df_end, left_on='cpf', right_on='cpf_ref', how='left')
        
        cols_drop = [c for c in df_full.columns if '_ref' in c]; df_full.drop(columns=cols_drop, inplace=True, errors='ignore')

        # 3. Empregos
        df_emp = pd.read_sql(f"SELECT cpf_ref, convenio, matricula FROM banco_pf.pf_emprego_renda WHERE cpf_ref IN ({placeholders})", conn, params=params_cpfs)
        
        # 4. Contratos Dinâmicos
        cur = conn.cursor()
        cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE (table_schema = 'banco_pf' OR table_schema = 'admin') AND table_name LIKE 'pf_contratos%'")
        tabelas_contratos = cur.fetchall()
        
        df_contratos_final = pd.DataFrame()
        if not df_emp.empty:
            mats = df_emp['matricula'].dropna().unique().tolist()
            if mats:
                ph_m = ",".join(["%s"] * len(mats))
                for schema, tabela in tabelas_contratos:
                    try:
                        df_t = pd.read_sql(f"SELECT *, '{tabela}' as origem FROM {schema}.{tabela} WHERE matricula_ref IN ({ph_m})", conn, params=tuple(mats))
                        df_t = df_t.drop(columns=['id', 'data_criacao', 'data_atualizacao', 'importacao_id'], errors='ignore')
                        df_contratos_final = pd.concat([df_contratos_final, df_t], ignore_index=True)
                    except: continue

        # Consolidação
        df_base = df_full.merge(df_emp, left_on='cpf', right_on='cpf_ref', how='inner')
        
        if not df_contratos_final.empty:
            df_export = df_base.merge(df_contratos_final, left_on='matricula', right_on='matricula_ref', how='inner')
        else:
            df_export = pd.DataFrame() # Sem contratos

        if not df_export.empty:
            cols_drop = [c for c in df_export.columns if '_ref' in c or c == 'id']; df_export.drop(columns=cols_drop, inplace=True, errors='ignore')
            
            # Reordena para priorizar identificação e contrato, jogando contatos pro final ou meio
            cols = list(df_export.columns)
            pri = ['nome', 'cpf', 'convenio', 'matricula', 'contrato']
            final_cols = [c for c in pri if c in cols] + [c for c in cols if c not in pri]
            df_export = df_export[final_cols]
            
        conn.close()
        return df_export
    except: 
        conn.close(); return pd.DataFrame()

# =============================================================================
# 3. INTERFACE DE GESTÃO (TELA "MODELOS")
# =============================================================================

@st.dialog("✏️ Editar Modelo")
def dialog_editar_modelo(modelo):
    with st.form("form_edit_mod"):
        n_nome = st.text_input("Nome", value=modelo['nome_modelo'])
        tipos = ["SIMPLES", "CONTRATOS_DETALHADO"]
        idx_t = tipos.index(modelo['tipo_processamento']) if modelo['tipo_processamento'] in tipos else 0
        n_tipo = st.selectbox("Tipo de Processamento", tipos, index=idx_t)
        n_desc = st.text_area("Descrição", value=modelo['descricao'])
        
        if st.form_submit_button("Salvar"):
            if atualizar_modelo(modelo['id'], n_nome, n_tipo, n_desc):
                st.success("Salvo!"); st.rerun()

@st.dialog("⚠️ Excluir Modelo")
def dialog_excluir_modelo(id_mod):
    st.error("Tem certeza que deseja excluir este modelo?")
    c1, c2 = st.columns(2)
    if c1.button("Sim, Excluir"):
        if excluir_modelo(id_mod): st.success("Excluído!"); time.sleep(1); st.rerun()
    if c2.button("Cancelar"): st.rerun()

def app_gestao_modelos():
    st.markdown("## 📤 Gestão de Modelos de Exportação")
    
    with st.expander("➕ Criar Novo Modelo"):
        with st.form("form_new_mod"):
            c1, c2 = st.columns(2)
            new_nome = c1.text_input("Nome do Modelo")
            new_tipo = c2.selectbox("Tipo de Processamento", ["SIMPLES", "CONTRATOS_DETALHADO"])
            new_desc = st.text_area("Descrição (Opcional)")
            if st.form_submit_button("Criar Modelo"):
                if new_nome:
                    if salvar_modelo(new_nome, new_tipo, new_desc):
                        st.success("Criado!"); time.sleep(1); st.rerun()
                else: st.warning("Nome obrigatório.")

    st.markdown("### Modelos Disponíveis")
    df = listar_modelos_ativos()
    
    if not df.empty:
        for _, row in df.iterrows():
            with st.expander(f"📑 {row['nome_modelo']} ({row['tipo_processamento']})"):
                st.write(f"**Descrição:** {row['descricao']}")
                st.caption(f"Criado em: {row['data_criacao']}")
                
                c1, c2 = st.columns([1, 5])
                if c1.button("✏️ Editar", key=f"ed_mod_{row['id']}"):
                    dialog_editar_modelo(row)
                if c2.button("🗑️ Excluir", key=f"del_mod_{row['id']}"):
                    dialog_excluir_modelo(row['id'])
    else:
        st.info("Nenhum modelo cadastrado.")