import streamlit as st
import pandas as pd
import modulo_pf_cadastro as pf_core

# =============================================================================
# 1. FUNÇÕES DE BANCO (CRUD MODELOS) - ATUALIZADO PARA NOVA COLUNA
# =============================================================================

def listar_modelos_ativos():
    conn = pf_core.get_conn()
    if conn:
        try:
            # Seleciona a nova coluna 'codigo_de_consulta' em vez da antiga
            df = pd.read_sql("SELECT id, nome_modelo, descricao, data_criacao, status, codigo_de_consulta FROM banco_pf.pf_modelos_exportacao WHERE status='ATIVO' ORDER BY id", conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Erro ao listar modelos: {e}")
            conn.close()
    return pd.DataFrame()

def salvar_modelo(nome, chave, desc):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            # CORREÇÃO: SQL agora aponta para 'codigo_de_consulta'
            sql = """
                INSERT INTO banco_pf.pf_modelos_exportacao 
                (nome_modelo, codigo_de_consulta, descricao, status, data_criacao) 
                VALUES (%s, %s, %s, 'ATIVO', CURRENT_DATE)
            """
            cur.execute(sql, (nome, chave, desc))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro técnico ao salvar no banco: {e}")
            conn.close()
            return False
    return False

def atualizar_modelo(id_mod, nome, chave, desc):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            # CORREÇÃO: UPDATE agora aponta para 'codigo_de_consulta'
            sql = """
                UPDATE banco_pf.pf_modelos_exportacao 
                SET nome_modelo=%s, codigo_de_consulta=%s, descricao=%s 
                WHERE id=%s
            """
            cur.execute(sql, (nome, chave, desc, id_mod))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}")
            conn.close()
            return False
    return False

def excluir_modelo(id_mod):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM banco_pf.pf_modelos_exportacao WHERE id=%s", (id_mod,))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    return False

# =============================================================================
# 2. MOTOR DE EXPORTAÇÃO FIXO (10 TELS / 3 EMAILS / 3 ENDS)
# ==========================================================

def gerar_dataframe_por_modelo(id_modelo, lista_cpfs):
    conn = pf_core.get_conn()
    if not conn or not lista_cpfs: 
        return pd.DataFrame()
    
    try:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        params = tuple(lista_cpfs)

        # 1. BUSCA DADOS PESSOAIS (pf_dados)
        df_dados = pd.read_sql(f"SELECT * FROM banco_pf.pf_dados WHERE cpf IN ({placeholders})", conn, params=params)
        df_dados['cpf'] = df_dados['cpf'].apply(pf_core.formatar_cpf_visual)

        # 2. BUSCA TELEFONES (pf_telefones)
        df_tel = pd.read_sql(f"SELECT cpf, numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf IN ({placeholders})", conn, params=params)
        df_tel['numero'] = df_tel['numero'].apply(lambda x: pf_core.limpar_apenas_numeros(x))
        
        # 3. BUSCA E-MAILS (pf_emails)
        df_mail = pd.read_sql(f"SELECT cpf, email FROM banco_pf.pf_emails WHERE cpf IN ({placeholders})", conn, params=params)

        # 4. BUSCA ENDEREÇOS (pf_enderecos)
        df_end = pd.read_sql(f"SELECT cpf, rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf IN ({placeholders})", conn, params=params)

        # LÓGICA DE PIVOTAGEM COM COLUNAS FIXAS
        def pivotar_fixo(df, qtd_max, col_id='cpf'):
            if df.empty:
                return pd.DataFrame(columns=[col_id])
            
            df['seq'] = df.groupby(col_id).cumcount() + 1
            df = df[df['seq'] <= qtd_max] 
            
            df_pivot = df.pivot(index=col_id, columns='seq')
            df_pivot.columns = [f"{c[0]}_{c[1]}" for c in df_pivot.columns]
            df_pivot = df_pivot.reset_index()

            colunas_originais = [c for c in df.columns if c not in [col_id, 'seq']]
            for i in range(1, qtd_max + 1):
                for col in colunas_originais:
                    nome_col = f"{col}_{i}"
                    if nome_col not in df_pivot.columns:
                        df_pivot[nome_col] = ""
            return df_pivot

        df_tel_p = pivotar_fixo(df_tel, 10)
        df_mail_p = pivotar_fixo(df_mail, 3)
        df_end_p = pivotar_fixo(df_end, 3)

        # 5. MERGE FINAL E FORMATAÇÃO (TUDO MAIÚSCULO)
        df_final = df_dados.merge(df_tel_p, on='cpf', how='left')\
                           .merge(df_mail_p, on='cpf', how='left')\
                           .merge(df_end_p, on='cpf', how='left')

        df_final = df_final.astype(str).apply(lambda x: x.str.upper())
        df_final = df_final.replace(['NONE', 'NAN', 'NAT', '#N/D', 'NULL'], '')

        conn.close()
        return df_final

    except Exception as e:
        st.error(f"Erro na exportação fixa: {e}")
        if conn: conn.close()
        return pd.DataFrame()