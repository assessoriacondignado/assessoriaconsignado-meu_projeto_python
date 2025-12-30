import streamlit as st
import pandas as pd
import modulo_pf_cadastro as pf_core

def gerar_dataframe_por_modelo(id_modelo, lista_cpfs):
    conn = pf_core.get_conn()
    if not conn or not lista_cpfs: 
        return pd.DataFrame()
    
    try:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        params = tuple(lista_cpfs)

        # 1. BUSCA DADOS PESSOAIS (pf_dados)
        df_dados = pd.read_sql(f"SELECT * FROM banco_pf.pf_dados WHERE cpf IN ({placeholders})", conn, params=params)
        # Formata CPF com ponto e traço
        df_dados['cpf'] = df_dados['cpf'].apply(pf_core.formatar_cpf_visual)

        # 2. BUSCA TELEFONES (pf_telefones) - Limite fixo de 10
        df_tel = pd.read_sql(f"SELECT cpf, numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf IN ({placeholders})", conn, params=params)
        # Limpa telefone para apenas DDD+Número
        df_tel['numero'] = df_tel['numero'].apply(lambda x: pf_core.limpar_apenas_numeros(x))
        
        # 3. BUSCA E-MAILS (pf_emails) - Limite fixo de 3
        df_mail = pd.read_sql(f"SELECT cpf, email FROM banco_pf.pf_emails WHERE cpf IN ({placeholders})", conn, params=params)

        # 4. BUSCA ENDEREÇOS (pf_enderecos) - Limite fixo de 3
        df_end = pd.read_sql(f"SELECT cpf, rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf IN ({placeholders})", conn, params=params)

        # --- LÓGICA DE PIVOTAGEM COM COLUNAS FIXAS ---
        def pivotar_fixo(df, col_prefix, qtd_max, col_id='cpf'):
            if df.empty:
                df = pd.DataFrame(columns=[col_id])
            
            df['seq'] = df.groupby(col_id).cumcount() + 1
            df = df[df['seq'] <= qtd_max] # Garante que não ultrapasse o limite fixo
            
            # Cria colunas vazias para garantir o layout mesmo sem dados
            df_pivot = df.pivot(index=col_id, columns='seq')
            df_pivot.columns = [f"{c[0]}_{c[1]}" for c in df_pivot.columns]
            df_pivot = df_pivot.reset_index()

            # Força a existência de todas as colunas até a qtd_max
            colunas_originais = [c for c in df.columns if c not in [col_id, 'seq']]
            for i in range(1, qtd_max + 1):
                for col in colunas_originais:
                    nome_col = f"{col}_{i}"
                    if nome_col not in df_pivot.columns:
                        df_pivot[nome_col] = ""
            return df_pivot

        df_tel_p = pivotar_fixo(df_tel, "Tel", 10)
        df_mail_p = pivotar_fixo(df_mail, "Email", 3)
        df_end_p = pivotar_fixo(df_end, "End", 3)

        # 5. MERGE FINAL E FORMATAÇÃO
        df_final = df_dados.merge(df_tel_p, on='cpf', how='left')\
                           .merge(df_mail_p, on='cpf', how='left')\
                           .merge(df_end_p, on='cpf', how='left')

        # PADRONIZAÇÃO: Maiúsculo e remover nulos/#N/D
        df_final = df_final.astype(str).apply(lambda x: x.str.upper())
        df_final = df_final.replace(['NONE', 'NAN', 'NAT', '#N/D', 'NULL'], '')

        conn.close()
        return df_final

    except Exception as e:
        st.error(f"Erro na exportação fixa: {e}")
        if conn: conn.close()
        return pd.DataFrame()