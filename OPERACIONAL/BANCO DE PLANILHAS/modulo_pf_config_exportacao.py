import streamlit as st
import pandas as pd
import modulo_pf_cadastro as pf_core

# =============================================================================
# 1. FUNÇÕES DE BANCO (CRUD MODELOS)
# =============================================================================

def listar_modelos_ativos():
    conn = pf_core.get_conn()
    if conn:
        try:
            # Seleciona os modelos ativos apontando para a nova coluna de chave técnica
            query = "SELECT id, nome_modelo, descricao, data_criacao, status, codigo_de_consulta FROM banco_pf.pf_modelos_exportacao WHERE status='ATIVO' ORDER BY id"
            df = pd.read_sql(query, conn)
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
            # Insere o novo modelo vinculando o nome comercial à chave técnica (codigo_de_consulta)
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
            st.error(f"Erro ao salvar no banco: {e}")
            conn.close()
    return False

def atualizar_modelo(id_mod, nome, chave, desc):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
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

# =============================================================================
# 2. MOTOR DE EXPORTAÇÃO (LÓGICA DE ROTAS E LAYOUT FIXO)
# =============================================================================

def gerar_dataframe_por_modelo(id_modelo, lista_cpfs):
    """Encaminha a solicitação para o motor correto baseado na chave do modelo"""
    conn = pf_core.get_conn()
    if not conn or not lista_cpfs: 
        return pd.DataFrame()
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo_de_consulta FROM banco_pf.pf_modelos_exportacao WHERE id=%s", (int(id_modelo),))
        res = cur.fetchone()
        chave = res[0] if res else 'Dados_Cadastrais_Simples'
        
        # Mapeamento de chaves para funções de processamento
        if chave == 'Dados_Cadastrais_Simples':
            return _motor_layout_fixo_completo(conn, lista_cpfs)
        else:
            # Caso a chave não seja reconhecida, usa o motor fixo como padrão
            return _motor_layout_fixo_completo(conn, lista_cpfs)
            
    except Exception as e:
        st.error(f"Erro no roteamento de exportação: {e}")
        return pd.DataFrame()

def _motor_layout_fixo_completo(conn, lista_cpfs):
    """Processa a exportação com colunas fixas: 10 Tels, 3 Emails, 3 Endereços"""
    try:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        params = tuple(lista_cpfs)

        # 1. Dados Pessoais (pf_dados)
        df_dados = pd.read_sql(f"SELECT * FROM banco_pf.pf_dados WHERE cpf IN ({placeholders})", conn, params=params)
        df_dados['cpf'] = df_dados['cpf'].apply(pf_core.formatar_cpf_visual)

        # 2. Dados Satélites
        df_tel = pd.read_sql(f"SELECT cpf, numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf IN ({placeholders})", conn, params=params)
        df_tel['numero'] = df_tel['numero'].apply(lambda x: pf_core.limpar_apenas_numeros(x))
        
        df_mail = pd.read_sql(f"SELECT cpf, email FROM banco_pf.pf_emails WHERE cpf IN ({placeholders})", conn, params=params)
        df_end = pd.read_sql(f"SELECT cpf, rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf IN ({placeholders})", conn, params=params)

        # Função interna para garantir o número fixo de colunas (Pivotagem)
        def pivotar_para_layout_fixo(df, col_id, qtd_max):
            if df.empty:
                return pd.DataFrame(columns=[col_id])
            
            df['seq'] = df.groupby(col_id).cumcount() + 1
            df = df[df['seq'] <= qtd_max] 
            
            df_pivot = df.pivot(index=col_id, columns='seq')
            df_pivot.columns = [f"{c[0]}_{c[1]}" for c in df_pivot.columns]
            df_pivot = df_pivot.reset_index()

            # Força a criação das colunas vazias se o cliente tiver menos dados que o máximo
            colunas_originais = [c for c in df.columns if c not in [col_id, 'seq']]
            for i in range(1, qtd_max + 1):
                for col in colunas_originais:
                    nome_col = f"{col}_{i}"
                    if nome_col not in df_pivot.columns:
                        df_pivot[nome_col] = ""
            return df_pivot

        # Aplica a pivotagem conforme as regras solicitadas
        df_tel_p = pivotar_para_layout_fixo(df_tel, 'cpf', 10)
        df_mail_p = pivotar_para_layout_fixo(df_mail, 'cpf', 3)
        df_end_p = pivotar_para_layout_fixo(df_end, 'cpf', 3)

        # 3. Consolidação Final
        df_final = df_dados.merge(df_tel_p, on='cpf', how='left')\
                           .merge(df_mail_p, on='cpf', how='left')\
                           .merge(df_end_p, on='cpf', how='left')

        # 4. Limpeza e Padronização Final (Tudo Maiúsculo)
        df_final = df_final.astype(str).apply(lambda x: x.str.upper())
        df_final = df_final.replace(['NONE', 'NAN', 'NAT', '#N/D', 'NULL', 'None'], '')

        conn.close()
        return df_final

    except Exception as e:
        if conn: conn.close()
        st.error(f"Erro ao processar motor fixo: {e}")
        return pd.DataFrame()