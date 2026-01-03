import streamlit as st
import pandas as pd
import time
import psycopg2
import re
from datetime import date, datetime
import modulo_pf_cadastro as pf_core

# =============================================================================
# MAPEAMENTO DE TABELAS BRUTAS (Chave -> Tabela SQL)
# =============================================================================
MAPA_TABELAS_BRUTAS = {
    "pf_telefones": "banco_pf.pf_telefones",
    "pf_e-mails": "banco_pf.pf_emails",
    "pf_endereços": "banco_pf.pf_enderecos",
    "pf_convenio": "banco_pf.cpf_convenio",
    "pf_campanhas": "banco_pf.pf_campanhas",
    "pf_campanhas_exportação": "banco_pf.pf_campanhas", 
    "pf_dados": "banco_pf.pf_dados",
    "pf_contratos": "banco_pf.pf_contratos",
    "pf_emprego_renda": "banco_pf.pf_emprego_renda",
    "pf_historico_importações": "banco_pf.pf_historico_importacoes",
    "pf_maricula_dados_clt": "banco_pf.pf_matricula_dados_clt",
    "pf_modelos_exportacao": "banco_pf.pf_modelos_exportacao",
    "pf_modelos_filtro_fixo": "banco_pf.pf_modelos_filtro_fixo",
    "pf_péradpres_de_filtro": "banco_pf.pf_operadores_de_filtro",
    "pf_referecias": "banco_pf.pf_referencias",
    "pf_tipo_exportacao": "banco_pf.pf_modelos_exportacao"
}

# =============================================================================
# PARTE 1: FUNÇÕES DE BANCO (CRUD)
# =============================================================================

def listar_modelos_ativos():
    conn = pf_core.get_conn()
    if conn:
        try:
            query = "SELECT id, nome_modelo, descricao, data_criacao, status, codigo_de_consulta FROM banco_pf.pf_modelos_exportacao WHERE status='ATIVO' ORDER BY id"
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Erro ao listar: {e}")
            conn.close()
    return pd.DataFrame()

def salvar_modelo(nome, chave, desc):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
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
            st.error(f"Erro ao salvar SQL: {e}")
            conn.close()
            return False
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
# PARTE 2: MOTOR DE EXPORTAÇÃO
# =============================================================================

# --- HELPER DE FORMATAÇÃO DE DATA (REGRA 4.1 e 4.2) ---
def formatar_data_exportacao(valor):
    """
    Formata datas para DD/MM/YYYY.
    Se houver componente de hora relevante, usa DD/MM/YYYY HH:MM:SS.
    """
    if pd.isna(valor) or valor == "" or str(valor).lower() in ['nat', 'none', 'nan']:
        return ""
    try:
        ts = pd.to_datetime(valor, errors='coerce')
        if pd.isna(ts): return str(valor)
        
        # Se tiver hora (diferente de 00:00:00), inclui hora
        if ts.time() != datetime.min.time():
            return ts.strftime("%d/%m/%Y %H:%M:%S")
        
        # Caso contrário, apenas data
        return ts.strftime("%d/%m/%Y")
    except:
        return str(valor)

def aplicar_formatacao_geral(df):
    """
    Aplica regras globais de exportação:
    1. CPF com pontuação (Regra 4.1 anterior).
    2. Datas em DD/MM/YYYY (Regra 4.1 atual).
    """
    if df.empty: return df
    
    for col in df.columns:
        col_lower = col.lower()
        
        # 1. Regra CPF
        if 'cpf' in col_lower:
            df[col] = df[col].apply(pf_core.formatar_cpf_visual)
            
        # 2. Regra Data (colunas que tem 'data', 'dt_', 'nascimento', 'criacao', 'atualizacao')
        # Evita formatar colunas calculadas de "anos" se houver conflito de nome, mas geralmente 'tempo_anos' não tem 'data'
        elif 'data' in col_lower or 'nascimento' in col_lower or 'criacao' in col_lower or 'atualizacao' in col_lower:
            df[col] = df[col].apply(formatar_data_exportacao)
            
    return df

def gerar_dataframe_por_modelo(id_modelo, lista_cpfs):
    conn = pf_core.get_conn()
    if not conn: return pd.DataFrame()
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo_de_consulta FROM banco_pf.pf_modelos_exportacao WHERE id=%s", (int(id_modelo),))
        res = cur.fetchone()
        codigo_consulta = res[0] if res else ""
        
        df_result = pd.DataFrame()
        
        # 1. Roteamento para Exportação CLT Completa (Matrícula)
        if codigo_consulta == 'exportação_clt_matricula':
            df_result = _motor_clt_matricula(conn, lista_cpfs)

        # 2. Roteamento para Tabelas Brutas (Dump Simples)
        elif codigo_consulta in MAPA_TABELAS_BRUTAS:
            tabela_sql = MAPA_TABELAS_BRUTAS[codigo_consulta]
            df_result = _motor_tabela_bruta(conn, tabela_sql, lista_cpfs)
        
        # 3. Padrão: Layout Fixo Completo (Dados Pessoais + Contatos Pivotados)
        else:
            if not lista_cpfs: return pd.DataFrame()
            df_result = _motor_layout_fixo_completo(conn, lista_cpfs)
        
        # Aplica formatação final em todos os DataFrames resultantes
        df_result = aplicar_formatacao_geral(df_result)
        
        # Padronização Upper e limpeza de nulos visuais
        df_result = df_result.astype(str).apply(lambda x: x.str.upper())
        df_result = df_result.replace(['NONE', 'NAN', 'NAT', '#N/D', 'NULL', 'None', '<NA>', ''], '')
        
        return df_result
            
    except Exception as e:
        st.error(f"Erro no roteamento: {e}")
        return pd.DataFrame()

# --- MOTORES ESPECÍFICOS ---

def _motor_clt_matricula(conn, lista_cpfs):
    try:
        if not lista_cpfs: return pd.DataFrame()
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        params = tuple(lista_cpfs)

        def calc_anos(dt_str):
            if not dt_str or pd.isna(dt_str): return ""
            try:
                if isinstance(dt_str, str): d = datetime.strptime(dt_str, '%Y-%m-%d').date()
                elif isinstance(dt_str, (datetime, date)): d = dt_str
                else: return ""
                today = date.today()
                anos = today.year - d.year - ((today.month, today.day) < (d.month, d.day))
                return anos
            except: return ""

        def fmt_cnpj(v):
            if not v: return ""
            v = re.sub(r'\D', '', str(v)).zfill(14)
            return f"{v[:2]}.{v[2:5]}.{v[5:8]}/{v[8:12]}-{v[12:]}"

        # 1. Busca Dados Pessoais
        q_dados = f"""
            SELECT id, cpf, nome, data_nascimento, rg, uf_rg, data_exp_rg, 
                   cnh, pis, ctps_serie, nome_mae, nome_pai, nome_procurador, cpf_procurador
            FROM banco_pf.pf_dados 
            WHERE cpf IN ({placeholders})
        """
        df_dados = pd.read_sql(q_dados, conn, params=params)

        # 2. Busca e Pivota Satélites
        q_tel = f"SELECT cpf_ref as cpf, numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf_ref IN ({placeholders})"
        df_tel = pd.read_sql(q_tel, conn, params=params)
        df_tel_p = _pivotar_fixo(df_tel, 'cpf', 10, ['numero', 'tag_whats', 'tag_qualificacao'])

        q_end = f"SELECT cpf_ref as cpf, rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf_ref IN ({placeholders})"
        df_end = pd.read_sql(q_end, conn, params=params)
        df_end_p = _pivotar_fixo(df_end, 'cpf', 3, ['rua', 'bairro', 'cidade', 'uf', 'cep'])

        q_mail = f"SELECT cpf_ref as cpf, email FROM banco_pf.pf_emails WHERE cpf_ref IN ({placeholders})"
        df_mail = pd.read_sql(q_mail, conn, params=params)
        df_mail_p = _pivotar_fixo(df_mail, 'cpf', 3, ['email'])

        # 3. Busca Vínculos
        q_emp = f"SELECT cpf_ref as cpf, convenio, matricula FROM banco_pf.pf_emprego_renda WHERE cpf_ref IN ({placeholders})"
        df_emp = pd.read_sql(q_emp, conn, params=params)

        # 4. Busca Detalhes CLT
        mats = df_emp['matricula'].dropna().unique().tolist()
        df_clt = pd.DataFrame()
        
        if mats:
            ph_mat = ",".join(["%s"] * len(mats))
            q_clt = f"""
                SELECT matricula as matricula, convenio as convenio_clt, 
                       cnpj_nome, cnpj_numero, qtd_funcionarios, 
                       data_abertura_empresa, 
                       cnae_nome, cnae_codigo, 
                       data_admissao, 
                       cbo_codigo, cbo_nome, 
                       data_inicio_emprego
                FROM banco_pf.pf_matricula_dados_clt 
                WHERE matricula IN ({ph_mat})
            """
            df_clt = pd.read_sql(q_clt, conn, params=tuple(mats))
            
            cols_calc = [
                ('data_abertura_empresa', 'tempo_abertura_anos'),
                ('data_admissao', 'tempo_admissao_anos'),
                ('data_inicio_emprego', 'tempo_inicio_emprego_anos')
            ]
            
            for col_dt, col_anos in cols_calc:
                df_clt[col_anos] = df_clt[col_dt].apply(calc_anos)
                # A formatação de data será feita no final pelo aplicar_formatacao_geral

            df_clt['cnpj_numero'] = df_clt['cnpj_numero'].apply(fmt_cnpj)

        # 5. CRUZAMENTO FINAL
        df_full = df_emp.merge(df_clt, on='matricula', how='left', suffixes=('', '_dup'))
        df_full = df_full.merge(df_dados, on='cpf', how='left')
        df_full = df_full.merge(df_tel_p, on='cpf', how='left')\
                         .merge(df_end_p, on='cpf', how='left')\
                         .merge(df_mail_p, on='cpf', how='left')

        # 6. Ordenação
        colunas_ordenadas = [
            'id', 'cpf', 'nome', 'data_nascimento', 'rg', 'uf_rg', 'data_exp_rg', 'cnh', 'pis', 'ctps_serie', 'nome_mae', 'nome_pai', 'nome_procurador', 'cpf_procurador',
        ]
        for i in range(1, 11): colunas_ordenadas.extend([f'numero_{i}', f'tag_whats_{i}', f'tag_qualificacao_{i}'])
        for i in range(1, 4): colunas_ordenadas.extend([f'rua_{i}', f'bairro_{i}', f'cidade_{i}', f'uf_{i}', f'cep_{i}'])
        for i in range(1, 4): colunas_ordenadas.append(f'email_{i}')
        colunas_ordenadas.extend([
            'convenio', 'matricula', 
            'cnpj_nome', 'cnpj_numero', 'qtd_funcionarios', 'data_abertura_empresa', 'tempo_abertura_anos',
            'cnae_nome', 'cnae_codigo', 'data_admissao', 'tempo_admissao_anos',
            'cbo_codigo', 'cbo_nome', 'data_inicio_emprego', 'tempo_inicio_emprego_anos'
        ])
        
        cols_finais = [c for c in colunas_ordenadas if c in df_full.columns]
        df_final = df_full[cols_finais]
        
        conn.close()
        return df_final

    except Exception as e:
        if conn: conn.close()
        st.error(f"Erro no Motor CLT: {e}")
        return pd.DataFrame()

def _motor_tabela_bruta(conn, tabela_sql, lista_cpfs):
    try:
        cur = conn.cursor()
        if '.' in tabela_sql: schema, table = tabela_sql.split('.')
        else: schema, table = 'public', tabela_sql
            
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, table))
        colunas = [r[0] for r in cur.fetchall()]
        if not colunas: return pd.DataFrame()

        cols_str = ", ".join(colunas)
        query = f"SELECT {cols_str} FROM {tabela_sql}"
        params = []
        
        if lista_cpfs:
            if 'cpf' in colunas:
                placeholders = ",".join(["%s"] * len(lista_cpfs))
                query += f" WHERE cpf IN ({placeholders})"
                params = tuple(lista_cpfs)
            elif 'cpf_ref' in colunas:
                placeholders = ",".join(["%s"] * len(lista_cpfs))
                query += f" WHERE cpf_ref IN ({placeholders})"
                params = tuple(lista_cpfs)
            elif 'matricula' in colunas or 'matricula_ref' in colunas:
                ph_cpf = ",".join(["%s"] * len(lista_cpfs))
                df_mats = pd.read_sql(f"SELECT matricula FROM banco_pf.pf_emprego_renda WHERE cpf_ref IN ({ph_cpf})", conn, params=tuple(lista_cpfs))
                if not df_mats.empty:
                    mats = df_mats['matricula'].dropna().unique().tolist()
                    if mats:
                        ph_mat = ",".join(["%s"] * len(mats))
                        col_mat = 'matricula' if 'matricula' in colunas else 'matricula_ref'
                        query += f" WHERE {col_mat} IN ({ph_mat})"
                        params = tuple(mats)
                    else: return pd.DataFrame(columns=colunas)
                else: return pd.DataFrame(columns=colunas)

        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro tabela bruta: {e}")
        conn.close()
        return pd.DataFrame()

def _motor_layout_fixo_completo(conn, lista_cpfs):
    try:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        params = tuple(lista_cpfs)

        df_dados = pd.read_sql(f"SELECT * FROM banco_pf.pf_dados WHERE cpf IN ({placeholders})", conn, params=params)
        df_dados.drop(columns=['data_criacao', 'importacao_id', 'id_campanha'], inplace=True, errors='ignore')

        q_tel = f"SELECT cpf_ref as cpf, numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf_ref IN ({placeholders})"
        df_tel_p = _pivotar_fixo(pd.read_sql(q_tel, conn, params=params), 'cpf', 10, ['numero', 'tag_whats', 'tag_qualificacao'])

        q_mail = f"SELECT cpf_ref as cpf, email FROM banco_pf.pf_emails WHERE cpf_ref IN ({placeholders})"
        df_mail_p = _pivotar_fixo(pd.read_sql(q_mail, conn, params=params), 'cpf', 3, ['email'])

        q_end = f"SELECT cpf_ref as cpf, rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf_ref IN ({placeholders})"
        df_end_p = _pivotar_fixo(pd.read_sql(q_end, conn, params=params), 'cpf', 3, ['rua', 'bairro', 'cidade', 'uf', 'cep'])

        df_final = df_dados.merge(df_tel_p, on='cpf', how='left')\
                           .merge(df_mail_p, on='cpf', how='left')\
                           .merge(df_end_p, on='cpf', how='left')

        conn.close()
        return df_final
    except Exception as e:
        conn.close(); st.error(f"Erro fixo: {e}"); return pd.DataFrame()

def _pivotar_fixo(df, id_col, limit, value_cols):
    if df.empty: return pd.DataFrame(columns=[id_col])
    if 'cpf' in id_col.lower():
         # Garante que a chave de merge esteja visualmente igual se necessário, 
         # mas aqui o foco é o merge interno, a formatação visual ocorre no final.
         pass 

    df['seq'] = df.groupby(id_col).cumcount() + 1
    df = df[df['seq'] <= limit]
    
    if len(value_cols) == 1:
        df_p = df.pivot(index=id_col, columns='seq', values=value_cols[0])
        df_p.columns = [f"{value_cols[0]}_{c}" for c in df_p.columns]
    else:
        df_p = df.pivot(index=id_col, columns='seq', values=value_cols)
        df_p.columns = [f"{c[0]}_{c[1]}" for c in df_p.columns]
        
    return df_p.reset_index()

# =============================================================================
# PARTE 3: INTERFACE E AUTO-CONFIGURAÇÃO
# =============================================================================

def verificar_criar_modelos_padrao():
    conn = pf_core.get_conn()
    if not conn: return
    try:
        cur = conn.cursor()
        modelos = [
            ("exportação CLT (com dados da matricula)", "exportação_clt_matricula", "Exportação cruzada completa CLT + Dados + Contatos"),
            ("Planilha: pf_telefones", "pf_telefones", "Exportação bruta: pf_telefones"),
            ("Planilha: pf_e-mails", "pf_e-mails", "Exportação bruta: pf_emails"),
            ("Planilha: pf_endereços", "pf_endereços", "Exportação bruta: pf_enderecos"),
            ("Planilha: pf_convenio", "pf_convenio", "Exportação bruta: cpf_convenio"),
            ("Planilha: pf_campanhas", "pf_campanhas", "Exportação bruta: pf_campanhas"),
            ("Planilha: pf_campanhas_exportação", "pf_campanhas_exportação", "Exportação bruta: pf_campanhas"),
            ("Planilha: pf_dados", "pf_dados", "Exportação bruta: pf_dados"),
            ("Planilha: pf_contratos", "pf_contratos", "Exportação bruta: pf_contratos"),
            ("Planilha: pf_emprego_renda", "pf_emprego_renda", "Exportação bruta: pf_emprego_renda"),
            ("Planilha: pf_historico_importações", "pf_historico_importações", "Exportação bruta: pf_historico_importacoes"),
            ("Planilha: pf_maricula_dados_clt", "pf_maricula_dados_clt", "Exportação bruta: pf_matricula_dados_clt"),
            ("Planilha: pf_modelos_exportacao", "pf_modelos_exportacao", "Exportação bruta: pf_modelos_exportacao"),
            ("Planilha: pf_modelos_filtro_fixo", "pf_modelos_filtro_fixo", "Exportação bruta: pf_modelos_filtro_fixo"),
            ("Planilha: pf_operadores_de_filtro", "pf_péradpres_de_filtro", "Exportação bruta: pf_operadores_de_filtro"),
            ("Planilha: pf_referencias", "pf_referecias", "Exportação bruta: pf_referencias"),
            ("Planilha: pf_tipo_exportacao", "pf_tipo_exportacao", "Exportação bruta: pf_modelos_exportacao")
        ]

        for nome, chave, desc in modelos:
            cur.execute("SELECT id FROM banco_pf.pf_modelos_exportacao WHERE codigo_de_consulta = %s", (chave,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO banco_pf.pf_modelos_exportacao 
                    (nome_modelo, codigo_de_consulta, descricao, status, data_criacao) 
                    VALUES (%s, %s, %s, 'ATIVO', CURRENT_DATE)
                """, (nome, chave, desc))
        conn.commit(); conn.close()
    except: conn.close()

def app_config_exportacao():
    verificar_criar_modelos_padrao()
    
    st.markdown("## ⚙️ Configuração de Modelos de Exportação")
    st.caption("Gerencie as chaves que conectam os modelos de tela às regras de código.")

    with st.expander("➕ Criar Novo Modelo Manual"):
        with st.form("form_novo_modelo"):
            nome = st.text_input("Nome")
            chave = st.text_input("Chave do Motor")
            desc = st.text_area("Descrição")
            if st.form_submit_button("Salvar"):
                if nome and chave:
                    salvar_modelo(nome, chave, desc)
                    st.success("Salvo!"); time.sleep(1); st.rerun()

    st.divider()
    df_modelos = listar_modelos_ativos()
    if not df_modelos.empty:
        for _, row in df_modelos.iterrows():
            chave = row['codigo_de_consulta']
            icone = "🚀" if chave == 'exportação_clt_matricula' else ("🗃️" if chave in MAPA_TABELAS_BRUTAS else "📦")
            
            with st.expander(f"{icone} {row['nome_modelo']} ({chave})"):
                st.write(row['descricao'])
                c1, c2 = st.columns(2)
                if c1.button("✏️ Editar", key=f"ed_{row['id']}"): dialog_editar_modelo(row)
                if c2.button("🗑️ Excluir", key=f"del_{row['id']}"): dialog_excluir_modelo(row['id'], row['nome_modelo'])
    else: st.info("Sem modelos.")

# --- DIALOGS ---
@st.dialog("✏️ Editar")
def dialog_editar_modelo(m):
    with st.form("fe"):
        nn = st.text_input("Nome", value=m['nome_modelo'])
        nc = st.text_input("Chave", value=m['codigo_de_consulta'])
        nd = st.text_area("Desc", value=m['descricao'])
        if st.form_submit_button("Salvar"):
            atualizar_modelo(m['id'], nn, nc, nd); st.rerun()

@st.dialog("🗑️ Excluir")
def dialog_excluir_modelo(id, nome):
    st.error(f"Excluir {nome}?")
    if st.button("Confirmar"): excluir_modelo(id); st.rerun()