import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date, timedelta
import io
import time
import math
import re
import os

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
BASE_DIR_IMPORTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ARQUIVO IMPORTAÇÕES")
if not os.path.exists(BASE_DIR_IMPORTS):
    os.makedirs(BASE_DIR_IMPORTS)

try:
    import conexao
except ImportError:
    st.error("Erro crítico: conexao.py não encontrado.")

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        return None

# --- INICIALIZAÇÃO DO BANCO (AUTO-MIGRATE) ---
def init_db_structures():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pf_historico_importacoes (
                    id SERIAL PRIMARY KEY,
                    nome_arquivo VARCHAR(255),
                    data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    qtd_novos INTEGER DEFAULT 0,
                    qtd_atualizados INTEGER DEFAULT 0,
                    qtd_erros INTEGER DEFAULT 0,
                    caminho_arquivo_original TEXT,
                    caminho_arquivo_erro TEXT,
                    usuario_responsavel VARCHAR(100)
                );
            """)
            
            tabelas = ['pf_dados', 'pf_telefones', 'pf_emails', 'pf_enderecos', 'pf_emprego_renda', 'pf_contratos']
            for tb in tabelas:
                cur.execute(f"ALTER TABLE {tb} ADD COLUMN IF NOT EXISTS importacao_id INTEGER REFERENCES pf_historico_importacoes(id);")
                
            conn.commit()
            conn.close()
        except: pass

# --- FUNÇÕES AUXILIARES E VALIDAÇÃO ---
def calcular_idade_completa(data_nasc):
    if not data_nasc: return "", "", ""
    hoje = date.today()
    if isinstance(data_nasc, datetime): data_nasc = data_nasc.date()
    anos = hoje.year - data_nasc.year - ((hoje.month, hoje.day) < (data_nasc.month, data_nasc.day))
    meses = (hoje.year - data_nasc.year) * 12 + hoje.month - data_nasc.month
    dias = (hoje - data_nasc).days
    return anos, meses, dias

def buscar_referencias(tipo):
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT nome FROM pf_referencias WHERE tipo = %s ORDER BY nome", conn, params=(tipo,))
            conn.close()
            return df['nome'].tolist()
        except: conn.close()
    return []

def limpar_apenas_numeros(valor):
    """Remove tudo que não é dígito"""
    if not valor: return ""
    return re.sub(r'\D', '', str(valor))

def formatar_cpf_visual(cpf_db):
    if not cpf_db: return ""
    cpf_limpo = str(cpf_db).strip()
    cpf_full = cpf_limpo.zfill(11)
    return f"{cpf_full[:3]}.{cpf_full[3:6]}.{cpf_full[6:9]}-{cpf_full[9:]}"

def validar_formatar_cpf(cpf_raw):
    numeros = limpar_apenas_numeros(cpf_raw)
    if len(numeros) != 11:
        return None, "CPF deve ter 11 dígitos."
    cpf_formatado = f"{numeros[:3]}.{numeros[3:6]}.{numeros[6:9]}-{numeros[9:]}"
    return cpf_formatado, None

def validar_formatar_telefone(tel_raw):
    numeros = limpar_apenas_numeros(tel_raw)
    if len(numeros) == 10 or len(numeros) == 11:
        return numeros, None
    return None, "Telefone deve ter 10 ou 11 dígitos (DDD + Número)."

def validar_email(email):
    if not email: return False
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(regex, email):
        return True
    return False

def validar_formatar_cep(cep_raw):
    numeros = limpar_apenas_numeros(cep_raw)
    if len(numeros) != 8:
        return None, "CEP deve ter 8 dígitos."
    return f"{numeros[:5]}-{numeros[5:]}"

def limpar_normalizar_cpf(cpf_raw):
    if not cpf_raw: return ""
    apenas_nums = re.sub(r'\D', '', str(cpf_raw))
    if not apenas_nums: return ""
    return apenas_nums.lstrip('0')

def verificar_cpf_existente(cpf_normalizado):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome FROM pf_dados WHERE cpf = %s", (cpf_normalizado,))
            res = cur.fetchone()
            conn.close()
            return res[0] if res else None
        except: conn.close()
    return None

def converter_data_br_iso(valor):
    if not valor or pd.isna(valor): return None
    valor_str = str(valor).strip()
    valor_str = valor_str.split(' ')[0] # Remove horas
    formatos = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"]
    for fmt in formatos:
        try: return datetime.strptime(valor_str, fmt).strftime("%Y-%m-%d")
        except ValueError: continue
    return None

# --- IMPORTAÇÃO RÁPIDA (BULK OTIMIZADO) ---
def processar_importacao_lote(conn, df, table_name, mapping, import_id):
    cur = conn.cursor()
    try:
        erros = []
        df_proc = pd.DataFrame()
        cols_order = []

        # --- LÓGICA ESPECIAL PARA TELEFONES (MULTI-COLUNAS) ---
        if table_name == 'pf_telefones':
            col_cpf = next((k for k, v in mapping.items() if v == 'cpf_ref (Vínculo)'), None)
            col_whats = next((k for k, v in mapping.items() if v == 'tag_whats'), None)
            col_qualif = next((k for k, v in mapping.items() if v == 'tag_qualificacao'), None)
            map_tels = {k: v for k, v in mapping.items() if v and v.startswith('telefone_')}
            
            if not col_cpf:
                return 0, 0, ["Erro: Coluna 'CPF (Vínculo)' é obrigatória para importar telefones."]
            
            new_rows = []
            for _, row in df.iterrows():
                cpf_val = str(row[col_cpf]) if pd.notna(row[col_cpf]) else ""
                cpf_limpo = limpar_normalizar_cpf(cpf_val)
                if not cpf_limpo: continue
                
                whats_val = str(row[col_whats]) if col_whats and pd.notna(row[col_whats]) else None
                qualif_val = str(row[col_qualif]) if col_qualif and pd.notna(row[col_qualif]) else None
                
                for col_origin, _ in map_tels.items():
                    tel_raw = row[col_origin]
                    if pd.notna(tel_raw):
                        tel_limpo = limpar_apenas_numeros(tel_raw)
                        if tel_limpo and len(tel_limpo) >= 8: 
                            new_rows.append({
                                'cpf_ref': cpf_limpo,
                                'numero': tel_limpo,
                                'tag_whats': whats_val,
                                'tag_qualificacao': qualif_val,
                                'importacao_id': import_id,
                                'data_atualizacao': datetime.now().strftime('%Y-%m-%d')
                            })
            
            if not new_rows:
                return 0, 0, ["Nenhum telefone válido encontrado para importação."]
                
            df_proc = pd.DataFrame(new_rows)
            cols_order = list(df_proc.columns)
            
        # --- LÓGICA PADRÃO PARA OUTRAS TABELAS ---
        else:
            df_proc = df.rename(columns=mapping)
            cols_db = list(mapping.values())
            df_proc = df_proc[cols_db].copy()
            df_proc['importacao_id'] = import_id
            
            if 'cpf' in df_proc.columns:
                df_proc['cpf'] = df_proc['cpf'].astype(str).apply(limpar_normalizar_cpf)
                
                # Regra de Duplicidade (Apenas para pf_dados): Remove do arquivo
                if table_name == 'pf_dados':
                    df_proc = df_proc.drop_duplicates(subset=['cpf'], keep='last')
                    invalidos = df_proc[df_proc['cpf'] == ""]
                    if not invalidos.empty:
                        for idx, _ in invalidos.iterrows():
                            erros.append(f"Linha {idx}: CPF inválido ou vazio.")
                    df_proc = df_proc[df_proc['cpf'] != ""]

            cols_data = ['data_nascimento', 'data_exp_rg', 'data_criacao', 'data_atualizacao']
            for col in cols_data:
                if col in df_proc.columns:
                    df_proc[col] = df_proc[col].apply(converter_data_br_iso)
            
            cols_order = list(df_proc.columns)

        # --- EXECUÇÃO DO COPY (COMUM) ---
        staging_table = f"staging_import_{import_id}"
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")
        
        output = io.StringIO()
        df_proc.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        
        cur.copy_expert(f"COPY {staging_table} ({', '.join(cols_order)}) FROM STDIN WITH CSV DELIMITER E'\t' NULL '\\N'", output)
        
        pk_field = 'cpf' if 'cpf' in df_proc.columns else ('matricula' if 'matricula' in df_proc.columns else None)
        qtd_novos = 0
        qtd_atualizados = 0
        
        if pk_field:
            # Lógica de UPSERT para Tabelas Principais (pf_dados, etc)
            set_clause = ', '.join([f'{c} = s.{c}' for c in cols_order if c != pk_field])
            
            sql_update = f"""
                UPDATE {table_name} t 
                SET {set_clause}
                FROM {staging_table} s 
                WHERE t.{pk_field} = s.{pk_field}
            """
            cur.execute(sql_update)
            qtd_atualizados = cur.rowcount
            
            sql_insert = f"INSERT INTO {table_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s WHERE NOT EXISTS (SELECT 1 FROM {table_name} t WHERE t.{pk_field} = s.{pk_field})"
            cur.execute(sql_insert)
            qtd_novos = cur.rowcount
        else:
            # Lógica para Tabelas Vinculadas (Telefones, Emails)
            cols_to_compare = [c for c in cols_order if c not in ['importacao_id', 'data_atualizacao', 'data_criacao', 'id']]
            conditions = " AND ".join([f"t.{c} IS NOT DISTINCT FROM s.{c}" for c in cols_to_compare])
            
            sql_insert = f"""
                INSERT INTO {table_name} ({', '.join(cols_order)}) 
                SELECT {', '.join(cols_order)} 
                FROM {staging_table} s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} t 
                    WHERE {conditions}
                )
            """
            cur.execute(sql_insert)
            qtd_novos = cur.rowcount
            
            # --- PROPAGAÇÃO DO ID DE IMPORTAÇÃO PARA O CLIENTE (PF_DADOS) ---
            if table_name in ['pf_telefones', 'pf_emails', 'pf_enderecos', 'pf_emprego_renda']:
                sql_propaga = f"""
                    UPDATE pf_dados d
                    SET importacao_id = %s
                    FROM {staging_table} s
                    WHERE d.cpf = s.cpf_ref
                """
                cur.execute(sql_propaga, (import_id,))
            
            elif table_name == 'pf_contratos':
                sql_propaga = f"""
                    UPDATE pf_dados d
                    SET importacao_id = %s
                    FROM pf_emprego_renda e
                    JOIN {staging_table} s ON e.matricula = s.matricula_ref
                    WHERE d.cpf = e.cpf_ref
                """
                cur.execute(sql_propaga, (import_id,))

        return qtd_novos, qtd_atualizados, erros
        
    except Exception as e:
        raise e

# --- FUNÇÕES DE BUSCA ---
def buscar_pf_simples(termo, filtro_importacao_id=None, pagina=1, itens_por_pagina=50, exportar=False):
    conn = get_conn()
    if conn:
        try:
            termo_limpo = re.sub(r'\D', '', termo).lstrip('0')
            param_nome = f"%{termo}%"
            
            sql_base_select = "SELECT d.id, d.nome, d.cpf, d.data_nascimento "
            sql_base_from = "FROM pf_dados d "
            
            conditions = []
            params = []

            if filtro_importacao_id:
                sub_queries = [
                    "d.importacao_id = %s",
                    "EXISTS (SELECT 1 FROM pf_telefones t WHERE t.cpf_ref = d.cpf AND t.importacao_id = %s)",
                    "EXISTS (SELECT 1 FROM pf_emails e WHERE e.cpf_ref = d.cpf AND e.importacao_id = %s)",
                    "EXISTS (SELECT 1 FROM pf_enderecos ed WHERE ed.cpf_ref = d.cpf AND ed.importacao_id = %s)",
                    "EXISTS (SELECT 1 FROM pf_emprego_renda er WHERE er.cpf_ref = d.cpf AND er.importacao_id = %s)"
                ]
                conditions.append(f"({' OR '.join(sub_queries)})")
                params.extend([filtro_importacao_id] * 5)
            
            if termo:
                if termo_limpo:
                    param_num = f"%{termo_limpo}%"
                    sql_base_from += " LEFT JOIN pf_telefones tel ON d.cpf = tel.cpf_ref"
                    sub_cond = ["d.nome ILIKE %s", "d.cpf ILIKE %s", "tel.numero ILIKE %s"]
                    sub_params = [param_nome, param_num, param_num]
                    conditions.append(f"({' OR '.join(sub_cond)})")
                    params.extend(sub_params)
                else:
                    conditions.append("d.nome ILIKE %s")
                    params.append(param_nome)
            
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            if exportar:
                query = f"{sql_base_select} {sql_base_from} {sql_where} GROUP BY d.id ORDER BY d.nome ASC LIMIT 1000000"
                df = pd.read_sql(query, conn, params=tuple(params))
                conn.close()
                return df, len(df)

            count_sql = f"SELECT COUNT(DISTINCT d.id) {sql_base_from} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total_registros = cur.fetchone()[0]

            offset = (pagina - 1) * itens_por_pagina
            query = f"{sql_base_select} {sql_base_from} {sql_where} GROUP BY d.id ORDER BY d.nome ASC LIMIT {itens_por_pagina} OFFSET {offset}"
            
            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close()
            return df, total_registros
        except: conn.close()
    return pd.DataFrame(), 0

def buscar_opcoes_filtro(coluna, tabela):
    conn = get_conn()
    opcoes = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT DISTINCT {coluna} FROM {tabela} WHERE {coluna} IS NOT NULL ORDER BY {coluna}")
            opcoes = [r[0] for r in cur.fetchall() if r[0]]
            conn.close()
        except: pass
    return opcoes

def executar_pesquisa_ampla(filtros, pagina=1, itens_por_pagina=50, exportar=False):
    conn = get_conn()
    if conn:
        try:
            sql_select = "SELECT DISTINCT d.id, d.nome, d.cpf, d.data_nascimento "
            sql_from = "FROM pf_dados d "
            joins = []
            conditions = []
            params = []

            if filtros.get('nome'):
                conditions.append("d.nome ILIKE %s")
                params.append(f"%{filtros['nome']}%")
            if filtros.get('cpf'):
                cpf_norm = limpar_normalizar_cpf(filtros['cpf'])
                conditions.append("d.cpf = %s") 
                params.append(cpf_norm)
            if filtros.get('rg'):
                conditions.append("d.rg ILIKE %s")
                params.append(f"%{filtros['rg']}%")
            if filtros.get('nascimento'):
                conditions.append("d.data_nascimento = %s")
                params.append(filtros['nascimento'])
            
            if filtros.get('importacao_id'):
                sub_queries = [
                    "d.importacao_id = %s",
                    "EXISTS (SELECT 1 FROM pf_telefones t WHERE t.cpf_ref = d.cpf AND t.importacao_id = %s)",
                    "EXISTS (SELECT 1 FROM pf_emails e WHERE e.cpf_ref = d.cpf AND e.importacao_id = %s)",
                    "EXISTS (SELECT 1 FROM pf_enderecos ed WHERE ed.cpf_ref = d.cpf AND ed.importacao_id = %s)",
                    "EXISTS (SELECT 1 FROM pf_emprego_renda er WHERE er.cpf_ref = d.cpf AND er.importacao_id = %s)"
                ]
                conditions.append(f"({' OR '.join(sub_queries)})")
                params.extend([filtros['importacao_id']] * 5)

            if any(k in filtros for k in ['uf', 'cidade', 'bairro', 'rua']):
                joins.append("JOIN pf_enderecos end ON d.cpf = end.cpf_ref")
                if filtros.get('uf'):
                    conditions.append("end.uf = %s")
                    params.append(filtros['uf'])
                if filtros.get('cidade'):
                    conditions.append("end.cidade ILIKE %s")
                    params.append(f"%{filtros['cidade']}%")
                if filtros.get('bairro'):
                    conditions.append("end.bairro ILIKE %s")
                    params.append(f"%{filtros['bairro']}%")
                if filtros.get('rua'):
                    conditions.append("end.rua ILIKE %s")
                    params.append(f"%{filtros['rua']}%")

            if filtros.get('ddd') or filtros.get('telefone'):
                joins.append("JOIN pf_telefones tel ON d.cpf = tel.cpf_ref")
            if filtros.get('ddd'):
                conditions.append("SUBSTRING(REGEXP_REPLACE(tel.numero, '[^0-9]', '', 'g'), 1, 2) = %s")
                params.append(filtros['ddd'])
            if filtros.get('telefone'):
                tel_clean = re.sub(r'\D', '', filtros['telefone']).lstrip('0')
                conditions.append("tel.numero LIKE %s")
                params.append(f"%{tel_clean}%")

            if filtros.get('email'):
                joins.append("JOIN pf_emails em ON d.cpf = em.cpf_ref")
                conditions.append("em.email ILIKE %s")
                params.append(f"%{filtros['email']}%")

            if any(k in filtros for k in ['convenio', 'matricula', 'contrato']):
                if filtros.get('contrato'):
                    joins.append("JOIN pf_emprego_renda emp ON d.cpf = emp.cpf_ref")
                    joins.append("JOIN pf_contratos ctr ON emp.matricula = ctr.matricula_ref")
                    conditions.append("ctr.contrato ILIKE %s")
                    params.append(f"%{filtros['contrato']}%")
                else:
                    joins.append("JOIN pf_emprego_renda emp ON d.cpf = emp.cpf_ref")
                if filtros.get('convenio'):
                    conditions.append("emp.convenio = %s")
                    params.append(filtros['convenio'])
                if filtros.get('matricula'):
                    conditions.append("emp.matricula ILIKE %s")
                    params.append(f"%{filtros['matricula']}%")

            joins = list(set(joins))
            sql_joins = " ".join(joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            if exportar:
                full_sql = f"{sql_select} {sql_from} {sql_joins} {sql_where} ORDER BY d.nome LIMIT 1000000"
                df = pd.read_sql(full_sql, conn, params=tuple(params))
                conn.close()
                return df, len(df)
            
            count_sql = f"SELECT COUNT(DISTINCT d.id) {sql_from} {sql_joins} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total_registros = cur.fetchone()[0]
            
            offset = (pagina - 1) * itens_por_pagina
            pag_sql = f"{sql_select} {sql_from} {sql_joins} {sql_where} ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}"
            
            df = pd.read_sql(pag_sql, conn, params=tuple(params))
            conn.close()
            return df, total_registros
        except: return pd.DataFrame(), 0
    return pd.DataFrame(), 0

# --- FUNÇÕES CRUD ---
def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {}
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            df_d = pd.read_sql("SELECT * FROM pf_dados WHERE cpf = %s", conn, params=(cpf_norm,))
            dados['geral'] = df_d.iloc[0] if not df_d.empty else None
            dados['telefones'] = pd.read_sql("SELECT numero, data_atualizacao, tag_whats, tag_qualificacao FROM pf_telefones WHERE cpf_ref = %s", conn, params=(cpf_norm,))
            dados['emails'] = pd.read_sql("SELECT email FROM pf_emails WHERE cpf_ref = %s", conn, params=(cpf_norm,))
            dados['enderecos'] = pd.read_sql("SELECT rua, bairro, cidade, uf, cep FROM pf_enderecos WHERE cpf_ref = %s", conn, params=(cpf_norm,))
            dados['empregos'] = pd.read_sql("SELECT id, convenio, matricula, dados_extras FROM pf_emprego_renda WHERE cpf_ref = %s", conn, params=(cpf_norm,))
            
            if not dados['empregos'].empty:
                matr_list = tuple(dados['empregos']['matricula'].dropna().tolist())
                if matr_list:
                    placeholders = ",".join(["%s"] * len(matr_list))
                    q_contratos = f"SELECT matricula_ref, contrato, dados_extras FROM pf_contratos WHERE matricula_ref IN ({placeholders})"
                    dados['contratos'] = pd.read_sql(q_contratos, conn, params=matr_list)
                else: dados['contratos'] = pd.DataFrame()
            else: dados['contratos'] = pd.DataFrame()
        except: pass
        finally: conn.close()
    return dados

def salvar_pf(dados_gerais, df_tel, df_email, df_end, df_emp, df_contr, modo="novo", cpf_original=None):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            
            cpf_limpo = limpar_normalizar_cpf(dados_gerais['cpf'])
            dados_gerais['cpf'] = cpf_limpo
            if cpf_original: cpf_original = limpar_normalizar_cpf(cpf_original)

            dados_gerais = {k: (v.upper() if isinstance(v, str) else v) for k, v in dados_gerais.items()}

            if modo == "novo":
                cols = list(dados_gerais.keys())
                vals = list(dados_gerais.values())
                placeholders = ", ".join(["%s"] * len(vals))
                col_names = ", ".join(cols)
                cur.execute(f"INSERT INTO pf_dados ({col_names}) VALUES ({placeholders})", vals)
            else:
                set_clause = ", ".join([f"{k}=%s" for k in dados_gerais.keys()])
                vals = list(dados_gerais.values()) + [cpf_original]
                cur.execute(f"UPDATE pf_dados SET {set_clause} WHERE cpf=%s", vals)
            
            cpf_chave = dados_gerais['cpf']
            if modo == "editar":
                cur.execute("DELETE FROM pf_telefones WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_emails WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_enderecos WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_contratos WHERE matricula_ref IN (SELECT matricula FROM pf_emprego_renda WHERE cpf_ref = %s)", (cpf_chave,))
                cur.execute("DELETE FROM pf_emprego_renda WHERE cpf_ref = %s", (cpf_chave,))
            
            def df_upper(df): return df.applymap(lambda x: x.upper() if isinstance(x, str) else x)

            if not df_tel.empty:
                for _, row in df_upper(df_tel).iterrows():
                    if row.get('numero'): cur.execute("INSERT INTO pf_telefones (cpf_ref, numero, tag_whats, tag_qualificacao, data_atualizacao) VALUES (%s, %s, %s, %s, %s)", (cpf_chave, row['numero'], row.get('tag_whats'), row.get('tag_qualificacao'), datetime.now().date()))
            
            if not df_email.empty:
                for _, row in df_upper(df_email).iterrows():
                    if row.get('email'): cur.execute("INSERT INTO pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, row['email']))
            
            if not df_end.empty:
                for _, row in df_upper(df_end).iterrows():
                    if row.get('rua') or row.get('cidade'): cur.execute("INSERT INTO pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, row['rua'], row.get('bairro'), row.get('cidade'), row.get('uf'), row.get('cep')))
            
            if not df_emp.empty:
                for _, row in df_upper(df_emp).iterrows():
                    if row.get('convenio') and row.get('matricula'):
                        try: cur.execute("INSERT INTO pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s)", (cpf_chave, row.get('convenio'), row.get('matricula'), row.get('dados_extras')))
                        except: pass

            if not df_contr.empty:
                for _, row in df_upper(df_contr).iterrows():
                    if row.get('matricula_ref'):
                        cur.execute("SELECT 1 FROM pf_emprego_renda WHERE matricula = %s", (row.get('matricula_ref'),))
                        if cur.fetchone():
                            cur.execute("INSERT INTO pf_contratos (matricula_ref, contrato, dados_extras) VALUES (%s, %s, %s)", (row.get('matricula_ref'), row.get('contrato'), row.get('dados_extras')))

            conn.commit()
            conn.close()
            return True, "Salvo com sucesso!"
        except psycopg2.IntegrityError:
            conn.rollback()
            return False, "Erro: CPF já cadastrado."
        except Exception as e: return False, str(e)
    return False, "Erro de conexão"

def excluir_pf(cpf):
    conn = get_conn()
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            cur = conn.cursor()
            cur.execute("DELETE FROM pf_dados WHERE cpf = %s", (cpf_norm,))
            conn.commit(); conn.close()
            return True
        except: return False
    return False

# --- FUNÇÕES DE IMPORTAÇÃO ---
def get_table_columns(table_name):
    conn = get_conn()
    cols = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
            cols = cur.fetchall()
            conn.close()
        except: pass
    return cols

# --- DIALOG: VISUALIZAR CLIENTE ---
@st.dialog("👁️ Detalhes do Cliente")
def dialog_visualizar_cliente(cpf_cliente):
    # Aplica formatação visual ao CPF exibido no título
    cpf_vis = formatar_cpf_visual(cpf_cliente)
    
    dados = carregar_dados_completos(cpf_cliente)
    g = dados.get('geral')
    
    if g is None:
        st.error("Cliente não encontrado.")
        return

    with st.expander("👤 Dados Cadastrais", expanded=True):
        c1, c2 = st.columns(2)
        c1.write(f"**Nome:** {g['nome']}")
        # Exibe CPF formatado
        c2.write(f"**CPF:** {cpf_vis}")
        
        # Mostra a origem da importação se disponível
        if g.get('importacao_id'):
            c2.caption(f"Origem (Importação): ID {g['importacao_id']}")
            
        c3, c4 = st.columns(2)
        dt_nasc = pd.to_datetime(g['data_nascimento']).strftime('%d/%m/%Y') if g['data_nascimento'] else "-"
        c3.write(f"**Nascimento:** {dt_nasc}")
        c4.write(f"**RG:** {g['rg']}")
    
    with st.expander("📞 Telefones"):
        df_tel = dados.get('telefones')
        if not df_tel.empty:
            for _, row in df_tel.iterrows():
                st.write(f"📱 {row['numero']} (WhatsApp: {row['tag_whats']})")
        else: st.info("Sem telefones cadastrados.")

    with st.expander("📧 E-mails"):
        df_email = dados.get('emails')
        if not df_email.empty:
            for _, row in df_email.iterrows():
                st.write(f"✉️ {row['email']}")
        else: st.info("Sem e-mails cadastrados.")

    with st.expander("🏠 Endereços"):
        df_end = dados.get('enderecos')
        if not df_end.empty:
            for _, row in df_end.iterrows():
                st.write(f"📍 {row['rua']}, {row['bairro']} - {row['cidade']}/{row['uf']} (CEP: {row['cep']})")
        else: st.info("Sem endereços cadastrados.")

    with st.expander("💼 Emprego, Renda e Contratos"):
        df_emp = dados.get('empregos')
        df_contr = dados.get('contratos')
        if not df_emp.empty:
            for _, row in df_emp.iterrows():
                st.markdown(f"**Convênio:** {row['convenio']} | **Matrícula:** {row['matricula']}")
                if row['dados_extras']: st.caption(f"Extras: {row['dados_extras']}")
                contratos_vinculados = df_contr[df_contr['matricula_ref'] == row['matricula']]
                if not contratos_vinculados.empty:
                    st.write("📄 *Contratos:*")
                    for _, c in contratos_vinculados.iterrows():
                        st.write(f"- {c['contrato']}")
                st.divider()
        else: st.info("Sem dados profissionais.")

# --- INTERFACE ---
@st.dialog("⚠️ Excluir Cadastro")
def dialog_excluir_pf(cpf, nome):
    st.error(f"Apagar **{nome}**?")
    st.warning("Esta ação é irreversível.")
    c1, c2 = st.columns(2)
    if c1.button("Confirmar Exclusão", type="primary"):
        if excluir_pf(cpf): st.success("Apagado!"); time.sleep(1); st.rerun()
    if c2.button("Cancelar"): st.rerun()

def app_pessoa_fisica():
    init_db_structures()
    
    # CSS PARA OTIMIZAR ESPAÇAMENTO
    st.markdown("""
        <style>
            /* Reduz espaçamento entre linhas da grade */
            [data-testid="stVerticalBlock"] > [style*="flex-direction: column;"] > [data-testid="stVerticalBlock"] {
                gap: 0.2rem;
            }
            hr {
                margin-top: 0 !important;
                margin-bottom: 0 !important;
            }
            .stButton button {
                height: 28px;
                padding-top: 0px;
                padding-bottom: 0px;
            }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'pf_cpf_selecionado' not in st.session_state: st.session_state['pf_cpf_selecionado'] = None
    if 'import_step' not in st.session_state: st.session_state['import_step'] = 1
    if 'import_stats' not in st.session_state: st.session_state['import_stats'] = {}
    if 'filtro_importacao_id' not in st.session_state: st.session_state['filtro_importacao_id'] = None
    
    if 'pagina_atual' not in st.session_state: st.session_state['pagina_atual'] = 1
    
    # CORREÇÃO CRÍTICA: Garante que 'selecionados' seja sempre um dicionário, mesmo se houver lixo na sessão
    if 'selecionados' not in st.session_state or not isinstance(st.session_state['selecionados'], dict):
        st.session_state['selecionados'] = {}
    
    if 'temp_telefones' not in st.session_state: st.session_state['temp_telefones'] = []
    if 'temp_emails' not in st.session_state: st.session_state['temp_emails'] = []
    if 'temp_enderecos' not in st.session_state: st.session_state['temp_enderecos'] = []
    if 'temp_empregos' not in st.session_state: st.session_state['temp_empregos'] = []
    if 'temp_contratos' not in st.session_state: st.session_state['temp_contratos'] = []
    if 'form_loaded' not in st.session_state: st.session_state['form_loaded'] = False

    # ==========================
    # 1. PESQUISA AMPLA (ATUALIZADA COM LAYOUT FIXO)
    # ==========================
    if st.session_state['pf_view'] == 'pesquisa_ampla':
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        st.markdown("### 🔎 Pesquisa Ampla")
        with st.form("form_pesquisa_ampla", enter_to_submit=False):
            t1, t2, t3, t4, t5, t6 = st.tabs(["Identificação", "Endereço", "Contatos", "Profissional", "Contratos", "Origem"])
            filtros = {}
            with t1:
                c1, c2, c3, c4 = st.columns(4)
                filtros['nome'] = c1.text_input("Nome")
                filtros['cpf'] = c2.text_input("CPF")
                filtros['rg'] = c3.text_input("RG")
                filtros['nascimento'] = c4.date_input("Nascimento", value=None, format="DD/MM/YYYY")
            with t2:
                c_uf, c_cid, c_bai, c_rua = st.columns([1, 3, 3, 3])
                lista_ufs = buscar_opcoes_filtro('uf', 'pf_enderecos')
                sel_uf = c_uf.selectbox("UF", [""] + lista_ufs)
                if sel_uf: filtros['uf'] = sel_uf
                filtros['cidade'] = c_cid.text_input("Cidade")
                filtros['bairro'] = c_bai.text_input("Bairro")
                filtros['rua'] = c_rua.text_input("Rua")
            with t3:
                c_ddd, c_tel, c_email = st.columns([0.5, 1.5, 4])
                filtros['ddd'] = c_ddd.text_input("DDD", max_chars=2)
                filtros['telefone'] = c_tel.text_input("Telefone", max_chars=9)
                filtros['email'] = c_email.text_input("E-mail")
            with t4:
                c_conv, c_matr = st.columns(2)
                lista_conv = buscar_referencias('CONVENIO')
                sel_conv = c_conv.selectbox("Convênio", [""] + lista_conv)
                if sel_conv: filtros['convenio'] = sel_conv
                filtros['matricula'] = c_matr.text_input("Matrícula")
            with t5:
                filtros['contrato'] = st.text_input("Número do Contrato")
            with t6:
                conn = get_conn()
                opcoes_imp = {}
                if conn:
                    try:
                        df_imp = pd.read_sql("SELECT id, nome_arquivo, data_importacao FROM pf_historico_importacoes ORDER BY id DESC", conn)
                        for _, row in df_imp.iterrows():
                             dt = row['data_importacao'].strftime('%d/%m/%Y %H:%M')
                             label = f"{dt} - {row['nome_arquivo']} (ID: {row['id']})"
                             opcoes_imp[label] = row['id']
                    except: pass
                    conn.close()
                sel_imp = st.selectbox("Filtrar por Importação", [""] + list(opcoes_imp.keys()))
                if sel_imp: filtros['importacao_id'] = opcoes_imp[sel_imp]

            btn_pesquisar = st.form_submit_button("Pesquisar")

        if btn_pesquisar:
            filtros_limpos = {k: v for k, v in filtros.items() if v}
            st.session_state['filtros_ativos'] = filtros_limpos
            st.session_state['pesquisa_pag'] = 1
            st.session_state['selecionados'] = {} # Limpa seleção ao nova busca
        
        if 'filtros_ativos' in st.session_state and st.session_state['filtros_ativos']:
            pag_atual = st.session_state['pesquisa_pag']
            df_res, total_registros = executar_pesquisa_ampla(st.session_state['filtros_ativos'], pag_atual)
            st.divider()
            st.write(f"**Resultados Encontrados:** {total_registros}")
            
            if not df_res.empty:
                if 'cpf' in df_res.columns: df_res['cpf'] = df_res['cpf'].apply(formatar_cpf_visual)
                
                # --- NOVO LAYOUT DE GRID (LINHA POR LINHA) ---
                
                # Botão Selecionar Todos
                if st.button("✅ Selecionar Todos da Página"):
                    for i, r in df_res.iterrows():
                        st.session_state['selecionados'][r['id']] = True
                    st.rerun()

                # Cabeçalho Fixo (ATUALIZADO)
                st.markdown("""
                <div style="background-color: #e6e9ef; padding: 10px; border-radius: 5px; display: flex; align-items: center; border-bottom: 2px solid #ccc; font-weight: bold; font-family: sans-serif;">
                    <div style="flex: 0.5; text-align: center;">Sel</div>
                    <div style="flex: 1.5; text-align: center;">Ações</div>
                    <div style="flex: 1; padding-left: 5px;">Cód.</div>
                    <div style="flex: 2;">CPF</div>
                    <div style="flex: 4;">Nome</div>
                </div>
                """, unsafe_allow_html=True)

                # Loop de Linhas
                for idx, row in df_res.iterrows():
                    c1, c2, c3, c4, c5 = st.columns([0.5, 1.5, 1, 2, 4])
                    
                    # Col 1: Selecionar
                    is_sel = c1.checkbox("", key=f"chk_sel_{row['id']}", value=st.session_state['selecionados'].get(row['id'], False))
                    st.session_state['selecionados'][row['id']] = is_sel

                    # Col 2: Botões de Ação
                    b1, b2, b3 = c2.columns(3)
                    cpf_limpo = limpar_normalizar_cpf(row['cpf'])
                    if b1.button("👁️", key=f"v_a_{row['id']}", help="Ver"):
                        dialog_visualizar_cliente(cpf_limpo)
                    if b2.button("✏️", key=f"e_a_{row['id']}", help="Editar"):
                        st.session_state.update({'pf_view': 'editar', 'pf_cpf_selecionado': cpf_limpo, 'form_loaded': False})
                        st.rerun()
                    if b3.button("🗑️", key=f"d_a_{row['id']}", help="Excluir"):
                        dialog_excluir_pf(cpf_limpo, row['nome'])

                    # Col 3, 4, 5: Dados (Apenas Leitura)
                    c3.write(str(row['id']))
                    c4.write(row['cpf'])
                    c5.write(row['nome'])
                    
                    # Linha de Grade (ATUALIZADO)
                    st.markdown("<div style='border-bottom: 1px solid #e0e0e0; margin-bottom: 5px;'></div>", unsafe_allow_html=True)
                
                # --- BOTÃO DE EXPORTAÇÃO ---
                df_export, _ = executar_pesquisa_ampla(st.session_state['filtros_ativos'], exportar=True)
                if not df_export.empty:
                    if 'cpf' in df_export.columns: df_export['cpf'] = df_export['cpf'].apply(formatar_cpf_visual)
                    if 'data_nascimento' in df_export.columns:
                         df_export['data_nascimento'] = pd.to_datetime(df_export['data_nascimento']).dt.strftime('%d/%m/%Y')
                    
                    csv = df_export.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button("📤 Exportar Pesquisa Completa", data=csv, file_name="resultado_pesquisa_ampla.csv", mime="text/csv")
                
                # --- PAGINAÇÃO ---
                total_paginas = math.ceil(total_registros / 50)
                st.divider()
                cp1, cp2, cp3 = st.columns([1, 3, 1])
                if cp1.button("⬅️ Anterior") and pag_atual > 1:
                    st.session_state['pesquisa_pag'] -= 1
                    st.session_state['selecionados'] = {}
                    st.rerun()
                cp2.markdown(f"<div style='text-align: center'>Página <b>{pag_atual}</b> de <b>{total_paginas}</b></div>", unsafe_allow_html=True)
                if cp3.button("Próximo ➡️") and pag_atual < total_paginas:
                    st.session_state['pesquisa_pag'] += 1
                    st.session_state['selecionados'] = {}
                    st.rerun()

            else: st.warning("Nenhum registro encontrado.")

    # ==========================
    # 2. HISTÓRICO DE IMPORTAÇÕES
    # ==========================
    elif st.session_state['pf_view'] == 'historico_importacao':
        # ... (Mantido código histórico sem alterações) ...
        st.button("⬅️ Voltar para Lista", on_click=lambda: st.session_state.update({'pf_view': 'importacao', 'import_step': 1}))
        st.markdown("### 📜 Histórico de Importações")
        conn = get_conn()
        if conn:
            df_hist = pd.read_sql("SELECT * FROM pf_historico_importacoes ORDER BY data_importacao DESC", conn)
            conn.close()
            if not df_hist.empty:
                for _, row in df_hist.iterrows():
                    data_fmt = row['data_importacao'].strftime("%d/%m/%Y %H:%M")
                    with st.expander(f"{data_fmt} - {row['nome_arquivo']}"):
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Novos", row['qtd_novos'])
                        c2.metric("Atualizados", row['qtd_atualizados'])
                        c3.metric("Erros", row['qtd_erros'])
                        col_btns = c4.columns(2)
                        if col_btns[0].button("🔎", key=f"src_{row['id']}", help="Ver clientes desta importação"):
                            st.session_state['pf_view'] = 'lista'
                            st.session_state['filtro_importacao_id'] = row['id']
                            st.rerun()
                        if row['qtd_erros'] > 0 and row['caminho_arquivo_erro']:
                            if os.path.exists(row['caminho_arquivo_erro']):
                                with open(row['caminho_arquivo_erro'], "rb") as f:
                                    col_btns[1].download_button("📥 Erros", f, file_name=os.path.basename(row['caminho_arquivo_erro']), key=f"dw_{row['id']}")
            else: st.info("Nenhum histórico.")

    # ==========================
    # 3. MODO IMPORTAÇÃO (BULK)
    # ==========================
    elif st.session_state['pf_view'] == 'importacao':
        # ... (Mantido código importação sem alterações) ...
        c_cancel, c_hist = st.columns([1, 4])
        c_cancel.button("⬅️ Cancelar", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'import_step': 1}))
        c_hist.button("📜 Ver Histórico Importação", on_click=lambda: st.session_state.update({'pf_view': 'historico_importacao'}))
        st.divider()
        st.markdown("""<style>section[data-testid="stFileUploader"] div[role="button"] > div > div > span {display: none;} section[data-testid="stFileUploader"] div[role="button"] > div > div::after {content: "Arraste o arquivo CSV aqui"; font-size: 1rem; visibility: visible; display: block;} section[data-testid="stFileUploader"] div[role="button"] > div > div > small {display: none;} section[data-testid="stFileUploader"] div[role="button"] > div > div::before {content: "Limite 200MB • CSV"; font-size: 0.8em; visibility: visible; display: block; margin-bottom: 10px;}</style>""", unsafe_allow_html=True)
        
        opcoes_tabelas = ["Dados Cadastrais (pf_dados)", "Telefones (pf_telefones)", "Emails (pf_emails)", "Endereços (pf_enderecos)", "Emprego/Renda (pf_emprego_renda)", "Contratos (pf_contratos)"]
        mapa_real = {"Dados Cadastrais (pf_dados)": "pf_dados", "Telefones (pf_telefones)": "pf_telefones", "Emails (pf_emails)": "pf_emails", "Endereços (pf_enderecos)": "pf_enderecos", "Emprego/Renda (pf_emprego_renda)": "pf_emprego_renda", "Contratos (pf_contratos)": "pf_contratos"}

        if st.session_state['import_step'] == 1:
            st.markdown("### 📤 Etapa 1: Upload")
            sel_amigavel = st.selectbox("Selecione a Tabela de Destino", opcoes_tabelas)
            st.session_state['import_table'] = mapa_real[sel_amigavel]
            uploaded_file = st.file_uploader("Carregar Arquivo CSV", type=['csv'])
            if uploaded_file:
                try:
                    uploaded_file.seek(0)
                    try:
                        df = pd.read_csv(uploaded_file, sep=';')
                        if len(df.columns) <= 1: uploaded_file.seek(0); df = pd.read_csv(uploaded_file, sep=',')
                    except: uploaded_file.seek(0); df = pd.read_csv(uploaded_file, sep=',')
                    st.session_state['import_df'] = df
                    st.session_state['uploaded_file_name'] = uploaded_file.name
                    st.success(f"Carregado! {len(df)} linhas.")
                    if st.button("Ir para Mapeamento", type="primary"):
                        st.session_state['csv_map'] = {col: None for col in df.columns}
                        st.session_state['current_csv_idx'] = 0
                        st.session_state['import_step'] = 2
                        st.rerun()
                except Exception as e: st.error(f"Erro: {e}")

        elif st.session_state['import_step'] == 2:
            st.markdown("### 🔗 Etapa 2: Mapeamento Visual")
            df = st.session_state['import_df']
            csv_cols = list(df.columns)
            table_name = st.session_state['import_table']
            
            # --- LÓGICA DE MAPEAMENTO (ATUALIZADA) ---
            if table_name == 'pf_telefones':
                # Opções especiais para telefones múltiplos
                db_fields = ['cpf_ref (Vínculo)', 'tag_whats', 'tag_qualificacao'] + [f'telefone_{i}' for i in range(1, 11)]
            else:
                # Padrão para outras tabelas
                db_cols_info = get_table_columns(table_name)
                ignore_db = ['id', 'data_criacao', 'data_atualizacao', 'cpf_ref', 'matricula_ref', 'importacao_id']
                db_fields = [c[0] for c in db_cols_info if c[0] not in ignore_db]

            c_l, c_r = st.columns([1, 2])
            with c_l:
                for idx, col in enumerate(csv_cols):
                    mapped = st.session_state['csv_map'].get(col)
                    txt = f"{idx+1}. {col} -> {'✅ '+mapped if mapped else '❓'}"
                    if idx == st.session_state['current_csv_idx']: st.info(txt, icon="👉")
                    else: 
                        if st.button(txt, key=f"s_{idx}"): st.session_state['current_csv_idx'] = idx; st.rerun()
            with c_r:
                cols_b = st.columns(4)
                if cols_b[0].button("🚫 IGNORAR", type="secondary"):
                    curr = csv_cols[st.session_state['current_csv_idx']]
                    st.session_state['csv_map'][curr] = "IGNORAR"
                    if st.session_state['current_csv_idx'] < len(csv_cols) - 1: st.session_state['current_csv_idx'] += 1
                    st.rerun()
                for i, field in enumerate(db_fields):
                    if cols_b[(i+1)%4].button(f"📌 {field}", key=f"m_{field}"):
                        curr = csv_cols[st.session_state['current_csv_idx']]
                        st.session_state['csv_map'][curr] = field
                        if st.session_state['current_csv_idx'] < len(csv_cols) - 1: st.session_state['current_csv_idx'] += 1
                        st.rerun()

            st.divider()
            if st.button("🚀 INICIAR IMPORTAÇÃO (BULK)", type="primary"):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                orig_name = st.session_state.get('uploaded_file_name', 'importacao')
                safe_name = f"{os.path.splitext(orig_name)[0]}_{timestamp}.csv"
                path_orig = os.path.join(BASE_DIR_IMPORTS, safe_name)
                df.to_csv(path_orig, index=False, sep=';')
                
                conn = get_conn()
                import_id = None
                if conn:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO pf_historico_importacoes (nome_arquivo, caminho_arquivo_original) VALUES (%s, %s) RETURNING id", (orig_name, path_orig))
                    import_id = cur.fetchone()[0]
                    conn.commit()
                
                final_map = {k: v for k, v in st.session_state['csv_map'].items() if v and v != "IGNORAR"}
                
                if not final_map: st.error("Mapeie pelo menos uma coluna."); st.stop()

                if conn:
                    with st.spinner("Processando em lote... Aguarde alguns instantes."):
                        try:
                            novos, atualizados, erros_list = processar_importacao_lote(conn, df, table_name, final_map, import_id)
                            conn.commit()
                            
                            # Filtrar amostra apenas com colunas mapeadas
                            cols_mapeadas = list(final_map.keys())
                            df_sample = df[cols_mapeadas].head(5)
                            
                            path_erro = None
                            if erros_list:
                                name_erro = f"{os.path.splitext(orig_name)[0]}_{timestamp}_ERRO.txt"
                                path_erro = os.path.join(BASE_DIR_IMPORTS, name_erro)
                                with open(path_erro, "w", encoding="utf-8") as f: f.write("\n".join(erros_list))
                            
                            cur = conn.cursor()
                            cur.execute("UPDATE pf_historico_importacoes SET qtd_novos=%s, qtd_atualizados=%s, qtd_erros=%s, caminho_arquivo_erro=%s WHERE id=%s", (novos, atualizados, len(erros_list), path_erro, import_id))
                            conn.commit(); cur.close(); conn.close()
                            
                            st.session_state['import_stats'] = {'novos': novos, 'atualizados': atualizados, 'erros': len(erros_list), 'path_erro': path_erro, 'sample': df_sample}
                            st.session_state['import_step'] = 3; st.rerun()
                        except Exception as e:
                            st.error(f"Erro Crítico na Importação: {e}")
                            if conn: conn.close()

        elif st.session_state['import_step'] == 3:
            st.markdown("### ✅ Etapa 3: Resultado da Importação")
            stats = st.session_state.get('import_stats', {})
            c1, c2, c3 = st.columns(3)
            c1.metric("Novos", stats.get('novos', 0)); c2.metric("Atualizados", stats.get('atualizados', 0)); c3.metric("Erros", stats.get('erros', 0), delta_color="inverse")
            st.markdown("#### Amostra"); st.dataframe(stats.get('sample', pd.DataFrame()))
            if stats.get('erros', 0) > 0 and stats.get('path_erro'):
                with open(stats['path_erro'], "rb") as f: st.download_button("⚠️ Baixar Erros (.txt)", f, file_name="erros.txt")
            if st.button("Concluir"): st.session_state['pf_view'] = 'lista'; st.session_state['import_step'] = 1; st.rerun()

    # ==========================
    # 5. MODO NOVO / EDITAR
    # ==========================
    elif st.session_state['pf_view'] in ['novo', 'editar']:
        is_edit = st.session_state['pf_view'] == 'editar'
        cpf_titulo = formatar_cpf_visual(st.session_state.get('pf_cpf_selecionado')) if is_edit else ""
        titulo = f"✏️ Editar Cadastro: {cpf_titulo}" if is_edit else "➕ Novo Cadastro"
        
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'form_loaded': False}))
        st.markdown(f"### {titulo}")

        if is_edit and not st.session_state['form_loaded']:
            dados_db = carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
            st.session_state['dados_gerais_temp'] = dados_db.get('geral', {})
            st.session_state['temp_telefones'] = dados_db.get('telefones', pd.DataFrame()).to_dict('records')
            st.session_state['temp_emails'] = dados_db.get('emails', pd.DataFrame()).to_dict('records')
            st.session_state['temp_enderecos'] = dados_db.get('enderecos', pd.DataFrame()).to_dict('records')
            st.session_state['temp_empregos'] = dados_db.get('empregos', pd.DataFrame()).to_dict('records')
            st.session_state['temp_contratos'] = dados_db.get('contratos', pd.DataFrame()).to_dict('records')
            st.session_state['form_loaded'] = True
        elif not is_edit and not st.session_state['form_loaded']:
            st.session_state['dados_gerais_temp'] = {}
            st.session_state['temp_telefones'] = []
            st.session_state['temp_emails'] = []
            st.session_state['temp_enderecos'] = []
            st.session_state['temp_empregos'] = []
            st.session_state['temp_contratos'] = []
            st.session_state['form_loaded'] = True

        g = st.session_state.get('dados_gerais_temp', {})

        with st.form("form_cadastro_pf"):
            t1, t2, t3, t4 = st.tabs(["👤 Dados Pessoais", "📞 Contatos e Endereço", "💼 Profissional", "📄 Contratos"])
            
            with t1:
                c1, c2, c3 = st.columns(3)
                nome = c1.text_input("Nome Completo *", value=g.get('nome', ''))
                
                cpf_banco = g.get('cpf', '')
                cpf_visual_inicial = formatar_cpf_visual(cpf_banco)
                cpf = c2.text_input("CPF *", value=cpf_visual_inicial, disabled=is_edit, help="Formato visual: 000.000.000-00")
                
                val_nasc = None
                if g.get('data_nascimento'):
                    try: val_nasc = pd.to_datetime(g['data_nascimento']).date()
                    except: val_nasc = None
                d_nasc = c3.date_input("Data Nascimento", value=val_nasc, format="DD/MM/YYYY")
                
                c4, c5, c6 = st.columns(3)
                rg = c4.text_input("RG", value=g.get('rg', ''))
                cnh = c5.text_input("CNH", value=g.get('cnh', ''))
                pis = c6.text_input("PIS", value=g.get('pis', ''))
                
                c7, c8 = st.columns(2)
                nome_mae = c7.text_input("Nome da Mãe", value=g.get('nome_mae', ''))
                nome_pai = c8.text_input("Nome do Pai", value=g.get('nome_pai', ''))

            with t2:
                st.markdown("#### 📞 Telefones")
                c_t1, c_t2, c_t3, c_t4 = st.columns([2, 1, 2, 1.5])
                
                novo_tel = c_t1.text_input("Telefone", key="in_tel", placeholder="(00) 00000-0000")
                novo_whats = c_t2.selectbox("WhatsApp", ["Não", "Sim"], key="in_whats")
                novo_qualif = c_t3.selectbox("Qualificação", ["NÃO CONFIRMADO", "CONFIRMADO"], key="in_qualif")
                
                if c_t4.form_submit_button("➕ Adicionar"):
                    fmt, erro = validar_formatar_telefone(novo_tel)
                    if fmt:
                        st.session_state['temp_telefones'].append({
                            'numero': fmt, 
                            'tag_whats': novo_whats, 
                            'tag_qualificacao': novo_qualif,
                            'data_atualizacao': date.today()
                        })
                        st.success("Telefone adicionado!")
                    else: st.error(erro)
                
                if st.session_state['temp_telefones']:
                    ch1, ch2, ch3, ch4, ch5 = st.columns([2, 1, 2, 1.5, 0.5])
                    ch1.caption("**Número**")
                    ch2.caption("**WhatsApp**")
                    ch3.caption("**Qualificação**")
                    ch4.caption("**Atualizado**")
                    ch5.caption("")
                    st.divider()

                    for i, t in enumerate(st.session_state['temp_telefones']):
                        col_l1, col_l2, col_l3, col_l4, col_l5 = st.columns([2, 1, 2, 1.5, 0.5])
                        col_l1.text(t['numero'])
                        col_l2.text(t['tag_whats'])
                        col_l3.text(t['tag_qualificacao'])
                        
                        d_show = t.get('data_atualizacao')
                        if isinstance(d_show, str):
                             try: d_show = datetime.strptime(d_show, '%Y-%m-%d').date()
                             except: pass
                        if isinstance(d_show, (date, datetime)):
                             d_show = d_show.strftime('%d/%m/%Y')
                        
                        col_l4.text(str(d_show))
                        
                        if col_l5.form_submit_button("🗑️", key=f"del_tel_{i}"):
                            st.session_state['temp_telefones'].pop(i)
                            st.rerun()
                
                st.divider()
                st.markdown("#### 📧 E-mails")
                c_e1, c_e2 = st.columns([4, 1])
                novo_email = c_e1.text_input("Novo E-mail", key="in_email")
                if c_e2.form_submit_button("➕ Adicionar Email"):
                    if validar_email(novo_email):
                        st.session_state['temp_emails'].append({'email': novo_email})
                        st.success("Email adicionado!")
                    else: st.error("Formato de e-mail inválido.")
                
                if st.session_state['temp_emails']:
                    for i, e in enumerate(st.session_state['temp_emails']):
                        col_l1, col_l2 = st.columns([5, 1])
                        col_l1.text(e['email'])
                        if col_l2.form_submit_button("🗑️", key=f"del_mail_{i}"):
                            st.session_state['temp_emails'].pop(i)
                            st.rerun()

                st.divider()
                st.markdown("#### 🏠 Endereço")
                ce1, ce2, ce3 = st.columns([1.5, 3, 1])
                n_cep = ce1.text_input("CEP", key="in_cep")
                n_rua = ce2.text_input("Logradouro", key="in_rua")
                n_num = ce3.text_input("Número", key="in_num")
                ce4, ce5, ce6 = st.columns([2, 2, 1])
                n_bairro = ce4.text_input("Bairro", key="in_bairro")
                n_cidade = ce5.text_input("Cidade", key="in_cidade")
                n_uf = ce6.text_input("UF", key="in_uf")
                
                if st.form_submit_button("➕ Adicionar Endereço"):
                    fmt_cep, erro_cep = validar_formatar_cep(n_cep)
                    if fmt_cep:
                        st.session_state['temp_enderecos'].append({
                            'cep': fmt_cep, 'rua': f"{n_rua}, {n_num}", 
                            'bairro': n_bairro, 'cidade': n_cidade, 'uf': n_uf
                        })
                        st.success("Endereço adicionado!")
                    else: st.error(erro_cep)

                if st.session_state['temp_enderecos']:
                    for i, end in enumerate(st.session_state['temp_enderecos']):
                        st.markdown(f"**{end['rua']}** - {end['bairro']}, {end['cidade']}/{end['uf']} ({end['cep']})")
                        if st.form_submit_button("🗑️ Remover este endereço", key=f"del_end_{i}"):
                            st.session_state['temp_enderecos'].pop(i)
                            st.rerun()

            with t3:
                st.markdown("#### 💼 Emprego e Renda")
                ce1, ce2, ce3 = st.columns(3)
                n_conv = ce1.text_input("Convênio", key="in_conv")
                n_matr = ce2.text_input("Matrícula", key="in_matr")
                n_extra = ce3.text_input("Dados Extras", key="in_extra")
                
                if st.form_submit_button("➕ Adicionar Vínculo"):
                    if n_conv and n_matr:
                        st.session_state['temp_empregos'].append({'convenio': n_conv, 'matricula': n_matr, 'dados_extras': n_extra})
                        st.success("Vínculo adicionado!")
                    else: st.error("Convênio e Matrícula são obrigatórios.")

                if st.session_state['temp_empregos']:
                    for i, emp in enumerate(st.session_state['temp_empregos']):
                        col_l1, col_l2, col_l3, col_l4 = st.columns([2, 2, 3, 1])
                        col_l1.text(emp['convenio'])
                        col_l2.text(emp['matricula'])
                        col_l3.text(emp['dados_extras'])
                        if col_l4.form_submit_button("🗑️", key=f"del_emp_{i}"):
                            st.session_state['temp_empregos'].pop(i)
                            st.rerun()

            with t4:
                st.markdown("#### 📄 Contratos")
                cc1, cc2, cc3 = st.columns(3)
                lista_matr = [e['matricula'] for e in st.session_state['temp_empregos'] if 'matricula' in e]
                n_matr_ref = cc1.selectbox("Matrícula Vinculada", lista_matr, key="in_ctr_matr")
                n_contrato = cc2.text_input("Número Contrato", key="in_contrato")
                n_ctr_extra = cc3.text_input("Detalhes", key="in_ctr_extra")
                
                if st.form_submit_button("➕ Adicionar Contrato"):
                    if n_matr_ref and n_contrato:
                        st.session_state['temp_contratos'].append({'matricula_ref': n_matr_ref, 'contrato': n_contrato, 'dados_extras': n_ctr_extra})
                        st.success("Contrato adicionado!")
                    else: st.error("Matrícula e Contrato são obrigatórios.")

                if st.session_state['temp_contratos']:
                    for i, ctr in enumerate(st.session_state['temp_contratos']):
                        col_l1, col_l2, col_l3, col_l4 = st.columns([2, 2, 3, 1])
                        col_l1.text(ctr['matricula_ref'])
                        col_l2.text(ctr['contrato'])
                        col_l3.text(ctr['dados_extras'])
                        if col_l4.form_submit_button("🗑️", key=f"del_ctr_{i}"):
                            st.session_state['temp_contratos'].pop(i)
                            st.rerun()

            st.markdown("---")
            col_b1, col_b2 = st.columns([1, 5])
            
            if col_b1.form_submit_button("💾 SALVAR CADASTRO COMPLETO", type="primary"):
                cpf_fmt, erro_cpf = validar_formatar_cpf(cpf)
                
                if not nome or not cpf:
                    st.error("Nome e CPF são obrigatórios.")
                elif erro_cpf:
                    st.error(erro_cpf)
                else:
                    dados_gerais = {
                        'cpf': limpar_normalizar_cpf(cpf), 'nome': nome, 'data_nascimento': d_nasc,
                        'rg': rg, 'cnh': cnh, 'pis': pis,
                        'nome_mae': nome_mae, 'nome_pai': nome_pai
                    }
                    
                    df_tel_save = pd.DataFrame(st.session_state['temp_telefones'])
                    df_email_save = pd.DataFrame(st.session_state['temp_emails'])
                    df_end_save = pd.DataFrame(st.session_state['temp_enderecos'])
                    df_emp_save = pd.DataFrame(st.session_state['temp_empregos'])
                    df_contr_save = pd.DataFrame(st.session_state['temp_contratos'])
                    
                    modo_salvar = "editar" if is_edit else "novo"
                    cpf_orig = limpar_normalizar_cpf(st.session_state.get('pf_cpf_selecionado')) if is_edit else None
                    
                    sucesso, msg = salvar_pf(dados_gerais, df_tel_save, df_email_save, df_end_save, df_emp_save, df_contr_save, modo_salvar, cpf_orig)
                    
                    if sucesso:
                        st.success(msg)
                        time.sleep(1)
                        st.session_state['form_loaded'] = False
                        st.session_state['pf_view'] = 'lista'
                        st.rerun()
                    else:
                        st.error(msg)

    # ==========================
    # 6. MODO LISTA (INICIAL)
    # ==========================
    elif st.session_state['pf_view'] == 'lista':
        # Reinicia contadores
        for k in ['count_tel', 'count_email', 'count_end', 'count_emp', 'count_ctr']:
            st.session_state[k] = 1

        filtro_imp = st.session_state.get('filtro_importacao_id')
        c1, c2 = st.columns([2, 2])
        with c2: 
            label_busca = "🔎 Pesquisar Rápida" + (" (Filtrado)" if filtro_imp else "")
            busca = st.text_input(label_busca, key="pf_busca")
        if filtro_imp and st.button("❌ Limpar Filtro"): st.session_state['filtro_importacao_id'] = None; st.rerun()
            
        # Barra de Ferramentas Superior
        col_b1, col_b2, col_b3, col_b4 = st.columns([1, 1, 1, 1])
        if col_b1.button("➕ Novo", type="primary", use_container_width=True): 
            st.session_state.update({'pf_view': 'novo', 'form_loaded': False})
            st.rerun()
        if col_b2.button("🔍 Pesquisa Ampla", type="primary", use_container_width=True): st.session_state.update({'pf_view': 'pesquisa_ampla'}); st.rerun()
        if col_b3.button("📥 Importar", type="primary", use_container_width=True): st.session_state.update({'pf_view': 'importacao', 'import_step': 1}); st.rerun()

        # Botão Exportar (Todas as páginas)
        if busca or filtro_imp:
             # Busca para exportação
             df_export, _ = buscar_pf_simples(busca, filtro_imp, exportar=True)
             if not df_export.empty:
                 # Formatação Visual para o Excel
                 if 'cpf' in df_export.columns: df_export['cpf'] = df_export['cpf'].apply(formatar_cpf_visual)
                 
                 csv = df_export.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig').encode('utf-8-sig')
                 # BOTÃO EXPORTAR MOVIDO PARA BAIXO DA TABELA
                 # (Mantido aqui como lógica, mas será renderizado abaixo)

        if busca or filtro_imp:
            # Busca Paginada para Visualização
            pagina = st.session_state['pagina_atual']
            df_lista, total_registros = buscar_pf_simples(busca, filtro_imp, pagina=pagina)
            
            if not df_lista.empty:
                # Aplica formatação visual na coluna CPF
                if 'cpf' in df_lista.columns:
                    df_lista['cpf'] = df_lista['cpf'].apply(formatar_cpf_visual)
                
                # --- NOVO LAYOUT DE GRID (LINHA POR LINHA) ---
                
                # Botão Selecionar Todos
                if st.button("✅ Selecionar Todos da Página"):
                    for i, r in df_lista.iterrows():
                        st.session_state['selecionados'][r['id']] = True
                    st.rerun()

                # Cabeçalho Fixo (ATUALIZADO)
                st.markdown("""
                <div style="background-color: #e6e9ef; padding: 10px; border-radius: 5px; display: flex; align-items: center; border-bottom: 2px solid #ccc; font-weight: bold; font-family: sans-serif;">
                    <div style="flex: 0.5; text-align: center;">Sel</div>
                    <div style="flex: 1.5; text-align: center;">Ações</div>
                    <div style="flex: 1; padding-left: 5px;">Cód.</div>
                    <div style="flex: 2;">CPF</div>
                    <div style="flex: 4;">Nome</div>
                </div>
                """, unsafe_allow_html=True)

                # Loop de Linhas
                for idx, row in df_lista.iterrows():
                    c1, c2, c3, c4, c5 = st.columns([0.5, 1.5, 1, 2, 4])
                    
                    # Col 1: Selecionar
                    is_sel = c1.checkbox("", key=f"chk_sel_rap_{row['id']}", value=st.session_state['selecionados'].get(row['id'], False))
                    st.session_state['selecionados'][row['id']] = is_sel

                    # Col 2: Botões de Ação
                    b1, b2, b3 = c2.columns(3)
                    cpf_limpo = limpar_normalizar_cpf(row['cpf'])
                    if b1.button("👁️", key=f"v_r_{row['id']}", help="Ver"):
                        dialog_visualizar_cliente(cpf_limpo)
                    if b2.button("✏️", key=f"e_r_{row['id']}", help="Editar"):
                        st.session_state.update({'pf_view': 'editar', 'pf_cpf_selecionado': cpf_limpo, 'form_loaded': False})
                        st.rerun()
                    if b3.button("🗑️", key=f"d_r_{row['id']}", help="Excluir"):
                        dialog_excluir_pf(cpf_limpo, row['nome'])

                    # Col 3, 4, 5: Dados (Apenas Leitura)
                    c3.write(str(row['id']))
                    c4.write(row['cpf'])
                    c5.write(row['nome'])
                    
                    # Linha de Grade (ATUALIZADO)
                    st.markdown("<div style='border-bottom: 1px solid #e0e0e0; margin-bottom: 5px;'></div>", unsafe_allow_html=True)

                # --- BOTÃO DE EXPORTAÇÃO (POSICIONADO ABAIXO DA TABELA) ---
                if 'df_export' in locals() and not df_export.empty:
                    st.download_button("📤 Exportar Pesquisa Completa", data=csv, file_name="resultado_pesquisa_rapida.csv", mime="text/csv")
                # ----------------------------------------------------------

                # --- PAGINAÇÃO ---
                total_paginas = math.ceil(total_registros / 50)
                st.divider()
                cp1, cp2, cp3 = st.columns([1, 3, 1])
                
                if cp1.button("⬅️ Anterior") and pagina > 1:
                    st.session_state['pagina_atual'] -= 1
                    st.session_state['selecionados'] = {}
                    st.rerun()
                
                cp2.markdown(f"<div style='text-align: center'>Página <b>{pagina}</b> de <b>{total_paginas}</b></div>", unsafe_allow_html=True)
                
                if cp3.button("Próximo ➡️") and pagina < total_paginas:
                    st.session_state['pagina_atual'] += 1
                    st.session_state['selecionados'] = {}
                    st.rerun()

            else: st.warning("Sem resultados.")
        else: st.info("Use a pesquisa para ver cadastros.")
    
    # RODAPÉ
    br_time = datetime.now() - timedelta(hours=3)
    st.caption(f"Atualizado em: {br_time.strftime('%d/%m/%Y %H:%M')}")

if __name__ == "__main__":
    app_pessoa_fisica()