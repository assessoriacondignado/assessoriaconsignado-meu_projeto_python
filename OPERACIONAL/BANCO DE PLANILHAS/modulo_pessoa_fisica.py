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
            
            # Tabela de Histórico
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
            
            # Adiciona coluna de rastreio em TODAS as tabelas
            tabelas = ['pf_dados', 'pf_telefones', 'pf_emails', 'pf_enderecos', 'pf_emprego_renda', 'pf_contratos']
            for tb in tabelas:
                try:
                    cur.execute(f"ALTER TABLE {tb} ADD COLUMN IF NOT EXISTS importacao_id INTEGER REFERENCES pf_historico_importacoes(id);")
                except: pass

            # Tenta adicionar na tabela admin se ela existir e for acessível
            try:
                cur.execute("ALTER TABLE admin.pf_contratos_clt ADD COLUMN IF NOT EXISTS importacao_id INTEGER;")
            except: pass
            
            # Tabela de Referências (Convênios)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pf_referencias (
                    id SERIAL PRIMARY KEY,
                    tipo VARCHAR(50),
                    nome VARCHAR(100),
                    UNIQUE(tipo, nome)
                );
            """)

            # --- NOVA TABELA DE OPERADORES ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pf_operadores_de_filtro (
                    id SERIAL PRIMARY KEY,
                    tipo VARCHAR(20), -- texto, numero, data
                    nome VARCHAR(50),
                    simbolo VARCHAR(10),
                    descricao VARCHAR(100),
                    UNIQUE(tipo, simbolo)
                );
            """)
            
            # Popula Operadores (Se vazio)
            cur.execute("SELECT COUNT(*) FROM pf_operadores_de_filtro")
            if cur.fetchone()[0] == 0:
                ops = [
                    # Texto
                    ('texto', 'Começa com', '=>', 'Busca registros que iniciam com o valor'),
                    ('texto', 'Contém', '<=>', 'Busca o valor em qualquer parte do texto'),
                    ('texto', 'Igual', '=', 'Exatamente igual'),
                    ('texto', 'Seleção', 'o', 'Pesquisa múltipla (separe por vírgula)'),
                    ('texto', 'Diferente', '≠', 'Diferente de'),
                    ('texto', 'Não Contém', '<≠>', 'Exclui resultados que tenham essa palavra'),
                    ('texto', 'Vazio', '∅', 'Campo não preenchido'),
                    
                    # Número
                    ('numero', 'Igual', '=', 'Valor exato'),
                    ('numero', 'Maior', '>', 'Maior que'),
                    ('numero', 'Menor', '<', 'Menor que'),
                    ('numero', 'Maior Igual', '≥', 'Maior ou igual a'),
                    ('numero', 'Menor Igual', '≤', 'Menor ou igual a'),
                    ('numero', 'Diferente', '≠', 'Diferente do valor'),
                    ('numero', 'Vazio', '∅', 'Sem valor numérico'),

                    # Data
                    ('data', 'Igual', '=', 'Data exata'),
                    ('data', 'A Partir', '≥', 'Desta data em diante'),
                    ('data', 'Até', '≤', 'Até esta data'),
                    ('data', 'Vazio', '∅', 'Sem data')
                ]
                cur.executemany("INSERT INTO pf_operadores_de_filtro (tipo, nome, simbolo, descricao) VALUES (%s, %s, %s, %s)", ops)

            conn.commit()
            conn.close()
        except: pass

# --- FUNÇÕES AUXILIARES E VALIDAÇÃO ---
def formatar_cpf_visual(cpf_db):
    if not cpf_db: return ""
    cpf_limpo = str(cpf_db).strip()
    cpf_full = cpf_limpo.zfill(11)
    return f"{cpf_full[:3]}.{cpf_full[3:6]}.{cpf_full[6:9]}-{cpf_full[9:]}"

def limpar_normalizar_cpf(cpf_raw):
    if not cpf_raw: return ""
    apenas_nums = re.sub(r'\D', '', str(cpf_raw))
    if not apenas_nums: return ""
    return apenas_nums.lstrip('0')

def limpar_apenas_numeros(valor):
    if not valor: return ""
    return re.sub(r'\D', '', str(valor))

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
    if re.match(regex, email): return True
    return False

def validar_formatar_cep(cep_raw):
    numeros = limpar_apenas_numeros(cep_raw)
    if len(numeros) != 8: return None, "CEP deve ter 8 dígitos."
    return f"{numeros[:5]}-{numeros[5:]}", None

def converter_data_br_iso(valor):
    if not valor or pd.isna(valor): return None
    valor_str = str(valor).strip().split(' ')[0]
    formatos = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"]
    for fmt in formatos:
        try: return datetime.strptime(valor_str, fmt).strftime("%Y-%m-%d")
        except ValueError: continue
    return None

def buscar_referencias(tipo):
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT nome FROM pf_referencias WHERE tipo = %s ORDER BY nome", conn, params=(tipo,))
            conn.close()
            return df['nome'].tolist()
        except: conn.close()
    return []

# --- IMPORTAÇÃO ---
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

def processar_importacao_lote(conn, df, table_name, mapping, import_id):
    cur = conn.cursor()
    try:
        erros = []
        df_proc = pd.DataFrame()
        cols_order = []
        if table_name == 'pf_telefones':
            col_cpf = next((k for k, v in mapping.items() if v == 'cpf_ref (Vínculo)'), None)
            col_whats = next((k for k, v in mapping.items() if v == 'tag_whats'), None)
            col_qualif = next((k for k, v in mapping.items() if v == 'tag_qualificacao'), None)
            map_tels = {k: v for k, v in mapping.items() if v and v.startswith('telefone_')}
            
            if not col_cpf: return 0, 0, ["Erro: Coluna 'CPF (Vínculo)' é obrigatória."]
            
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
                            new_rows.append({'cpf_ref': cpf_limpo, 'numero': tel_limpo, 'tag_whats': whats_val, 'tag_qualificacao': qualif_val, 'importacao_id': import_id, 'data_atualizacao': datetime.now().strftime('%Y-%m-%d')})
            if not new_rows: return 0, 0, ["Nenhum telefone válido."]
            df_proc = pd.DataFrame(new_rows)
            cols_order = list(df_proc.columns)
        else:
            df_proc = df.rename(columns=mapping)
            cols_db = list(mapping.values())
            df_proc = df_proc[cols_db].copy()
            df_proc['importacao_id'] = import_id
            if 'cpf' in df_proc.columns:
                df_proc['cpf'] = df_proc['cpf'].astype(str).apply(limpar_normalizar_cpf)
                if table_name == 'pf_dados': df_proc = df_proc[df_proc['cpf'] != ""]
            cols_data = ['data_nascimento', 'data_exp_rg', 'data_criacao', 'data_atualizacao']
            for col in cols_data:
                if col in df_proc.columns: df_proc[col] = df_proc[col].apply(converter_data_br_iso)
            cols_order = list(df_proc.columns)

        staging_table = f"staging_import_{import_id}"
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP")
        output = io.StringIO()
        df_proc.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        cur.copy_expert(f"COPY {staging_table} ({', '.join(cols_order)}) FROM STDIN WITH CSV DELIMITER E'\t' NULL '\\N'", output)
        
        pk_field = 'cpf' if 'cpf' in df_proc.columns else ('matricula' if 'matricula' in df_proc.columns else None)
        qtd_novos, qtd_atualizados = 0, 0
        if pk_field:
            set_clause = ', '.join([f'{c} = s.{c}' for c in cols_order if c != pk_field])
            cur.execute(f"UPDATE {table_name} t SET {set_clause} FROM {staging_table} s WHERE t.{pk_field} = s.{pk_field}")
            qtd_atualizados = cur.rowcount
            cur.execute(f"INSERT INTO {table_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s WHERE NOT EXISTS (SELECT 1 FROM {table_name} t WHERE t.{pk_field} = s.{pk_field})")
            qtd_novos = cur.rowcount
        else:
            cur.execute(f"INSERT INTO {table_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s")
            qtd_novos = cur.rowcount
            if table_name in ['pf_telefones', 'pf_emails', 'pf_enderecos', 'pf_emprego_renda']:
                cur.execute(f"UPDATE pf_dados d SET importacao_id = %s FROM {staging_table} s WHERE d.cpf = s.cpf_ref", (import_id,))
            elif table_name == 'pf_contratos':
                cur.execute(f"UPDATE pf_dados d SET importacao_id = %s FROM pf_emprego_renda e JOIN {staging_table} s ON e.matricula = s.matricula_ref WHERE d.cpf = e.cpf_ref", (import_id,))

        return qtd_novos, qtd_atualizados, erros
    except Exception as e: raise e

# --- FUNÇÃO DE BUSCA: EXECUÇÃO DINÂMICA (CORRIGIDO PARA DATA) ---
def executar_pesquisa_ampla(regras_ativas, pagina=1, itens_por_pagina=50, exportar=False):
    conn = get_conn()
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
            
            active_joins = []
            conditions = []
            params = []

            for regra in regras_ativas:
                tabela = regra['tabela']
                coluna = regra['coluna']
                op = regra['operador']
                val_raw = regra['valor']
                tipo = regra['tipo']
                
                # Identifica Join necessário
                if tabela in joins_map and joins_map[tabela] not in active_joins:
                    active_joins.append(joins_map[tabela])
                
                col_sql = f"{coluna}" 
                
                if op == "∅": 
                    conditions.append(f"({col_sql} IS NULL OR {col_sql}::TEXT = '')")
                    continue
                
                if val_raw is None or str(val_raw).strip() == "": continue
                
                valores = [v.strip() for v in str(val_raw).split(',') if v.strip()]
                if not valores: continue

                conds_or = []
                for val in valores:
                    if 'cpf' in coluna or 'cnpj' in coluna: val = limpar_normalizar_cpf(val)
                    if tipo == 'numero': val = re.sub(r'\D', '', val)

                    # TRATAMENTO DATA
                    if tipo == 'data':
                        if op == "=": conds_or.append(f"{col_sql} = %s"); params.append(val)
                        elif op == "≥": conds_or.append(f"{col_sql} >= %s"); params.append(val)
                        elif op == "≤": conds_or.append(f"{col_sql} <= %s"); params.append(val)
                        elif op == "≠": conds_or.append(f"{col_sql} <> %s"); params.append(val)
                        continue 

                    # TRATAMENTO TEXTO/NUMERO
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
                    conditions.append(f"{col_sql} IN ({placeholders})")
                    params.extend(valores)
                elif conds_or:
                    conditions.append(f"({' OR '.join(conds_or)})")

            full_joins = " ".join(active_joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            if exportar:
                query = f"{sql_select} {sql_from} {full_joins} {sql_where} ORDER BY d.nome LIMIT 1000000"
                df = pd.read_sql(query, conn, params=tuple(params))
                conn.close(); return df.fillna(""), len(df)
            
            count_sql = f"SELECT COUNT(DISTINCT d.id) {sql_from} {full_joins} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total = cur.fetchone()[0]
            
            offset = (pagina - 1) * itens_por_pagina
            query = f"{sql_select} {sql_from} {full_joins} {sql_where} ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}"
            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close(); return df.fillna(""), total
        except Exception as e:
            st.error(f"Erro SQL: {e}"); return pd.DataFrame(), 0
    return pd.DataFrame(), 0

# --- CRUD BÁSICO E LUPA (MANTIDOS) ---
def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {}
    if conn:
        try:
            cpf_norm = limpar_normalizar_cpf(cpf)
            df_d = pd.read_sql("SELECT * FROM pf_dados WHERE cpf = %s", conn, params=(cpf_norm,))
            if not df_d.empty: dados['geral'] = df_d.fillna("").iloc[0]
            else: dados['geral'] = None
            
            dados['telefones'] = pd.read_sql("SELECT numero, data_atualizacao, tag_whats, tag_qualificacao FROM pf_telefones WHERE cpf_ref = %s", conn, params=(cpf_norm,)).fillna("")
            dados['emails'] = pd.read_sql("SELECT email FROM pf_emails WHERE cpf_ref = %s", conn, params=(cpf_norm,)).fillna("")
            dados['enderecos'] = pd.read_sql("SELECT rua, bairro, cidade, uf, cep FROM pf_enderecos WHERE cpf_ref = %s", conn, params=(cpf_norm,)).fillna("")
            dados['empregos'] = pd.read_sql("SELECT id, convenio, matricula, dados_extras FROM pf_emprego_renda WHERE cpf_ref = %s", conn, params=(cpf_norm,)).fillna("")
            dados['contratos'] = pd.DataFrame()
            dados['dados_clt'] = pd.DataFrame() 

            if not dados['empregos'].empty:
                matr_list = tuple(dados['empregos']['matricula'].dropna().tolist())
                if matr_list:
                    placeholders = ",".join(["%s"] * len(matr_list))
                    q_contratos = f"SELECT matricula_ref, contrato, dados_extras FROM pf_contratos WHERE matricula_ref IN ({placeholders})"
                    dados['contratos'] = pd.read_sql(q_contratos, conn, params=matr_list).fillna("")
                    try:
                        q_clt = f"""
                            SELECT matricula_ref, nome_convenio, cnpj_nome, cnpj_numero, cnae_nome, cnae_codigo, data_admissao, cbo_nome, cbo_codigo, 
                                   qtd_funcionarios, data_abertura_empresa, tempo_abertura_anos, tempo_admissao_anos
                            FROM admin.pf_contratos_clt WHERE matricula_ref IN ({placeholders})
                        """
                        dados['dados_clt'] = pd.read_sql(q_clt, conn, params=matr_list).fillna("")
                        for col in ['data_admissao', 'data_abertura_empresa']:
                            if col in dados['dados_clt'].columns: dados['dados_clt'][col] = pd.to_datetime(dados['dados_clt'][col], errors='coerce').dt.strftime('%d/%m/%Y')
                    except: pass
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
                cols = list(dados_gerais.keys()); vals = list(dados_gerais.values())
                placeholders = ", ".join(["%s"] * len(vals)); col_names = ", ".join(cols)
                cur.execute(f"INSERT INTO pf_dados ({col_names}) VALUES ({placeholders})", vals)
            else:
                set_clause = ", ".join([f"{k}=%s" for k in dados_gerais.keys()])
                vals = list(dados_gerais.values()) + [cpf_original]
                cur.execute(f"UPDATE pf_dados SET {set_clause} WHERE cpf=%s", vals)
            
            cpf_chave = dados_gerais['cpf']
            if modo == "editar":
                for tb in ['pf_telefones', 'pf_emails', 'pf_enderecos', 'pf_emprego_renda']:
                    cur.execute(f"DELETE FROM {tb} WHERE cpf_ref = %s", (cpf_chave,))
            
            def df_upper(df): return df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
            if not df_tel.empty:
                for _, r in df_upper(df_tel).iterrows(): cur.execute("INSERT INTO pf_telefones (cpf_ref, numero, tag_whats, tag_qualificacao, data_atualizacao) VALUES (%s, %s, %s, %s, %s)", (cpf_chave, r['numero'], r.get('tag_whats'), r.get('tag_qualificacao'), date.today()))
            if not df_email.empty:
                for _, r in df_upper(df_email).iterrows(): cur.execute("INSERT INTO pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, r['email']))
            if not df_end.empty:
                for _, r in df_upper(df_end).iterrows(): cur.execute("INSERT INTO pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, r['rua'], r['bairro'], r['cidade'], r['uf'], r['cep']))
            if not df_emp.empty:
                for _, r in df_upper(df_emp).iterrows(): cur.execute("INSERT INTO pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s)", (cpf_chave, r['convenio'], r['matricula'], r['dados_extras']))
            if not df_contr.empty:
                for _, r in df_upper(df_contr).iterrows():
                    cur.execute("SELECT 1 FROM pf_emprego_renda WHERE matricula=%s", (r['matricula_ref'],))
                    if cur.fetchone(): cur.execute("INSERT INTO pf_contratos (matricula_ref, contrato, dados_extras) VALUES (%s, %s, %s)", (r['matricula_ref'], r['contrato'], r['dados_extras']))

            conn.commit(); conn.close(); return True, "Salvo com sucesso!"
        except Exception as e: return False, str(e)
    return False, "Erro conexão"

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

@st.dialog("👁️ Detalhes do Cliente")
def dialog_visualizar_cliente(cpf_cliente):
    cpf_vis = formatar_cpf_visual(cpf_cliente)
    dados = carregar_dados_completos(cpf_cliente)
    g = dados.get('geral')
    if g is None: st.error("Cliente não encontrado."); return
    
    st.markdown(f"### 👤 {g['nome']}")
    st.markdown(f"**CPF:** {cpf_vis}")
    st.divider()
    t1, t2, t3 = st.tabs(["📋 Cadastro", "💼 Profissional & CLT", "📞 Contatos"])
    with t1:
        c1, c2 = st.columns(2)
        c1.write(f"**Nascimento:** {pd.to_datetime(g['data_nascimento']).strftime('%d/%m/%Y') if g['data_nascimento'] else '-'}")
        c1.write(f"**RG:** {g['rg']}"); c2.write(f"**PIS:** {g['pis']}")
        st.markdown("##### 🏠 Endereços")
        df_end = dados.get('enderecos')
        if not df_end.empty:
            for _, row in df_end.iterrows(): st.info(f"📍 {row['rua']}, {row['bairro']} - {row['cidade']}/{row['uf']}")
    with t2:
        df_emp = dados.get('empregos'); df_clt = dados.get('dados_clt')
        if not df_emp.empty:
            for _, row in df_emp.iterrows():
                with st.expander(f"🏢 {row['convenio']} | Matr: {row['matricula']}", expanded=True):
                    if not df_clt.empty:
                        vinc = df_clt[df_clt['matricula_ref'] == row['matricula']]
                        if not vinc.empty:
                            d = vinc.iloc[0]
                            st.write(f"**Empresa:** {d['cnpj_nome']}"); st.write(f"**Cargo:** {d['cbo_nome']}")
        else: st.info("Sem dados profissionais.")
    with t3:
        df_tel = dados.get('telefones')
        if not df_tel.empty:
            for _, r in df_tel.iterrows(): st.write(f"📱 {r['numero']}")
        df_email = dados.get('emails')
        if not df_email.empty:
            for _, r in df_email.iterrows(): st.write(f"📧 {r['email']}")

# --- DICIONÁRIO DE CAMPOS DISPONÍVEIS PARA PESQUISA ---
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

def buscar_pf_simples(termo, filtro_importacao_id=None, pagina=1, itens_por_pagina=50, exportar=False):
    conn = get_conn()
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
            
            if exportar:
                df = pd.read_sql(f"{sql_base} {where} GROUP BY d.id ORDER BY d.nome LIMIT 1000", conn, params=tuple(params))
                conn.close(); return df, len(df)
            
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(DISTINCT d.id) FROM pf_dados d {sql_base.split('pf_dados d')[1]} {where}", tuple(params))
            total = cur.fetchone()[0]
            
            offset = (pagina-1)*itens_por_pagina
            df = pd.read_sql(f"{sql_base} {where} GROUP BY d.id ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}", conn, params=tuple(params))
            conn.close()
            return df, total
        except: conn.close()
    return pd.DataFrame(), 0

# --- APP PRINCIPAL ---
def app_pessoa_fisica():
    init_db_structures()
    
    st.markdown("""
        <style>
            .stButton button { height: 28px; padding-top: 0px; padding-bottom: 0px; }
            div[data-testid="stExpander"] details summary p { font-weight: bold; font-size: 1.1em; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'regras_pesquisa' not in st.session_state: st.session_state['regras_pesquisa'] = []
    if 'pagina_atual' not in st.session_state: st.session_state['pagina_atual'] = 1

    # --- PESQUISA AMPLA (QUERY BUILDER) ---
    if st.session_state['pf_view'] == 'pesquisa_ampla':
        
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

        conn = get_conn()
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
                                'label': campo['label'],
                                'coluna': campo['coluna'],
                                'tabela': campo['tabela'],
                                'tipo': campo['tipo'],
                                'operador': None,
                                'valor': ''
                            })
                            st.rerun()

        with c_regras:
            st.markdown("### 🎯 Regras Ativas")
            if not st.session_state['regras_pesquisa']: st.info("Nenhuma regra selecionada. Clique nos itens à esquerda para adicionar filtros.")
            
            regras_para_remover = []
            
            for i, regra in enumerate(st.session_state['regras_pesquisa']):
                with st.container(border=True):
                    c1, c2, c3, c4 = st.columns([2, 2, 3, 0.5])
                    c1.markdown(f"**{regra['label']}**")
                    
                    opcoes = ops_cache.get(regra['tipo'], [])
                    idx_sel = 0
                    if regra['operador'] and regra['operador'] in opcoes:
                         idx_sel = opcoes.index(regra['operador'])
                    
                    novo_op_full = c2.selectbox("Op.", options=opcoes, index=idx_sel, key=f"op_{i}", label_visibility="collapsed")
                    novo_op_simbolo = novo_op_full.split(' : ')[0] if novo_op_full else "="
                    
                    if novo_op_simbolo == '∅':
                        c3.text_input("Valor", value="[Vazio]", disabled=True, key=f"val_{i}", label_visibility="collapsed")
                        novo_valor = None
                    elif regra['tipo'] == 'data':
                        novo_valor = c3.date_input("Data", value=None, min_value=date(1900,1,1), max_value=date(2025,12,31), key=f"val_{i}", format="DD/MM/YYYY", label_visibility="collapsed")
                    else:
                        novo_valor = c3.text_input("Valor", value=regra['valor'], key=f"val_{i}", label_visibility="collapsed", placeholder="Separe por vírgula para múltiplos")

                    st.session_state['regras_pesquisa'][i]['operador'] = novo_op_full # Guarda o valor completo para manter o indice
                    st.session_state['regras_pesquisa'][i]['valor'] = novo_valor

                    if c4.button("🗑️", key=f"del_{i}"):
                        regras_para_remover.append(i)

            if regras_para_remover:
                for idx in sorted(regras_para_remover, reverse=True): st.session_state['regras_pesquisa'].pop(idx)
                st.rerun()

            st.divider()
            if st.button("🔎 FILTRAR AGORA", type="primary", use_container_width=True):
                st.session_state['executar_busca'] = True

        if st.session_state.get('executar_busca'):
            # Prepara as regras para envio (limpa o operador para só enviar o símbolo)
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
                        if st.button("👁️", key=f"v_{row['id']}"): dialog_visualizar_cliente(str(row['cpf']))
                    c2.write(str(row['id'])); c3.write(formatar_cpf_visual(row['cpf'])); c4.write(row['nome'])
                    st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
                
                cp1, cp2, cp3 = st.columns([1, 3, 1])
                if cp1.button("⬅️ Ant.") and st.session_state['pagina_atual'] > 1: st.session_state['pagina_atual'] -= 1; st.rerun()
                if cp3.button("Próx. ➡️"): st.session_state['pagina_atual'] += 1; st.rerun()
            else: st.warning("Nenhum registro encontrado.")

    # ==========================
    # 6. MODO LISTA (PADRÃO)
    # ==========================
    elif st.session_state['pf_view'] == 'lista':
        c1, c2 = st.columns([2, 2])
        busca = c2.text_input("🔎 Pesquisa Rápida (Nome/CPF)", key="pf_busca")
        
        col_b1, col_b2, col_b3 = st.columns([1, 1, 1])
        if col_b1.button("➕ Novo"): st.session_state.update({'pf_view': 'novo', 'form_loaded': False}); st.rerun()
        if col_b2.button("🔍 Pesquisa Ampla"): st.session_state.update({'pf_view': 'pesquisa_ampla'}); st.rerun()
        if col_b3.button("📥 Importar"): st.session_state.update({'pf_view': 'importacao', 'import_step': 1}); st.rerun()
        
        if busca:
            df_lista, total = buscar_pf_simples(busca, pagina=st.session_state['pagina_atual'])
            if not df_lista.empty:
                df_lista['cpf'] = df_lista['cpf'].apply(formatar_cpf_visual)
                st.dataframe(df_lista[['id', 'nome', 'cpf']], use_container_width=True)
            else: st.warning("Nada encontrado.")
        else:
            st.info("Utilize a busca para listar clientes.")
    
    # ==========================
    # 7. MODO IMPORTAÇÃO (RESTAURADO COMPLETO)
    # ==========================
    elif st.session_state['pf_view'] == 'importacao':
        c_cancel, c_hist = st.columns([1, 4])
        if c_cancel.button("⬅️ Cancelar"): st.session_state.update({'pf_view': 'lista', 'import_step': 1}); st.rerun()
        if c_hist.button("📜 Ver Histórico"): pass # Implementar histórico visual se necessário
        
        st.divider()
        opcoes_tabelas = ["Dados Cadastrais (pf_dados)", "Telefones (pf_telefones)", "Emails (pf_emails)", "Endereços (pf_enderecos)", "Emprego/Renda (pf_emprego_renda)", "Contratos (pf_contratos)"]
        mapa_real = {"Dados Cadastrais (pf_dados)": "pf_dados", "Telefones (pf_telefones)": "pf_telefones", "Emails (pf_emails)": "pf_emails", "Endereços (pf_enderecos)": "pf_enderecos", "Emprego/Renda (pf_emprego_renda)": "pf_emprego_renda", "Contratos (pf_contratos)": "pf_contratos"}

        if st.session_state.get('import_step', 1) == 1:
            st.markdown("### 📤 Etapa 1: Upload")
            sel_amigavel = st.selectbox("Selecione a Tabela de Destino", opcoes_tabelas)
            st.session_state['import_table'] = mapa_real[sel_amigavel]
            uploaded_file = st.file_uploader("Carregar Arquivo CSV", type=['csv'])
            if uploaded_file:
                try:
                    uploaded_file.seek(0)
                    try: df = pd.read_csv(uploaded_file, sep=';')
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
            if table_name == 'pf_telefones':
                db_fields = ['cpf_ref (Vínculo)', 'tag_whats', 'tag_qualificacao'] + [f'telefone_{i}' for i in range(1, 11)]
            else:
                db_cols_info = get_table_columns(table_name)
                ignore = ['id', 'data_criacao', 'data_atualizacao', 'cpf_ref', 'matricula_ref', 'importacao_id']
                db_fields = [c[0] for c in db_cols_info if c[0] not in ignore]

            c_l, c_r = st.columns([1, 2])
            with c_l:
                for idx, col in enumerate(csv_cols):
                    mapped = st.session_state['csv_map'].get(col)
                    txt = f"{idx+1}. {col} -> {'✅ '+mapped if mapped else '❓'}"
                    if idx == st.session_state.get('current_csv_idx', 0): st.info(txt, icon="👉")
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
                conn = get_conn()
                if conn:
                    with st.spinner("Processando..."):
                        try:
                            cur = conn.cursor()
                            cur.execute("INSERT INTO pf_historico_importacoes (nome_arquivo) VALUES (%s) RETURNING id", (st.session_state['uploaded_file_name'],))
                            imp_id = cur.fetchone()[0]
                            conn.commit()
                            final_map = {k: v for k, v in st.session_state['csv_map'].items() if v and v != "IGNORAR"}
                            novos, atualizados, erros = processar_importacao_lote(conn, df, table_name, final_map, imp_id)
                            conn.commit()
                            cur.execute("UPDATE pf_historico_importacoes SET qtd_novos=%s, qtd_atualizados=%s, qtd_erros=%s WHERE id=%s", (novos, atualizados, len(erros), imp_id))
                            conn.commit(); conn.close()
                            st.session_state['import_stats'] = {'novos': novos, 'atualizados': atualizados, 'erros': len(erros)}
                            st.session_state['import_step'] = 3; st.rerun()
                        except Exception as e: st.error(f"Erro: {e}")

        elif st.session_state['import_step'] == 3:
            st.markdown("### ✅ Concluído")
            s = st.session_state.get('import_stats', {})
            c1, c2, c3 = st.columns(3)
            c1.metric("Novos", s.get('novos', 0)); c2.metric("Atualizados", s.get('atualizados', 0)); c3.metric("Erros", s.get('erros', 0))
            if st.button("Finalizar"): st.session_state.update({'pf_view': 'lista', 'import_step': 1}); st.rerun()

    # ==========================
    # 8. MODO NOVO / EDITAR
    # ==========================
    elif st.session_state['pf_view'] in ['novo', 'editar']:
        is_edit = st.session_state['pf_view'] == 'editar'
        cpf_titulo = formatar_cpf_visual(st.session_state.get('pf_cpf_selecionado')) if is_edit else ""
        titulo = f"✏️ Editar: {cpf_titulo}" if is_edit else "➕ Novo Cadastro"
        
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'form_loaded': False}))
        st.markdown(f"### {titulo}")

        if is_edit and not st.session_state.get('form_loaded'):
            dados_db = carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
            st.session_state['dados_gerais_temp'] = dados_db.get('geral', {})
            st.session_state['form_loaded'] = True
        elif not is_edit and not st.session_state.get('form_loaded'):
            st.session_state['dados_gerais_temp'] = {}
            st.session_state['form_loaded'] = True

        g = st.session_state.get('dados_gerais_temp', {})

        with st.form("form_cadastro_pf"):
            t1, t2 = st.tabs(["👤 Dados Pessoais", "📞 Contatos"])
            with t1:
                c1, c2, c3 = st.columns(3)
                nome = c1.text_input("Nome *", value=g.get('nome', ''))
                cpf = c2.text_input("CPF *", value=g.get('cpf', ''), disabled=is_edit)
                d_nasc = c3.date_input("Nascimento", value=pd.to_datetime(g.get('data_nascimento')).date() if g.get('data_nascimento') else None, format="DD/MM/YYYY")
            with t2:
                st.info("Para adicionar contatos, salve o cadastro primeiro.")
            
            if st.form_submit_button("💾 Salvar"):
                if nome and cpf:
                    suc, msg = salvar_pf({'nome': nome, 'cpf': cpf, 'data_nascimento': d_nasc}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "editar" if is_edit else "novo", cpf if is_edit else None)
                    if suc: st.success(msg); time.sleep(1); st.session_state['pf_view'] = 'lista'; st.rerun()
                    else: st.error(msg)
                else: st.warning("Nome e CPF obrigatórios.")

if __name__ == "__main__":
    app_pessoa_fisica()