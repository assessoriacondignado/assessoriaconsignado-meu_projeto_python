import streamlit as st
import pandas as pd
import io
import os
from datetime import datetime
import modulo_pf_cadastro as pf_core

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
BASE_DIR_IMPORTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ARQUIVO IMPORTAÇÕES")
if not os.path.exists(BASE_DIR_IMPORTS):
    os.makedirs(BASE_DIR_IMPORTS)

def get_table_columns(table_name):
    conn = pf_core.get_conn()
    cols = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = 'banco_pf'")
            cols = cur.fetchall()
            conn.close()
        except: pass
    return cols

def processar_importacao_lote(conn, df, table_name, mapping, import_id, file_path_original):
    cur = conn.cursor()
    try:
        erros = []
        df_proc = pd.DataFrame()
        cols_order = []
        
        table_full_name = f"banco_pf.{table_name}"
        
        cur.execute("UPDATE banco_pf.pf_historico_importacoes SET caminho_arquivo_original = %s WHERE id = %s", (file_path_original, import_id))

        # Verifica colunas do banco
        cols_banco = [c[0] for c in get_table_columns(table_name)]

        # --- LÓGICA ESPECÍFICA PARA TELEFONES ---
        if table_name == 'pf_telefones':
            col_cpf = next((k for k, v in mapping.items() if v == 'cpf_ref (Vínculo)'), None)
            col_whats = next((k for k, v in mapping.items() if v == 'tag_whats'), None)
            col_qualif = next((k for k, v in mapping.items() if v == 'tag_qualificacao'), None)
            map_tels = {k: v for k, v in mapping.items() if v and v.startswith('telefone_')}
            
            if not col_cpf: return 0, 0, ["Erro: Coluna 'CPF (Vínculo)' é obrigatória."]
            
            new_rows = []
            for _, row in df.iterrows():
                cpf_val = str(row[col_cpf]) if pd.notna(row[col_cpf]) else ""
                cpf_limpo = pf_core.limpar_normalizar_cpf(cpf_val)
                if not cpf_limpo: continue
                
                whats_val = str(row[col_whats]).upper().strip() if col_whats and pd.notna(row[col_whats]) else None
                qualif_val = str(row[col_qualif]).upper().strip() if col_qualif and pd.notna(row[col_qualif]) else None
                
                for col_origin, _ in map_tels.items():
                    tel_raw = row[col_origin]
                    if pd.notna(tel_raw):
                        tel_limpo = pf_core.limpar_apenas_numeros(tel_raw)
                        numero_final = None
                        if len(tel_limpo) == 11: numero_final = tel_limpo
                        elif len(tel_limpo) == 13 and tel_limpo.startswith("55"): numero_final = tel_limpo[2:]
                        elif len(tel_limpo) == 9: numero_final = "82" + tel_limpo 
                        
                        if numero_final: 
                            row_dict = {
                                'cpf_ref': cpf_limpo, 
                                'numero': numero_final, 
                                'tag_whats': whats_val, 
                                'tag_qualificacao': qualif_val, 
                                'data_atualizacao': datetime.now().strftime('%Y-%m-%d')
                            }
                            if 'importacao_id' in cols_banco:
                                row_dict['importacao_id'] = str(import_id)
                            new_rows.append(row_dict)
                            
            if not new_rows: return 0, 0, ["Nenhum telefone válido encontrado após limpeza."]
            df_proc = pd.DataFrame(new_rows)
            df_proc.drop_duplicates(subset=['cpf_ref', 'numero'], inplace=True)
            cols_order = list(df_proc.columns)

        # --- LÓGICA GENÉRICA ---
        else:
            df_proc = df.rename(columns=mapping)
            cols_db_validas = [col for col in df_proc.columns if col in cols_banco or col in ['cpf', 'cpf_ref', 'matricula', 'matricula_ref']]
            
            # Preserva coluna temporária para gerar matrícula
            col_cpf_temp = None
            if 'cpf_gerador_key' in mapping.values():
                 csv_col_cpf = next((k for k, v in mapping.items() if v == 'cpf_gerador_key'), None)
                 if csv_col_cpf:
                     col_cpf_temp = csv_col_cpf
                     df_proc['cpf_temp_gen'] = df[col_cpf_temp]

            # Filtra colunas úteis
            df_proc = df_proc[[c for c in df_proc.columns if c in cols_banco or c == 'cpf_temp_gen']]

            # Padronização Maiúscula
            df_proc = df_proc.applymap(lambda x: str(x).upper().strip() if isinstance(x, str) else x)

            if 'importacao_id' in cols_banco:
                df_proc['importacao_id'] = str(import_id)

            # --- Validação CPF (11 dígitos) ---
            # Aplicada em cpf, cpf_ref e agora TAMBÉM para a tabela 'cpf_convenio'
            def validar_cpf_11_digitos(val):
                nums = pf_core.limpar_apenas_numeros(val)
                return len(nums) == 11

            for c in ['cpf', 'cpf_ref']:
                if c in df_proc.columns:
                    mask = df_proc[c].astype(str).apply(validar_cpf_11_digitos)
                    df_proc = df_proc[mask]
                    df_proc[c] = df_proc[c].astype(str).apply(pf_core.limpar_normalizar_cpf)

            # Gerador de Matrícula (Se necessário)
            target_matricula = 'matricula' if 'matricula' in cols_banco else 'matricula_ref'
            if target_matricula in cols_banco:
                if target_matricula not in df_proc.columns:
                    df_proc[target_matricula] = ""
                
                def gerar_mat(row):
                    mat = str(row.get(target_matricula, '')).strip()
                    if not mat or mat in ['NAN', 'NONE', '']:
                        cpf_origem = row.get('cpf_ref') or row.get('cpf') or row.get('cpf_temp_gen', '')
                        cpf_limpo = pf_core.limpar_normalizar_cpf(cpf_origem)
                        if cpf_limpo:
                            conv = str(row.get('convenio', 'CLT')).strip().upper()
                            if table_name == 'pf_contratos_clt': conv = 'CLT'
                            dt = datetime.now().strftime('%d%m%Y')
                            return f"{conv}{cpf_limpo}NULO{dt}"
                    return mat

                df_proc[target_matricula] = df_proc.apply(gerar_mat, axis=1)

            if 'cpf_temp_gen' in df_proc.columns: df_proc.drop(columns=['cpf_temp_gen'], inplace=True)
                
            # Limpeza de Datas
            cols_data = ['data_nascimento', 'data_exp_rg', 'data_criacao', 'data_atualizacao', 'data_admissao', 'data_inicio_emprego', 'data_abertura_empresa']
            for col in cols_data:
                if col in df_proc.columns: df_proc[col] = df_proc[col].apply(pf_core.converter_data_br_iso)
            
            cols_order = list(df_proc.columns)
            
            # Deduplicação
            pk = 'cpf' if 'cpf' in df_proc.columns else (target_matricula if target_matricula in df_proc.columns else None)
            
            # Para a nova tabela cpf_convenio, a chave pode ser composta ou apenas o cpf_ref,
            # mas vamos manter a lógica padrão de deduplicação simples.
            if table_name == 'cpf_convenio' and 'cpf_ref' in df_proc.columns:
                pk = 'cpf_ref'

            if pk: df_proc.drop_duplicates(subset=[pk], keep='last', inplace=True)

        # 3. Carga no Banco (Bulk)
        staging_table = f"staging_import_{import_id}"
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_full_name} INCLUDING DEFAULTS) ON COMMIT DROP")
        
        output = io.StringIO()
        df_proc.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        cur.copy_expert(f"COPY {staging_table} ({', '.join(cols_order)}) FROM STDIN WITH CSV DELIMITER E'\t' NULL '\\N'", output)
        
        pk_field = 'cpf' if 'cpf' in df_proc.columns else ('matricula' if 'matricula' in df_proc.columns else None)
        
        # Ajuste para a nova tabela cpf_convenio (que não tem pk 'cpf' mas tem 'cpf_ref' único)
        if table_name == 'cpf_convenio': pk_field = 'cpf_ref'

        qtd_novos, qtd_atualizados = 0, 0
        
        if pk_field:
            set_parts = []
            for c in cols_order:
                if c == pk_field: continue
                if c == 'importacao_id' and table_name == 'pf_dados':
                    expr = f"CASE WHEN t.importacao_id IS NULL OR t.importacao_id = '' THEN s.importacao_id::text ELSE t.importacao_id || ', ' || s.importacao_id::text END"
                    set_parts.append(f"{c} = {expr}")
                else:
                    set_parts.append(f"{c} = s.{c}")
            
            if set_parts:
                set_clause = ', '.join(set_parts)
                # No caso de cpf_convenio, o UPSERT pode falhar se houver duplicatas de chave.
                # A lógica abaixo tenta atualizar se existir.
                cur.execute(f"UPDATE {table_full_name} t SET {set_clause} FROM {staging_table} s WHERE t.{pk_field} = s.{pk_field}")
                qtd_atualizados = cur.rowcount
            
            cur.execute(f"INSERT INTO {table_full_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s WHERE NOT EXISTS (SELECT 1 FROM {table_full_name} t WHERE t.{pk_field} = s.{pk_field})")
            qtd_novos = cur.rowcount
        else:
            # REGRA DE CONTRATOS (IGNORAR SEM VÍNCULO)
            if table_name in ['pf_contratos', 'pf_contratos_clt']:
                query_insert_safe = f"""
                    INSERT INTO {table_full_name} ({', '.join(cols_order)}) 
                    SELECT {', '.join(cols_order)} 
                    FROM {staging_table} s 
                    WHERE EXISTS (
                        SELECT 1 FROM banco_pf.pf_emprego_renda e 
                        WHERE e.matricula = s.matricula_ref
                    )
                """
                cur.execute(query_insert_safe)
            else:
                cur.execute(f"INSERT INTO {table_full_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s")
            
            qtd_novos = cur.rowcount
            
            # ATUALIZAÇÃO DO VÍNCULO NA TABELA PAI
            str_imp = str(import_id)
            if table_name in ['pf_telefones', 'pf_emails', 'pf_enderecos', 'pf_emprego_renda', 'cpf_convenio']:
                # A nova tabela cpf_convenio também atualiza o cadastro principal pelo CPF
                cur.execute(f"""UPDATE banco_pf.pf_dados d SET importacao_id = CASE WHEN d.importacao_id IS NULL OR d.importacao_id = '' THEN %s ELSE d.importacao_id || ', ' || %s END FROM {staging_table} s WHERE d.cpf = s.cpf_ref""", (str_imp, str_imp))
                
            elif table_name in ['pf_contratos', 'pf_contratos_clt']:
                cur.execute(f"""
                    UPDATE banco_pf.pf_dados d 
                    SET importacao_id = CASE 
                        WHEN d.importacao_id IS NULL OR d.importacao_id = '' THEN %s 
                        ELSE d.importacao_id || ', ' || %s 
                    END 
                    FROM banco_pf.pf_emprego_renda e 
                    JOIN {staging_table} s ON e.matricula = s.matricula_ref 
                    WHERE d.cpf = e.cpf_ref
                """, (str_imp, str_imp))
        
        return qtd_novos, qtd_atualizados, erros

    except Exception as e: raise e

# --- INTERFACE HISTÓRICO E PRINCIPAL ---
def interface_historico():
    st.markdown("### 📜 Histórico de Importações")
    if st.button("⬅️ Voltar"): st.session_state['import_step'] = 1; st.rerun()
    conn = pf_core.get_conn()
    if not conn: return
    try:
        df = pd.read_sql("SELECT * FROM banco_pf.pf_historico_importacoes ORDER BY id DESC", conn)
        conn.close()
        st.dataframe(df, hide_index=True)
    except: st.error("Erro histórico")

def interface_importacao():
    if st.session_state.get('import_step') == 'historico': interface_historico(); return

    c1, c2 = st.columns([1, 4])
    if c1.button("⬅️ Cancelar"): st.session_state.update({'pf_view': 'lista', 'import_step': 1}); st.rerun()
    if c2.button("📜 Histórico"): st.session_state['import_step'] = 'historico'; st.rerun()
    
    st.divider()
    
    # ATUALIZADO: Incluída a nova tabela na lista
    mapa = {
        "Dados Cadastrais": "pf_dados",
        "Telefones": "pf_telefones",
        "E-mails": "pf_emails",
        "Endereços": "pf_enderecos",
        "Emprego e Renda": "pf_emprego_renda",
        "Contratos": "pf_contratos",
        "Contratos CLT": "pf_contratos_clt",
        "CPF x Convênio": "cpf_convenio"  # <-- NOVA OPÇÃO
    }
    
    if st.session_state.get('import_step', 1) == 1:
        sel = st.selectbox("Tipo de Importação", list(mapa.keys()))
        st.session_state['import_table'] = mapa[sel]
        
        uploaded = st.file_uploader("Arquivo CSV", type=['csv'])
        if uploaded:
            try:
                df = pd.read_csv(uploaded, sep=';', encoding='utf-8')
            except:
                try: df = pd.read_csv(uploaded, sep=',', encoding='latin-1')
                except: df = None
            
            if df is not None:
                st.session_state['import_df'] = df
                st.session_state['uploaded_file_name'] = uploaded.name
                path = os.path.join(BASE_DIR_IMPORTS, f"{datetime.now().strftime('%Y%m%d%H%M')}_{uploaded.name}")
                with open(path, "wb") as f: f.write(uploaded.getbuffer())
                st.session_state['uploaded_file_path'] = path
                
                st.success(f"{len(df)} linhas lidas.")
                if st.button("Avançar"):
                    st.session_state['csv_map'] = {col: None for col in df.columns}
                    st.session_state['current_csv_idx'] = 0
                    st.session_state['import_step'] = 2
                    st.rerun()
            else: st.error("Erro leitura CSV.")

    elif st.session_state['import_step'] == 2:
        st.markdown("### Mapeamento")
        df = st.session_state['import_df']
        cols_csv = list(df.columns)
        tbl = st.session_state['import_table']
        cols_db = [c[0] for c in get_table_columns(tbl) if c[0] not in ['id', 'data_criacao', 'importacao_id']]
        
        if tbl in ['pf_contratos', 'pf_contratos_clt']:
            cols_db.insert(0, "cpf_gerador_key")

        c_l, c_r = st.columns([1, 2])
        with c_l:
            for idx, col in enumerate(cols_csv):
                mapped = st.session_state['csv_map'].get(col)
                txt = f"{col} -> {mapped if mapped else '❓'}"
                if idx == st.session_state.get('current_csv_idx', 0): st.info(txt)
                else: 
                    if st.button(txt, key=f"btn_{idx}"): st.session_state['current_csv_idx'] = idx; st.rerun()
        
        with c_r:
            st.write(f"Mapeando: **{cols_csv[st.session_state['current_csv_idx']]}**")
            if st.button("IGNORAR"): 
                st.session_state['csv_map'][cols_csv[st.session_state['current_csv_idx']]] = "IGNORAR"
                if st.session_state['current_csv_idx'] < len(cols_csv)-1: st.session_state['current_csv_idx'] += 1
                st.rerun()
            
            for field in cols_db:
                if st.button(f"📌 {field}", key=f"map_{field}"):
                    st.session_state['csv_map'][cols_csv[st.session_state['current_csv_idx']]] = field
                    if st.session_state['current_csv_idx'] < len(cols_csv)-1: st.session_state['current_csv_idx'] += 1
                    st.rerun()

        st.divider()
        if st.button("🚀 IMPORTAR"):
            conn = pf_core.get_conn()
            if conn:
                with st.spinner("Importando..."):
                    try:
                        cur = conn.cursor()
                        cur.execute("INSERT INTO banco_pf.pf_historico_importacoes (nome_arquivo) VALUES (%s) RETURNING id", (st.session_state['uploaded_file_name'],))
                        imp_id = cur.fetchone()[0]
                        conn.commit()
                        
                        mapping = {k: v for k, v in st.session_state['csv_map'].items() if v and v != "IGNORAR"}
                        res = processar_importacao_lote(conn, df, tbl, mapping, imp_id, st.session_state['uploaded_file_path'])
                        conn.commit()
                        
                        cur.execute("UPDATE banco_pf.pf_historico_importacoes SET qtd_novos=%s, qtd_atualizados=%s WHERE id=%s", (res[0], res[1], imp_id))
                        conn.commit(); conn.close()
                        
                        st.session_state['import_stats'] = res
                        st.session_state['import_step'] = 3; st.rerun()
                    except Exception as e: st.error(f"Erro: {e}")

    elif st.session_state['import_step'] == 3:
        st.success("Importação Concluída!")
        res = st.session_state.get('import_stats', (0,0,[]))
        st.write(f"Novos: {res[0]} | Atualizados: {res[1]}")
        if st.button("Finalizar"): st.session_state['import_step'] = 1; st.rerun()