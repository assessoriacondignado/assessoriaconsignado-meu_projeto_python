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
        
        # 1. Atualiza caminho do arquivo no banco
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
                
                # Validação de CPF
                if not cpf_limpo: continue
                
                # Padronização Maiúscula para Tags
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
                                row_dict['importacao_id'] = import_id
                            new_rows.append(row_dict)
                            
            if not new_rows: return 0, 0, ["Nenhum telefone válido encontrado após limpeza."]
            df_proc = pd.DataFrame(new_rows)
            df_proc.drop_duplicates(subset=['cpf_ref', 'numero'], inplace=True)
            cols_order = list(df_proc.columns)

        # --- LÓGICA GENÉRICA (DADOS, EMPREGO, CONTRATOS) ---
        else:
            df_proc = df.rename(columns=mapping)
            cols_db = list(mapping.values())
            df_proc = df_proc[cols_db].copy()
            
            # === [NOVO] PADRONIZAÇÃO TOTAL EM MAIÚSCULO ===
            # Converte todas as colunas de texto para maiúsculo automaticamente
            # Isso garante que "inss", "Inss" e "INSS" virem "INSS"
            df_proc = df_proc.applymap(lambda x: str(x).upper().strip() if isinstance(x, str) else x)
            # ===============================================

            if 'importacao_id' in cols_banco:
                df_proc['importacao_id'] = str(import_id) # Garante que ID é string
            
            # --- 1. VALIDAÇÃO DE CPF (11 DÍGITOS) ---
            def validar_cpf_11_digitos(val):
                nums = pf_core.limpar_apenas_numeros(val)
                return len(nums) == 11

            # Valida Tabela Principal
            if 'cpf' in df_proc.columns:
                mask_valid = df_proc['cpf'].astype(str).apply(validar_cpf_11_digitos)
                df_proc = df_proc[mask_valid]
                df_proc['cpf'] = df_proc['cpf'].astype(str).apply(pf_core.limpar_normalizar_cpf)
                if table_name == 'pf_dados': df_proc = df_proc[df_proc['cpf'] != ""]

            # Valida Tabelas Vinculadas
            if 'cpf_ref' in df_proc.columns:
                mask_valid = df_proc['cpf_ref'].astype(str).apply(validar_cpf_11_digitos)
                df_proc = df_proc[mask_valid]
                df_proc['cpf_ref'] = df_proc['cpf_ref'].astype(str).apply(pf_core.limpar_normalizar_cpf)
            
            # --- 2. GERADOR DE MATRÍCULA AUTOMÁTICA ---
            if table_name == 'pf_emprego_renda':
                if 'matricula' not in df_proc.columns:
                    df_proc['matricula'] = ""

                def regra_matricula(row):
                    mat = str(row.get('matricula', '')).strip()
                    # Verifica se é vazio/nulo/nan/none
                    if not mat or mat.upper() == 'NAN' or mat.upper() == 'NONE':
                        conv = str(row.get('convenio', '')).strip().upper()
                        cpf = str(row.get('cpf_ref', '')).strip()
                        dt = datetime.now().strftime('%d%m%Y')
                        # Formato: CONVENIO + CPF + NULO + DATA
                        return f"{conv}{cpf}NULO{dt}"
                    return mat
                
                df_proc['matricula'] = df_proc.apply(regra_matricula, axis=1)

            # --- 3. Limpeza de Datas ---
            # Como aplicamos .upper() em tudo, datas como "20/jan/2023" viram "20/JAN/2023"
            # O conversor deve lidar com isso (mas o ideal é formato numérico dd/mm/aaaa)
            cols_data = ['data_nascimento', 'data_exp_rg', 'data_criacao', 'data_atualizacao', 'data_admissao', 'data_inicio_emprego']
            for col in cols_data:
                if col in df_proc.columns: df_proc[col] = df_proc[col].apply(pf_core.converter_data_br_iso)
            
            cols_order = list(df_proc.columns)
            
            # Deduplicação interna
            pk_field = 'cpf' if 'cpf' in df_proc.columns else ('matricula' if 'matricula' in df_proc.columns else None)
            if pk_field:
                df_proc.drop_duplicates(subset=[pk_field], keep='last', inplace=True)

        # 3. Processo de Carga (Bulk)
        staging_table = f"staging_import_{import_id}"
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_full_name} INCLUDING DEFAULTS) ON COMMIT DROP")
        
        output = io.StringIO()
        # CSV com separador TAB, tratando nulos
        df_proc.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        
        cur.copy_expert(f"COPY {staging_table} ({', '.join(cols_order)}) FROM STDIN WITH CSV DELIMITER E'\t' NULL '\\N'", output)
        
        pk_field = 'cpf' if 'cpf' in df_proc.columns else ('matricula' if 'matricula' in df_proc.columns else None)
        qtd_novos, qtd_atualizados = 0, 0
        
        if pk_field:
            # UPDATE (com append para importacao_id)
            set_parts = []
            for c in cols_order:
                if c == pk_field: continue
                if c == 'importacao_id' and table_name == 'pf_dados':
                    expr = f"CASE WHEN t.importacao_id IS NULL OR t.importacao_id = '' THEN s.importacao_id::text ELSE t.importacao_id || ', ' || s.importacao_id::text END"
                    set_parts.append(f"{c} = {expr}")
                else:
                    set_parts.append(f"{c} = s.{c}")
            
            set_clause = ', '.join(set_parts)
            
            cur.execute(f"UPDATE {table_full_name} t SET {set_clause} FROM {staging_table} s WHERE t.{pk_field} = s.{pk_field}")
            qtd_atualizados = cur.rowcount
            
            # INSERT
            cur.execute(f"INSERT INTO {table_full_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s WHERE NOT EXISTS (SELECT 1 FROM {table_full_name} t WHERE t.{pk_field} = s.{pk_field})")
            qtd_novos = cur.rowcount
        else:
            cur.execute(f"INSERT INTO {table_full_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s")
            qtd_novos = cur.rowcount
            
            # Atualização de Vínculo na Tabela Pai
            str_imp = str(import_id)
            if table_name in ['pf_telefones', 'pf_emails', 'pf_enderecos', 'pf_emprego_renda']:
                cur.execute(f"""
                    UPDATE banco_pf.pf_dados d 
                    SET importacao_id = CASE 
                        WHEN d.importacao_id IS NULL OR d.importacao_id = '' THEN %s 
                        ELSE d.importacao_id || ', ' || %s 
                    END 
                    FROM {staging_table} s 
                    WHERE d.cpf = s.cpf_ref
                """, (str_imp, str_imp))
                
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
    if st.button("⬅️ Voltar para Importação"):
        st.session_state['import_step'] = 1
        st.rerun()
    
    conn = pf_core.get_conn()
    if not conn: return

    try:
        df_hist = pd.read_sql("""
            SELECT id, nome_arquivo, data_importacao, qtd_novos, qtd_atualizados, qtd_erros, caminho_arquivo_original, caminho_arquivo_erro 
            FROM banco_pf.pf_historico_importacoes 
            ORDER BY id DESC
        """, conn)
        conn.close()

        if df_hist.empty: st.info("Nenhum histórico encontrado."); return

        st.markdown("""<div style="display: flex; font-weight: bold; padding: 10px; background-color: #f0f2f6; border-radius: 5px;"><div style="flex: 0.5;">ID</div><div style="flex: 2;">Arquivo</div><div style="flex: 1.5;">Data</div><div style="flex: 1.5;">Status (N/A/E)</div><div style="flex: 1.5; text-align: center;">Arquivo Original</div><div style="flex: 1.5; text-align: center;">Arquivo Erros</div></div><hr style="margin: 5px 0;">""", unsafe_allow_html=True)

        for _, row in df_hist.iterrows():
            c1, c2, c3, c4, c5, c6 = st.columns([0.5, 2, 1.5, 1.5, 1.5, 1.5])
            c1.write(f"#{row['id']}")
            c2.write(row['nome_arquivo'])
            c3.write(pd.to_datetime(row['data_importacao']).strftime('%d/%m/%Y %H:%M'))
            c4.write(f"✅{row['qtd_novos']} 🔄{row['qtd_atualizados']} ❌{row['qtd_erros']}")
            with c5:
                path_orig = row['caminho_arquivo_original']
                if path_orig and os.path.exists(path_orig):
                    try:
                        with open(path_orig, "rb") as f: st.download_button("⬇️ Baixar", f, file_name=os.path.basename(path_orig), key=f"dw_orig_{row['id']}")
                    except: st.error("Erro")
                else: st.caption("-")
            with c6:
                path_erro = row['caminho_arquivo_erro']
                if path_erro and os.path.exists(path_erro):
                    try:
                        with open(path_erro, "rb") as f: st.download_button("⚠️ Ver Erros", f, file_name=os.path.basename(path_erro), key=f"dw_err_{row['id']}")
                    except: st.error("Erro")
                else: st.caption("-")
            st.markdown("<hr style='margin: 5px 0; border-top: 1px solid #eee;'>", unsafe_allow_html=True)
    except Exception as e: st.error(f"Erro ao carregar histórico: {e}")

def interface_importacao():
    if st.session_state.get('import_step') == 'historico':
        interface_historico(); return

    c_cancel, c_hist = st.columns([1, 4])
    if c_cancel.button("⬅️ Cancelar"): st.session_state.update({'pf_view': 'lista', 'import_step': 1}); st.rerun()
    if c_hist.button("📜 Ver Histórico Importação"): st.session_state['import_step'] = 'historico'; st.rerun()
    st.divider()
    
    mapa_tabelas = {
        "Dados Cadastrais (CPF, Nome, RG...)": "pf_dados",
        "Telefones": "pf_telefones",
        "E-mails": "pf_emails",
        "Endereços": "pf_enderecos",
        "Emprego e Renda": "pf_emprego_renda",
        "Contratos": "pf_contratos",
        "Contratos CLT": "pf_contratos_clt"
    }
    opcoes_tabelas = list(mapa_tabelas.keys())

    if st.session_state.get('import_step', 1) == 1:
        st.markdown("### 📤 Etapa 1: Upload")
        st.warning("⚠️ **Atenção:** Ao salvar no Excel, escolha a opção: **CSV UTF-8 (Delimitado por vírgulas) (*.csv)**.")
        sel_amigavel = st.selectbox("Selecione o Tipo de Dado para Importar", opcoes_tabelas)
        st.session_state['import_table'] = mapa_tabelas[sel_amigavel]
        
        tabela_selecionada = st.session_state['import_table']
        cols_info = get_table_columns(tabela_selecionada)
        
        ignorar = ['id', 'data_criacao', 'data_atualizacao', 'importacao_id']
        
        campos_visiveis = [col[0] for col in cols_info if col[0] not in ignorar]
        
        if campos_visiveis:
            with st.expander("📋 Ver colunas esperadas para este arquivo", expanded=False):
                st.info(f"O sistema espera um arquivo contendo informações para os seguintes campos:")
                st.markdown(" ".join([f"`{c}`" for c in campos_visiveis]), unsafe_allow_html=True)
        else: st.warning("Não foi possível ler as colunas da tabela selecionada (verifique se a tabela existe no schema banco_pf).")

        uploaded_file = st.file_uploader("Carregar Arquivo CSV", type=['csv'])
        if uploaded_file:
            try:
                uploaded_file.seek(0)
                df = None
                erro_leitura = None
                try: df = pd.read_csv(uploaded_file, sep=';', encoding='utf-8')
                except UnicodeDecodeError:
                    uploaded_file.seek(0)
                    try: df = pd.read_csv(uploaded_file, sep=';', encoding='latin-1')
                    except: pass
                except Exception: pass
                if df is None:
                    uploaded_file.seek(0)
                    try: df = pd.read_csv(uploaded_file, sep=',', encoding='utf-8')
                    except UnicodeDecodeError:
                        uploaded_file.seek(0)
                        try: df = pd.read_csv(uploaded_file, sep=',', encoding='latin-1')
                        except Exception as e: erro_leitura = str(e)
                
                if df is not None:
                    st.session_state['import_df'] = df
                    st.session_state['uploaded_file_name'] = uploaded_file.name
                    caminho_fisico = os.path.join(BASE_DIR_IMPORTS, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uploaded_file.name}")
                    with open(caminho_fisico, "wb") as f:
                        uploaded_file.seek(0)
                        f.write(uploaded_file.getbuffer())
                    st.session_state['uploaded_file_path'] = caminho_fisico
                    st.success(f"Carregado com sucesso! {len(df)} linhas identificadas.")
                    if st.button("Ir para Mapeamento", type="primary"):
                        st.session_state['csv_map'] = {col: None for col in df.columns}
                        st.session_state['current_csv_idx'] = 0
                        st.session_state['import_step'] = 2
                        st.rerun()
                else: st.error(f"Não foi possível ler o arquivo. Verifique se é um CSV válido.\nDetalhe: {erro_leitura}")
            except Exception as e: st.error(f"Erro crítico no upload: {e}")

    elif st.session_state['import_step'] == 2:
        st.markdown("### 🔗 Etapa 2: Mapeamento Visual")
        df = st.session_state['import_df']
        csv_cols = list(df.columns)
        table_name = st.session_state['import_table']
        
        if table_name == 'pf_telefones':
            db_fields = ['cpf_ref (Vínculo)', 'tag_whats', 'tag_qualificacao'] + [f'telefone_{i}' for i in range(1, 11)]
        else:
            db_cols_info = get_table_columns(table_name)
            ignore = ['id', 'data_criacao', 'data_atualizacao', 'importacao_id']
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
            conn = pf_core.get_conn()
            if conn:
                with st.spinner("Processando..."):
                    try:
                        cur = conn.cursor()
                        cur.execute("INSERT INTO banco_pf.pf_historico_importacoes (nome_arquivo) VALUES (%s) RETURNING id", (st.session_state['uploaded_file_name'],))
                        imp_id = cur.fetchone()[0]
                        conn.commit()
                        final_map = {k: v for k, v in st.session_state['csv_map'].items() if v and v != "IGNORAR"}
                        path_file = st.session_state.get('uploaded_file_path', '')
                        
                        novos, atualizados, erros = processar_importacao_lote(conn, df, table_name, final_map, imp_id, path_file)
                        conn.commit()
                        
                        cur = conn.cursor()
                        cur.execute("UPDATE banco_pf.pf_historico_importacoes SET qtd_novos=%s, qtd_atualizados=%s, qtd_erros=%s WHERE id=%s", (novos, atualizados, len(erros), imp_id))
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