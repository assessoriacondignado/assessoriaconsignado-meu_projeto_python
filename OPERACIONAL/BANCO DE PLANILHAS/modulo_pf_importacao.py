import streamlit as st
import pandas as pd
import io
import os
import openpyxl  # Necessário para ler formatações do Excel
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

def validar_planilha_estrita(caminho_arquivo):
    """
    Valida se as linhas 1 e 2 possuem formatação 'Geral'.
    Retorna: (bool, msg_erro)
    """
    try:
        # Carrega o workbook preservando estilos (data_only=False)
        wb = openpyxl.load_workbook(caminho_arquivo, data_only=False)
        ws = wb.active
        
        # Itera sobre todas as colunas, limitando a busca às linhas 1 e 2
        for col_index, col_cells in enumerate(ws.iter_cols(min_row=1, max_row=2), start=1):
            
            # Tenta pegar o nome do cabeçalho para facilitar o erro
            cabecalho_val = col_cells[0].value
            nome_coluna = str(cabecalho_val) if cabecalho_val else openpyxl.utils.get_column_letter(col_index)
            
            for cell in col_cells:
                # Pega o formato. Excel padrão é 'General' (ou 'Geral' dependendo da loc).
                # O openpyxl geralmente retorna 'General' para o padrão.
                fmt = str(cell.number_format).lower()
                
                if fmt == 'general':
                    return False, (
                        f"⛔ **Bloqueio de Importação**: A coluna **'{nome_coluna}'** está com formatação **'Geral'** na linha {cell.row}. "
                        "Para garantir a integridade (e evitar erros de notação científica), converta todas as colunas "
                        "para **TEXTO** ou **NÚMERO** no Excel antes de importar."
                    )
        return True, None

    except Exception as e:
        return False, f"Erro ao validar planilha: {str(e)}"

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
            col_cpf = next((k for k, v in mapping.items() if v == 'cpf'), None)
            col_whats = next((k for k, v in mapping.items() if v == 'tag_whats'), None)
            col_qualif = next((k for k, v in mapping.items() if v == 'tag_qualificacao'), None)
            map_tels = {k: v for k, v in mapping.items() if v and v.startswith('telefone_')}
            
            if not col_cpf: return 0, 0, ["Erro: Coluna 'CPF' é obrigatória."]
            
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
                                'cpf': cpf_limpo, 
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
            df_proc.drop_duplicates(subset=['cpf', 'numero'], inplace=True)
            cols_order = list(df_proc.columns)

        # --- LÓGICA GENÉRICA ---
        else:
            df_proc = df.rename(columns=mapping)
            
            # Aceita 'cpf', 'matricula', 'convenio'
            cols_permitidas = cols_banco + ['cpf', 'matricula', 'convenio']
            
            # Filtra apenas colunas que existem no banco ou são chaves de ligação
            df_proc = df_proc[[c for c in df_proc.columns if c in cols_permitidas]]

            # Padronização Maiúscula
            df_proc = df_proc.applymap(lambda x: str(x).upper().strip() if isinstance(x, str) else x)

            if 'importacao_id' in cols_banco:
                df_proc['importacao_id'] = str(import_id)

            # --- Validação CPF (11 dígitos) ---
            def validar_cpf_digitos(val):
                nums = pf_core.limpar_apenas_numeros(val)
                return 10 <= len(nums) <= 11

            for c in ['cpf']:
                if c in df_proc.columns:
                    mask = df_proc[c].astype(str).apply(validar_cpf_digitos)
                    df_proc = df_proc[mask]
                    
                    if table_name == 'cpf_convenio':
                        df_proc[c] = df_proc[c].astype(str).apply(lambda x: pf_core.limpar_apenas_numeros(x).zfill(11))
                    else:
                        df_proc[c] = df_proc[c].astype(str).apply(pf_core.limpar_normalizar_cpf)

            # Gerador de Matrícula
            target_matricula = 'matricula'
            if target_matricula in cols_banco:
                if target_matricula not in df_proc.columns:
                    df_proc[target_matricula] = ""
                
                def gerar_mat(row):
                    mat = str(row.get(target_matricula, '')).strip()
                    if not mat or mat in ['NAN', 'NONE', '']:
                        cpf_origem = row.get('cpf')
                        cpf_limpo = pf_core.limpar_normalizar_cpf(cpf_origem)
                        if cpf_limpo:
                            conv = str(row.get('convenio', 'CLT')).strip().upper()
                            if table_name == 'pf_matricula_dados_clt': conv = 'CLT'
                            dt = datetime.now().strftime('%d%m%Y')
                            return f"{conv}{cpf_limpo}NULO{dt}"
                    return mat

                df_proc[target_matricula] = df_proc.apply(gerar_mat, axis=1)

            cols_data = ['data_nascimento', 'data_exp_rg', 'data_criacao', 'data_atualizacao', 'data_admissao', 'data_inicio_emprego', 'data_abertura_empresa']
            for col in cols_data:
                if col in df_proc.columns: df_proc[col] = df_proc[col].apply(pf_core.converter_data_br_iso)
            
            cols_order = list(df_proc.columns)
            
            # Deduplicação
            pk = 'cpf' if 'cpf' in df_proc.columns else ('matricula' if 'matricula' in df_proc.columns else None)
            if table_name == 'convenio_por_planilha': pk = 'convenio' 
            
            if pk: df_proc.drop_duplicates(subset=[pk], keep='last', inplace=True)

        # 3. Carga no Banco (Bulk)
        staging_table = f"staging_import_{import_id}"
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_full_name} INCLUDING DEFAULTS) ON COMMIT DROP")
        
        output = io.StringIO()
        df_proc.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        cur.copy_expert(f"COPY {staging_table} ({', '.join(cols_order)}) FROM STDIN WITH CSV DELIMITER E'\t' NULL '\\N'", output)
        
        pk_field = 'cpf' if 'cpf' in df_proc.columns else ('matricula' if 'matricula' in df_proc.columns else None)
        if table_name == 'convenio_por_planilha': pk_field = 'convenio'

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
                cur.execute(f"UPDATE {table_full_name} t SET {set_clause} FROM {staging_table} s WHERE t.{pk_field} = s.{pk_field}")
                qtd_atualizados = cur.rowcount
            
            cur.execute(f"INSERT INTO {table_full_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s WHERE NOT EXISTS (SELECT 1 FROM {table_full_name} t WHERE t.{pk_field} = s.{pk_field})")
            qtd_novos = cur.rowcount
        else:
            if table_name in ['pf_contratos', 'pf_matricula_dados_clt']:
                query_insert_safe = f"""
                    INSERT INTO {table_full_name} ({', '.join(cols_order)}) 
                    SELECT {', '.join(cols_order)} 
                    FROM {staging_table} s 
                    WHERE EXISTS (
                        SELECT 1 FROM banco_pf.pf_emprego_renda e 
                        WHERE e.matricula = s.matricula
                    )
                """
                cur.execute(query_insert_safe)
            else:
                cur.execute(f"INSERT INTO {table_full_name} ({', '.join(cols_order)}) SELECT {', '.join(cols_order)} FROM {staging_table} s")
            
            qtd_novos = cur.rowcount
            
            str_imp = str(import_id)
            if table_name in ['pf_telefones', 'pf_emails', 'pf_enderecos', 'pf_emprego_renda', 'cpf_convenio']:
                cur.execute(f"""UPDATE banco_pf.pf_dados d SET importacao_id = CASE WHEN d.importacao_id IS NULL OR d.importacao_id = '' THEN %s ELSE d.importacao_id || ', ' || %s END FROM {staging_table} s WHERE d.cpf = s.cpf""", (str_imp, str_imp))
                
            elif table_name in ['pf_contratos', 'pf_matricula_dados_clt']:
                cur.execute(f"""
                    UPDATE banco_pf.pf_dados d 
                    SET importacao_id = CASE 
                        WHEN d.importacao_id IS NULL OR d.importacao_id = '' THEN %s 
                        ELSE d.importacao_id || ', ' || %s 
                    END 
                    FROM banco_pf.pf_emprego_renda e 
                    JOIN {staging_table} s ON e.matricula = s.matricula 
                    WHERE d.cpf = e.cpf
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
    
    mapa = {
        "Dados Cadastrais": "pf_dados",
        "Telefones": "pf_telefones",
        "E-mails": "pf_emails",
        "Endereços": "pf_enderecos",
        "Emprego e Renda": "pf_emprego_renda",
        "Contratos": "pf_contratos",
        "Contratos CLT": "pf_matricula_dados_clt",
        "CPF x Convênio": "cpf_convenio",
        "Convênio x Planilha": "convenio_por_planilha"
    }
    
    if st.session_state.get('import_step', 1) == 1:
        sel = st.selectbox("Tipo de Importação", list(mapa.keys()))
        st.session_state['import_table'] = mapa[sel]
        
        # --- AVISO E CABEÇALHO ESPERADO ---
        st.info("ℹ️ Aceita arquivos **.CSV** e **.XLSX (Excel)**.")
        st.warning("⚠️ **Regra de Importação:** Todas as colunas devem estar formatadas como **Texto** ou **Número**. O formato 'Geral' (General) será bloqueado para evitar erros de notação científica.")
        
        # Gera lista de colunas esperadas
        tbl = mapa[sel]
        cols_esperadas = [c[0] for c in get_table_columns(tbl) if c[0] not in ['id', 'data_criacao', 'importacao_id', 'data_atualizacao']]
        if tbl == 'pf_telefones': cols_esperadas = ['cpf', 'telefone_1', 'telefone_2', '...']
        elif tbl == 'pf_matricula_dados_clt': cols_esperadas += ['cpf (opcional para gerar matricula)']
        
        st.caption(f"📋 **Colunas sugeridas:** {', '.join(cols_esperadas)}")
        
        # Uploader aceitando CSV e XLSX
        uploaded = st.file_uploader("Selecione o arquivo", type=['csv', 'xlsx'])
        
        if uploaded:
            # Salva o arquivo temporariamente para validação e processamento
            path = os.path.join(BASE_DIR_IMPORTS, f"{datetime.now().strftime('%Y%m%d%H%M')}_{uploaded.name}")
            with open(path, "wb") as f: f.write(uploaded.getbuffer())
            st.session_state['uploaded_file_path'] = path
            st.session_state['uploaded_file_name'] = uploaded.name
            
            df = None
            
            # --- PROCESSAMENTO EXCEL ---
            if uploaded.name.endswith('.xlsx'):
                # 1. Validação Estrita de Formatação
                with st.spinner("Validando formatação do Excel..."):
                    valido, msg_erro = validar_planilha_estrita(path)
                
                if not valido:
                    st.error(msg_erro)
                    # Remove arquivo inválido
                    try: os.remove(path)
                    except: pass
                    return # Interrompe o fluxo
                
                # 2. Carregamento (se válido)
                try:
                    # Lê como string para preservar zeros à esquerda se o usuário formatou como Texto
                    df = pd.read_excel(path, dtype=str)
                except Exception as e:
                    st.error(f"Erro ao ler Excel: {e}")

            # --- PROCESSAMENTO CSV ---
            else:
                try:
                    df = pd.read_csv(path, sep=';', encoding='utf-8', dtype=str)
                    if len(df.columns) <= 1: 
                         df = pd.read_csv(path, sep=',', encoding='utf-8', dtype=str)
                except:
                    try: 
                        df = pd.read_csv(path, sep=';', encoding='latin-1', dtype=str)
                    except: df = None
            
            if df is not None:
                st.session_state['import_df'] = df
                st.success(f"Arquivo aprovado! {len(df)} linhas encontradas.")
                if st.button("Avançar para Mapeamento"):
                    st.session_state['csv_map'] = {col: None for col in df.columns}
                    st.session_state['current_csv_idx'] = 0
                    st.session_state['import_step'] = 2
                    st.rerun()
            else: st.error("Falha na leitura do arquivo.")

    elif st.session_state['import_step'] == 2:
        st.markdown("### 🔗 Mapeamento de Colunas")
        df = st.session_state['import_df']
        cols_csv = list(df.columns)
        tbl = st.session_state['import_table']
        cols_db = [c[0] for c in get_table_columns(tbl) if c[0] not in ['id', 'data_criacao', 'importacao_id']]
        
        if tbl == 'pf_telefones' and 'cpf' not in cols_db: cols_db.insert(0, 'cpf')
        if tbl in ['pf_contratos', 'pf_matricula_dados_clt']:
             if 'matricula' in cols_db: cols_db.remove('matricula')
             cols_db = ['matricula', 'cpf (Gerar Matrícula)'] + cols_db

        c_l, c_r = st.columns([1, 2])
        with c_l:
            for idx, col in enumerate(cols_csv):
                mapped = st.session_state['csv_map'].get(col)
                if mapped == "IGNORAR": status_icon = "❌"
                elif mapped: status_icon = "✅"
                else: status_icon = "❓"
                
                btn_label = f"{status_icon} {col} -> {mapped if mapped else '...'}"
                
                tipo_btn = "primary" if idx == st.session_state.get('current_csv_idx', 0) else "secondary"
                if st.button(btn_label, key=f"btn_col_{idx}", type=tipo_btn, use_container_width=True): 
                    st.session_state['current_csv_idx'] = idx; st.rerun()
        
        with c_r:
            curr_idx = st.session_state['current_csv_idx']
            col_atual = cols_csv[curr_idx]
            st.info(f"Mapeando coluna do Arquivo: **{col_atual}**")
            
            st.write("Amostra de dados desta coluna:")
            st.code(df[col_atual].head(3).to_string(index=False))

            st.write("Selecione a coluna de destino no Banco de Dados:")
            
            c_ig, c_prox = st.columns([1, 2])
            if c_ig.button("🚫 IGNORAR COLUNA", use_container_width=True): 
                st.session_state['csv_map'][col_atual] = "IGNORAR"
                if curr_idx < len(cols_csv)-1: st.session_state['current_csv_idx'] += 1
                st.rerun()
            
            cols_banco_cols = st.columns(3)
            for i, field in enumerate(cols_db):
                with cols_banco_cols[i % 3]:
                    if st.button(f"📥 {field}", key=f"map_to_{field}"):
                        st.session_state['csv_map'][col_atual] = 'cpf' if 'cpf' in field else ('matricula' if 'matricula' == field else field)
                        if 'Gerar Matrícula' in field: st.session_state['csv_map'][col_atual] = 'cpf' 
                        
                        if curr_idx < len(cols_csv)-1: st.session_state['current_csv_idx'] += 1
                        st.rerun()

        st.divider()
        if st.button("🚀 INICIAR IMPORTAÇÃO", type="primary", use_container_width=True):
            conn = pf_core.get_conn()
            if conn:
                with st.spinner("Processando dados... Isso pode levar alguns minutos."):
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
                    except Exception as e: st.error(f"Erro durante a importação: {e}")

    elif st.session_state['import_step'] == 3:
        st.balloons()
        st.success("✅ Importação Concluída com Sucesso!")
        res = st.session_state.get('import_stats', (0,0,[]))
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Novos Registros", res[0])
        c2.metric("Atualizados", res[1])
        
        if res[2]:
            st.error("Erros encontrados:")
            st.write(res[2])
            
        if st.button("Voltar ao Início"): st.session_state['import_step'] = 1; st.rerun()