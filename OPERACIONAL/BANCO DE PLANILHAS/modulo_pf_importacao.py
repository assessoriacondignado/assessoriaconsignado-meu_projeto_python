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
                cpf_limpo = pf_core.limpar_normalizar_cpf(cpf_val)
                if not cpf_limpo: continue
                whats_val = str(row[col_whats]) if col_whats and pd.notna(row[col_whats]) else None
                qualif_val = str(row[col_qualif]) if col_qualif and pd.notna(row[col_qualif]) else None
                for col_origin, _ in map_tels.items():
                    tel_raw = row[col_origin]
                    if pd.notna(tel_raw):
                        tel_limpo = pf_core.limpar_apenas_numeros(tel_raw)
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
                df_proc['cpf'] = df_proc['cpf'].astype(str).apply(pf_core.limpar_normalizar_cpf)
                if table_name == 'pf_dados': df_proc = df_proc[df_proc['cpf'] != ""]
            cols_data = ['data_nascimento', 'data_exp_rg', 'data_criacao', 'data_atualizacao']
            for col in cols_data:
                if col in df_proc.columns: df_proc[col] = df_proc[col].apply(pf_core.converter_data_br_iso)
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

def interface_importacao():
    c_cancel, c_hist = st.columns([1, 4])
    if c_cancel.button("⬅️ Cancelar"): st.session_state.update({'pf_view': 'lista', 'import_step': 1}); st.rerun()
    if c_hist.button("📜 Ver Histórico Importação"): pass # (Simplificado)
    st.divider()
    
    # --- MUDANÇA AQUI: Nomes Amigáveis ---
    mapa_tabelas = {
        "Dados Cadastrais (CPF, Nome, RG...)": "pf_dados",
        "Telefones": "pf_telefones",
        "E-mails": "pf_emails",
        "Endereços": "pf_enderecos",
        "Emprego e Renda": "pf_emprego_renda",
        "Contratos": "pf_contratos"
    }
    
    opcoes_tabelas = list(mapa_tabelas.keys())

    if st.session_state.get('import_step', 1) == 1:
        st.markdown("### 📤 Etapa 1: Upload")
        
        # O selectbox agora usa a lista de chaves amigáveis
        sel_amigavel = st.selectbox("Selecione o Tipo de Dado para Importar", opcoes_tabelas)
        
        # Recupera o nome técnico para usar no código
        st.session_state['import_table'] = mapa_tabelas[sel_amigavel]
        
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
            conn = pf_core.get_conn()
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
                        
                        # Atualiza stats
                        cur = conn.cursor()
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