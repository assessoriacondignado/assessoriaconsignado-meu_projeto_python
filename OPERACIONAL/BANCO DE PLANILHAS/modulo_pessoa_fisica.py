import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date
import io

# Tenta importar conexao do diretório raiz (adicionado ao sys.path pelo sistema.py)
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

# --- FUNÇÕES AUXILIARES ---
def calcular_idade_completa(data_nasc):
    if not data_nasc: return "", "", ""
    hoje = date.today()
    if isinstance(data_nasc, datetime): data_nasc = data_nasc.date()
    
    anos = hoje.year - data_nasc.year - ((hoje.month, hoje.day) < (data_nasc.month, data_nasc.day))
    
    # Cálculo aproximado de meses e dias para exibição
    meses = (hoje.year - data_nasc.year) * 12 + hoje.month - data_nasc.month
    dias = (hoje - data_nasc).days
    
    return anos, meses, dias

def buscar_pf(termo):
    conn = get_conn()
    if conn:
        # Busca unificada por CPF, Nome ou Telefone (via JOIN)
        query = """
            SELECT d.id, d.nome, d.cpf, d.data_nascimento 
            FROM pf_dados d
            LEFT JOIN pf_telefones t ON d.cpf = t.cpf_ref
            WHERE d.cpf ILIKE %s OR d.nome ILIKE %s OR t.numero ILIKE %s
            GROUP BY d.id
            ORDER BY d.nome ASC
            LIMIT 50
        """
        param = f"%{termo}%"
        df = pd.read_sql(query, conn, params=(param, param, param))
        conn.close()
        return df
    return pd.DataFrame()

def carregar_dados_completos(cpf):
    conn = get_conn()
    dados = {}
    if conn:
        # Dados Cadastrais
        df_d = pd.read_sql("SELECT * FROM pf_dados WHERE cpf = %s", conn, params=(cpf,))
        dados['geral'] = df_d.iloc[0] if not df_d.empty else None
        
        # Tabelas Filhas
        dados['telefones'] = pd.read_sql("SELECT numero, data_atualizacao, tag_whats, tag_qualificacao FROM pf_telefones WHERE cpf_ref = %s", conn, params=(cpf,))
        dados['emails'] = pd.read_sql("SELECT email FROM pf_emails WHERE cpf_ref = %s", conn, params=(cpf,))
        dados['enderecos'] = pd.read_sql("SELECT rua, bairro, cidade, uf, cep FROM pf_enderecos WHERE cpf_ref = %s", conn, params=(cpf,))
        conn.close()
    return dados

def salvar_pf(dados_gerais, df_tel, df_email, df_end, modo="novo", cpf_original=None):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            
            # 1. Dados Cadastrais
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
            
            # Define o CPF chave (novo ou editado)
            cpf_chave = dados_gerais['cpf']
            
            # 2. Limpar tabelas filhas para recriar (estratégia simples para update)
            if modo == "editar":
                cur.execute("DELETE FROM pf_telefones WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_emails WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_enderecos WHERE cpf_ref = %s", (cpf_chave,))
            
            # 3. Inserir Telefones
            if not df_tel.empty:
                for _, row in df_tel.iterrows():
                    if row.get('numero'):
                        cur.execute("INSERT INTO pf_telefones (cpf_ref, numero, data_atualizacao, tag_whats, tag_qualificacao) VALUES (%s, %s, %s, %s, %s)",
                                    (cpf_chave, row['numero'], row.get('data_atualizacao'), row.get('tag_whats'), row.get('tag_qualificacao')))
            
            # 4. Inserir Emails
            if not df_email.empty:
                for _, row in df_email.iterrows():
                    if row.get('email'):
                        cur.execute("INSERT INTO pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, row['email']))

            # 5. Inserir Endereços
            if not df_end.empty:
                for _, row in df_end.iterrows():
                    if row.get('rua') or row.get('cidade'):
                        cur.execute("INSERT INTO pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)",
                                    (cpf_chave, row['rua'], row.get('bairro'), row.get('cidade'), row.get('uf'), row.get('cep')))

            conn.commit()
            conn.close()
            return True, "Salvo com sucesso!"
        except Exception as e:
            return False, str(e)
    return False, "Erro de conexão"

def excluir_pf(cpf):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            # O DELETE CASCADE no banco cuidará das tabelas filhas
            cur.execute("DELETE FROM pf_dados WHERE cpf = %s", (cpf,))
            conn.commit()
            conn.close()
            return True
        except: return False
    return False

def exportar_dados(lista_cpfs):
    conn = get_conn()
    if conn and lista_cpfs:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        # Query unificada (flat) para exportação
        query = f"""
            SELECT d.*, 
                   t.numero as tel_numero, t.tag_whats, 
                   e.email as email_contato,
                   end.cidade, end.uf
            FROM pf_dados d
            LEFT JOIN pf_telefones t ON d.cpf = t.cpf_ref
            LEFT JOIN pf_emails e ON d.cpf = e.cpf_ref
            LEFT JOIN pf_enderecos end ON d.cpf = end.cpf_ref
            WHERE d.cpf IN ({placeholders})
        """
        df = pd.read_sql(query, conn, params=tuple(lista_cpfs))
        conn.close()
        return df
    return pd.DataFrame()

# --- INTERFACE ---
@st.dialog("🖨️ Imprimir Dados")
def dialog_imprimir(dados):
    d = dados['geral']
    st.markdown(f"### Ficha Cadastral: {d['nome']}")
    st.markdown("---")
    st.markdown(f"**CPF:** {d['cpf']} | **RG:** {d['rg']} {d['uf_rg']}")
    st.markdown(f"**Nascimento:** {d['data_nascimento']} | **Mãe:** {d['nome_mae']}")
    
    st.markdown("#### 📞 Contatos")
    if not dados['telefones'].empty:
        st.table(dados['telefones'])
    
    st.markdown("#### 📧 E-mails")
    if not dados['emails'].empty:
        st.table(dados['emails'])
        
    st.markdown("#### 🏠 Endereços")
    if not dados['enderecos'].empty:
        st.table(dados['enderecos'])
    
    st.button("Fechar")

@st.dialog("⚠️ Excluir Cadastro")
def dialog_excluir_pf(cpf, nome):
    st.error(f"Tem certeza que deseja excluir **{nome}**?")
    st.warning("Esta ação apagará todos os telefones, e-mails e endereços vinculados.")
    if st.button("Confirmar Exclusão", type="primary"):
        if excluir_pf(cpf):
            st.success("Excluído!")
            st.rerun()

def app_pessoa_fisica():
    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'pf_cpf_selecionado' not in st.session_state: st.session_state['pf_cpf_selecionado'] = None

    # --- TELA DE LISTAGEM ---
    if st.session_state['pf_view'] == 'lista':
        # Barra de Pesquisa (Topo Direito)
        c1, c2 = st.columns([2, 2])
        with c2:
            busca = st.text_input("🔎 Pesquisar (CPF / Nome / Telefone)", key="pf_busca")
        
        c_act1, c_act2 = st.columns([1, 5])
        with c_act1:
            if st.button("➕ Novo", type="primary"):
                st.session_state['pf_view'] = 'novo'
                st.session_state['pf_cpf_selecionado'] = None
                st.rerun()

        # Listagem
        df_lista = pd.DataFrame()
        if busca:
            df_lista = buscar_pf(busca)
        
        if not df_lista.empty:
            # Checkbox para seleção
            df_lista.insert(0, "Sel", False)
            
            edited_df = st.data_editor(
                df_lista, 
                column_config={"Sel": st.column_config.CheckboxColumn(required=True)},
                disabled=["id", "nome", "cpf", "data_nascimento"],
                hide_index=True,
                use_container_width=True
            )
            
            # Exportação
            sel_cpfs = edited_df[edited_df["Sel"]]["cpf"].tolist()
            if sel_cpfs:
                if st.button(f"📥 Exportar ({len(sel_cpfs)})"):
                    df_exp = exportar_dados(sel_cpfs)
                    csv = df_exp.to_csv(index=False).encode('utf-8')
                    st.download_button("Baixar CSV", data=csv, file_name="export_pf.csv", mime="text/csv")

            st.markdown("---")
            for index, row in df_lista.iterrows():
                with st.container():
                    c1, c2, c3, c4 = st.columns([1, 4, 2, 2])
                    c1.write(row['id'])
                    c2.write(f"**{row['nome']}**")
                    c3.write(row['cpf'])
                    
                    b_col1, b_col2, b_col3 = c4.columns(3)
                    if b_col1.button("🔍", key=f"v_{row['id']}", help="Visualizar/Editar"):
                        st.session_state['pf_view'] = 'editar'
                        st.session_state['pf_cpf_selecionado'] = row['cpf']
                        st.rerun()
                    if b_col3.button("🗑️", key=f"d_{row['id']}", help="Excluir"):
                        dialog_excluir_pf(row['cpf'], row['nome'])
                    st.divider()
        else:
            if busca: st.warning("Nenhum resultado encontrado.")
            else: st.info("Utilize a pesquisa para visualizar os cadastros.")

    # --- TELA DE CADASTRO / DETALHES ---
    elif st.session_state['pf_view'] in ['novo', 'editar']:
        st.button("⬅️ Voltar para Lista", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        
        modo = st.session_state['pf_view']
        cpf_atual = st.session_state['pf_cpf_selecionado']
        
        # Carregar dados
        dados_db = carregar_dados_completos(cpf_atual) if modo == 'editar' and cpf_atual else {}
        geral = dados_db.get('geral')
        
        titulo = f"📝 Editar: {geral['nome']}" if geral is not None else "📝 Novo Cadastro"
        st.markdown(f"### {titulo}")

        with st.form("form_pf_completo"):
            # Abas Harmônicas
            tab1, tab2, tab3, tab4 = st.tabs(["👤 Dados Cadastrais", "📞 Telefones", "📧 E-mails", "🏠 Endereços"])
            
            with tab1:
                c1, c2, c3 = st.columns(3)
                nome = c1.text_input("Nome *", value=geral['nome'] if geral is not None else "")
                cpf = c2.text_input("CPF * (Apenas números)", value=geral['cpf'] if geral is not None else "")
                nasc = c3.date_input("Data Nascimento", value=pd.to_datetime(geral['data_nascimento']) if geral is not None and geral['data_nascimento'] else None)
                
                if nasc:
                    anos, meses, dias = calcular_idade_completa(nasc)
                    st.info(f"📆 Idade Calculada: **{anos} anos**, {meses} meses e {dias} dias.")

                c4, c5, c6, c7 = st.columns(4)
                rg = c4.text_input("RG", value=geral['rg'] if geral is not None else "")
                uf_rg = c5.text_input("UF RG", value=geral['uf_rg'] if geral is not None else "")
                dt_rg = c6.date_input("Expedição RG", value=pd.to_datetime(geral['data_exp_rg']) if geral is not None and geral['data_exp_rg'] else None)
                cnh = c7.text_input("CNH", value=geral['cnh'] if geral is not None else "")
                
                c8, c9 = st.columns(2)
                pis = c8.text_input("PIS", value=geral['pis'] if geral is not None else "")
                ctps = c9.text_input("CTPS/Série", value=geral['ctps_serie'] if geral is not None else "")
                
                c10, c11 = st.columns(2)
                mae = c10.text_input("Nome Mãe", value=geral['nome_mae'] if geral is not None else "")
                pai = c11.text_input("Nome Pai", value=geral['nome_pai'] if geral is not None else "")
                
                c12, c13 = st.columns(2)
                proc = c12.text_input("Procurador", value=geral['nome_procurador'] if geral is not None else "")
                cpf_proc = c13.text_input("CPF Procurador", value=geral['cpf_procurador'] if geral is not None else "")

            with tab2:
                st.caption("Adicione até 10 telefones.")
                df_tel_empty = pd.DataFrame(columns=["numero", "data_atualizacao", "tag_whats", "tag_qualificacao"])
                df_tel_input = dados_db.get('telefones') if modo == 'editar' else df_tel_empty
                edited_tel = st.data_editor(df_tel_input, num_rows="dynamic", key="editor_tel", use_container_width=True)

            with tab3:
                st.caption("Adicione até 10 e-mails.")
                df_email_empty = pd.DataFrame(columns=["email"])
                df_email_input = dados_db.get('emails') if modo == 'editar' else df_email_empty
                edited_email = st.data_editor(df_email_input, num_rows="dynamic", key="editor_email", use_container_width=True)

            with tab4:
                st.caption("Adicione até 3 endereços.")
                df_end_empty = pd.DataFrame(columns=["rua", "bairro", "cidade", "uf", "cep"])
                df_end_input = dados_db.get('enderecos') if modo == 'editar' else df_end_empty
                edited_end = st.data_editor(df_end_input, num_rows="dynamic", key="editor_end", use_container_width=True)

            st.markdown("---")
            col_b1, col_b2 = st.columns([1, 5])
            
            if col_b1.form_submit_button("💾 Salvar Dados"):
                if nome and cpf:
                    dados_gerais = {
                        "cpf": cpf, "nome": nome, "data_nascimento": nasc,
                        "rg": rg, "uf_rg": uf_rg, "data_exp_rg": dt_rg,
                        "cnh": cnh, "pis": pis, "ctps_serie": ctps,
                        "nome_mae": mae, "nome_pai": pai,
                        "nome_procurador": proc, "cpf_procurador": cpf_proc
                    }
                    ok, msg = salvar_pf(dados_gerais, edited_tel, edited_email, edited_end, modo, cpf_atual)
                    if ok:
                        st.success(msg)
                        st.session_state['pf_view'] = 'lista'
                        st.rerun()
                    else:
                        st.error(f"Erro: {msg}")
                else:
                    st.warning("Preencha Nome e CPF.")

        if modo == 'editar':
            if st.button("🖨️ Imprimir Visualização"):
                dialog_imprimir(dados_db)

if __name__ == "__main__":
    app_pessoa_fisica()