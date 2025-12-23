import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date
import io

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
    meses = (hoje.year - data_nasc.year) * 12 + hoje.month - data_nasc.month
    dias = (hoje - data_nasc).days
    return anos, meses, dias

def buscar_referencias(tipo):
    conn = get_conn()
    if conn:
        df = pd.read_sql("SELECT nome FROM pf_referencias WHERE tipo = %s ORDER BY nome", conn, params=(tipo,))
        conn.close()
        return df['nome'].tolist()
    return []

def adicionar_referencia(tipo, nome):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO pf_referencias (tipo, nome) VALUES (%s, %s) ON CONFLICT DO NOTHING", (tipo, nome.upper()))
            conn.commit(); conn.close()
            return True
        except: pass
    return False

def buscar_pf(termo):
    conn = get_conn()
    if conn:
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
        
        # Novas Tabelas Profissionais
        dados['empregos'] = pd.read_sql("SELECT id, convenio, matricula, dados_extras FROM pf_emprego_renda WHERE cpf_ref = %s", conn, params=(cpf,))
        
        # Carrega contratos de todas as matrículas deste CPF
        if not dados['empregos'].empty:
            matr_list = tuple(dados['empregos']['matricula'].tolist())
            if matr_list:
                # Ajuste para tupla de 1 elemento no SQL
                placeholders = ",".join(["%s"] * len(matr_list))
                q_contratos = f"SELECT matricula_ref, contrato, dados_extras FROM pf_contratos WHERE matricula_ref IN ({placeholders})"
                dados['contratos'] = pd.read_sql(q_contratos, conn, params=matr_list)
            else:
                dados['contratos'] = pd.DataFrame()
        else:
            dados['contratos'] = pd.DataFrame()
            
        conn.close()
    return dados

def salvar_pf(dados_gerais, df_tel, df_email, df_end, df_emp, df_contr, modo="novo", cpf_original=None):
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
            
            cpf_chave = dados_gerais['cpf']
            
            # 2. Limpar tabelas filhas (estratégia delete/insert simplificada)
            if modo == "editar":
                cur.execute("DELETE FROM pf_telefones WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_emails WHERE cpf_ref = %s", (cpf_chave,))
                cur.execute("DELETE FROM pf_enderecos WHERE cpf_ref = %s", (cpf_chave,))
                # Nota: Empregos e Contratos requerem cuidado maior para não perder IDs se fosse complexo, 
                # mas seguiremos o padrão de recriação para garantir consistência com o editor visual.
                # Como contratos dependem de matricula, deletamos empregos (que deleta contratos via cascade)
                cur.execute("DELETE FROM pf_emprego_renda WHERE cpf_ref = %s", (cpf_chave,))
            
            # 3. Inserir Contatos/Endereços
            for _, row in df_tel.iterrows():
                if row.get('numero'): cur.execute("INSERT INTO pf_telefones (cpf_ref, numero, data_atualizacao, tag_whats, tag_qualificacao) VALUES (%s, %s, %s, %s, %s)", (cpf_chave, row['numero'], row.get('data_atualizacao'), row.get('tag_whats'), row.get('tag_qualificacao')))
            for _, row in df_email.iterrows():
                if row.get('email'): cur.execute("INSERT INTO pf_emails (cpf_ref, email) VALUES (%s, %s)", (cpf_chave, row['email']))
            for _, row in df_end.iterrows():
                if row.get('rua') or row.get('cidade'): cur.execute("INSERT INTO pf_enderecos (cpf_ref, rua, bairro, cidade, uf, cep) VALUES (%s, %s, %s, %s, %s, %s)", (cpf_chave, row['rua'], row.get('bairro'), row.get('cidade'), row.get('uf'), row.get('cep')))

            # 4. Inserir Empregos e Gerar Matrícula
            mapa_matriculas = [] # Para vincular contratos depois
            if not df_emp.empty:
                for _, row in df_emp.iterrows():
                    conv = row.get('convenio')
                    matr = row.get('matricula')
                    
                    if conv:
                        # Regra: Se matrícula vazia, criar (Produto + CPF)
                        if not matr:
                            matr = f"{conv}{cpf_chave}".strip().upper()
                        
                        # Salva
                        try:
                            cur.execute("INSERT INTO pf_emprego_renda (cpf_ref, convenio, matricula, dados_extras) VALUES (%s, %s, %s, %s)",
                                        (cpf_chave, conv, matr, row.get('dados_extras')))
                            mapa_matriculas.append(matr)
                        except Exception as e_emp:
                            print(f"Erro ao salvar emprego: {e_emp}")

            # 5. Inserir Contratos
            # AVISO: O editor de contratos precisa saber a qual matrícula vincular.
            # Nesta interface simplificada, assumimos que o usuário digita a matrícula correta na linha do contrato.
            if not df_contr.empty:
                for _, row in df_contr.iterrows():
                    matr_ref = row.get('matricula_ref')
                    # Só salva se a matrícula existir no banco (ou acabou de ser criada)
                    if matr_ref:
                        # Verifica se matrícula existe (pode ter sido criada agora)
                        cur.execute("SELECT 1 FROM pf_emprego_renda WHERE matricula = %s", (matr_ref,))
                        if cur.fetchone():
                            cur.execute("INSERT INTO pf_contratos (matricula_ref, contrato, dados_extras) VALUES (%s, %s, %s)",
                                        (matr_ref, row.get('contrato'), row.get('dados_extras')))

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
            cur.execute("DELETE FROM pf_dados WHERE cpf = %s", (cpf,))
            conn.commit(); conn.close()
            return True
        except: return False
    return False

def exportar_dados(lista_cpfs):
    conn = get_conn()
    if conn and lista_cpfs:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        # Exportação simplificada (focada em dados cadastrais e primeiro telefone)
        query = f"""
            SELECT d.cpf, d.nome, d.data_nascimento, t.numero as telefone_principal, e.email
            FROM pf_dados d
            LEFT JOIN pf_telefones t ON d.cpf = t.cpf_ref
            LEFT JOIN pf_emails e ON d.cpf = e.cpf_ref
            WHERE d.cpf IN ({placeholders})
        """
        df = pd.read_sql(query, conn, params=tuple(lista_cpfs))
        conn.close()
        return df
    return pd.DataFrame()

# --- INTERFACE ---
@st.dialog("⚠️ Excluir Cadastro")
def dialog_excluir_pf(cpf, nome):
    st.error(f"Apagar **{nome}**?")
    if st.button("Confirmar", type="primary"):
        if excluir_pf(cpf): st.success("Apagado!"); st.rerun()

def app_pessoa_fisica():
    st.markdown("## 👤 Banco de Dados Pessoa Física")
    
    if 'pf_view' not in st.session_state: st.session_state['pf_view'] = 'lista'
    if 'pf_cpf_selecionado' not in st.session_state: st.session_state['pf_cpf_selecionado'] = None

    if st.session_state['pf_view'] == 'lista':
        c1, c2 = st.columns([2, 2])
        with c2: busca = st.text_input("🔎 Pesquisar (CPF / Nome / Telefone)", key="pf_busca")
        
        if st.button("➕ Novo Cadastro", type="primary"):
            st.session_state['pf_view'] = 'novo'; st.session_state['pf_cpf_selecionado'] = None; st.rerun()

        if busca:
            df_lista = buscar_pf(busca)
            if not df_lista.empty:
                df_lista.insert(0, "Sel", False)
                edited_df = st.data_editor(df_lista, column_config={"Sel": st.column_config.CheckboxColumn(required=True)}, disabled=["id", "nome", "cpf"], hide_index=True, use_container_width=True)
                
                # Exportar
                sel = edited_df[edited_df["Sel"]]["cpf"].tolist()
                if sel and st.button(f"📥 Exportar ({len(sel)})"):
                    csv = exportar_dados(sel).to_csv(index=False).encode('utf-8')
                    st.download_button("Baixar CSV", csv, "export_pf.csv", "text/csv")

                for i, row in df_lista.iterrows():
                    with st.expander(f"👤 {row['nome']} ({row['cpf']})"):
                        c1, c2 = st.columns(2)
                        if c1.button("✏️ Editar/Ver", key=f"e_{row['id']}"):
                            st.session_state['pf_view'] = 'editar'; st.session_state['pf_cpf_selecionado'] = row['cpf']; st.rerun()
                        if c2.button("🗑️ Excluir", key=f"d_{row['id']}"): dialog_excluir_pf(row['cpf'], row['nome'])
            else: st.warning("Sem resultados.")
        else: st.info("Use a pesquisa para ver os cadastros.")

    elif st.session_state['pf_view'] in ['novo', 'editar']:
        st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista'}))
        
        modo = st.session_state['pf_view']
        cpf_atual = st.session_state['pf_cpf_selecionado']
        
        dados_db = carregar_dados_completos(cpf_atual) if modo == 'editar' and cpf_atual else {}
        geral = dados_db.get('geral')
        
        st.markdown(f"### {geral['nome'] if geral is not None else 'Novo Cadastro'}")

        with st.form("form_pf"):
            t1, t2, t3, t4, t5, t6 = st.tabs(["Dados Pessoais", "Telefones", "Emails", "Endereços", "💼 Emprego/Renda", "📄 Contratos"])
            
            with t1:
                c1, c2, c3 = st.columns(3)
                nome = c1.text_input("Nome *", value=geral['nome'] if geral is not None else "")
                cpf = c2.text_input("CPF *", value=geral['cpf'] if geral is not None else "")
                nasc = c3.date_input("Nascimento", value=pd.to_datetime(geral['data_nascimento']) if geral is not None and geral['data_nascimento'] else None)
                if nasc:
                    a, m, d = calcular_idade_completa(nasc)
                    st.caption(f"Idade: {a} anos, {m} meses, {d} dias")
                # ... Outros campos cadastrais simplificados para o exemplo ...
                rg = st.text_input("RG", value=geral['rg'] if geral is not None else "")

            with t2:
                df_tel = dados_db.get('telefones') if modo=='editar' else pd.DataFrame(columns=["numero", "tag_whats"])
                ed_tel = st.data_editor(df_tel, num_rows="dynamic", use_container_width=True)

            with t3:
                df_email = dados_db.get('emails') if modo=='editar' else pd.DataFrame(columns=["email"])
                ed_email = st.data_editor(df_email, num_rows="dynamic", use_container_width=True)

            with t4:
                df_end = dados_db.get('enderecos') if modo=='editar' else pd.DataFrame(columns=["rua", "cidade", "uf", "cep"])
                ed_end = st.data_editor(df_end, num_rows="dynamic", use_container_width=True)

            with t5: # Emprego e Renda
                st.markdown("##### Dados Profissionais")
                
                # Gestão rápida de Referências
                with st.expander("➕ Adicionar Novo Convênio (Referência)"):
                    novo_conv = st.text_input("Nome do Convênio")
                    if st.button("Adicionar"):
                        if adicionar_referencia('CONVENIO', novo_conv): st.success("Adicionado!")
                
                # Carrega opções atualizadas
                lista_convenios = buscar_referencias('CONVENIO')
                
                df_emp = dados_db.get('empregos') if modo=='editar' else pd.DataFrame(columns=["convenio", "matricula", "dados_extras"])
                
                # Configura coluna de convênio como dropdown
                col_config_emp = {
                    "convenio": st.column_config.SelectboxColumn("Convênio", options=lista_convenios, required=True),
                    "matricula": st.column_config.TextColumn("Matrícula (Vazio = Auto)", help="Deixe vazio para gerar: Convenio+CPF")
                }
                ed_emp = st.data_editor(df_emp, column_config=col_config_emp, num_rows="dynamic", use_container_width=True)

            with t6: # Contratos
                st.markdown("##### Contratos e Financiamentos")
                st.caption("Atenção: A 'Matrícula Referência' deve existir na aba Emprego/Renda.")
                df_contr = dados_db.get('contratos') if modo=='editar' else pd.DataFrame(columns=["matricula_ref", "contrato", "dados_extras"])
                ed_contr = st.data_editor(df_contr, num_rows="dynamic", use_container_width=True)

            if st.form_submit_button("💾 Salvar Tudo"):
                if nome and cpf:
                    dg = {"cpf": cpf, "nome": nome, "data_nascimento": nasc, "rg": rg} # Campos básicos mapeados
                    ok, msg = salvar_pf(dg, ed_tel, ed_email, ed_end, ed_emp, ed_contr, modo, cpf_atual)
                    if ok: st.success(msg); st.session_state['pf_view'] = 'lista'; st.rerun()
                    else: st.error(msg)
                else: st.warning("Nome e CPF obrigatórios.")

if __name__ == "__main__":
    app_pessoa_fisica()