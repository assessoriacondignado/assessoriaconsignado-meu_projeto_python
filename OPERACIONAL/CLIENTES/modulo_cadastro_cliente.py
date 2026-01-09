import streamlit as st
import pandas as pd
import psycopg2
import time
import re
import bcrypt
import io

# Tenta importar conexao. Se falhar, usa st.secrets direto ou avisa.
try:
    import conexao
except ImportError:
    st.error("Erro: conexao.py não encontrado na raiz.")

# --- FUNÇÕES DE CONEXÃO E AUXILIARES ---

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        print(f"Erro conexão: {e}")
        return None

def limpar_formatacao_texto(texto):
    if not texto: return ""
    return str(texto).replace('*', '').strip()

def hash_senha(senha):
    if senha.startswith('$2b$'): return senha
    return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# --- ESTILIZAÇÃO CSS ---
def aplicar_estilo_tabela():
    st.markdown("""
    <style>
    /* Ajustes para o Data Editor */
    div[data-testid="stDataEditor"] {
        border: 1px solid #cccccc;
        border-radius: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- FUNÇÕES DE BANCO DE DADOS ---

def listar_agrupamentos(tipo):
    conn = get_conn()
    if not conn: return pd.DataFrame()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        df = pd.read_sql(f"SELECT id, nome_agrupamento FROM {tabela} ORDER BY id", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def listar_cliente_cnpj():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, cnpj, nome_empresa FROM admin.cliente_cnpj ORDER BY nome_empresa", conn)
        conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def excluir_cliente_db(id_cliente):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("DELETE FROM admin.clientes WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def buscar_usuarios_disponiveis():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        query = "SELECT id, nome, email, cpf FROM clientes_usuarios WHERE id NOT IN (SELECT id_usuario_vinculo FROM admin.clientes WHERE id_usuario_vinculo IS NOT NULL) ORDER BY nome"
        df = pd.read_sql(query, conn); conn.close(); return df
    except: 
        if conn: conn.close()
        return pd.DataFrame()

def vincular_usuario_cliente(id_cliente, id_usuario):
    conn = get_conn()
    if not conn: return False, "Erro Conexão"
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = %s WHERE id = %s", (int(id_usuario), int(id_cliente)))
        conn.commit(); conn.close(); return True, "Vinculado!"
    except Exception as e: 
        conn.close()
        return False, str(e)

def desvincular_usuario_cliente(id_cliente):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = NULL WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close(); return True
    except: 
        if conn: conn.close()
        return False

def salvar_usuario_novo(nome, email, cpf, tel, senha, nivel, ativo):
    conn = get_conn()
    if not conn: return None
    try:
        cur = conn.cursor(); senha_f = hash_senha(senha)
        if not nivel: nivel = 'Cliente sem permissão'
        cur.execute("INSERT INTO clientes_usuarios (nome, email, cpf, telefone, senha, nivel, ativo) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", (nome, email, cpf, tel, senha_f, nivel, ativo))
        nid = cur.fetchone()[0]; conn.commit(); conn.close(); return nid
    except: 
        if conn: conn.close()
        return None

# --- NOVA FUNÇÃO: LISTAR CLIENTES PARA PLANILHA ---
def listar_clientes_completo(filtro=""):
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        sql = """
            SELECT id, nome, cpf, nome_empresa, email, telefone, telefone2, status, id_usuario_vinculo
            FROM admin.clientes 
            WHERE 1=1
        """
        if filtro:
            sql += f" AND (nome ILIKE '%%{filtro}%%' OR cpf ILIKE '%%{filtro}%%' OR nome_empresa ILIKE '%%{filtro}%%')"
            
        sql += " ORDER BY id DESC"
        
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except:
        if conn: conn.close()
        return pd.DataFrame()

# --- NOVA FUNÇÃO: SALVAR EM LOTE (PLANILHA) ---
def salvar_alteracoes_clientes(df_original, df_editado):
    conn = get_conn()
    if not conn: return False, "Erro de conexão"
    
    try:
        cur = conn.cursor()
        
        # 1. Identificar IDs Originais
        ids_originais = set(df_original['id'].dropna().astype(int).tolist()) if 'id' in df_original.columns else set()
        ids_editados = set()
        
        # Coletar IDs que permaneceram no editor
        for _, row in df_editado.iterrows():
            if pd.notna(row.get('id')):
                try: ids_editados.add(int(row['id']))
                except: pass
        
        # 2. Processar DELETES (IDs que sumiram)
        ids_para_excluir = ids_originais - ids_editados
        if ids_para_excluir:
            ids_str = ",".join(map(str, ids_para_excluir))
            cur.execute(f"DELETE FROM admin.clientes WHERE id IN ({ids_str})")

        # 3. Processar INSERTS e UPDATES
        # Colunas que serão persistidas via planilha
        cols_db = ['nome', 'cpf', 'nome_empresa', 'email', 'telefone', 'telefone2', 'status']
        
        for index, row in df_editado.iterrows():
            # Limpeza básica de dados
            vals = [
                str(row.get(c, '')).strip() if pd.notna(row.get(c)) else '' 
                for c in cols_db
            ]
            
            row_id = row.get('id')
            
            # Verifica se é NOVO (ID vazio ou NaN)
            eh_novo = pd.isna(row_id) or str(row_id).strip() == ''
            
            if eh_novo:
                # INSERT
                placeholders = ",".join(["%s"] * len(cols_db))
                cols_str = ",".join(cols_db)
                if not vals[6]: vals[6] = 'ATIVO' # Status padrão
                
                cur.execute(f"INSERT INTO admin.clientes ({cols_str}) VALUES ({placeholders})", vals)
            
            elif int(row_id) in ids_originais:
                # UPDATE
                set_clause = ",".join([f"{c}=%s" for c in cols_db])
                vals.append(int(row_id)) 
                cur.execute(f"UPDATE admin.clientes SET {set_clause} WHERE id=%s", vals)

        conn.commit()
        conn.close()
        return True, "Alterações salvas com sucesso!"
        
    except Exception as e:
        if conn: conn.close()
        return False, f"Erro ao salvar: {e}"

# --- DIALOGS (MODAIS) ---

@st.dialog("🔗 Gestão de Acesso do Cliente")
def dialog_gestao_usuario_vinculo(dados_cliente):
    # Lógica mantida da versão anterior
    raw_id = dados_cliente.get('id_vinculo') or dados_cliente.get('id_usuario_vinculo')
    
    id_vinculo = None
    if pd.notna(raw_id) and raw_id is not None:
        try: id_vinculo = int(float(raw_id))
        except: id_vinculo = None

    if id_vinculo:
        st.success("✅ Este cliente já possui um usuário vinculado.")
        conn = get_conn()
        if conn:
            df_u = pd.read_sql(f"SELECT nome, email, telefone, cpf FROM clientes_usuarios WHERE id = {id_vinculo}", conn); conn.close()
            if not df_u.empty:
                usr = df_u.iloc[0]
                st.write(f"**Nome:** {usr['nome']}")
                st.write(f"**Login:** {usr['email']}")
                st.markdown("---")
                if st.button("🔓 Desvincular Usuário", type="primary"):
                    if desvincular_usuario_cliente(dados_cliente['id']): st.success("Desvinculado!"); time.sleep(1.5); st.rerun()
                    else: st.error("Erro.")
            else:
                st.warning("Usuário vinculado não encontrado.")
                if st.button("Forçar Desvinculo"): desvincular_usuario_cliente(dados_cliente['id']); st.rerun()
    else:
        st.warning("⚠️ Este cliente não tem acesso ao sistema.")
        tab_novo, tab_existente = st.tabs(["✨ Criar Novo", "🔍 Vincular Existente"])
        with tab_novo:
            with st.form("form_cria_vincula"):
                u_email = st.text_input("Login (Email)", value=dados_cliente['email'])
                u_senha = st.text_input("Senha Inicial", value="1234")
                u_cpf = st.text_input("CPF", value=dados_cliente['cpf'])
                u_nome = st.text_input("Nome", value=limpar_formatacao_texto(dados_cliente['nome']))
                if st.form_submit_button("Criar e Vincular"):
                    novo_id = salvar_usuario_novo(u_nome, u_email, u_cpf, dados_cliente['telefone'], u_senha, 'Cliente sem permissão', True)
                    if novo_id: 
                        ok, msg = vincular_usuario_cliente(dados_cliente['id'], novo_id)
                        if ok: st.success("Criado e vinculado!"); time.sleep(1); st.rerun()
                        else: st.error(f"Erro ao vincular: {msg}")
                    else: st.error("Erro ao criar usuário.")
        with tab_existente:
            df_livres = buscar_usuarios_disponiveis()
            if not df_livres.empty:
                opcoes = df_livres.apply(lambda x: f"{x['nome']} ({x['email']})", axis=1)
                idx_sel = st.selectbox("Selecione o Usuário", range(len(df_livres)), format_func=lambda x: opcoes[x])
                if st.button("Vincular Selecionado"):
                    ok, msg = vincular_usuario_cliente(dados_cliente['id'], df_livres.iloc[idx_sel]['id'])
                    if ok: st.success("Vinculado!"); time.sleep(1); st.rerun()
                    else: st.error(f"Erro: {msg}")
            else: st.info("Sem usuários livres.")

@st.dialog("🚨 Excluir Cliente")
def dialog_excluir_cliente(id_cli, nome):
    st.error(f"Excluir **{nome}**?"); st.warning("Apenas a ficha cadastral será apagada.")
    c1, c2 = st.columns(2)
    if c1.button("Sim, Excluir"):
        if excluir_cliente_db(id_cli): st.success("Removido."); time.sleep(1); st.session_state['view_cliente'] = 'lista'; st.rerun()
    if c2.button("Cancelar"): st.rerun()

# --- FUNÇÃO PRINCIPAL DO MÓDULO ---

def app_cadastro_cliente():
    aplicar_estilo_tabela()
    
    # --- BARRA DE TÍTULO E AÇÕES GERAIS ---
    c_busca, c_novo = st.columns([6, 1])
    filtro = c_busca.text_input("🔍 Buscar na Tabela", placeholder="Filtrar por Nome, CPF ou Empresa...")
    
    if c_novo.button("➕ Novo", type="primary"): 
        st.session_state['view_cliente'] = 'novo'
        st.rerun()

    # --- MODO LISTA / PLANILHA (PADRÃO) ---
    if st.session_state.get('view_cliente', 'lista') == 'lista':
        
        # Carregar dados (com cache simples de sessão para não recarregar toda hora se não precisar)
        if 'df_clientes_cache' not in st.session_state or filtro:
            st.session_state['df_clientes_cache'] = listar_clientes_completo(filtro)
        
        df_atual = st.session_state['df_clientes_cache']
        
        st.caption("💡 **Dica:** Cole dados do Excel (Ctrl+V) na última linha vazia para inserir em massa. Edite células diretamente.")

        # --- DATA EDITOR (PLANILHA) ---
        df_editado = st.data_editor(
            df_atual,
            key="editor_clientes_principal",
            num_rows="dynamic",
            use_container_width=True,
            height=500,
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "nome": st.column_config.TextColumn("Nome Completo", required=True, width="large"),
                "cpf": st.column_config.TextColumn("CPF", width="medium"),
                "nome_empresa": st.column_config.TextColumn("Empresa", width="medium"),
                "email": st.column_config.TextColumn("E-mail", width="medium"),
                "telefone": st.column_config.TextColumn("Tel 1", width="small"),
                "telefone2": st.column_config.TextColumn("Tel 2", width="small"),
                "status": st.column_config.SelectboxColumn("Status", options=["ATIVO", "INATIVO"], default="ATIVO", width="small"),
                "id_usuario_vinculo": st.column_config.NumberColumn("Vinculo", disabled=True, help="Use 'Ações Avançadas' para alterar")
            },
            column_order=["id", "nome", "cpf", "nome_empresa", "email", "telefone", "telefone2", "status"]
        )

        # --- RODAPÉ DA TABELA (SALVAR E EXPORTAR) ---
        c_save, c_exp = st.columns([2, 10])
        
        with c_save:
            if st.button("💾 Salvar Alterações", type="primary", use_container_width=True):
                if not df_editado.equals(df_atual):
                    ok, msg = salvar_alteracoes_clientes(df_atual, df_editado)
                    if ok:
                        st.success(msg)
                        time.sleep(1)
                        st.session_state['df_clientes_cache'] = listar_clientes_completo(filtro)
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    st.info("Nenhuma alteração detectada.")

        with c_exp:
            if not df_editado.empty:
                csv = df_editado.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Baixar CSV", data=csv, file_name=f"clientes_{pd.Timestamp.now().strftime('%d%m%y')}.csv", mime="text/csv")

        st.divider()

        # --- AÇÕES AVANÇADAS (Para suprir botões que existiam na lista) ---
        with st.expander("🛠️ Ações Avançadas (Vincular Usuário / Editar Detalhes)"):
            st.caption("Selecione um cliente para gerenciar acesso ou editar via formulário completo.")
            
            # Seletor de cliente baseado no DF atual
            if not df_atual.empty:
                opcoes_cli = df_atual.apply(lambda x: f"{x['id']} - {x['nome']}", axis=1)
                sel_cli_idx = st.selectbox("Selecione o Cliente:", options=range(len(df_atual)), format_func=lambda x: opcoes_cli.iloc[x])
                
                if sel_cli_idx is not None:
                    cli_selecionado = df_atual.iloc[sel_cli_idx]
                    
                    c_act1, c_act2 = st.columns(2)
                    if c_act1.button("🔗 Gerenciar Acesso (Usuário)", use_container_width=True):
                        dialog_gestao_usuario_vinculo(cli_selecionado.to_dict())
                    
                    if c_act2.button("✏️ Abrir Formulário de Edição", use_container_width=True):
                        st.session_state.update({'view_cliente': 'editar', 'cli_id': int(cli_selecionado['id'])})
                        st.rerun()

    # --- MODO NOVO / EDITAR (FORMULÁRIO DETALHADO) ---
    elif st.session_state['view_cliente'] in ['novo', 'editar']:
        st.markdown(f"### {'📝 Novo Cliente' if st.session_state['view_cliente']=='novo' else '✏️ Editar Cliente'}")
        
        dados = {}
        if st.session_state['view_cliente'] == 'editar':
            conn = get_conn()
            if conn:
                try:
                    df = pd.read_sql(f"SELECT * FROM admin.clientes WHERE id = {st.session_state['cli_id']}", conn)
                    if not df.empty: dados = df.iloc[0]
                except: pass
                finally: conn.close()

        df_empresas = listar_cliente_cnpj() 
        df_ag_cli = listar_agrupamentos("cliente")
        df_ag_emp = listar_agrupamentos("empresa")

        with st.form("form_cliente"):
            c1, c2, c3 = st.columns(3)
            nome = c1.text_input("Nome Completo *", value=limpar_formatacao_texto(dados.get('nome', '')))
            
            lista_empresas = df_empresas['nome_empresa'].unique().tolist()
            idx_emp = 0
            val_emp_atual = dados.get('nome_empresa', '')
            if val_emp_atual in lista_empresas: idx_emp = lista_empresas.index(val_emp_atual)
            
            nome_emp = c2.selectbox("Empresa", options=[""] + lista_empresas, index=idx_emp + 1 if val_emp_atual else 0)
            cnpj_display = dados.get('cnpj_empresa', '')
            c3.text_input("CNPJ (Vinculado)", value=cnpj_display, disabled=True)

            c4, c5, c6, c7 = st.columns(4)
            email = c4.text_input("E-mail", value=dados.get('email', ''))
            cpf = c5.text_input("CPF", value=dados.get('cpf', ''))
            tel1 = c6.text_input("Telefone 1", value=dados.get('telefone', ''))
            tel2 = c7.text_input("Telefone 2", value=dados.get('telefone2', ''))
            
            c8, c9, c10 = st.columns([1, 1, 1])
            id_gp = c8.text_input("ID Grupo WhatsApp", value=dados.get('id_grupo_whats', ''))
            
            padrao_cli = []
            if dados.get('ids_agrupamento_cliente'):
                try: padrao_cli = [int(x.strip()) for x in str(dados.get('ids_agrupamento_cliente')).split(',') if x.strip().isdigit()]
                except: pass
            sel_ag_cli = c9.multiselect("Agrupamento Cliente", options=df_ag_cli['id'], format_func=lambda x: df_ag_cli[df_ag_cli['id']==x]['nome_agrupamento'].values[0] if not df_ag_cli[df_ag_cli['id']==x].empty else x, default=[x for x in padrao_cli if x in df_ag_cli['id'].values])

            padrao_emp = []
            if dados.get('ids_agrupamento_empresa'):
                try: padrao_emp = [int(x.strip()) for x in str(dados.get('ids_agrupamento_empresa')).split(',') if x.strip().isdigit()]
                except: pass
            sel_ag_emp = c10.multiselect("Agrupamento Empresa", options=df_ag_emp['id'], format_func=lambda x: df_ag_emp[df_ag_emp['id']==x]['nome_agrupamento'].values[0] if not df_ag_emp[df_ag_emp['id']==x].empty else x, default=[x for x in padrao_emp if x in df_ag_emp['id'].values])
            
            status_final = "ATIVO"
            if st.session_state['view_cliente'] == 'editar':
                st.divider(); cs1, _ = st.columns([1, 4])
                status_final = cs1.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if dados.get('status','ATIVO')=="ATIVO" else 1)

            st.markdown("<br>", unsafe_allow_html=True); ca = st.columns([1, 1, 4])
            
            if ca[0].form_submit_button("💾 Salvar"):
                # Lógica de salvar do formulário (mantida para edições detalhadas)
                cnpj_final = ""
                if nome_emp:
                    filtro_cnpj = df_empresas[df_empresas['nome_empresa'] == nome_emp]
                    if not filtro_cnpj.empty: cnpj_final = filtro_cnpj.iloc[0]['cnpj']
                
                str_ag_cli = ",".join(map(str, sel_ag_cli))
                str_ag_emp = ",".join(map(str, sel_ag_emp))

                conn = get_conn()
                if conn:
                    cur = conn.cursor()
                    if st.session_state['view_cliente'] == 'novo':
                        cur.execute("INSERT INTO admin.clientes (nome, nome_empresa, cnpj_empresa, email, cpf, telefone, telefone2, id_grupo_whats, ids_agrupamento_cliente, ids_agrupamento_empresa, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ATIVO')", (nome, nome_emp, cnpj_final, email, cpf, tel1, tel2, id_gp, str_ag_cli, str_ag_emp))
                    else:
                        cur.execute("UPDATE admin.clientes SET nome=%s, nome_empresa=%s, cnpj_empresa=%s, email=%s, cpf=%s, telefone=%s, telefone2=%s, id_grupo_whats=%s, ids_agrupamento_cliente=%s, ids_agrupamento_empresa=%s, status=%s WHERE id=%s", (nome, nome_emp, cnpj_final, email, cpf, tel1, tel2, id_gp, str_ag_cli, str_ag_emp, status_final, st.session_state['cli_id']))
                    conn.commit(); conn.close(); st.success("Salvo!"); time.sleep(1); st.session_state['view_cliente'] = 'lista'; st.rerun()
            
            if ca[1].form_submit_button("Cancelar"): 
                st.session_state['view_cliente'] = 'lista'
                st.rerun()

        if st.session_state['view_cliente'] == 'editar':
            st.markdown("---")
            if st.button("🗑️ Excluir Cliente (Formulário)", type="primary"): 
                dialog_excluir_cliente(st.session_state['cli_id'], nome)

if __name__ == "__main__":
    app_cadastro_cliente()