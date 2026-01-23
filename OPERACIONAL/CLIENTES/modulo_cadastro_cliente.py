import streamlit as st
import pandas as pd
import psycopg2
import time
import re
import bcrypt

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

# --- FUNÇÕES DE BANCO DE DADOS ESPECÍFICAS PARA CLIENTE ---

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

# --- DIALOGS (MODAIS) ---

@st.dialog("🔗 Gestão de Acesso do Cliente")
def dialog_gestao_usuario_vinculo(dados_cliente):
    # Recupera o ID
    raw_id = dados_cliente.get('id_vinculo') or dados_cliente.get('id_usuario_vinculo')
    
    # CORREÇÃO: Tratamento robusto para NaN e conversão segura para inteiro
    id_vinculo = None
    if pd.notna(raw_id) and raw_id is not None:
        try:
            id_vinculo = int(float(raw_id))
        except:
            id_vinculo = None

    if id_vinculo:
        st.success("✅ Este cliente já possui um usuário vinculado.")
        conn = get_conn()
        if conn:
            # Agora id_vinculo é um inteiro garantido, evitando o erro 'nan' no SQL
            df_u = pd.read_sql(f"SELECT nome, email, telefone, cpf FROM clientes_usuarios WHERE id = {id_vinculo}", conn); conn.close()
            if not df_u.empty:
                usr = df_u.iloc[0]
                st.write(f"**Nome:** {usr['nome']}"); st.write(f"**Login:** {usr['email']}"); st.write(f"**CPF:** {usr['cpf']}")
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
                    if ok:
                        st.success("Vinculado com sucesso!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"Erro ao vincular: {msg}")
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
    c1, c2 = st.columns([6, 1])
    filtro = c1.text_input("🔍 Buscar Cliente", placeholder="Nome, CPF ou Nome Empresa")
    if c2.button("➕ Novo", type="primary"): st.session_state['view_cliente'] = 'novo'; st.rerun()

    if st.session_state.get('view_cliente', 'lista') == 'lista':
        conn = get_conn()
        if not conn:
            st.error("Sem conexão com banco de dados.")
            return

        # ATUALIZAÇÃO: Join com a tabela de usuários para pegar o nome
        sql = """
            SELECT c.*, c.id_usuario_vinculo as id_vinculo, u.nome as nome_usuario_vinculado
            FROM admin.clientes c
            LEFT JOIN clientes_usuarios u ON c.id_usuario_vinculo = u.id
        """
        if filtro: 
            # ATUALIZAÇÃO: Uso do alias 'c.' para evitar ambiguidade
            sql += f" WHERE c.nome ILIKE '%%{filtro}%%' OR c.cpf ILIKE '%%{filtro}%%' OR c.nome_empresa ILIKE '%%{filtro}%%'"
        sql += " ORDER BY c.id DESC LIMIT 50"
        
        try:
            df_cli = pd.read_sql(sql, conn)
        except Exception as e:
            st.error(f"Erro ao ler clientes: {e}")
            df_cli = pd.DataFrame()
        finally:
            conn.close()

        if not df_cli.empty:
            # ATUALIZAÇÃO: Inclusão da coluna Usuário no cabeçalho
            st.markdown("""
            <div style="display:flex; font-weight:bold; color:#555; padding:8px; border-bottom:2px solid #ddd; margin-bottom:10px; background-color:#f8f9fa;">
                <div style="flex:3;">Nome</div>
                <div style="flex:2;">CPF</div>
                <div style="flex:2;">Empresa</div>
                <div style="flex:2;">Usuário</div>
                <div style="flex:1;">Status</div>
                <div style="flex:2; text-align:center;">Ações</div>
            </div>
            """, unsafe_allow_html=True)
            
            for _, row in df_cli.iterrows():
                with st.container():
                    # ATUALIZAÇÃO: Ajuste de pesos e nova coluna c4 para o usuário
                    c1, c2, c3, c4, c5, c6 = st.columns([3, 2, 2, 2, 1, 2])
                    c1.write(f"**{limpar_formatacao_texto(row['nome'])}**")
                    c2.write(row['cpf'] or "-")
                    c3.write(row['nome_empresa'] or "-")
                    
                    # Nova Coluna: Usuário Vinculado
                    nome_vinculo = row['nome_usuario_vinculado']
                    c4.write(limpar_formatacao_texto(nome_vinculo) if nome_vinculo else "-")

                    cor_st = 'green' if row.get('status','ATIVO')=='ATIVO' else 'red'
                    c5.markdown(f":{cor_st}[{row.get('status','ATIVO')}]")
                    
                    with c6:
                        b1, b3, b4 = st.columns(3) # Botão extrato removido (b2)
                        if b1.button("✏️", key=f"e_{row['id']}", help="Editar Cadastro"): 
                            st.session_state.update({'view_cliente': 'editar', 'cli_id': row['id']}); st.rerun()
                        
                        # O botão de extrato foi removido para evitar dependência circular com módulo financeiro.
                        # O usuário deve ver o extrato na aba Financeiro.
                            
                        if b3.button("🔗" if row['id_vinculo'] else "👤", key=f"u_{row['id']}", help="Acesso Usuário"): 
                            dialog_gestao_usuario_vinculo(row)
                            
                        if b4.button("🗑️", key=f"d_{row['id']}", help="Excluir"):
                            dialog_excluir_cliente(row['id'], row['nome'])
                    
                    st.markdown("<hr style='margin: 5px 0; border-color: #eee;'>", unsafe_allow_html=True)
        else: st.info("Nenhum cliente encontrado.")

    elif st.session_state['view_cliente'] in ['novo', 'editar']:
        st.markdown(f"### {'📝 Novo' if st.session_state['view_cliente']=='novo' else '✏️ Editar'}")
        
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
            
            nome_emp = c2.selectbox("Empresa (Selecionar)", options=[""] + lista_empresas, index=idx_emp + 1 if val_emp_atual else 0, help="Ao selecionar, o CNPJ será preenchido automaticamente ao salvar.")
            cnpj_display = dados.get('cnpj_empresa', '')
            c3.text_input("CNPJ (Vinculado)", value=cnpj_display, disabled=True, help="Este campo é atualizado automaticamente com base na Empresa selecionada.")

            c4, c5, c6, c7 = st.columns(4)
            email = c4.text_input("E-mail *", value=dados.get('email', ''))
            cpf = c5.text_input("CPF *", value=dados.get('cpf', ''))
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
            
            if ca[1].form_submit_button("Cancelar"): st.session_state['view_cliente'] = 'lista'; st.rerun()

        if st.session_state['view_cliente'] == 'editar':
            st.markdown("---")
            if st.button("🗑️ Excluir Cliente", type="primary"): dialog_excluir_cliente(st.session_state['cli_id'], nome)