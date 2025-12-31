import streamlit as st
import pandas as pd
import psycopg2
import bcrypt
import re
import time
from datetime import datetime

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
    except: return None

# =============================================================================
# 1. FUNÇÕES AUXILIARES E DB
# =============================================================================

def formatar_cnpj(v):
    v = re.sub(r'\D', '', str(v))
    return f"{v[:2]}.{v[2:5]}.{v[5:8]}/{v[8:12]}-{v[12:]}" if len(v) == 14 else v

def listar_agrupamentos(tipo):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        df = pd.read_sql(f"SELECT id, nome_agrupamento FROM {tabela} ORDER BY id", conn)
        conn.close()
        return df
    except: 
        conn.close()
        return pd.DataFrame()

def salvar_agrupamento(tipo, nome):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {tabela} (nome_agrupamento) VALUES (%s)", (nome,))
        conn.commit(); conn.close()
        return True
    except: 
        conn.close(); return False

def excluir_agrupamento(tipo, id_agrup):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {tabela} WHERE id = %s", (id_agrup,))
        conn.commit(); conn.close()
        return True
    except: 
        conn.close(); return False

# =============================================================================
# 2. FUNÇÕES DE USUÁRIO (MIGRADO DE MODULO_USUARIO.PY)
# =============================================================================

def hash_senha(senha):
    if senha.startswith('$2b$'): return senha
    return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def salvar_usuario(nome, email, cpf, tel, senha, hierarquia, ativo, id_cliente_vinculo=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        # Se for criar com vinculo direto
        # Nota: A tabela clientes_usuarios não tem campo id_cliente_vinculo nativo no seu SQL original, 
        # vamos assumir que o vinculo é feito pelo CPF ou Email.
        
        senha_final = hash_senha(senha)
        cur.execute("""
            INSERT INTO clientes_usuarios (nome, email, cpf, telefone, senha, hierarquia, ativo)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE SET 
            nome=EXCLUDED.nome, cpf=EXCLUDED.cpf, telefone=EXCLUDED.telefone, hierarquia=EXCLUDED.hierarquia, ativo=EXCLUDED.ativo
        """, (nome, email, cpf, tel, senha_final, hierarquia, ativo))
        
        conn.commit(); conn.close()
        return True
    except Exception as e:
        conn.close(); return False

# =============================================================================
# 3. INTERFACE PRINCIPAL
# =============================================================================

def app_clientes():
    st.markdown("## 👥 Central de Clientes e Usuários")
    
    # 1. MENU SUPERIOR (ABAS)
    tab_cli, tab_user, tab_agrup, tab_rel = st.tabs(["🏢 Clientes", "👤 Usuários", "🏷️ Agrupamentos", "📊 Relatórios"])

    # --- ABA CLIENTES ---
    with tab_cli:
        c1, c2 = st.columns([6, 1])
        filtro = c1.text_input("🔍 Buscar Cliente", placeholder="Nome, CPF ou Nome Empresa")
        if c2.button("➕ Novo", type="primary"):
            st.session_state['view_cliente'] = 'novo'
            st.rerun()

        # VIEW: LISTA DE CLIENTES
        if st.session_state.get('view_cliente', 'lista') == 'lista':
            conn = get_conn()
            sql = "SELECT * FROM admin.clientes"
            if filtro:
                sql += f" WHERE nome ILIKE '%%{filtro}%%' OR cpf ILIKE '%%{filtro}%%' OR nome_empresa ILIKE '%%{filtro}%%'"
            sql += " ORDER BY id DESC LIMIT 50"
            df_cli = pd.read_sql(sql, conn)
            conn.close()

            if not df_cli.empty:
                st.markdown("""
                <div style="display: flex; font-weight: bold; background: #f0f2f6; padding: 10px; border-radius: 5px;">
                    <div style="flex: 2;">Nome</div>
                    <div style="flex: 1;">CPF</div>
                    <div style="flex: 2;">Empresa</div>
                    <div style="flex: 1;">Ações</div>
                </div>
                """, unsafe_allow_html=True)

                for _, row in df_cli.iterrows():
                    with st.container():
                        c1, c2, c3, c4 = st.columns([2, 1, 2, 1])
                        c1.write(f"{row['nome']}")
                        c2.write(row['cpf'] or "-")
                        c3.write(row['nome_empresa'] or "-")
                        
                        with c4:
                            b1, b2 = st.columns(2)
                            if b1.button("👁️", key=f"ver_{row['id']}", help="Ver/Editar"):
                                st.session_state['view_cliente'] = 'editar'
                                st.session_state['cli_id'] = row['id']
                                st.rerun()
                            
                            if b2.button("👤", key=f"usr_{row['id']}", help="Criar Usuário"):
                                dialog_criar_usuario_rapido(row)
                        st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else:
                st.info("Nenhum cliente encontrado.")

        # VIEW: NOVO / EDITAR CLIENTE
        elif st.session_state['view_cliente'] in ['novo', 'editar']:
            st.markdown(f"### {'📝 Novo Cadastro' if st.session_state['view_cliente'] == 'novo' else '✏️ Editar Cliente'}")
            
            dados = {}
            if st.session_state['view_cliente'] == 'editar':
                conn = get_conn()
                df = pd.read_sql(f"SELECT * FROM admin.clientes WHERE id = {st.session_state['cli_id']}", conn)
                conn.close()
                if not df.empty: dados = df.iloc[0]

            with st.form("form_cliente"):
                c1, c2, c3 = st.columns(3)
                nome = c1.text_input("Nome Completo *", value=dados.get('nome', ''))
                nome_emp = c2.text_input("Nome Empresa", value=dados.get('nome_empresa', ''))
                cnpj_emp = c3.text_input("CNPJ Empresa", value=dados.get('cnpj_empresa', ''))

                c4, c5, c6, c7 = st.columns(4)
                email = c4.text_input("E-mail *", value=dados.get('email', ''))
                cpf = c5.text_input("CPF *", value=dados.get('cpf', ''))
                tel1 = c6.text_input("Telefone 1", value=dados.get('telefone', ''))
                tel2 = c7.text_input("Telefone 2", value=dados.get('telefone2', ''))

                c8, c9, c10 = st.columns([1, 1, 1])
                id_gp = c8.text_input("ID Grupo WhatsApp", value=dados.get('id_grupo_whats', ''))
                # Campos de IDs manuais (1;2;3)
                agr_cli = c9.text_input("Agrupamento Cliente (IDs ex: 1;2)", value=dados.get('ids_agrupamento_cliente', ''))
                agr_emp = c10.text_input("Agrupamento Empresa (IDs ex: 1;2)", value=dados.get('ids_agrupamento_empresa', ''))

                b_salvar, b_cancel = st.columns([1, 5])
                if b_salvar.form_submit_button("💾 Salvar"):
                    # Lógica de Salvar/Update SQL
                    conn = get_conn(); cur = conn.cursor()
                    cnpj_limpo = formatar_cnpj(cnpj_emp)
                    
                    if st.session_state['view_cliente'] == 'novo':
                        sql = """INSERT INTO admin.clientes (nome, nome_empresa, cnpj_empresa, email, cpf, telefone, telefone2, id_grupo_whats, ids_agrupamento_cliente, ids_agrupamento_empresa) 
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                        cur.execute(sql, (nome, nome_emp, cnpj_limpo, email, cpf, tel1, tel2, id_gp, agr_cli, agr_emp))
                    else:
                        sql = """UPDATE admin.clientes SET nome=%s, nome_empresa=%s, cnpj_empresa=%s, email=%s, cpf=%s, telefone=%s, telefone2=%s, id_grupo_whats=%s, ids_agrupamento_cliente=%s, ids_agrupamento_empresa=%s 
                                 WHERE id=%s"""
                        cur.execute(sql, (nome, nome_emp, cnpj_limpo, email, cpf, tel1, tel2, id_gp, agr_cli, agr_emp, st.session_state['cli_id']))
                    
                    conn.commit(); conn.close()
                    st.success("Salvo!"); time.sleep(1)
                    st.session_state['view_cliente'] = 'lista'
                    st.rerun()

                if b_cancel.form_submit_button("Cancelar"):
                    st.session_state['view_cliente'] = 'lista'
                    st.rerun()

    # --- ABA USUÁRIOS (ANTIGO MODULO_USUARIO.PY) ---
    with tab_user:
        st.markdown("### Gestão de Acesso")
        
        # Filtro de Usuários
        busca_user = st.text_input("Buscar Usuário", placeholder="Nome ou Email")
        conn = get_conn()
        sql_u = "SELECT id, nome, email, hierarquia, ativo FROM clientes_usuarios WHERE 1=1"
        if busca_user: sql_u += f" AND (nome ILIKE '%{busca_user}%' OR email ILIKE '%{busca_user}%')"
        sql_u += " ORDER BY id DESC"
        df_users = pd.read_sql(sql_u, conn)
        conn.close()

        for _, u in df_users.iterrows():
            with st.expander(f"{u['nome']} ({u['hierarquia']})"):
                with st.form(f"form_user_{u['id']}"):
                    c_n, c_e = st.columns(2)
                    n_nome = c_n.text_input("Nome", value=u['nome'])
                    n_mail = c_e.text_input("Email", value=u['email'])
                    
                    c_h, c_s, c_a = st.columns(3)
                    n_hier = c_h.selectbox("Nível", ["Cliente", "Admin", "Gerente"], index=["Cliente", "Admin", "Gerente"].index(u['hierarquia']) if u['hierarquia'] in ["Cliente", "Admin", "Gerente"] else 0)
                    n_senha = c_s.text_input("Nova Senha", type="password", placeholder="Vazio mantêm atual")
                    n_ativo = c_a.checkbox("Ativo", value=u['ativo'])
                    
                    if st.form_submit_button("Atualizar Usuário"):
                        conn = get_conn(); cur = conn.cursor()
                        if n_senha:
                            sh = hash_senha(n_senha)
                            cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, hierarquia=%s, senha=%s, ativo=%s WHERE id=%s", (n_nome, n_mail, n_hier, sh, n_ativo, u['id']))
                        else:
                            cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, hierarquia=%s, ativo=%s WHERE id=%s", (n_nome, n_mail, n_hier, n_ativo, u['id']))
                        conn.commit(); conn.close()
                        st.success("Atualizado!"); st.rerun()

    # --- ABA AGRUPAMENTOS ---
    with tab_agrup:
        c_ag1, c_ag2 = st.columns(2)
        
        with c_ag1:
            st.markdown("##### 🏷️ Agrupamento Clientes")
            novo_ac = st.text_input("Novo Agrupamento Cliente")
            if st.button("Adicionar", key="add_ac"):
                if novo_ac: salvar_agrupamento("cliente", novo_ac); st.rerun()
            
            df_ac = listar_agrupamentos("cliente")
            if not df_ac.empty:
                for _, r in df_ac.iterrows():
                    ca1, ca2 = st.columns([4, 1])
                    ca1.write(f"{r['id']} - {r['nome_agrupamento']}")
                    if ca2.button("🗑️", key=f"del_ac_{r['id']}"): excluir_agrupamento("cliente", r['id']); st.rerun()

        with c_ag2:
            st.markdown("##### 🏢 Agrupamento Empresas")
            novo_ae = st.text_input("Novo Agrupamento Empresa")
            if st.button("Adicionar", key="add_ae"):
                if novo_ae: salvar_agrupamento("empresa", novo_ae); st.rerun()
            
            df_ae = listar_agrupamentos("empresa")
            if not df_ae.empty:
                for _, r in df_ae.iterrows():
                    ca1, ca2 = st.columns([4, 1])
                    ca1.write(f"{r['id']} - {r['nome_agrupamento']}")
                    if ca2.button("🗑️", key=f"del_ae_{r['id']}"): excluir_agrupamento("empresa", r['id']); st.rerun()

    # --- ABA RELATÓRIOS (INTEGRAÇÃO FATOR CONFERI) ---
    with tab_rel:
        st.markdown("### 📊 Relatórios Integrados")
        
        # Filtro de Cliente para o Relatório
        conn = get_conn()
        clientes_opts = pd.read_sql("SELECT id, nome, cpf FROM admin.clientes ORDER BY nome", conn)
        conn.close()
        
        sel_cli_id = st.selectbox("Selecione o Cliente", options=clientes_opts['id'], format_func=lambda x: clientes_opts[clientes_opts['id']==x]['nome'].values[0])
        
        if sel_cli_id:
            cli_row = clientes_opts[clientes_opts['id']==sel_cli_id].iloc[0]
            st.divider()
            
            col_r1, col_r2 = st.columns(2)
            
            # Relatório 1: Saldo Fator (Via tabela carteira)
            with col_r1:
                st.info("💰 Histórico de Saldo (Fator Conferi)")
                conn = get_conn()
                try:
                    # Busca ID da carteira Fator vinculado a este cliente admin
                    q_cart = f"SELECT id FROM conexoes.fator_cliente_carteira WHERE id_cliente_admin = {sel_cli_id}"
                    df_cart = pd.read_sql(q_cart, conn)
                    if not df_cart.empty:
                        id_cart = df_cart.iloc[0]['id']
                        q_ext = f"SELECT data_transacao, tipo, valor, saldo_novo FROM conexoes.fator_cliente_transacoes WHERE id_carteira = {id_cart} ORDER BY id DESC LIMIT 20"
                        df_ext = pd.read_sql(q_ext, conn)
                        st.dataframe(df_ext, hide_index=True)
                    else:
                        st.warning("Este cliente não tem carteira Fator ativa.")
                except: st.error("Erro ao buscar dados.")
                finally: conn.close()

            # Relatório 2: Consultas Realizadas
            with col_r2:
                st.info("🔎 Últimas Consultas (CPF)")
                # Pop-up simulado
                if st.button("Abrir Histórico Detalhado"):
                    dialog_historico_consultas(cli_row['cpf'])

# --- DIALOGS AUXILIARES ---

@st.dialog("👤 Criar Usuário Rápido")
def dialog_criar_usuario_rapido(dados_cliente):
    st.write(f"Criando acesso para: **{dados_cliente['nome']}**")
    
    with st.form("form_create_user"):
        u_email = st.text_input("Login (Email)", value=dados_cliente['email'])
        u_senha = st.text_input("Senha Inicial", value="1234")
        u_cpf = st.text_input("CPF", value=dados_cliente['cpf'])
        
        if st.form_submit_button("Confirmar Criação"):
            if salvar_usuario(dados_cliente['nome'], u_email, u_cpf, dados_cliente['telefone'], u_senha, 'Cliente', True):
                st.success("Usuário criado!")
                time.sleep(1); st.rerun()
            else:
                st.error("Erro ao criar (Email já existe?).")

@st.dialog("🔎 Histórico de Consultas", width="large")
def dialog_historico_consultas(cpf_cliente):
    st.markdown("###### Últimas 200 Consultas")
    # Aqui assumimos que o log de consultas tem o CPF do USUÁRIO QUE CONSULTOU ou algum vinculo. 
    # Como o log atual grava ID_USUARIO, precisamos cruzar.
    # Mas para simplificar, vamos filtrar onde o cpf_consultado foi feito por um usuário com o CPF do cliente.
    
    # Busca ID do usuario pelo CPF do cliente
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT id FROM clientes_usuarios WHERE cpf = '{cpf_cliente}'")
        res = cur.fetchone()
        
        if res:
            id_user = res[0]
            query = f"""
                SELECT data_hora, tipo_consulta, cpf_consultado, valor_pago 
                FROM conexoes.fatorconferi_registo_consulta 
                WHERE id_usuario = {id_user}
                ORDER BY id DESC LIMIT 200
            """
            df = pd.read_sql(query, conn)
            
            # Ajuste visual (letra menor)
            st.markdown("""<style>.small-font {font-size:12px !important;}</style>""", unsafe_allow_html=True)
            st.dataframe(df, hide_index=True)
        else:
            st.warning("Nenhum usuário de sistema vinculado a este CPF de cliente.")
    except: pass
    finally: conn.close()

if __name__ == "__main__":
    app_clientes()