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
# 1. FUNÇÕES AUXILIARES E DB (GERAL)
# =============================================================================

def formatar_cnpj(v):
    v = re.sub(r'\D', '', str(v))
    return f"{v[:2]}.{v[2:5]}.{v[5:8]}/{v[8:12]}-{v[12:]}" if len(v) == 14 else v

def limpar_formatacao_texto(texto):
    """Remove caracteres markdown como ** e espaços extras"""
    if not texto: return ""
    return str(texto).replace('*', '').strip()

# --- AGRUPAMENTOS (CLIENTE/EMPRESA) ---

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

def atualizar_agrupamento(tipo, id_agrup, novo_nome):
    conn = get_conn()
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {tabela} SET nome_agrupamento = %s WHERE id = %s", (novo_nome, id_agrup))
        conn.commit(); conn.close()
        return True
    except: 
        conn.close(); return False

# --- CLIENTE CNPJ (NOVO) ---

def listar_cliente_cnpj():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, cnpj, nome_empresa FROM admin.cliente_cnpj ORDER BY nome_empresa", conn)
        conn.close()
        return df
    except: 
        conn.close(); return pd.DataFrame()

def salvar_cliente_cnpj(cnpj, nome):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO admin.cliente_cnpj (cnpj, nome_empresa) VALUES (%s, %s)", (cnpj, nome))
        conn.commit(); conn.close()
        return True
    except: 
        conn.close(); return False

def excluir_cliente_cnpj(id_reg):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM admin.cliente_cnpj WHERE id = %s", (id_reg,))
        conn.commit(); conn.close()
        return True
    except: 
        conn.close(); return False

def atualizar_cliente_cnpj(id_reg, cnpj, nome):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.cliente_cnpj SET cnpj=%s, nome_empresa=%s WHERE id=%s", (cnpj, nome, id_reg))
        conn.commit(); conn.close()
        return True
    except: 
        conn.close(); return False

# --- USUÁRIOS E CLIENTES (VINCULOS) ---

def buscar_usuarios_disponiveis():
    conn = get_conn()
    try:
        query = """
            SELECT id, nome, email, cpf 
            FROM clientes_usuarios 
            WHERE id NOT IN (SELECT id_usuario_vinculo FROM admin.clientes WHERE id_usuario_vinculo IS NOT NULL)
            ORDER BY nome
        """
        df = pd.read_sql(query, conn); conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def vincular_usuario_cliente(id_cliente, id_usuario):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = %s WHERE id = %s", (id_usuario, id_cliente))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def desvincular_usuario_cliente(id_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = NULL WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_cliente_db(id_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM admin.clientes WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

# =============================================================================
# 2. FUNÇÕES DE USUÁRIO (MIGRADO E ADAPTADO)
# =============================================================================

def hash_senha(senha):
    if senha.startswith('$2b$'): return senha
    return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def salvar_usuario_novo(nome, email, cpf, tel, senha, hierarquia, ativo):
    conn = get_conn()
    try:
        cur = conn.cursor()
        senha_final = hash_senha(senha)
        cur.execute("""
            INSERT INTO clientes_usuarios (nome, email, cpf, telefone, senha, hierarquia, ativo)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (nome, email, cpf, tel, senha_final, hierarquia, ativo))
        novo_id = cur.fetchone()[0]; conn.commit(); conn.close(); return novo_id
    except Exception as e: conn.close(); return None

# =============================================================================
# 3. DIALOGS (POP-UPS)
# =============================================================================

@st.dialog("✏️ Editar Agrupamento")
def dialog_editar_agrupamento(tipo, id_agrup, nome_atual):
    st.write(f"Editando: **{nome_atual}**")
    novo_nome = st.text_input("Novo Nome", value=nome_atual)
    if st.button("Salvar Alteração"):
        if novo_nome:
            if atualizar_agrupamento(tipo, id_agrup, novo_nome):
                st.success("Atualizado!"); time.sleep(0.5); st.rerun()
            else: st.error("Erro ao atualizar.")

@st.dialog("✏️ Editar Empresa/CNPJ")
def dialog_editar_cliente_cnpj(id_reg, cnpj_atual, nome_atual):
    st.write(f"Editando ID: {id_reg}")
    novo_cnpj = st.text_input("CNPJ", value=cnpj_atual)
    novo_nome = st.text_input("Nome Empresa", value=nome_atual)
    if st.button("Salvar Alteração"):
        if atualizar_cliente_cnpj(id_reg, novo_cnpj, novo_nome):
            st.success("Atualizado!"); time.sleep(0.5); st.rerun()
        else: st.error("Erro ao atualizar.")

@st.dialog("🔗 Gestão de Acesso do Cliente")
def dialog_gestao_usuario_vinculo(dados_cliente):
    id_vinculo = dados_cliente.get('id_vinculo') or dados_cliente.get('id_usuario_vinculo')
    if id_vinculo:
        st.success("✅ Este cliente já possui um usuário vinculado.")
        conn = get_conn()
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
                    novo_id = salvar_usuario_novo(u_nome, u_email, u_cpf, dados_cliente['telefone'], u_senha, 'Cliente', True)
                    if novo_id: vincular_usuario_cliente(dados_cliente['id'], novo_id); st.success("Criado e vinculado!"); time.sleep(1); st.rerun()
                    else: st.error("Erro ao criar.")
        with tab_existente:
            df_livres = buscar_usuarios_disponiveis()
            if not df_livres.empty:
                opcoes = df_livres.apply(lambda x: f"{x['nome']} ({x['email']})", axis=1)
                idx_sel = st.selectbox("Selecione o Usuário", range(len(df_livres)), format_func=lambda x: opcoes[x])
                if st.button("Vincular Selecionado"):
                    if vincular_usuario_cliente(dados_cliente['id'], df_livres.iloc[idx_sel]['id']): st.success("Vinculado!"); time.sleep(1); st.rerun()
                    else: st.error("Erro.")
            else: st.info("Sem usuários livres.")

@st.dialog("🚨 Excluir Cliente")
def dialog_excluir_cliente(id_cli, nome):
    st.error(f"Excluir **{nome}**?"); st.warning("Apenas a ficha cadastral será apagada.")
    c1, c2 = st.columns(2)
    if c1.button("Sim, Excluir"):
        if excluir_cliente_db(id_cli): st.success("Removido."); time.sleep(1); st.session_state['view_cliente'] = 'lista'; st.rerun()
    if c2.button("Cancelar"): st.rerun()

@st.dialog("🔎 Histórico de Consultas", width="large")
def dialog_historico_consultas(cpf_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor(); cur.execute(f"SELECT id FROM clientes_usuarios WHERE cpf = '{cpf_cliente}'"); res = cur.fetchone()
        if res:
            df = pd.read_sql(f"SELECT data_hora, tipo_consulta, cpf_consultado, valor_pago FROM conexoes.fatorconferi_registo_consulta WHERE id_usuario = {res[0]} ORDER BY id DESC LIMIT 200", conn)
            st.dataframe(df, hide_index=True)
        else: st.warning("Nenhum usuário vinculado.")
    except: pass
    finally: conn.close()

# =============================================================================
# 4. INTERFACE PRINCIPAL
# =============================================================================

def app_clientes():
    st.markdown("## 👥 Central de Clientes e Usuários")
    
    tab_cli, tab_user, tab_agrup, tab_rel = st.tabs(["🏢 Clientes", "👤 Usuários", "🏷️ Agrupamentos", "📊 Relatórios"])

    # --- ABA CLIENTES ---
    with tab_cli:
        c1, c2 = st.columns([6, 1])
        filtro = c1.text_input("🔍 Buscar Cliente", placeholder="Nome, CPF ou Nome Empresa")
        if c2.button("➕ Novo", type="primary"): st.session_state['view_cliente'] = 'novo'; st.rerun()

        if st.session_state.get('view_cliente', 'lista') == 'lista':
            conn = get_conn()
            sql = "SELECT *, id_usuario_vinculo as id_vinculo FROM admin.clientes"
            if filtro: sql += f" WHERE nome ILIKE '%%{filtro}%%' OR cpf ILIKE '%%{filtro}%%' OR nome_empresa ILIKE '%%{filtro}%%'"
            sql += " ORDER BY id DESC LIMIT 50"
            df_cli = pd.read_sql(sql, conn); conn.close()

            if not df_cli.empty:
                st.markdown("""<div style="display: flex; font-weight: bold; background: #f0f2f6; padding: 10px; border-radius: 5px;"><div style="flex: 2;">Nome</div><div style="flex: 1;">CPF</div><div style="flex: 2;">Empresa</div><div style="flex: 1; text-align: center;">Status</div><div style="flex: 1; text-align: center;">Ações</div></div>""", unsafe_allow_html=True)
                for _, row in df_cli.iterrows():
                    with st.container():
                        c1, c2, c3, c4, c5 = st.columns([2, 1, 2, 1, 1])
                        c1.write(f"**{limpar_formatacao_texto(row['nome'])}**"); c2.write(row['cpf'] or "-"); c3.write(row['nome_empresa'] or "-")
                        c4.markdown(f":{'green' if row.get('status','ATIVO')=='ATIVO' else 'red'}[{row.get('status','ATIVO')}]")
                        with c5:
                            b1, b2 = st.columns(2)
                            if b1.button("👁️", key=f"v_{row['id']}"): st.session_state.update({'view_cliente': 'editar', 'cli_id': row['id']}); st.rerun()
                            if b2.button("🔗" if row['id_vinculo'] else "👤", key=f"u_{row['id']}"): dialog_gestao_usuario_vinculo(row)
                        st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Nenhum cliente encontrado.")

        elif st.session_state['view_cliente'] in ['novo', 'editar']:
            st.markdown(f"### {'📝 Novo' if st.session_state['view_cliente']=='novo' else '✏️ Editar'}")
            dados = {}
            if st.session_state['view_cliente'] == 'editar':
                conn = get_conn(); df = pd.read_sql(f"SELECT * FROM admin.clientes WHERE id = {st.session_state['cli_id']}", conn); conn.close()
                if not df.empty: dados = df.iloc[0]

            with st.form("form_cliente"):
                c1, c2, c3 = st.columns(3)
                nome = c1.text_input("Nome Completo *", value=limpar_formatacao_texto(dados.get('nome', '')))
                nome_emp = c2.text_input("Nome Empresa", value=dados.get('nome_empresa', ''))
                cnpj_emp = c3.text_input("CNPJ Empresa", value=dados.get('cnpj_empresa', ''))
                c4, c5, c6, c7 = st.columns(4)
                email = c4.text_input("E-mail *", value=dados.get('email', ''))
                cpf = c5.text_input("CPF *", value=dados.get('cpf', ''))
                tel1 = c6.text_input("Telefone 1", value=dados.get('telefone', ''))
                tel2 = c7.text_input("Telefone 2", value=dados.get('telefone2', ''))
                c8, c9, c10 = st.columns([1, 1, 1])
                id_gp = c8.text_input("ID Grupo WhatsApp", value=dados.get('id_grupo_whats', ''))
                agr_cli = c9.text_input("Agrupamento Cliente (IDs)", value=dados.get('ids_agrupamento_cliente', ''))
                agr_emp = c10.text_input("Agrupamento Empresa (IDs)", value=dados.get('ids_agrupamento_empresa', ''))
                
                status_final = "ATIVO"
                if st.session_state['view_cliente'] == 'editar':
                    st.divider(); cs1, _ = st.columns([1, 4])
                    status_final = cs1.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if dados.get('status','ATIVO')=="ATIVO" else 1)

                st.markdown("<br>", unsafe_allow_html=True); ca = st.columns([1, 1, 4])
                if ca[0].form_submit_button("💾 Salvar"):
                    conn = get_conn(); cur = conn.cursor(); cnpj_l = formatar_cnpj(cnpj_emp)
                    if st.session_state['view_cliente'] == 'novo':
                        cur.execute("INSERT INTO admin.clientes (nome, nome_empresa, cnpj_empresa, email, cpf, telefone, telefone2, id_grupo_whats, ids_agrupamento_cliente, ids_agrupamento_empresa, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ATIVO')", (nome, nome_emp, cnpj_l, email, cpf, tel1, tel2, id_gp, agr_cli, agr_emp))
                    else:
                        cur.execute("UPDATE admin.clientes SET nome=%s, nome_empresa=%s, cnpj_empresa=%s, email=%s, cpf=%s, telefone=%s, telefone2=%s, id_grupo_whats=%s, ids_agrupamento_cliente=%s, ids_agrupamento_empresa=%s, status=%s WHERE id=%s", (nome, nome_emp, cnpj_l, email, cpf, tel1, tel2, id_gp, agr_cli, agr_emp, status_final, st.session_state['cli_id']))
                    conn.commit(); conn.close(); st.success("Salvo!"); time.sleep(1); st.session_state['view_cliente'] = 'lista'; st.rerun()
                if ca[1].form_submit_button("Cancelar"): st.session_state['view_cliente'] = 'lista'; st.rerun()

            if st.session_state['view_cliente'] == 'editar':
                st.markdown("---")
                if st.button("🗑️ Excluir Cliente", type="primary"): dialog_excluir_cliente(st.session_state['cli_id'], nome)

    # --- ABA USUÁRIOS ---
    with tab_user:
        st.markdown("### Gestão de Acesso")
        busca_user = st.text_input("Buscar Usuário", placeholder="Nome ou Email")
        conn = get_conn(); sql_u = "SELECT id, nome, email, hierarquia, ativo FROM clientes_usuarios WHERE 1=1"
        if busca_user: sql_u += f" AND (nome ILIKE '%{busca_user}%' OR email ILIKE '%{busca_user}%')"
        sql_u += " ORDER BY id DESC"
        df_users = pd.read_sql(sql_u, conn); conn.close()
        for _, u in df_users.iterrows():
            with st.expander(f"{u['nome']} ({u['hierarquia']})"):
                with st.form(f"form_user_{u['id']}"):
                    c_n, c_e = st.columns(2); n_nome = c_n.text_input("Nome", value=u['nome']); n_mail = c_e.text_input("Email", value=u['email'])
                    c_h, c_s, c_a = st.columns(3); n_hier = c_h.selectbox("Nível", ["Cliente", "Admin", "Gerente"], index=["Cliente", "Admin", "Gerente"].index(u['hierarquia']) if u['hierarquia'] in ["Cliente", "Admin", "Gerente"] else 0); n_senha = c_s.text_input("Nova Senha", type="password"); n_ativo = c_a.checkbox("Ativo", value=u['ativo'])
                    if st.form_submit_button("Atualizar"):
                        conn = get_conn(); cur = conn.cursor()
                        if n_senha: cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, hierarquia=%s, senha=%s, ativo=%s WHERE id=%s", (n_nome, n_mail, n_hier, hash_senha(n_senha), n_ativo, u['id']))
                        else: cur.execute("UPDATE clientes_usuarios SET nome=%s, email=%s, hierarquia=%s, ativo=%s WHERE id=%s", (n_nome, n_mail, n_hier, n_ativo, u['id']))
                        conn.commit(); conn.close(); st.success("Atualizado!"); st.rerun()

    # --- ABA AGRUPAMENTOS (LAYOUT VERTICAL - UM ABAIXO DO OUTRO) ---
    with tab_agrup:
        
        # 1. AGRUPAMENTO CLIENTES
        with st.expander("🏷️ Agrupamento Clientes", expanded=True):
            with st.container(border=True):
                st.caption("Novo Item")
                c_in, c_bt = st.columns([4, 1]) # Ajustei proporção para ficar melhor em largura total
                n_ac = c_in.text_input("Nome", key="in_ac", label_visibility="collapsed")
                if c_bt.button("➕", key="add_ac", use_container_width=True):
                    if n_ac: salvar_agrupamento("cliente", n_ac); st.rerun()
            
            st.divider()
            
            df_ac = listar_agrupamentos("cliente")
            if not df_ac.empty:
                for _, r in df_ac.iterrows():
                    # Ajuste de colunas para largura total: Nome ocupa mais espaço
                    ca1, ca2, ca3 = st.columns([8, 1, 1]) 
                    ca1.markdown(f"**{r['id']}** - {r['nome_agrupamento']}")
                    if ca2.button("✏️", key=f"ed_ac_{r['id']}"): dialog_editar_agrupamento("cliente", r['id'], r['nome_agrupamento'])
                    if ca3.button("🗑️", key=f"del_ac_{r['id']}"): excluir_agrupamento("cliente", r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

        # 2. AGRUPAMENTO EMPRESAS
        with st.expander("🏢 Agrupamento Empresas", expanded=True):
            with st.container(border=True):
                st.caption("Novo Item")
                c_in, c_bt = st.columns([4, 1])
                n_ae = c_in.text_input("Nome", key="in_ae", label_visibility="collapsed")
                if c_bt.button("➕", key="add_ae", use_container_width=True):
                    if n_ae: salvar_agrupamento("empresa", n_ae); st.rerun()
            
            st.divider()
            
            df_ae = listar_agrupamentos("empresa")
            if not df_ae.empty:
                for _, r in df_ae.iterrows():
                    ca1, ca2, ca3 = st.columns([8, 1, 1])
                    ca1.markdown(f"**{r['id']}** - {r['nome_agrupamento']}")
                    if ca2.button("✏️", key=f"ed_ae_{r['id']}"): dialog_editar_agrupamento("empresa", r['id'], r['nome_agrupamento'])
                    if ca3.button("🗑️", key=f"del_ae_{r['id']}"): excluir_agrupamento("empresa", r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

        # 3. CLIENTE CNPJ
        with st.expander("💼 Cliente CNPJ", expanded=True):
            with st.container(border=True):
                st.caption("Novo Cadastro")
                # Ajuste de inputs para ficarem lado a lado na largura total
                c_inp1, c_inp2, c_bt = st.columns([2, 3, 1])
                n_cnpj = c_inp1.text_input("CNPJ", key="n_cnpj", placeholder="00.000.000/0000-00", label_visibility="collapsed")
                n_emp = c_inp2.text_input("Nome Empresa", key="n_emp", placeholder="Razão Social", label_visibility="collapsed")
                if c_bt.button("Adicionar", key="add_cnpj", use_container_width=True):
                    if n_cnpj and n_emp: salvar_cliente_cnpj(n_cnpj, n_emp); st.rerun()
            
            st.divider()
            
            df_cnpj = listar_cliente_cnpj()
            if not df_cnpj.empty:
                for _, r in df_cnpj.iterrows():
                    cc1, cc2, cc3 = st.columns([8, 1, 1])
                    # Exibição melhorada para linha única
                    cc1.markdown(f"**{r['cnpj']}** | {r['nome_empresa']}")
                    if cc2.button("✏️", key=f"ed_cn_{r['id']}"): dialog_editar_cliente_cnpj(r['id'], r['cnpj'], r['nome_empresa'])
                    if cc3.button("🗑️", key=f"del_cn_{r['id']}"): excluir_cliente_cnpj(r['id']); st.rerun()
                    st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
            else: st.info("Vazio.")

    # --- ABA RELATÓRIOS ---
    with tab_rel:
        st.markdown("### 📊 Relatórios")
        conn = get_conn(); opts = pd.read_sql("SELECT id, nome, cpf FROM admin.clientes ORDER BY nome", conn); conn.close()
        sel = st.selectbox("Cliente", opts['id'], format_func=lambda x: opts[opts['id']==x]['nome'].values[0])
        if sel:
            row = opts[opts['id']==sel].iloc[0]; st.divider(); c1, c2 = st.columns(2)
            with c1:
                st.info("💰 Saldo Fator")
                try:
                    conn = get_conn(); id_c = pd.read_sql(f"SELECT id FROM conexoes.fator_cliente_carteira WHERE id_cliente_admin = {sel}", conn).iloc[0]['id']
                    st.dataframe(pd.read_sql(f"SELECT data_transacao, tipo, valor, saldo_novo FROM conexoes.fator_cliente_transacoes WHERE id_carteira = {id_c} ORDER BY id DESC LIMIT 20", conn), hide_index=True)
                    conn.close()
                except: st.warning("Sem carteira.")
            with c2:
                st.info("🔎 Consultas"); 
                if st.button("Ver Histórico"): dialog_historico_consultas(row['cpf'])

if __name__ == "__main__":
    app_clientes()