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

def limpar_formatacao_texto(texto):
    """Remove caracteres markdown como ** e espaços extras"""
    if not texto: return ""
    return str(texto).replace('*', '').strip()

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

def buscar_usuarios_disponiveis():
    """Busca usuários que ainda não estão vinculados a nenhum cliente"""
    conn = get_conn()
    try:
        # Busca usuários que NÃO estão na coluna id_usuario_vinculo da tabela clientes
        query = """
            SELECT id, nome, email, cpf 
            FROM clientes_usuarios 
            WHERE id NOT IN (SELECT id_usuario_vinculo FROM admin.clientes WHERE id_usuario_vinculo IS NOT NULL)
            ORDER BY nome
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except:
        conn.close()
        return pd.DataFrame()

def vincular_usuario_cliente(id_cliente, id_usuario):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = %s WHERE id = %s", (id_usuario, id_cliente))
        conn.commit(); conn.close()
        return True
    except:
        conn.close(); return False

def desvincular_usuario_cliente(id_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = NULL WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close()
        return True
    except:
        conn.close(); return False

def excluir_cliente_db(id_cliente):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM admin.clientes WHERE id = %s", (id_cliente,))
        conn.commit(); conn.close()
        return True
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
        
        novo_id = cur.fetchone()[0]
        conn.commit(); conn.close()
        return novo_id
    except Exception as e:
        conn.close(); return None

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
                st.success("Atualizado!")
                time.sleep(0.5)
                st.rerun()
            else:
                st.error("Erro ao atualizar.")

@st.dialog("🔗 Gestão de Acesso do Cliente")
def dialog_gestao_usuario_vinculo(dados_cliente):
    # Verifica se já tem vinculo
    id_vinculo = dados_cliente.get('id_vinculo') or dados_cliente.get('id_usuario_vinculo')
    
    if id_vinculo:
        # Cenario 1.2: Já vinculado
        st.success("✅ Este cliente já possui um usuário vinculado.")
        
        # Busca dados do usuário vinculado
        conn = get_conn()
        df_u = pd.read_sql(f"SELECT nome, email, telefone, cpf FROM clientes_usuarios WHERE id = {id_vinculo}", conn)
        conn.close()
        
        if not df_u.empty:
            usr = df_u.iloc[0]
            st.write(f"**Nome:** {usr['nome']}")
            st.write(f"**Login:** {usr['email']}")
            st.write(f"**CPF:** {usr['cpf']}")
            
            st.markdown("---")
            if st.button("🔓 Desvincular Usuário", type="primary"):
                if desvincular_usuario_cliente(dados_cliente['id']):
                    st.success("Usuário desvinculado! O cadastro de login permanece ativo, mas solto.")
                    time.sleep(1.5); st.rerun()
                else:
                    st.error("Erro ao desvincular.")
        else:
            st.warning("Usuário vinculado não encontrado no banco (Id inválido).")
            if st.button("Forçar Desvinculo"):
                desvincular_usuario_cliente(dados_cliente['id']); st.rerun()

    else:
        # Cenario 1.1: Não possui usuário
        st.warning("⚠️ Este cliente não tem acesso ao sistema.")
        
        tab_novo, tab_existente = st.tabs(["✨ Criar Novo", "🔍 Vincular Existente"])
        
        with tab_novo:
            st.caption("Cria um novo login baseado nos dados do cliente.")
            with st.form("form_cria_vincula"):
                u_email = st.text_input("Login (Email)", value=dados_cliente['email'])
                u_senha = st.text_input("Senha Inicial", value="1234")
                u_cpf = st.text_input("CPF", value=dados_cliente['cpf'])
                u_nome = st.text_input("Nome", value=limpar_formatacao_texto(dados_cliente['nome']))
                
                if st.form_submit_button("Criar e Vincular"):
                    novo_id = salvar_usuario_novo(u_nome, u_email, u_cpf, dados_cliente['telefone'], u_senha, 'Cliente', True)
                    if novo_id:
                        vincular_usuario_cliente(dados_cliente['id'], novo_id)
                        st.success("Usuário criado e vinculado!")
                        time.sleep(1); st.rerun()
                    else:
                        st.error("Erro ao criar (Email já existe?).")

        with tab_existente:
            st.caption("Selecione um usuário que já existe mas não está vinculado a ninguém.")
            df_livres = buscar_usuarios_disponiveis()
            
            if not df_livres.empty:
                opcoes = df_livres.apply(lambda x: f"{x['nome']} ({x['email']})", axis=1)
                idx_sel = st.selectbox("Selecione o Usuário", range(len(df_livres)), format_func=lambda x: opcoes[x])
                
                if st.button("Vincular Selecionado"):
                    id_user_sel = df_livres.iloc[idx_sel]['id']
                    if vincular_usuario_cliente(dados_cliente['id'], id_user_sel):
                        st.success("Vinculado com sucesso!")
                        time.sleep(1); st.rerun()
                    else:
                        st.error("Erro ao vincular.")
            else:
                st.info("Não há usuários livres disponíveis.")

@st.dialog("🚨 Excluir Cliente")
def dialog_excluir_cliente(id_cli, nome):
    st.error(f"Tem certeza que deseja excluir o cliente **{nome}**?")
    st.warning("Isso não apaga o usuário de login vinculado, apenas a ficha cadastral.")
    
    c1, c2 = st.columns(2)
    if c1.button("Sim, Excluir"):
        if excluir_cliente_db(id_cli):
            st.success("Cliente removido."); time.sleep(1)
            st.session_state['view_cliente'] = 'lista'
            st.rerun()
    if c2.button("Cancelar"):
        st.rerun()

@st.dialog("🔎 Histórico de Consultas", width="large")
def dialog_historico_consultas(cpf_cliente):
    st.markdown("###### Últimas 200 Consultas")
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
            st.dataframe(df, hide_index=True)
        else:
            st.warning("Nenhum usuário de sistema vinculado a este CPF de cliente.")
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
        if c2.button("➕ Novo", type="primary"):
            st.session_state['view_cliente'] = 'novo'
            st.rerun()

        # VIEW: LISTA DE CLIENTES
        if st.session_state.get('view_cliente', 'lista') == 'lista':
            conn = get_conn()
            # Traz também o vinculo e status
            sql = "SELECT *, id_usuario_vinculo as id_vinculo FROM admin.clientes"
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
                    <div style="flex: 1; text-align: center;">Status</div>
                    <div style="flex: 1; text-align: center;">Ações</div>
                </div>
                """, unsafe_allow_html=True)

                for _, row in df_cli.iterrows():
                    with st.container():
                        c1, c2, c3, c4, c5 = st.columns([2, 1, 2, 1, 1])
                        
                        # Limpeza visual (remove **)
                        nome_limpo = limpar_formatacao_texto(row['nome'])
                        
                        c1.write(f"**{nome_limpo}**")
                        c2.write(row['cpf'] or "-")
                        c3.write(row['nome_empresa'] or "-")
                        
                        status = row.get('status', 'ATIVO')
                        cor_st = "green" if status == "ATIVO" else "red"
                        c4.markdown(f":{cor_st}[{status}]")
                        
                        with c5:
                            b1, b2 = st.columns(2)
                            if b1.button("👁️", key=f"ver_{row['id']}", help="Ver/Editar"):
                                st.session_state['view_cliente'] = 'editar'
                                st.session_state['cli_id'] = row['id']
                                st.rerun()
                            
                            # Botão de Usuário (Muda ícone se já tiver vinculo)
                            icon_user = "🔗" if row['id_vinculo'] else "👤"
                            if b2.button(icon_user, key=f"usr_{row['id']}", help="Gerenciar Usuário"):
                                dialog_gestao_usuario_vinculo(row)
                        
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
                # Limpa asteriscos ao carregar para edição
                nome_val = limpar_formatacao_texto(dados.get('nome', ''))
                nome = c1.text_input("Nome Completo *", value=nome_val)
                nome_emp = c2.text_input("Nome Empresa", value=dados.get('nome_empresa', ''))
                cnpj_emp = c3.text_input("CNPJ Empresa", value=dados.get('cnpj_empresa', ''))

                c4, c5, c6, c7 = st.columns(4)
                email = c4.text_input("E-mail *", value=dados.get('email', ''))
                cpf = c5.text_input("CPF *", value=dados.get('cpf', ''))
                tel1 = c6.text_input("Telefone 1", value=dados.get('telefone', ''))
                tel2 = c7.text_input("Telefone 2", value=dados.get('telefone2', ''))

                c8, c9, c10 = st.columns([1, 1, 1])
                id_gp = c8.text_input("ID Grupo WhatsApp", value=dados.get('id_grupo_whats', ''))
                agr_cli = c9.text_input("Agrupamento Cliente (IDs ex: 1;2)", value=dados.get('ids_agrupamento_cliente', ''))
                agr_emp = c10.text_input("Agrupamento Empresa (IDs ex: 1;2)", value=dados.get('ids_agrupamento_empresa', ''))

                # --- Botões de Controle na Edição ---
                status_atual = dados.get('status', 'ATIVO')
                if st.session_state['view_cliente'] == 'editar':
                    st.divider()
                    cs1, cs2 = st.columns([1, 4])
                    novo_status = cs1.selectbox("Status do Cliente", ["ATIVO", "INATIVO"], index=0 if status_atual=="ATIVO" else 1)
                else:
                    novo_status = "ATIVO"

                st.markdown("<br>", unsafe_allow_html=True)
                
                # Layout de Botões Salvar e Cancelar
                col_actions = st.columns([1, 1, 4])
                submit = col_actions[0].form_submit_button("💾 Salvar")
                cancel = col_actions[1].form_submit_button("Cancelar")
                
                if submit:
                    conn = get_conn(); cur = conn.cursor()
                    cnpj_limpo = formatar_cnpj(cnpj_emp)
                    
                    if st.session_state['view_cliente'] == 'novo':
                        sql = """INSERT INTO admin.clientes (nome, nome_empresa, cnpj_empresa, email, cpf, telefone, telefone2, id_grupo_whats, ids_agrupamento_cliente, ids_agrupamento_empresa, status) 
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ATIVO')"""
                        cur.execute(sql, (nome, nome_emp, cnpj_limpo, email, cpf, tel1, tel2, id_gp, agr_cli, agr_emp))
                    else:
                        sql = """UPDATE admin.clientes SET nome=%s, nome_empresa=%s, cnpj_empresa=%s, email=%s, cpf=%s, telefone=%s, telefone2=%s, id_grupo_whats=%s, ids_agrupamento_cliente=%s, ids_agrupamento_empresa=%s, status=%s
                                 WHERE id=%s"""
                        cur.execute(sql, (nome, nome_emp, cnpj_limpo, email, cpf, tel1, tel2, id_gp, agr_cli, agr_emp, novo_status, st.session_state['cli_id']))
                    
                    conn.commit(); conn.close()
                    st.success("Salvo!"); time.sleep(1)
                    st.session_state['view_cliente'] = 'lista'
                    st.rerun()

                if cancel:
                    st.session_state['view_cliente'] = 'lista'
                    st.rerun()

            # Botão de Excluir fora do Form (para não submeter)
            if st.session_state['view_cliente'] == 'editar':
                st.markdown("---")
                if st.button("🗑️ Excluir Cliente", type="primary"):
                    dialog_excluir_cliente(st.session_state['cli_id'], nome_val)

    with tab_user:
        st.markdown("### Gestão de Acesso")
        busca_user = st.text_input("Buscar Usuário", placeholder="Nome ou Email")
        conn = get_conn()
        sql_u = "SELECT id, nome, email, hierarquia, ativo FROM clientes_usuarios WHERE 1=1"
        if busca_user: sql_u += f" AND (nome ILIKE '%{busca_user}%' OR email ILIKE '%{busca_user}%')"
        sql_u += " ORDER BY id DESC"
        df_users = pd.read_sql(sql_u, conn); conn.close()
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
                        conn.commit(); conn.close(); st.success("Atualizado!"); st.rerun()

    with tab_agrup:
        c_ag1, c_ag2 = st.columns(2)
        
        # --- COLUNA 1: AGRUPAMENTO CLIENTES ---
        with c_ag1:
            # Caixa Recolhível (Expander)
            with st.expander("🏷️ Agrupamento Clientes", expanded=True):
                # Área de Adicionar
                with st.container(border=True):
                    st.caption("Novo Agrupamento Cliente")
                    c_in, c_bt = st.columns([3, 1])
                    novo_ac = c_in.text_input("Nome", key="in_ac", label_visibility="collapsed")
                    if c_bt.button("Adicionar", key="add_ac", use_container_width=True):
                        if novo_ac: salvar_agrupamento("cliente", novo_ac); st.rerun()
                
                st.divider()
                
                # Lista de Itens
                df_ac = listar_agrupamentos("cliente")
                if not df_ac.empty:
                    for _, r in df_ac.iterrows():
                        # Layout: ID - Nome | Editar | Excluir
                        ca1, ca2, ca3 = st.columns([6, 1, 1])
                        
                        ca1.markdown(f"**{r['id']}** - {r['nome_agrupamento']}")
                        
                        # Botão Editar
                        if ca2.button("✏️", key=f"ed_ac_{r['id']}", help="Editar"):
                            dialog_editar_agrupamento("cliente", r['id'], r['nome_agrupamento'])
                        
                        # Botão Excluir
                        if ca3.button("🗑️", key=f"del_ac_{r['id']}", help="Excluir"):
                            excluir_agrupamento("cliente", r['id']); st.rerun()
                        
                        st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
                else:
                    st.info("Nenhum agrupamento cadastrado.")

        # --- COLUNA 2: AGRUPAMENTO EMPRESAS ---
        with c_ag2:
            # Caixa Recolhível (Expander)
            with st.expander("🏢 Agrupamento Empresas", expanded=True):
                # Área de Adicionar
                with st.container(border=True):
                    st.caption("Novo Agrupamento Empresa")
                    c_in, c_bt = st.columns([3, 1])
                    novo_ae = c_in.text_input("Nome", key="in_ae", label_visibility="collapsed")
                    if c_bt.button("Adicionar", key="add_ae", use_container_width=True):
                        if novo_ae: salvar_agrupamento("empresa", novo_ae); st.rerun()
                
                st.divider()
                
                # Lista de Itens
                df_ae = listar_agrupamentos("empresa")
                if not df_ae.empty:
                    for _, r in df_ae.iterrows():
                        # Layout: ID - Nome | Editar | Excluir
                        ca1, ca2, ca3 = st.columns([6, 1, 1])
                        
                        ca1.markdown(f"**{r['id']}** - {r['nome_agrupamento']}")
                        
                        # Botão Editar
                        if ca2.button("✏️", key=f"ed_ae_{r['id']}", help="Editar"):
                            dialog_editar_agrupamento("empresa", r['id'], r['nome_agrupamento'])
                            
                        # Botão Excluir
                        if ca3.button("🗑️", key=f"del_ae_{r['id']}", help="Excluir"):
                            excluir_agrupamento("empresa", r['id']); st.rerun()
                            
                        st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
                else:
                    st.info("Nenhum agrupamento cadastrado.")

    with tab_rel:
        st.markdown("### 📊 Relatórios Integrados")
        conn = get_conn()
        clientes_opts = pd.read_sql("SELECT id, nome, cpf FROM admin.clientes ORDER BY nome", conn)
        conn.close()
        sel_cli_id = st.selectbox("Selecione o Cliente", options=clientes_opts['id'], format_func=lambda x: clientes_opts[clientes_opts['id']==x]['nome'].values[0])
        if sel_cli_id:
            cli_row = clientes_opts[clientes_opts['id']==sel_cli_id].iloc[0]
            st.divider()
            col_r1, col_r2 = st.columns(2)
            with col_r1:
                st.info("💰 Histórico de Saldo (Fator Conferi)")
                conn = get_conn()
                try:
                    q_cart = f"SELECT id FROM conexoes.fator_cliente_carteira WHERE id_cliente_admin = {sel_cli_id}"
                    df_cart = pd.read_sql(q_cart, conn)
                    if not df_cart.empty:
                        id_cart = df_cart.iloc[0]['id']
                        q_ext = f"SELECT data_transacao, tipo, valor, saldo_novo FROM conexoes.fator_cliente_transacoes WHERE id_carteira = {id_cart} ORDER BY id DESC LIMIT 20"
                        df_ext = pd.read_sql(q_ext, conn)
                        st.dataframe(df_ext, hide_index=True)
                    else: st.warning("Este cliente não tem carteira Fator ativa.")
                except: st.error("Erro ao buscar dados.")
                finally: conn.close()
            with col_r2:
                st.info("🔎 Últimas Consultas (CPF)")
                if st.button("Abrir Histórico Detalhado"): dialog_historico_consultas(cli_row['cpf'])

if __name__ == "__main__":
    app_clientes()