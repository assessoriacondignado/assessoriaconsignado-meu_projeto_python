import streamlit as st
import pandas as pd
import psycopg2
try: import conexao
except: pass

def get_conn():
    return psycopg2.connect(host=conexao.host, port=conexao.port, database=conexao.database, user=conexao.user, password=conexao.password)

# --- FUNÇÕES DE PERMISSÃO ---
def salvar_perms(id_user, mods):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM permissoes WHERE id_usuario = %s", (id_user,))
    for m in mods:
        cur.execute("INSERT INTO permissoes (id_usuario, modulo, acesso) VALUES (%s, %s, TRUE)", (id_user, m))
    conn.commit()
    conn.close()

def ler_perms(id_user):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT modulo FROM permissoes WHERE id_usuario = %s", (id_user,))
    res = [r[0] for r in cur.fetchall()]
    conn.close()
    return res

# --- TELA DE LOGS ---
def app_logs():
    st.markdown("### 📋 Logs de Acesso (30 Dias)")
    conn = get_conn()
    df = pd.read_sql("SELECT nome_usuario, ip_acesso, to_char(data_hora, 'DD/MM/YYYY HH24:MI') as data, local_acesso FROM logs_acesso ORDER BY id DESC LIMIT 100", conn)
    conn.close()
    st.dataframe(df, use_container_width=True)

# --- TELA DE USUÁRIOS ---
def app_usuarios():
    # --- CSS IDENTICO AO DE CLIENTES ---
    st.markdown("""
    <style>
        div[data-testid="stVerticalBlock"] > div[style*="flex-direction: column;"] > div[data-testid="stVerticalBlock"] {
            gap: 0.1rem;
        }
        .stButton button {
            padding: 0px 10px;
            height: 28px;
            line-height: 28px; 
        }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("## 🔐 Gestão de Usuários e Permissões")

    # Estados
    if 'modo_user' not in st.session_state: st.session_state['modo_user'] = None
    if 'id_user' not in st.session_state: st.session_state['id_user'] = None

    # Abas internas
    tab1, tab2 = st.tabs(["Usuários do Sistema", "Logs de Acesso"])

    with tab1:
        # TELA DE PERMISSÕES/SENHA (FORMULÁRIO)
        if st.session_state['modo_user'] == 'editar_acesso':
            st.divider()
            st.markdown(f"### Configurar Acesso: {st.session_state.get('nome_user', '')}")
            
            conn = get_conn()
            df = pd.read_sql(f"SELECT * FROM clientes_usuarios WHERE id = {st.session_state['id_user']}", conn)
            conn.close()
            d = df.iloc[0] if not df.empty else {}
            perms = ler_perms(st.session_state['id_user'])

            with st.form("form_acesso"):
                c1, c2, c3 = st.columns(3)
                login = c1.text_input("Login (Email/User)", value=d.get('email', ''))
                senha = c2.text_input("Senha de Acesso", value=d.get('senha', ''), type="password")
                cargo = c3.selectbox("Hierarquia", ["Cliente", "Gerente", "Admin"], index=["Cliente", "Gerente", "Admin"].index(d.get('hierarquia', 'Cliente')))
                
                st.markdown("#### Permissões de Módulos")
                cp1, cp2 = st.columns(2)
                p_com = cp1.checkbox("Acesso COMERCIAL", value="COMERCIAL" in perms)
                p_fin = cp2.checkbox("Acesso FINANCEIRO", value="FINANCEIRO" in perms)

                ativo = st.checkbox("Usuário Ativo?", value=d.get('ativo', True))

                c_b1, c_b2 = st.columns([1, 6])
                if c_b1.form_submit_button("💾 Salvar"):
                    conn = get_conn()
                    cur = conn.cursor()
                    # Atualiza Login/Senha
                    cur.execute("UPDATE clientes_usuarios SET email=%s, senha=%s, hierarquia=%s, ativo=%s WHERE id=%s", 
                                (login, senha, cargo, ativo, st.session_state['id_user']))
                    conn.commit()
                    conn.close()
                    
                    # Atualiza Permissões
                    novas = []
                    if p_com: novas.append("COMERCIAL")
                    if p_fin: novas.append("FINANCEIRO")
                    salvar_perms(st.session_state['id_user'], novas)
                    
                    st.success("Acesso atualizado!")
                    st.session_state['modo_user'] = None
                    st.rerun()
                
                if c_b2.form_submit_button("Cancelar"):
                    st.session_state['modo_user'] = None
                    st.rerun()

        # --- LISTAGEM (NOVO LAYOUT DE TABELA) ---
        st.markdown("<br>", unsafe_allow_html=True)
        conn = get_conn()
        df = pd.read_sql("SELECT id, nome, email, hierarquia, ativo FROM clientes_usuarios ORDER BY id DESC", conn)
        conn.close()

        if not df.empty:
            # Cabeçalho da Tabela
            with st.container():
                st.markdown(
                    """
                    <div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px 5px 0 0; border: 1px solid #ddd; display: flex; font-weight: bold;">
                        <div style="flex: 3;">Nome</div>
                        <div style="flex: 2;">Login</div>
                        <div style="flex: 1.5;">Cargo</div>
                        <div style="flex: 1;">Ativo</div>
                        <div style="flex: 1.5;">Ações</div>
                    </div>
                    """, 
                    unsafe_allow_html=True
                )

            # Linhas da Tabela
            for i, row in df.iterrows():
                with st.container():
                    c = st.columns([3, 2, 1.5, 1, 1.5])
                    
                    # Dados com padding para alinhar
                    c[0].markdown(f"<div style='padding-top: 5px;'>{row['nome']}</div>", unsafe_allow_html=True)
                    c[1].markdown(f"<div style='padding-top: 5px;'>{row['email']}</div>", unsafe_allow_html=True)
                    c[2].markdown(f"<div style='padding-top: 5px;'>{row['hierarquia']}</div>", unsafe_allow_html=True)
                    
                    # Ícone de Ativo
                    status_icon = "✅" if row['ativo'] else "❌"
                    c[3].markdown(f"<div style='padding-top: 5px;'>{status_icon}</div>", unsafe_allow_html=True)
                    
                    # Botão de Ação
                    if c[4].button("🔓 Configurar", key=f"user_{row['id']}"):
                        st.session_state['modo_user'] = 'editar_acesso'
                        st.session_state['id_user'] = row['id']
                        st.session_state['nome_user'] = row['nome']
                        st.rerun()
                    
                    # Linha divisória fina
                    st.markdown("<div style='border-bottom: 1px solid #e0e0e0; margin-bottom: 2px;'></div>", unsafe_allow_html=True)
        else:
            st.info("Nenhum usuário encontrado.")

    with tab2:
        app_logs()