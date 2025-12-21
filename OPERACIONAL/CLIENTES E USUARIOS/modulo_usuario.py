import streamlit as st
import pandas as pd
import psycopg2
import os
import bcrypt  # Importação adicionada para segurança

# Tentativa robusta de importar a conexão
try: 
    import conexao
except ImportError: 
    st.error("Erro crítico: Arquivo conexao.py não encontrado no servidor.")

def get_conn():
    return psycopg2.connect(
        host=conexao.host, 
        port=conexao.port, 
        database=conexao.database, 
        user=conexao.user, 
        password=conexao.password
    )

def hash_senha(senha):
    if senha.startswith('$2b$'):
        return senha
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(senha.encode('utf-8'), salt).decode('utf-8')

def salvar_perms(id_user, mods):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM permissoes WHERE id_usuario = %s", (id_user,))
        for m in mods:
            cur.execute("INSERT INTO permissoes (id_usuario, modulo, acesso) VALUES (%s, %s, TRUE)", (id_user, m))
        conn.commit(); conn.close()
    except Exception as e: st.error(f"Erro ao salvar permissões: {e}")

def ler_perms(id_user):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT modulo FROM permissoes WHERE id_usuario = %s", (id_user,))
        res = [r[0] for r in cur.fetchall()]
        conn.close()
        return res
    except: return []

def app_logs():
    st.markdown("### 📋 Logs de Acesso (30 Dias)")
    try:
        conn = get_conn()
        query = "SELECT nome_usuario, ip_acesso, to_char(data_hora, 'DD/MM/YYYY HH24:MI') as data, local_acesso FROM logs_acesso ORDER BY id DESC LIMIT 100"
        df = pd.read_sql(query, conn)
        conn.close()
        st.dataframe(df, use_container_width=True)
    except: st.info("Sem registros de logs.")

def app_usuarios():
    st.markdown("""<style>div[data-testid="stVerticalBlock"] > div[style*="flex-direction: column;"] > div[data-testid="stVerticalBlock"] { gap: 0.1rem; } .stButton button { padding: 0px 10px; height: 28px; line-height: 28px; } </style>""", unsafe_allow_html=True)
    st.markdown("## 🔐 Gestão de Usuários e Permissões")
    
    if 'modo_user' not in st.session_state: st.session_state['modo_user'] = None
    tab1, tab2 = st.tabs(["Usuários do Sistema", "Logs de Acesso"])

    with tab1:
        if st.session_state['modo_user'] == 'editar_acesso':
            st.divider()
            st.markdown(f"### Configurar Acesso: {st.session_state.get('nome_user', '')}")
            try:
                conn = get_conn()
                df = pd.read_sql(f"SELECT * FROM clientes_usuarios WHERE id = {st.session_state['id_user']}", conn)
                conn.close()
                d = df.iloc[0] if not df.empty else {}
                perms = ler_perms(st.session_state['id_user'])

                with st.form("form_acesso"):
                    c1, c2, c3 = st.columns(3)
                    login = c1.text_input("Login (Email/User)", value=d.get('email', ''))
                    senha = c2.text_input("Senha de Acesso", value=d.get('senha', ''), type="password")
                    
                    # SINCRONIA: Adicionado 'Grupo' para não sumir dados entre os módulos
                    hierarquia_atual = d.get('hierarquia', 'Cliente')
                    opcoes_h = ["Cliente", "Grupo", "Gerente", "Admin"]
                    idx_h = opcoes_h.index(hierarquia_atual) if hierarquia_atual in opcoes_h else 0
                    cargo = c3.selectbox("Hierarquia", opcoes_h, index=idx_h)
                    
                    st.markdown("#### Permissões de Módulos")
                    cp1, cp2 = st.columns(2)
                    p_com = cp1.checkbox("Acesso COMERCIAL", value="COMERCIAL" in perms)
                    p_fin = cp2.checkbox("Acesso FINANCEIRO", value="FINANCEIRO" in perms)
                    ativo = st.checkbox("Usuário Ativo?", value=d.get('ativo', True))

                    c_b1, c_b2 = st.columns([1, 6])
                    if c_b1.form_submit_button("💾 Salvar"):
                        senha_final = hash_senha(senha)
                        conn = get_conn(); cur = conn.cursor()
                        cur.execute("UPDATE clientes_usuarios SET email=%s, senha=%s, hierarquia=%s, ativo=%s WHERE id=%s", (login, senha_final, cargo, ativo, st.session_state['id_user']))
                        conn.commit(); conn.close()
                        novas = []
                        if p_com: novas.append("COMERCIAL")
                        if p_fin: novas.append("FINANCEIRO")
                        salvar_perms(st.session_state['id_user'], novas)
                        st.success("Acesso atualizado!"); st.session_state['modo_user'] = None; st.rerun()
                    if c_b2.form_submit_button("Cancelar"): st.session_state['modo_user'] = None; st.rerun()
            except Exception as e: st.error(f"Erro: {e}")

        st.markdown("<br>", unsafe_allow_html=True)
        try:
            conn = get_conn()
            df = pd.read_sql("SELECT id, nome, email, hierarquia, ativo FROM clientes_usuarios ORDER BY id DESC", conn)
            conn.close()
            if not df.empty:
                st.markdown("""<div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px 5px 0 0; border: 1px solid #ddd; display: flex; font-weight: bold;"><div style="flex: 3;">Nome</div><div style="flex: 2;">Login</div><div style="flex: 1.5;">Cargo</div><div style="flex: 1;">Ativo</div><div style="flex: 1.5;">Ações</div></div>""", unsafe_allow_html=True)
                for i, row in df.iterrows():
                    with st.container():
                        c = st.columns([3, 2, 1.5, 1, 1.5])
                        c[0].write(row['nome']); c[1].write(row['email']); c[2].write(row['hierarquia'])
                        c[3].write("✅" if row['ativo'] else "❌")
                        if c[4].button("🔓 Configurar", key=f"user_{row['id']}"):
                            st.session_state['modo_user'] = 'editar_acesso'; st.session_state['id_user'] = row['id']; st.session_state['nome_user'] = row['nome']; st.rerun()
                        st.markdown("<div style='border-bottom: 1px solid #e0e0e0; margin-bottom: 2px;'></div>", unsafe_allow_html=True)
        except Exception as e: st.error(f"Erro: {e}")

    with tab2: app_logs()