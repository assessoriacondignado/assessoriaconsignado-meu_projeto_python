import streamlit as st
import pandas as pd
import psycopg2
import os
import bcrypt  
import modulo_wapi  

def get_conn():
    try:
        import conexao
        return psycopg2.connect(host=conexao.host, port=conexao.port, database=conexao.database, user=conexao.user, password=conexao.password)
    except: return None

def hash_senha(senha):
    if senha.startswith('$2b$'): return senha
    return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def salvar_perms(id_user, mods):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM permissoes WHERE id_usuario = %s", (id_user,))
    for m in mods: cur.execute("INSERT INTO permissoes (id_usuario, modulo, acesso) VALUES (%s, %s, TRUE)", (id_user, m))
    conn.commit(); conn.close()

def ler_perms(id_user):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT modulo FROM permissoes WHERE id_usuario = %s", (id_user,))
        res = [r[0] for r in cur.fetchall()]
        conn.close(); return res
    except: return []

def app_usuarios():
    st.markdown("## 🔐 Gestão de Usuários e Permissões")
    if 'modo_user' not in st.session_state: st.session_state['modo_user'] = None
    t1, t2 = st.tabs(["Usuários", "Logs"])

    with t1:
        if st.session_state['modo_user'] == 'editar_acesso':
            conn = get_conn(); df = pd.read_sql(f"SELECT * FROM clientes_usuarios WHERE id = {st.session_state['id_user']}", conn); conn.close()
            d = df.iloc[0] if not df.empty else {}
            perms_atuais = ler_perms(st.session_state['id_user'])

            with st.form("form_acesso"):
                c1, c2, c3 = st.columns(3)
                login = c1.text_input("Login", value=d.get('email', ''))
                senha = c2.text_input("Senha", value=d.get('senha', ''), type="password")
                
                opcoes_h = ["Cliente", "Grupo", "Gerente", "Admin"]
                idx_h = opcoes_h.index(d.get('hierarquia', 'Cliente')) if d.get('hierarquia') in opcoes_h else 0
                cargo = c3.selectbox("Hierarquia", opcoes_h, index=idx_h)
                
                st.markdown("#### Permissões")
                cp1, cp2 = st.columns(2)
                p_com = cp1.checkbox("Acesso COMERCIAL", value="COMERCIAL" in perms_atuais)
                p_fin = cp2.checkbox("Acesso FINANCEIRO", value="FINANCEIRO" in perms_atuais)
                ativo = st.checkbox("Ativo?", value=d.get('ativo', True))

                b1, b2, b3 = st.columns([1, 1.5, 4.5])
                login_limpo = str(login).strip().lower()

                if b1.form_submit_button("💾 Salvar"):
                    senha_f = hash_senha(senha)
                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("UPDATE clientes_usuarios SET email=%s, senha=%s, hierarquia=%s, ativo=%s WHERE id=%s", (login_limpo, senha_f, cargo, ativo, st.session_state['id_user']))
                    conn.commit(); conn.close()
                    novas = []
                    if p_com: novas.append("COMERCIAL")
                    if p_fin: novas.append("FINANCEIRO")
                    salvar_perms(st.session_state['id_user'], novas)
                    st.success("Atualizado!"); st.session_state['modo_user'] = None; st.rerun()

                if b2.form_submit_button("🔐 Reset via WhatsApp"):
                    if d.get('telefone'):
                        conn = get_conn(); cur = conn.cursor()
                        cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
                        inst = cur.fetchone()
                        if inst:
                            senha_f = hash_senha(senha)
                            cur.execute("UPDATE clientes_usuarios SET email=%s, senha=%s, hierarquia=%s, ativo=%s WHERE id=%s", (login_limpo, senha_f, cargo, ativo, st.session_state['id_user']))
                            conn.commit()
                            msg = f"Olá! 🔐 Sua senha foi resetada.\nLogin: {login_limpo}\nNova Senha: {senha}"
                            modulo_wapi.enviar_msg_api(inst[0], inst[1], d.get('telefone'), msg)
                            st.success("Senha enviada!"); st.session_state['modo_user'] = None; st.rerun()
                        conn.close()
                    else: st.error("Sem telefone.")

                if b3.form_submit_button("Cancelar"): st.session_state['modo_user'] = None; st.rerun()

        # Listagem de Usuários
        conn = get_conn(); df_u = pd.read_sql("SELECT id, nome, email, hierarquia, ativo FROM clientes_usuarios ORDER BY id DESC", conn); conn.close()
        for i, row in df_u.iterrows():
            with st.container():
                c = st.columns([4, 2, 1.5])
                c[0].write(f"**{row['nome']}** ({row['email']})")
                c[1].write(f"Cargo: {row['hierarquia']}")
                if c[2].button("Configurar", key=f"u_{row['id']}"):
                    st.session_state.update({'modo_user': 'editar_acesso', 'id_user': row['id'], 'nome_user': row['nome']})
                    st.rerun()

    with t2:
        conn = get_conn(); df_l = pd.read_sql("SELECT nome_usuario, data_hora, local_acesso FROM logs_acesso ORDER BY id DESC LIMIT 50", conn); conn.close()
        st.dataframe(df_l, use_container_width=True)

if __name__ == "__main__": app_usuarios()