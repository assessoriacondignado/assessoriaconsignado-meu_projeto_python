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
    # Se já for um hash (começa com $2b$), retorna ele mesmo para evitar dupla criptografia
    if senha.startswith('$2b$'): return senha
    # Se for senha nova, cria o hash
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
        # --- ÁREA DE EDIÇÃO (APARECE QUANDO CLICA NO LÁPIS) ---
        if st.session_state['modo_user'] == 'editar_acesso':
            st.markdown(f"### ✏️ Editando: {st.session_state.get('nome_user', 'Usuário')}")
            conn = get_conn()
            df = pd.read_sql(f"SELECT * FROM clientes_usuarios WHERE id = {st.session_state['id_user']}", conn)
            conn.close()
            
            d = df.iloc[0] if not df.empty else {}
            perms_atuais = ler_perms(st.session_state['id_user'])

            with st.form("form_acesso"):
                c1, c2, c3 = st.columns(3)
                login = c1.text_input("Login (E-mail)", value=d.get('email', ''))
                
                # Campo senha vazio para segurança
                senha_nova = c2.text_input("Nova Senha", value="", type="password", placeholder="Deixe vazio para manter a atual")
                
                opcoes_h = ["Cliente", "Grupo", "Gerente", "Admin"]
                idx_h = opcoes_h.index(d.get('hierarquia', 'Cliente')) if d.get('hierarquia') in opcoes_h else 0
                cargo = c3.selectbox("Hierarquia", opcoes_h, index=idx_h)
                
                st.markdown("#### Permissões e Status")
                cp1, cp2, cp3 = st.columns(3)
                p_com = cp1.checkbox("Acesso COMERCIAL", value="COMERCIAL" in perms_atuais)
                p_fin = cp2.checkbox("Acesso FINANCEIRO", value="FINANCEIRO" in perms_atuais)
                ativo = cp3.checkbox("Ativo?", value=d.get('ativo', True))

                b1, b2, b3 = st.columns([1, 1.5, 4.5])
                login_limpo = str(login).strip().lower()

                if b1.form_submit_button("💾 Salvar"):
                    # Lógica inteligente: Só atualiza a senha se o usuário digitou algo novo
                    if senha_nova:
                        senha_final = hash_senha(senha_nova)
                    else:
                        senha_final = d.get('senha') # Mantém a senha antiga

                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("UPDATE clientes_usuarios SET email=%s, senha=%s, hierarquia=%s, ativo=%s WHERE id=%s", (login_limpo, senha_final, cargo, ativo, st.session_state['id_user']))
                    conn.commit(); conn.close()
                    
                    novas = []
                    if p_com: novas.append("COMERCIAL")
                    if p_fin: novas.append("FINANCEIRO")
                    salvar_perms(st.session_state['id_user'], novas)
                    st.success("Atualizado com sucesso!"); st.session_state['modo_user'] = None; st.rerun()

                if b2.form_submit_button("🔐 Reset via WhatsApp"):
                    if d.get('telefone'):
                        conn = get_conn(); cur = conn.cursor()
                        cur.execute("SELECT api_instance_id, api_token FROM wapi_instancias LIMIT 1")
                        inst = cur.fetchone()
                        if inst:
                            senha_reset = senha_nova if senha_nova else "1234"
                            senha_f = hash_senha(senha_reset)
                            
                            cur.execute("UPDATE clientes_usuarios SET email=%s, senha=%s, hierarquia=%s, ativo=%s WHERE id=%s", (login_limpo, senha_f, cargo, ativo, st.session_state['id_user']))
                            conn.commit()
                            
                            msg = f"Olá! 🔐 Sua senha foi resetada pelo administrador.\nLogin: {login_limpo}\nNova Senha: {senha_reset}"
                            modulo_wapi.enviar_msg_api(inst[0], inst[1], d.get('telefone'), msg)
                            st.success(f"Senha enviada para o WhatsApp!"); st.session_state['modo_user'] = None; st.rerun()
                        else: st.error("Instância W-API não configurada.")
                        conn.close()
                    else: st.error("Usuário sem telefone cadastrado.")

                if b3.form_submit_button("Cancelar"): st.session_state['modo_user'] = None; st.rerun()
            st.divider()

        # --- LISTAGEM DOS USUÁRIOS (LAYOUT TABELA) ---
        
        # Cabeçalho da Tabela
        st.markdown("<br>", unsafe_allow_html=True)
        col_h1, col_h2, col_h3, col_h4 = st.columns([3, 3, 3, 1])
        col_h1.markdown("**Nome**")
        col_h2.markdown("**E-mail**")
        col_h3.markdown("**Telefone / Grupo**")
        col_h4.markdown("**Ações**")
        st.markdown("<hr style='margin: 5px 0; border-top: 2px solid #bbb;'>", unsafe_allow_html=True)

        conn = get_conn()
        # Busca atualizada incluindo telefone e id_grupo
        query = """
            SELECT id, nome, email, hierarquia, ativo, telefone, id_grupo_whats 
            FROM clientes_usuarios 
            ORDER BY id DESC
        """
        df_u = pd.read_sql(query, conn)
        conn.close()

        if not df_u.empty:
            for i, row in df_u.iterrows():
                with st.container():
                    c1, c2, c3, c4 = st.columns([3, 3, 3, 1])
                    
                    # Nome (com indicador de inativo se necessário)
                    status_icon = "🔴 " if not row['ativo'] else ""
                    c1.write(f"{status_icon}{row['nome']}")
                    
                    # E-mail
                    c2.write(row['email'])
                    
                    # Telefone e Grupo
                    tel = row['telefone'] if row['telefone'] else ""
                    grp = row['id_grupo_whats'] if row['id_grupo_whats'] else ""
                    
                    contato_display = tel
                    if grp:
                        contato_display += f" | {grp}"
                    
                    if not contato_display:
                        contato_display = "-"
                        
                    c3.write(contato_display)
                    
                    # Botão de Ação (Ícone Lápis)
                    if c4.button("✏️", key=f"u_{row['id']}", help="Editar Usuário"):
                        st.session_state.update({'modo_user': 'editar_acesso', 'id_user': row['id'], 'nome_user': row['nome']})
                        st.rerun()
                    
                    # Divisória entre linhas
                    st.markdown("<div style='border-bottom: 1px solid #f0f0f0; margin-bottom: 8px;'></div>", unsafe_allow_html=True)
        else:
            st.info("Nenhum usuário encontrado.")

    with t2:
        # Logs de acesso
        try:
            conn = get_conn()
            df_l = pd.read_sql("SELECT nome_usuario, data_hora, local_acesso FROM logs_acesso ORDER BY id DESC LIMIT 50", conn)
            conn.close()
            st.dataframe(df_l, use_container_width=True)
        except: st.info("Sem logs ainda.")

if __name__ == "__main__": app_usuarios()