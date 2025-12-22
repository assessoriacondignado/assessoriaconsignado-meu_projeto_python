import streamlit as st
import pandas as pd
import psycopg2
import os
import re
import bcrypt

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

# --- CSS PARA VISUAL COMPACTO ---
st.markdown("""
<style>
    div[data-testid="stVerticalBlock"] > div[style*="flex-direction: column;"] > div[data-testid="stVerticalBlock"] { gap: 0.1rem; }
    .stButton button { padding: 0px 10px; height: 28px; line-height: 28px; }
</style>
""", unsafe_allow_html=True)

def app_clientes():
    st.markdown("## 👥 Cadastro de Clientes")
    
    # --- GARANTE ESTRUTURA DAS TABELAS (CONFORME PRINTS) ---
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("ALTER TABLE admin.clientes ADD COLUMN IF NOT EXISTS id_grupo_whats TEXT;")
        cur.execute("ALTER TABLE admin.clientes ADD COLUMN IF NOT EXISTS cpf TEXT;") # Adicionado conforme necessidade do print
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"Erro ao validar estrutura da tabela: {e}")

    if 'modo_cliente' not in st.session_state: st.session_state['modo_cliente'] = None
    if 'id_cli' not in st.session_state: st.session_state['id_cli'] = None

    c1, c2 = st.columns([6,1])
    filtro = c1.text_input("🔍 Buscar Cliente (Nome, Email, Telefone ou Grupo)", key="busca_cli")
    
    with c2:
        if st.button("+ Novo Cliente", type="primary", use_container_width=True):
            st.session_state['modo_cliente'] = 'novo'
            st.rerun()
        
        # --- LÓGICA DE CRIAÇÃO DE USUÁRIO CORRIGIDA ---
        if st.button("👤 Criar Usuário", use_container_width=True):
            if st.session_state.get('id_cli'):
                try:
                    conn = get_conn()
                    cur = conn.cursor()
                    # Busca dados do cliente selecionado (incluindo o novo campo CPF)
                    cur.execute("SELECT nome, email, telefone, COALESCE(cpf, '') FROM admin.clientes WHERE id = %s", (st.session_state['id_cli'],))
                    res = cur.fetchone()
                    if res:
                        nome_cli, email_cli, tel_cli, cpf_cli = res
                        
                        if not cpf_cli:
                            st.error("⚠️ O cliente selecionado não possui CPF. Preencha o CPF no cadastro do cliente antes de criar o usuário.")
                        else:
                            # Gera senha padrão '1234'
                            senha_hash = bcrypt.hashpw('1234'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                            
                            # SQL Corrigido: Agora o ON CONFLICT funcionará pois adicionamos a CONSTRAINT no passo 1
                            sql_user = """INSERT INTO clientes_usuarios (nome, email, cpf, senha, hierarquia, telefone, ativo) 
                                         VALUES (%s, %s, %s, %s, 'Cliente', %s, TRUE)
                                         ON CONFLICT (email) DO NOTHING"""
                            cur.execute(sql_user, (nome_cli, email_cli, cpf_cli, senha_hash, tel_cli))
                            
                            if cur.rowcount > 0:
                                conn.commit()
                                st.success(f"✅ Usuário criado para {nome_cli}! Login: {email_cli} | Senha: 1234")
                            else:
                                st.warning("ℹ️ Este e-mail já possui um acesso cadastrado.")
                    else:
                        st.error("Cliente não localizado.")
                    cur.close()
                    conn.close()
                except Exception as e:
                    st.error(f"Erro ao criar usuário: {e}")
            else:
                st.warning("Selecione um cliente (clique no ✏️) antes de criar o usuário.")

    # --- FORMULÁRIO DE CADASTRO/EDIÇÃO ---
    if st.session_state['modo_cliente'] in ['novo', 'editar']:
        st.divider()
        titulo = "📝 Novo Cliente" if st.session_state['modo_cliente'] == 'novo' else "✏️ Editar Cliente"
        st.markdown(f"### {titulo}")
        d = {}
        if st.session_state['modo_cliente'] == 'editar':
            conn = get_conn()
            df = pd.read_sql(f"SELECT * FROM admin.clientes WHERE id = {st.session_state['id_cli']}", conn)
            conn.close()
            if not df.empty: d = df.iloc[0]

        with st.form("form_cliente"):
            nome = st.text_input("Nome Completo *", value=d.get('nome', ''))
            
            c1, c2, c3 = st.columns(3)
            email = c1.text_input("E-mail *", value=d.get('email', ''))
            cpf = c2.text_input("CPF *", value=d.get('cpf', '')) # Campo CPF adicionado ao formulário
            telefone = c3.text_input("Telefone", value=d.get('telefone', ''))
            
            id_val = d.get('id_grupo_whats', '') if d.get('id_grupo_whats') else ''
            if id_val: id_val = id_val.replace('@g.us', '')
            id_grupo_input = st.text_input("ID do Grupo WhatsApp", value=id_val, help="Digite apenas o código numérico do grupo")
            
            c_b1, c_b2 = st.columns([1,6])
            if c_b1.form_submit_button("💾 Salvar"):
                id_final = None
                if id_grupo_input:
                    id_limpo = re.sub(r'[^0-9-]', '', id_grupo_input)
                    id_final = f"{id_limpo}@g.us"

                conn = get_conn()
                cur = conn.cursor()
                try:
                    if st.session_state['modo_cliente'] == 'novo':
                        sql = "INSERT INTO admin.clientes (nome, email, cpf, telefone, id_grupo_whats) VALUES (%s,%s,%s,%s,%s)"
                        cur.execute(sql, (nome, email, cpf, telefone, id_final))
                    else:
                        sql = "UPDATE admin.clientes SET nome=%s, email=%s, cpf=%s, telefone=%s, id_grupo_whats=%s WHERE id=%s"
                        cur.execute(sql, (nome, email, cpf, telefone, id_final, st.session_state['id_cli']))
                    
                    conn.commit()
                    st.success("Dados salvos!")
                    st.session_state['modo_cliente'] = None
                    st.rerun()
                except Exception as e: st.error(f"Erro ao salvar: {e}")
                finally: conn.close()
            
            if c_b2.form_submit_button("Cancelar"):
                st.session_state['modo_cliente'] = None
                st.rerun()

    # --- LISTAGEM ---
    st.markdown("<br>", unsafe_allow_html=True)
    conn = get_conn()
    sql = "SELECT id, nome, email, telefone, id_grupo_whats FROM admin.clientes"
    if filtro: 
        sql += f" WHERE (nome ILIKE '%%{filtro}%%' OR email ILIKE '%%{filtro}%%' OR telefone ILIKE '%%{filtro}%%' OR id_grupo_whats ILIKE '%%{filtro}%%')"
    sql += " ORDER BY id DESC"
    
    try:
        df = pd.read_sql(sql, conn)
    except Exception as e:
        st.error(f"Erro na leitura: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()

    if not df.empty:
        st.markdown("""<div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px 5px 0 0; border: 1px solid #ddd; display: flex; font-weight: bold;"><div style="flex: 3;">Nome</div><div style="flex: 2;">E-mail</div><div style="flex: 2;">Telefone / Grupo</div><div style="flex: 1.5;">Ações</div></div>""", unsafe_allow_html=True)
        for i, row in df.iterrows():
            with st.container():
                c = st.columns([3, 2, 2, 1.5])
                c[0].write(row['nome'])
                c[1].write(row['email'])
                contato_str = row['telefone'] if row['telefone'] else "---"
                if row['id_grupo_whats']: contato_str += f" | {row['id_grupo_whats']}"
                c[2].write(contato_str)
                
                col_botoes = c[3].columns([1, 1])
                if col_botoes[0].button("✏️", key=f"btn_e_{row['id']}"):
                    st.session_state['modo_cliente'] = 'editar'
                    st.session_state['id_cli'] = row['id']
                    st.rerun()
                st.markdown("<div style='border-bottom: 1px solid #e0e0e0; margin-bottom: 2px;'></div>", unsafe_allow_html=True)
    else:
        st.info("Nenhum cliente na base administrativa.")