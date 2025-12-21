import streamlit as st
import pandas as pd
import psycopg2
import os
import re

try: 
    import conexao
except ImportError: 
    st.error("Erro crítico: Arquivo conexao.py não encontrado no servidor.")

# --- CONFIGURAÇÕES DE DIRETÓRIO DINÂMICO ---
BASE_DIR_ARQUIVOS = os.path.join(os.getcwd(), "OPERACIONAL", "CLIENTES E USUARIOS", "ARQUIVOS_CLIENTES")

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

@st.dialog("Arquivos do Cliente")
def mostrar_arquivos(caminho_pasta):
    if not caminho_pasta or not os.path.exists(caminho_pasta):
        st.error("Pasta não localizada no servidor de nuvem.")
        return
    
    try:
        arquivos = [f for f in os.listdir(caminho_pasta) if os.path.isfile(os.path.join(caminho_pasta, f))]
        if not arquivos: 
            st.info("A pasta está vazia.")
        else:
            st.write(f"Encontrados {len(arquivos)} arquivo(s):")
            st.markdown("---")
            for arquivo in arquivos:
                caminho_completo = os.path.join(caminho_pasta, arquivo)
                col_nome, col_btn = st.columns([4, 1])
                col_nome.text(arquivo)
                try:
                    with open(caminho_completo, "rb") as file:
                        col_btn.download_button("⬇️", data=file, file_name=arquivo, mime="application/octet-stream", key=f"down_{arquivo}")
                except Exception:
                    col_btn.write("⚠️")
                st.markdown('<hr style="margin: 5px 0; opacity: 0.2;">', unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Erro ao ler pasta: {e}")

def app_clientes():
    st.markdown("## 👥 Cadastro de Clientes")
    if 'modo_cliente' not in st.session_state: st.session_state['modo_cliente'] = None
    if 'id_cli' not in st.session_state: st.session_state['id_cli'] = None

    c1, c2 = st.columns([6,1])
    filtro = c1.text_input("🔍 Buscar Cliente (Nome, CPF, Telefone ou Email)", key="busca_cli")
    if c2.button("+ Novo Cliente", type="primary"):
        st.session_state['modo_cliente'] = 'novo'
        st.rerun()

    if st.session_state['modo_cliente'] in ['novo', 'editar']:
        st.divider()
        titulo = "📝 Novo Cliente" if st.session_state['modo_cliente'] == 'novo' else "✏️ Editar Cliente"
        st.markdown(f"### {titulo}")
        d = {}
        if st.session_state['modo_cliente'] == 'editar':
            conn = get_conn()
            df = pd.read_sql(f"SELECT * FROM clientes_usuarios WHERE id = {st.session_state['id_cli']}", conn)
            conn.close()
            if not df.empty: d = df.iloc[0]

        with st.form("form_cliente"):
            c_tipo, c_nome = st.columns([1, 2])
            tipo_contato = c_tipo.selectbox("Tipo de Contato", ["Pessoa Física", "Grupo de WhatsApp"], 
                                          index=1 if d.get('hierarquia') == 'Grupo' else 0)
            nome = c_nome.text_input("Nome Completo / Nome do Grupo *", value=d.get('nome', ''))
            
            c1, c2 = st.columns(2)
            cpf = c1.text_input("CPF *", value=d.get('cpf', ''))
            telefone = c2.text_input("Telefone *", value=d.get('telefone', ''))
            
            c_id_email, c_id_grupo = st.columns(2)
            email = c_id_email.text_input("E-mail (Contato)", value=d.get('email', ''))
            
            id_val = d.get('id_grupo_whats', '') if d.get('id_grupo_whats') else ''
            if id_val: id_val = id_val.replace('@g.us', '')
            id_grupo_input = c_id_grupo.text_input("ID do Grupo WhatsApp", value=id_val, help="Digite apenas o código numérico do grupo")
            
            obs = st.text_area("Observação", value=d.get('observacao', ''))

            c_b1, c_b2 = st.columns([1,6])
            if c_b1.form_submit_button("💾 Salvar"):
                id_final = None
                if id_grupo_input:
                    id_limpo = re.sub(r'[^0-9-]', '', id_grupo_input)
                    id_final = f"{id_limpo}@g.us"

                h_tipo = 'Grupo' if tipo_contato == "Grupo de WhatsApp" else 'Cliente'

                conn = get_conn()
                cur = conn.cursor()
                try:
                    cur.execute("ALTER TABLE clientes_usuarios ADD COLUMN IF NOT EXISTS id_grupo_whats TEXT;")
                    if st.session_state['modo_cliente'] == 'novo':
                        pasta = os.path.join(BASE_DIR_ARQUIVOS, nome)
                        if not os.path.exists(pasta): os.makedirs(pasta, exist_ok=True)
                        sql = """INSERT INTO clientes_usuarios 
                                 (nome, cpf, telefone, email, observacao, pasta_caminho, hierarquia, id_grupo_whats) 
                                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
                        cur.execute(sql, (nome, cpf, telefone, email, obs, pasta, h_tipo, id_final))
                    else:
                        sql = """UPDATE clientes_usuarios SET 
                                 nome=%s, cpf=%s, telefone=%s, email=%s, observacao=%s, hierarquia=%s, id_grupo_whats=%s 
                                 WHERE id=%s"""
                        cur.execute(sql, (nome, cpf, telefone, email, obs, h_tipo, id_final, st.session_state['id_cli']))
                    conn.commit()
                    st.success("Dados salvos com sucesso!")
                    st.session_state['modo_cliente'] = None
                    st.rerun()
                except Exception as e: st.error(f"Erro ao salvar: {e}")
                finally: conn.close()
            
            if c_b2.form_submit_button("Cancelar"):
                st.session_state['modo_cliente'] = None
                st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    conn = get_conn()
    # SINCRONIA: Mostra tudo que não for Staff (Admin/Gerente) para não sumir dados
    sql = "SELECT id, nome, cpf, telefone, hierarquia, pasta_caminho, id_grupo_whats FROM clientes_usuarios WHERE hierarquia NOT IN ('Admin', 'Gerente', 'Supervisor')"
    if filtro: sql += f" AND (nome ILIKE '%{filtro}%' OR cpf ILIKE '%{filtro}%' OR telefone ILIKE '%{filtro}%')"
    sql += " ORDER BY id DESC"
    df = pd.read_sql(sql, conn)
    conn.close()

    if not df.empty:
        st.markdown("""<div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px 5px 0 0; border: 1px solid #ddd; display: flex; font-weight: bold;"><div style="flex: 3;">Nome</div><div style="flex: 2;">CPF</div><div style="flex: 2;">Telefone / ID Grupo</div><div style="flex: 1.5;">Ações</div></div>""", unsafe_allow_html=True)
        for i, row in df.iterrows():
            with st.container():
                c = st.columns([3, 2, 2, 1.5])
                icon = "👥 " if row['hierarquia'] == 'Grupo' else ""
                c[0].markdown(f"<div style='padding-top: 5px;'>{icon}{row['nome']}</div>", unsafe_allow_html=True)
                c[1].markdown(f"<div style='padding-top: 5px;'>{row['cpf']}</div>", unsafe_allow_html=True)
                contato_str = row['telefone'] if row['telefone'] else "---"
                if row['id_grupo_whats']: contato_str += f" | {row['id_grupo_whats']}"
                c[2].markdown(f"<div style='padding-top: 5px;'>{contato_str}</div>", unsafe_allow_html=True)
                col_botoes = c[3].columns([1, 1])
                if col_botoes[0].button("📂", key=f"btn_p_{row['id']}"): mostrar_arquivos(row['pasta_caminho'])
                if col_botoes[1].button("✏️", key=f"btn_e_{row['id']}"):
                    st.session_state['modo_cliente'] = 'editar'
                    st.session_state['id_cli'] = row['id']
                    st.rerun()
                st.markdown("<div style='border-bottom: 1px solid #e0e0e0; margin-bottom: 2px;'></div>", unsafe_allow_html=True)