import streamlit as st
import pandas as pd
import psycopg2
import time
import conexao

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

def salvar_template(modulo, chave, texto):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO wapi_templates (modulo, chave_status, conteudo_mensagem) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (modulo, chave_status) DO UPDATE SET conteudo_mensagem = EXCLUDED.conteudo_mensagem
            """, (modulo, chave, texto))
            conn.commit(); conn.close()
            return True
        except: 
            conn.close(); return False
    return False

@st.dialog("✏️ Editar Modelo de Mensagem")
def dialog_editar_template_msg(modulo, chave, texto_atual):
    st.write(f"Módulo: **{modulo}** | Status: **{chave}**")
    novo_texto = st.text_area("Mensagem", value=texto_atual, height=200)
    st.info("Tags comuns: {nome}, {pedido}, {produto}, {status}, {obs_status}")
    if st.button("💾 Salvar Modelo"):
        if salvar_template(modulo, chave, novo_texto):
            st.success("Modelo salvo com sucesso!")
            time.sleep(1); st.rerun()
        else: st.error("Erro ao salvar.")

def app_modelos():
    st.markdown("### 📝 Gestão de Modelos de Mensagem")
    col_filtro, col_add = st.columns([3, 1])
    mod_sel = col_filtro.selectbox("Filtrar por Módulo", ["PEDIDOS", "TAREFAS", "RENOVACAO"])
    
    conn = get_conn()
    try:
        df_tpl = pd.read_sql(f"SELECT chave_status, conteudo_mensagem FROM wapi_templates WHERE modulo = '{mod_sel}' ORDER BY chave_status", conn)
    except: df_tpl = pd.DataFrame()
    conn.close()
    
    if not df_tpl.empty:
        for _, row in df_tpl.iterrows():
            with st.expander(f"Status: {row['chave_status'].upper()}"):
                st.text(row['conteudo_mensagem'])
                if st.button("✏️ Editar", key=f"edt_{mod_sel}_{row['chave_status']}"):
                    dialog_editar_template_msg(mod_sel, row['chave_status'], row['conteudo_mensagem'])
    else: st.info(f"Nenhum modelo cadastrado para {mod_sel}.")
    
    st.divider()
    with st.expander("➕ Adicionar Novo Modelo"):
        with st.form("form_add_tpl"):
            novo_chave = st.text_input("Nome do Status (chave)", help="Ex: cancelado, pago, em_analise")
            novo_txt = st.text_area("Texto da Mensagem")
            if st.form_submit_button("Criar Modelo"):
                if novo_chave and novo_txt:
                    clean_chave = novo_chave.strip().lower().replace(" ", "_")
                    salvar_template(mod_sel, clean_chave, novo_txt)
                    st.success("Criado!"); time.sleep(1); st.rerun()
                else: st.warning("Preencha todos os campos.")