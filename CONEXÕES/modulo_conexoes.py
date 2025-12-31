import streamlit as st
import pandas as pd
import psycopg2
import time
from datetime import datetime
import conexao

# --- CONEXÃO COM O BANCO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

# --- FUNÇÕES DE CRUD ---
def salvar_conexao(nome, tipo, desc, user, senha, key, status):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            sql = """
                INSERT INTO conexoes.relacao 
                (nome_conexao, tipo_conexao, descricao, usuario_conexao, senha_conexao, key_conexao, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql, (nome, tipo, desc, user, senha, key, status))
            conn.commit(); conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao salvar: {e}"); conn.close()
    return False

def listar_conexoes(filtro_tipo=None):
    conn = get_conn()
    if conn:
        try:
            sql = "SELECT * FROM conexoes.relacao"
            if filtro_tipo and filtro_tipo != "Todos":
                sql += f" WHERE tipo_conexao = '{filtro_tipo}'"
            
            sql += " ORDER BY id DESC"
            df = pd.read_sql(sql, conn)
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def excluir_conexao(id_con):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM conexoes.relacao WHERE id = %s", (id_con,))
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

# --- INTERFACE PRINCIPAL ---
def app_conexoes():
    st.markdown("## 🔌 Módulo de Conexões")
    
    # Menu Superior de Filtros
    col_f1, col_f2 = st.columns([4, 1])
    
    # CORREÇÃO AQUI: Nome da variável unificado para 'tipos_disponiveis'
    tipos_disponiveis = ["Todos", "SAIDA", "ENTRADA", "API", "BANCO DE DADOS"]
    filtro_tipo = col_f1.selectbox("📂 Filtrar Tipo de Conexão", tipos_disponiveis)
    
    if col_f2.button("➕ Nova Conexão", type="primary"):
        dialog_nova_conexao()

    st.divider()

    # Listagem
    df = listar_conexoes(filtro_tipo)
    
    if not df.empty:
        # Layout de Cards/Linhas
        st.markdown("""
        <div style="background-color: #f0f0f0; padding: 10px; font-weight: bold; display: flex; border-radius: 5px;">
            <div style="flex: 2;">Nome</div>
            <div style="flex: 1;">Tipo</div>
            <div style="flex: 1;">Status</div>
            <div style="flex: 2;">Usuário/Key</div>
            <div style="flex: 1;">Ações</div>
        </div>
        """, unsafe_allow_html=True)
        
        for _, row in df.iterrows():
            with st.container():
                c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 2, 1])
                c1.write(f"**{row['nome_conexao']}**")
                c1.caption(row['descricao'])
                
                c2.write(f"`{row['tipo_conexao']}`")
                
                icon_status = "🟢" if row['status'] == 'ATIVO' else "🔴"
                c3.write(f"{icon_status} {row['status']}")
                
                # Exibe Usuário ou Key mascarada
                credencial = row['usuario_conexao'] if row['usuario_conexao'] else (row['key_conexao'][:5] + "..." if row['key_conexao'] else "-")
                c4.write(credencial)
                
                if c5.button("🗑️", key=f"del_{row['id']}"):
                    excluir_conexao(row['id'])
                    st.rerun()
                
                st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
    else:
        st.info(f"Nenhuma conexão do tipo '{filtro_tipo}' encontrada.")

# --- DIALOGS (POP-UPS) ---
@st.dialog("➕ Nova Conexão")
def dialog_nova_conexao():
    with st.form("form_add_con"):
        nome = st.text_input("Nome da Conexão")
        tipo = st.selectbox("Tipo", ["SAIDA", "ENTRADA", "API", "BANCO DE DADOS"])
        desc = st.text_area("Descrição")
        
        c1, c2 = st.columns(2)
        user = c1.text_input("Usuário (Opcional)")
        senha = c2.text_input("Senha (Opcional)", type="password")
        key = st.text_input("Key / Token (Opcional)")
        status = st.selectbox("Status Inicial", ["ATIVO", "INATIVO"])
        
        if st.form_submit_button("💾 Salvar Conexão"):
            if nome:
                if salvar_conexao(nome, tipo, desc, user, senha, key, status):
                    st.success("Salvo com sucesso!")
                    time.sleep(1)
                    st.rerun()
            else:
                st.warning("O Nome é obrigatório.")