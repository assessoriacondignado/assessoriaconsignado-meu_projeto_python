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

def listar_conexoes(filtro_tipo=None, termo_busca=None):
    conn = get_conn()
    if conn:
        try:
            sql = "SELECT * FROM conexoes.relacao WHERE 1=1"
            
            if filtro_tipo and filtro_tipo != "Todos":
                sql += f" AND tipo_conexao = '{filtro_tipo}'"
            
            if termo_busca:
                sql += f" AND (nome_conexao ILIKE '%{termo_busca}%' OR descricao ILIKE '%{termo_busca}%')"
            
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
    
    # --- FILTROS E BOTÃO SUPERIOR ---
    c_filtros, c_btn = st.columns([5, 1])
    with c_filtros:
        col_tipo, col_busca = st.columns([1, 2])
        tipos_disponiveis = ["Todos", "SAIDA", "ENTRADA", "API", "BANCO DE DADOS"]
        filtro_tipo = col_tipo.selectbox("Filtrar Tipo", tipos_disponiveis)
        busca = col_busca.text_input("Buscar Conexão", placeholder="Nome ou descrição...")
    
    with c_btn:
        st.write("") # Espaçamento para alinhar o botão
        if st.button("➕ Nova", type="primary", use_container_width=True):
            dialog_nova_conexao()

    st.divider()

    # --- LISTAGEM ---
    df = listar_conexoes(filtro_tipo, busca)
    
    if not df.empty:
        # Cabeçalho Visual (Apenas texto estático para referência)
        st.markdown("""
        <div style="display: flex; font-weight: bold; color: #555; margin-bottom: 5px; padding-left: 10px;">
            <div style="flex: 2;">Nome</div>
            <div style="flex: 1;">Tipo</div>
            <div style="flex: 1;">Status</div>
            <div style="flex: 2;">Usuário/Key</div>
            <div style="flex: 0.5; text-align: right;">Ações</div>
        </div>
        """, unsafe_allow_html=True)
        
        for _, row in df.iterrows():
            # --- CARD DA CONEXÃO ---
            with st.container(border=True):
                # Linha Principal (Resumo)
                c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 2, 0.5])
                
                # Nome e Descrição
                c1.markdown(f"**{row['nome_conexao']}**")
                if row['descricao']:
                    c1.caption(row['descricao'])
                
                # Tipo
                # Badge visual simples usando HTML/CSS inline
                cor_badge = "#e3f2fd" if row['tipo_conexao'] == 'SAIDA' else "#f3e5f5"
                cor_texto = "#0d47a1" if row['tipo_conexao'] == 'SAIDA' else "#4a148c"
                c2.markdown(f"<span style='background-color:{cor_badge}; color:{cor_texto}; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;'>{row['tipo_conexao']}</span>", unsafe_allow_html=True)
                
                # Status
                icon_status = "🟢 ATIVO" if row['status'] == 'ATIVO' else "🔴 INATIVO"
                c3.write(icon_status)
                
                # Credencial (Mascarada)
                credencial = row['usuario_conexao'] if row['usuario_conexao'] else (row['key_conexao'][:5] + "••••" if row['key_conexao'] else "-")
                c4.code(credencial, language="text")
                
                # Botão Excluir
                if c5.button("🗑️", key=f"del_{row['id']}", help="Excluir Conexão"):
                    excluir_conexao(row['id'])
                    st.rerun()

                # --- ÁREA RETRÁTIL (MENU DE FUNÇÕES) ---
                # Aqui é a área vermelha da sua imagem solicitada
                with st.expander(f"⚙️ Menu de Funções: {row['nome_conexao']}"):
                    st.markdown("Selecione uma operação para esta conexão:")
                    
                    # Exemplo de layout de botões internos (Baseado na sua imagem)
                    col_func1, col_func2, col_func3 = st.columns(3)
                    
                    if col_func1.button("🔍 Pesquisa PF", key=f"btn_pf_{row['id']}", use_container_width=True):
                        st.toast(f"Iniciando pesquisa na conexão {row['nome_conexao']}...")
                        # Aqui você chamaria a lógica real
                        
                    if col_func2.button("💰 Consulta Saldo", key=f"btn_saldo_{row['id']}", use_container_width=True):
                        st.toast(f"Consultando saldo em {row['nome_conexao']}...")
                        
                    if col_func3.button("📜 Histórico", key=f"btn_hist_{row['id']}", use_container_width=True):
                        st.info("Visualizando histórico...")
                    
                    # Se for tipo BANCO DE DADOS, mostra opção de teste
                    if row['tipo_conexao'] == 'BANCO DE DADOS':
                        st.divider()
                        if st.button("🔌 Testar Conexão SQL", key=f"test_sql_{row['id']}"):
                            st.write("Tentando conectar ao banco remoto...")
                            # Lógica de teste de conexão aqui

    else:
        st.info(f"Nenhuma conexão encontrada para os filtros.")

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