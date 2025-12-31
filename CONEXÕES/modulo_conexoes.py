import streamlit as st
import pandas as pd
import psycopg2
import time
from datetime import datetime
import conexao

# Tenta importar o módulo específico do Fator Conferi
try:
    import modulo_fator_conferi
except ImportError:
    modulo_fator_conferi = None

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
    # [LÓGICA DE NAVEGAÇÃO] Verifica se deve mostrar o Painel Fator
    if st.session_state.get('navegacao_conexoes') == 'FATOR_CONFERI':
        if st.button("⬅️ Voltar para Lista de Conexões"):
            st.session_state['navegacao_conexoes'] = None
            st.rerun()
        
        if modulo_fator_conferi:
            modulo_fator_conferi.app_fator_conferi()
        else:
            st.error("Módulo 'modulo_fator_conferi.py' não encontrado na pasta CONEXÕES.")
        return # Interrompe a função aqui para não mostrar a lista

    # --- TELA PADRÃO: LISTA DE CONEXÕES ---
    st.markdown("## 🔌 Módulo de Conexões")
    
    # Filtros e Botão Superior
    c_filtros, c_btn = st.columns([5, 1])
    with c_filtros:
        col_tipo, col_busca = st.columns([1, 2])
        tipos_disponiveis = ["Todos", "SAIDA", "ENTRADA", "API", "BANCO DE DADOS"]
        filtro_tipo = col_tipo.selectbox("Filtrar Tipo", tipos_disponiveis)
        busca = col_busca.text_input("Buscar Conexão", placeholder="Nome ou descrição...")
    
    with c_btn:
        st.write("") # Espaçamento
        if st.button("➕ Nova", type="primary", use_container_width=True):
            dialog_nova_conexao()

    st.divider()

    # Listagem
    df = listar_conexoes(filtro_tipo, busca)
    
    if not df.empty:
        # Cabeçalho Visual
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
            # CARD DA CONEXÃO
            with st.container(border=True):
                # Linha Principal (Resumo)
                c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 2, 0.5])
                
                # Nome e Descrição
                c1.markdown(f"**{row['nome_conexao']}**")
                if row['descricao']:
                    c1.caption(row['descricao'])
                
                # Tipo (Badge Visual)
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
                with st.expander(f"⚙️ Menu de Funções: {row['nome_conexao']}"):
                    
                    # [ATUALIZAÇÃO] Botão Especial para FATOR CONFERI
                    if "FATOR" in row['nome_conexao'].upper():
                        st.info("Painel Especializado Disponível")
                        if st.button(f"🚀 Acessar Painel Fator", key=f"btn_fator_{row['id']}", type="primary", use_container_width=True):
                            st.session_state['navegacao_conexoes'] = 'FATOR_CONFERI'
                            st.rerun()
                        st.divider()

                    # Funções Genéricas
                    st.markdown("Operações Rápidas:")
                    col_func1, col_func2, col_func3 = st.columns(3)
                    
                    if col_func1.button("🔍 Teste Conexão", key=f"btn_test_{row['id']}", use_container_width=True):
                        st.toast(f"Testando ping para {row['nome_conexao']}...")
                        
                    if col_func2.button("💰 Consulta Saldo", key=f"btn_saldo_{row['id']}", use_container_width=True):
                        st.toast(f"Consultando saldo genérico...")
                        
                    if col_func3.button("📜 Ver Logs", key=f"btn_hist_{row['id']}", use_container_width=True):
                        st.info("Sem logs recentes.")

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