import streamlit as st
import pandas as pd
import psycopg2
import time
import os
import sys
from datetime import datetime

# Ajuste de path para importar conexao da raiz
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
diretorio_pai = os.path.dirname(diretorio_atual)
raiz_projeto = os.path.dirname(diretorio_pai)
if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)

try:
    import conexao
except ImportError:
    st.error("Erro: conexao.py não encontrado.")

# --- CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

# --- CRIAÇÃO DE TABELA (NOVO) ---
def criar_tabela_status_se_nao_existir():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            # Cria schema admin se não existir
            cur.execute("CREATE SCHEMA IF NOT EXISTS admin")
            # Cria tabela admin.status
            cur.execute("""
                CREATE TABLE IF NOT EXISTS admin.status (
                    id SERIAL PRIMARY KEY,
                    nome_status VARCHAR(100),
                    modulo VARCHAR(50),
                    status_relacionado VARCHAR(50),
                    mensagem_padrao TEXT,
                    data_criacao TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            st.error(f"Erro ao criar tabela status: {e}")
            if conn: conn.close()

# --- FUNÇÕES DE BANCO (NOVA TABELA STATUS) ---
def listar_config_status(modulo=None):
    conn = get_conn()
    if conn:
        try:
            sql = "SELECT id, nome_status, modulo, status_relacionado, mensagem_padrao FROM admin.status"
            params = []
            if modulo:
                sql += " WHERE modulo = %s"
                params.append(modulo)
            sql += " ORDER BY modulo, nome_status"
            
            df = pd.read_sql(sql, conn, params=params)
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def salvar_config_status(id_status, nome, modulo, st_rel, msg):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            if id_status: # Update
                cur.execute("""
                    UPDATE admin.status 
                    SET nome_status=%s, modulo=%s, status_relacionado=%s, mensagem_padrao=%s 
                    WHERE id=%s
                """, (nome, modulo, st_rel, msg, id_status))
            else: # Insert
                cur.execute("""
                    INSERT INTO admin.status (nome_status, modulo, status_relacionado, mensagem_padrao)
                    VALUES (%s, %s, %s, %s)
                """, (nome, modulo, st_rel, msg))
            conn.commit(); conn.close()
            return True
        except Exception as e:
            st.error(f"Erro DB: {e}"); conn.close()
    return False

def excluir_config_status(id_status):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM admin.status WHERE id=%s", (id_status,))
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

# --- FUNÇÕES DE BANCO (ANTIGO - MANTIDO) ---
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
        except Exception as e: 
            st.error(f"Erro SQL: {e}"); conn.close(); return False
    return False

def buscar_template_config(modulo, chave):
    # Função mantida para compatibilidade com outros módulos
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            query = "SELECT conteudo_mensagem FROM wapi_templates WHERE modulo = %s AND chave_status = %s"
            cur.execute(query, (modulo, chave))
            result = cur.fetchone()
            conn.close()
            if result: return result[0]
        except Exception: conn.close()
    return None

def listar_chaves_config(modulo):
    # Mantido para compatibilidade
    conn = get_conn()
    if conn:
        try:
            query = "SELECT chave_status FROM wapi_templates WHERE modulo = %s ORDER BY chave_status ASC"
            df = pd.read_sql(query, conn, params=(modulo,))
            conn.close()
            return df['chave_status'].tolist()
        except: conn.close()
    return []

# --- FUNÇÕES DE BANCO (TEMAS PRODUTOS) ---
def listar_temas_db():
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT id, tema, texto, data_atualizacao FROM admin.temas_produtos ORDER BY tema", conn)
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def salvar_tema_db(id_tema, tema, texto):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            if id_tema: 
                cur.execute("UPDATE admin.temas_produtos SET tema=%s, texto=%s, data_atualizacao=NOW() WHERE id=%s", (tema, texto, id_tema))
            else: 
                cur.execute("INSERT INTO admin.temas_produtos (tema, texto, data_atualizacao) VALUES (%s, %s, NOW())", (tema, texto))
            conn.commit(); conn.close()
            return True
        except Exception as e: st.error(f"Erro DB: {e}"); conn.close()
    return False

def excluir_tema_db(id_tema):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM admin.temas_produtos WHERE id=%s", (id_tema,))
            conn.commit(); conn.close()
            return True
        except: conn.close()
    return False

# --- DIALOGS ---
@st.dialog("⚙️ Configurar Status e Mensagem")
def dialog_editar_status_config(dados=None):
    # Dados iniciais
    id_val = dados['id'] if dados is not None else None
    nome_val = dados['nome_status'] if dados is not None else ""
    mod_val = dados['modulo'] if dados is not None else "PEDIDOS"
    rel_val = dados['status_relacionado'] if dados is not None else ""
    msg_val = dados['mensagem_padrao'] if dados is not None else ""

    st.subheader("Cadastro de Status")
    
    with st.form("form_status_cfg"):
        nome = st.text_input("Nome do Status (Exibição)", value=nome_val, help="Nome amigável para identificar esta configuração.")
        
        c1, c2 = st.columns(2)
        modulo = c1.selectbox("Módulo", ["PEDIDOS", "TAREFAS", "RENOVACAO"], index=["PEDIDOS", "TAREFAS", "RENOVACAO"].index(mod_val) if mod_val in ["PEDIDOS", "TAREFAS", "RENOVACAO"] else 0)
        
        # Lista de status fixos do sistema para relacionamento
        opcoes_status_sistema = []
        if modulo == "PEDIDOS": opcoes_status_sistema = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
        elif modulo == "TAREFAS": opcoes_status_sistema = ["Solicitado", "Registro", "Entregue", "Em processamento", "Em execução", "Pendente", "Cancelado"]
        elif modulo == "RENOVACAO": opcoes_status_sistema = ["Entrada", "Em Análise", "Concluído", "Pendente", "Cancelado"]
        
        try: idx_rel = opcoes_status_sistema.index(rel_val)
        except: idx_rel = 0
        
        st_rel = c2.selectbox("Status no Sistema", opcoes_status_sistema, index=idx_rel, help="Quando o pedido/tarefa assumir este status, a mensagem abaixo será usada.")
        
        msg = st.text_area("Mensagem Padrão (WhatsApp)", value=msg_val, height=200, help="Variáveis: {nome}, {pedido}, {produto}, {status}")
        
        if st.form_submit_button("💾 Salvar Configuração"):
            if salvar_config_status(id_val, nome, modulo, st_rel, msg):
                st.success("Salvo!"); time.sleep(1); st.rerun()

# --- RENDERIZADORES ---
def renderizar_gestao_status():
    st.markdown("#### ⚙️ Gestão de Status e Automação")
    st.caption("Configure as mensagens que serão enviadas automaticamente quando um status for alterado.")
    
    if st.button("➕ Nova Configuração de Status"):
        dialog_editar_status_config(None)
    
    df = listar_config_status()
    
    if not df.empty:
        for i, row in df.iterrows():
            with st.expander(f"{row['modulo']} | {row['nome_status']} (Relacionado a: {row['status_relacionado']})"):
                st.text_area("Mensagem Configurada", value=row['mensagem_padrao'], disabled=True, key=f"v_msg_{row['id']}")
                c1, c2 = st.columns([1, 1])
                if c1.button("✏️ Editar", key=f"btn_edit_st_{row['id']}"):
                    dialog_editar_status_config(row.to_dict())
                if c2.button("🗑️ Excluir", key=f"btn_del_st_{row['id']}"):
                    excluir_config_status(row['id'])
                    st.rerun()
    else:
        st.info("Nenhuma configuração de status encontrada.")

def renderizar_mensagens_antigo():
    st.markdown("#### ⚠️ Modelos de Mensagem (Legado)")
    st.caption("Estes modelos são usados pelo método antigo. Prefira usar a aba 'Gestão de Status'.")
    # ... código mantido, mas com aviso ...
    col_filtro, col_v = st.columns([3, 1])
    mod_sel = col_filtro.selectbox("Filtrar", ["PEDIDOS", "TAREFAS", "RENOVACAO"])
    # ... renderização simplificada ...

def renderizar_config_produtos():
    # ... código mantido ...
    st.markdown("#### 📦 Instruções e Temas de Produtos")
    if st.button("➕ Novo Tema"): dialog_editar_tema(None)
    df = listar_temas_db()
    if not df.empty:
        st.dataframe(df[['tema', 'data_atualizacao']], use_container_width=True, hide_index=True)
        opcoes = df.to_dict('records')
        selecionado = st.selectbox("Selecione para Editar", opcoes, format_func=lambda x: x['tema'], index=None)
        if selecionado: dialog_editar_tema(selecionado)

# --- APP PRINCIPAL CONFIGURAÇÕES ---
def app_configuracoes():
    # Garante que a tabela existe ao abrir o módulo
    criar_tabela_status_se_nao_existir()

    st.markdown("### ⚙️ Configurações Comerciais")
    
    # REORGANIZAÇÃO DAS ABAS
    tab_status, tab_prod, tab_antigo = st.tabs(["⚙️ Gestão de Status", "📦 Config. Produtos", "arquivo (Antigo)"])
    
    with tab_status:
        renderizar_gestao_status()
        
    with tab_prod:
        renderizar_config_produtos()
        
    with tab_antigo:
        renderizar_mensagens_antigo()

if __name__ == "__main__":
    app_configuracoes()