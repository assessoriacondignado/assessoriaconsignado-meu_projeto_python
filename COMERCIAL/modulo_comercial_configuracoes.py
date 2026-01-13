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

# =============================================================================
# 1. GESTÃO DE STATUS (NOVO)
# =============================================================================

def criar_tabela_status_se_nao_existir():
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("CREATE SCHEMA IF NOT EXISTS admin")
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

@st.dialog("⚙️ Configurar Status e Mensagem")
def dialog_editar_status_config(dados=None):
    id_val = dados['id'] if dados is not None else None
    nome_val = dados['nome_status'] if dados is not None else ""
    mod_val = dados['modulo'] if dados is not None else "PEDIDOS"
    rel_val = dados['status_relacionado'] if dados is not None else ""
    msg_val = dados['mensagem_padrao'] if dados is not None else ""

    st.subheader("Cadastro de Status")
    
    with st.form("form_status_cfg"):
        nome = st.text_input("Nome do Status (Exibição)", value=nome_val, help="Nome para identificar esta configuração.")
        
        c1, c2 = st.columns(2)
        modulo = c1.selectbox("Módulo", ["PEDIDOS", "TAREFAS", "RENOVACAO"], index=["PEDIDOS", "TAREFAS", "RENOVACAO"].index(mod_val) if mod_val in ["PEDIDOS", "TAREFAS", "RENOVACAO"] else 0)
        
        # Opções dinâmicas baseadas no módulo selecionado
        opcoes_status_sistema = []
        if modulo == "PEDIDOS": opcoes_status_sistema = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
        elif modulo == "TAREFAS": opcoes_status_sistema = ["Solicitado", "Registro", "Entregue", "Em processamento", "Em execução", "Pendente", "Cancelado"]
        elif modulo == "RENOVACAO": opcoes_status_sistema = ["Entrada", "Em Análise", "Concluído", "Pendente", "Cancelado"]
        else: opcoes_status_sistema = ["Geral"]

        try: idx_rel = opcoes_status_sistema.index(rel_val)
        except: idx_rel = 0
        
        st_rel = c2.selectbox("Status no Sistema", opcoes_status_sistema, index=idx_rel, help="A mensagem será enviada quando o pedido/tarefa assumir este status.")
        
        st.caption("Variáveis disponíveis: {nome}, {nome_completo}, {pedido}, {produto}, {status}, {obs_status}")
        msg = st.text_area("Mensagem Padrão (WhatsApp)", value=msg_val, height=200)
        
        if st.form_submit_button("💾 Salvar Configuração"):
            if salvar_config_status(id_val, nome, modulo, st_rel, msg):
                st.success("Salvo!"); time.sleep(1); st.rerun()

def renderizar_gestao_status():
    st.markdown("#### ⚙️ Gestão de Status e Automação")
    st.caption("Configure as mensagens automáticas enviadas ao alterar o status.")
    
    if st.button("➕ Nova Configuração"):
        dialog_editar_status_config(None)
    
    df = listar_config_status()
    
    if not df.empty:
        for i, row in df.iterrows():
            with st.expander(f"{row['modulo']} | {row['nome_status']} (Gatilho: {row['status_relacionado']})"):
                st.text_area("Mensagem", value=row['mensagem_padrao'], disabled=True, key=f"view_msg_{row['id']}")
                c1, c2 = st.columns([1, 4])
                if c1.button("✏️ Editar", key=f"btn_edt_s_{row['id']}"):
                    dialog_editar_status_config(row.to_dict())
                if c1.button("🗑️ Excluir", key=f"btn_del_s_{row['id']}"):
                    excluir_config_status(row['id'])
                    st.rerun()
    else:
        st.info("Nenhuma configuração encontrada.")

# --- FUNÇÕES DE BANCO (TEMPLATES MENSAGEM) ---
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
    conn = get_conn()
    if conn:
        try:
            query = "SELECT chave_status FROM wapi_templates WHERE modulo = %s ORDER BY chave_status ASC"
            df = pd.read_sql(query, conn, params=(modulo,))
            conn.close()
            return df['chave_status'].tolist()
        except: conn.close()
    return []

# --- FUNÇÕES DE BANCO (TEMAS PRODUTOS - NOVO) ---
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
            if id_tema: # Atualizar
                cur.execute("UPDATE admin.temas_produtos SET tema=%s, texto=%s, data_atualizacao=NOW() WHERE id=%s", (tema, texto, id_tema))
            else: # Inserir
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
@st.dialog("✏️ Editar Modelo Mensagem")
def dialog_editar_template_msg(modulo, chave, texto_atual):
    st.write(f"Módulo: **{modulo}** | Status: **{chave}**")
    novo_texto = st.text_area("Mensagem", value=texto_atual, height=300)
    st.caption("Variáveis disponíveis: {nome}, {pedido}, {produto}, {status}")
    if st.button("💾 Salvar Modelo", use_container_width=True):
        if salvar_template(modulo, chave, novo_texto):
            st.success("Modelo salvo!"); time.sleep(1); st.rerun()
        else: st.error("Erro ao salvar.")

@st.dialog("📝 Editar Tema Produto")
def dialog_editar_tema(dados_tema=None):
    # Se dados_tema for None, é criação
    t_val = dados_tema['tema'] if dados_tema is not None else ""
    txt_val = dados_tema['texto'] if dados_tema is not None else ""
    id_val = dados_tema['id'] if dados_tema is not None else None
    
    titulo = "Criar Novo Tema" if id_val is None else "Editar Tema"
    st.subheader(titulo)
    
    with st.form("form_tema_prod"):
        novo_tema = st.text_input("Título do Tema", value=t_val)
        novo_texto = st.text_area("Texto / Instruções", value=txt_val, height=400)
        
        c1, c2 = st.columns(2)
        if c1.form_submit_button("💾 Salvar"):
            if novo_tema and novo_texto:
                if salvar_tema_db(id_val, novo_tema, novo_texto):
                    st.success("Salvo com sucesso!"); time.sleep(1); st.rerun()
            else: st.warning("Preencha todos os campos.")
        
        if id_val and c2.form_submit_button("🗑️ Excluir", type="primary"):
            if excluir_tema_db(id_val):
                st.success("Excluído!"); time.sleep(1); st.rerun()

# --- RENDERIZADORES ---
def renderizar_mensagens_padrao():
    st.markdown("#### 💬 Modelos de Mensagem Automática")
    st.caption("Configure os textos enviados via WhatsApp para cada status do sistema.")
    
    col_filtro, col_v = st.columns([3, 1])
    mod_sel = col_filtro.selectbox("Filtrar por Módulo", ["PEDIDOS", "TAREFAS", "RENOVACAO"])
    
    conn = get_conn()
    if conn:
        try:
            df_tpl = pd.read_sql(f"SELECT chave_status, conteudo_mensagem FROM wapi_templates WHERE modulo = '{mod_sel}' ORDER BY chave_status", conn)
            conn.close()
            
            if not df_tpl.empty:
                for _, row in df_tpl.iterrows():
                    with st.expander(f"Status: {row['chave_status'].upper()}"):
                        st.text(row['conteudo_mensagem'])
                        if st.button("Editar", key=f"edt_{mod_sel}_{row['chave_status']}"):
                            dialog_editar_template_msg(mod_sel, row['chave_status'], row['conteudo_mensagem'])
            else: st.info(f"Nenhum modelo encontrado para {mod_sel}.")
        except Exception as e: st.error(f"Erro ao buscar modelos: {e}"); conn.close()
            
    st.divider()
    with st.expander("➕ Criar Novo Modelo"):
        with st.form("form_add_tpl"):
            novo_chave = st.text_input("Nome do Status (chave)", help="Ex: aguardando_pagamento")
            novo_txt = st.text_area("Texto da Mensagem")
            if st.form_submit_button("Criar"):
                if novo_chave and novo_txt:
                    clean_chave = novo_chave.strip().lower().replace(" ", "_")
                    if salvar_template(mod_sel, clean_chave, novo_txt):
                        st.success("Criado!"); time.sleep(1); st.rerun()
                else: st.warning("Preencha todos os campos.")

def renderizar_config_produtos():
    st.markdown("#### 📦 Instruções e Temas de Produtos")
    st.caption("Crie textos padrão de instruções que podem ser vinculados aos produtos.")
    
    if st.button("➕ Novo Tema"):
        dialog_editar_tema(None)
    
    df = listar_temas_db()
    if not df.empty:
        st.dataframe(df[['tema', 'data_atualizacao']], use_container_width=True, hide_index=True)
        
        # Seletor para editar
        opcoes = df.to_dict('records')
        selecionado = st.selectbox("Selecione para Editar/Excluir", opcoes, format_func=lambda x: x['tema'], index=None)
        
        if selecionado:
            dialog_editar_tema(selecionado)
    else:
        st.info("Nenhum tema cadastrado.")

# --- APP PRINCIPAL CONFIGURAÇÕES ---
def app_configuracoes():
    criar_tabela_status_se_nao_existir() # Garante que a tabela existe
    st.markdown("### ⚙️ Configurações Comerciais")
    
    # NOVA ABA ADICIONADA: Config. Produtos
    tab_st, tab_msg, tab_prod, tab_outros = st.tabs(["⚙️ Gestão de Status", "💬 Mensagens Padrão (Antigo)", "📦 Config. Produtos", "🔧 Outros"])
    
    with tab_st:
        renderizar_gestao_status()

    with tab_msg:
        renderizar_mensagens_padrao()
        
    with tab_prod:
        renderizar_config_produtos()
        
    with tab_outros:
        st.info("Outras configurações podem ser adicionadas aqui futuramente.")

if __name__ == "__main__":
    app_configuracoes()