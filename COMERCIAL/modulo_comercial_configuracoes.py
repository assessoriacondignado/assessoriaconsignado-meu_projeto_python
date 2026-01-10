import streamlit as st
import pandas as pd
import psycopg2
import time
import os
import sys

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

# --- FUNÇÕES DE BANCO (Gerais) ---
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
            st.error(f"Erro SQL: {e}")
            conn.close(); return False
    return False

def buscar_template_config(modulo, chave):
    """
    Busca o conteúdo de um template de mensagem específico.
    Utilizado por outros módulos para enviar mensagens.
    """
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            query = "SELECT conteudo_mensagem FROM wapi_templates WHERE modulo = %s AND chave_status = %s"
            cur.execute(query, (modulo, chave))
            result = cur.fetchone()
            conn.close()
            if result:
                return result[0]
        except Exception:
            conn.close()
    return None

def listar_chaves_config(modulo):
    """
    Retorna uma lista com os nomes (chaves) dos status cadastrados para um módulo.
    """
    conn = get_conn()
    if conn:
        try:
            query = "SELECT chave_status FROM wapi_templates WHERE modulo = %s ORDER BY chave_status ASC"
            df = pd.read_sql(query, conn, params=(modulo,))
            conn.close()
            return df['chave_status'].tolist()
        except:
            conn.close()
    return []

# --- DIALOGS ---
@st.dialog("✏️ Editar Modelo")
def dialog_editar_template_msg(modulo, chave, texto_atual):
    st.write(f"Módulo: **{modulo}** | Status: **{chave}**")
    novo_texto = st.text_area("Mensagem", value=texto_atual, height=300)
    st.caption("Variáveis disponíveis: {nome}, {pedido}, {produto}, {status}")
    
    if st.button("💾 Salvar Modelo", use_container_width=True):
        if salvar_template(modulo, chave, novo_texto):
            st.success("Modelo salvo!")
            time.sleep(1); st.rerun()
        else: st.error("Erro ao salvar.")

# --- SUBMENU: MENSAGENS PADRÃO ---
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
            else:
                st.info(f"Nenhum modelo encontrado para {mod_sel}.")
        except Exception as e:
            st.error(f"Erro ao buscar modelos: {e}")
            if conn: conn.close()
            
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

# --- APP PRINCIPAL CONFIGURAÇÕES ---
def app_configuracoes():
    st.markdown("### ⚙️ Configurações Comerciais")
    
    # Menu lateral ou Abas internas para as configurações
    # Conforme solicitado: "submenu separado por funções"
    tab_msg, tab_outros = st.tabs(["💬 Mensagens Padrão", "🔧 Outros"])
    
    with tab_msg:
        renderizar_mensagens_padrao()
        
    with tab_outros:
        st.info("Outras configurações podem ser adicionadas aqui futuramente.")

if __name__ == "__main__":
    app_configuracoes()