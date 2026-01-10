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
    st.markdown("### ⚙️ Configurações Comerciais")
    
    # NOVA ABA ADICIONADA: Config. Produtos
    tab_msg, tab_prod, tab_outros = st.tabs(["💬 Mensagens Padrão", "📦 Config. Produtos", "🔧 Outros"])
    
    with tab_msg:
        renderizar_mensagens_padrao()
        
    with tab_prod:
        renderizar_config_produtos()
        
    with tab_outros:
        st.info("Outras configurações podem ser adicionadas aqui futuramente.")

if __name__ == "__main__":
    app_configuracoes()