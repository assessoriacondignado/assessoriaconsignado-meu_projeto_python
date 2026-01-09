import streamlit as st
import pandas as pd
import psycopg2
import os
import shutil
import uuid
import re
import time
from datetime import datetime
import conexao

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR, exist_ok=True)

# --- CONEXÃO COM BANCO ---
def get_conn():
    try:
        conn = psycopg2.connect(
            host=conexao.host,
            port=conexao.port,
            database=conexao.database,
            user=conexao.user,
            password=conexao.password
        )
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")
        return None

# --- FUNÇÕES AUXILIARES ---
def gerar_codigo_automatico():
    data = datetime.now().strftime("%y%m%d")
    sufixo = str(uuid.uuid4())[:4].upper()
    return f"ITEM-{data}-{sufixo}"

def listar_origens_custo():
    """Busca as origens de custo na tabela de ambiente de consulta."""
    conn = get_conn()
    lista = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT origem FROM conexoes.fatorconferi_ambiente_consulta ORDER BY origem ASC")
            lista = [row[0] for row in cur.fetchall()]
            conn.close()
        except:
            if conn: conn.close()
    return lista

# --- FUNÇÕES DE ARQUIVO E PASTA ---
def criar_pasta_produto(codigo, nome):
    data_str = datetime.now().strftime("%Y-%m-%d")
    nome_pasta = f"{codigo} - {nome} - {data_str}"
    nome_pasta = "".join(c for c in nome_pasta if c.isalnum() or c in (' ', '-', '_')).strip()
    caminho_completo = os.path.join(BASE_DIR, nome_pasta)
    if not os.path.exists(caminho_completo):
        os.makedirs(caminho_completo, exist_ok=True)
    return caminho_completo

def salvar_arquivos(uploaded_files, caminho_destino):
    if uploaded_files:
        if not os.path.exists(caminho_destino):
            os.makedirs(caminho_destino, exist_ok=True)
        for file in uploaded_files:
            file_path = os.path.join(caminho_destino, file.name)
            with open(file_path, "wb") as f:
                f.write(file.getbuffer())

# --- FUNÇÕES DE BANCO DE DADOS (CRUD) ---
def listar_produtos():
    conn = get_conn()
    if conn:
        try:
            query = """
                SELECT id, codigo, nome, tipo, preco, data_criacao, caminho_pasta, 
                       ativo, resumo, origem_custo 
                FROM produtos_servicos 
                ORDER BY id DESC
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except: 
            if conn: conn.close()
            return pd.DataFrame()
    return pd.DataFrame()

def cadastrar_produto_db(codigo, nome, tipo, resumo, preco, caminho_pasta, origem_custo):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            query = """
                INSERT INTO produtos_servicos (codigo, nome, tipo, resumo, preco, caminho_pasta, origem_custo, data_criacao, ativo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), TRUE)
                RETURNING id
            """
            cur.execute(query, (codigo, nome, tipo, resumo, preco, caminho_pasta, origem_custo))
            novo_id = cur.fetchone()[0]
            conn.commit()
            conn.close()
            return novo_id
        except Exception as e:
            st.error(f"Erro SQL: {e}")
            if conn: conn.close()
    return None

def atualizar_produto_db(id_prod, nome, tipo, resumo, preco, origem_custo):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            query = """
                UPDATE produtos_servicos 
                SET nome=%s, tipo=%s, resumo=%s, preco=%s, origem_custo=%s, data_atualizacao=NOW() 
                WHERE id=%s
            """
            cur.execute(query, (nome, tipo, resumo, preco, origem_custo, id_prod))
            conn.commit()
            conn.close()
            return True
        except: 
            if conn: conn.close()
    return False

def excluir_produto(id_prod, caminho_pasta):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM produtos_servicos WHERE id = %s", (id_prod,))
            conn.commit()
            conn.close()
            if caminho_pasta and os.path.exists(caminho_pasta):
                shutil.rmtree(caminho_pasta)
            return True
        except: 
            if conn: conn.close()
    return False

def alternar_status(id_prod, status_atual):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            novo_status = not status_atual
            cur.execute("UPDATE produtos_servicos SET ativo = %s WHERE id = %s", (novo_status, id_prod))
            conn.commit()
            conn.close()
            return True
        except: 
            if conn: conn.close()
    return False

# --- DIALOGS ---

@st.dialog("📂 Arquivos do Item")
def dialog_visualizar_arquivos(caminho_pasta, nome_item):
    st.write(f"Arquivos de: **{nome_item}**")
    if caminho_pasta and os.path.exists(caminho_pasta):
        arquivos = os.listdir(caminho_pasta)
        if arquivos:
            st.markdown("---")
            for arquivo in arquivos:
                c1, c2, c3 = st.columns([0.5, 3, 1.5])
                c1.write("📄")
                c2.write(arquivo)
                caminho_completo = os.path.join(caminho_pasta, arquivo)
                try:
                    with open(caminho_completo, "rb") as f:
                        c3.download_button("⬇️ Baixar", data=f, file_name=arquivo, key=f"dl_{arquivo}_{uuid.uuid4()}")
                except:
                    c3.write("Erro")
        else:
            st.warning("Pasta vazia.")
    else:
        st.error("Pasta não encontrada.")

@st.dialog("✏️ Editar Produto")
def dialog_editar_produto(dados):
    lista_origens = listar_origens_custo()
    opcoes_origem = [""] + lista_origens

    with st.form("form_editar_prod"):
        c1, c2 = st.columns(2)
        nome = c1.text_input("Nome *", value=dados['nome'])
        
        tipos_prods = ["PRODUTO", "SERVIÇO RECORRENTE", "SERVIÇO CRÉDITO"]
        idx_tipo = 0
        if dados['tipo'] in tipos_prods: idx_tipo = tipos_prods.index(dados['tipo'])
        tipo = c2.selectbox("Tipo", tipos_prods, index=idx_tipo)
        
        c3, c4 = st.columns(2)
        preco = c3.number_input("Preço (R$)", value=float(dados['preco'] or 0.0), format="%.2f")
        
        idx_orig = 0
        if dados.get('origem_custo') in lista_origens:
            idx_orig = lista_origens.index(dados.get('origem_custo')) + 1
        origem = c4.selectbox("Origem de Custo", options=opcoes_origem, index=idx_orig)
        
        resumo = st.text_area("Resumo", value=dados.get('resumo', ''))
        
        st.divider()
        
        if st.form_submit_button("💾 Salvar Alterações"):
            if nome:
                if atualizar_produto_db(dados['id'], nome, tipo, resumo, preco, origem):
                    st.success("Atualizado!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Erro ao atualizar.")
            else:
                st.warning("Preencha o nome.")

# --- FUNÇÃO PRINCIPAL ---
def app_produtos():
    # Criação das Abas na ordem solicitada
    tab_novo, tab_lista = st.tabs(["➕ Novo Produto", "📋 Lista de Produtos"])

    # ==========================
    # ABA 1: NOVO PRODUTO
    # ==========================
    with tab_novo:
        # Título removido conforme solicitado
        
        lista_origens = listar_origens_custo()
        opcoes_origem = [""] + lista_origens

        with st.container(border=True):
            with st.form("form_novo_produto"):
                c1, c2 = st.columns(2)
                nome = c1.text_input("Nome *")
                tipo = c2.selectbox("Tipo", ["PRODUTO", "SERVIÇO RECORRENTE", "SERVIÇO CRÉDITO"])
                
                c3, c4 = st.columns(2)
                preco = c3.number_input("Preço (R$)", value=0.0, format="%.2f")
                origem = c4.selectbox("Origem de Custo", options=opcoes_origem)
                
                resumo = st.text_area("Resumo")
                
                arquivos = st.file_uploader("Arquivos Iniciais (Opcional)", accept_multiple_files=True)

                st.divider()
                
                if st.form_submit_button("✅ Cadastrar Produto", type="primary"):
                    if nome:
                        codigo = gerar_codigo_automatico()
                        caminho = criar_pasta_produto(codigo, nome)
                        if arquivos: salvar_arquivos(arquivos, caminho)
                        
                        cadastrar_produto_db(codigo, nome, tipo, resumo, preco, caminho, origem)
                        st.success(f"Produto {codigo} cadastrado com sucesso!")
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.warning("O campo Nome é obrigatório.")

    # ==========================
    # ABA 2: LISTA DE PRODUTOS
    # ==========================
    with tab_lista:
        # Título removido conforme solicitado
        
        df = listar_produtos()
        
        if not df.empty:
            filtro = st.text_input("🔍 Buscar Produto", placeholder="Nome ou Código")
            if filtro:
                df = df[df['nome'].str.contains(filtro, case=False, na=False) | df['codigo'].str.contains(filtro, case=False, na=False)]
            
            st.markdown("---")
            
            for index, row in df.iterrows():
                status_icon = "🟢" if row['ativo'] else "🔴"
                with st.expander(f"{status_icon} {row['nome']} ({row['codigo']})"):
                    c1, c2 = st.columns(2)
                    c1.write(f"**Tipo:** {row['tipo']}")
                    c1.write(f"**Preço:** R$ {row['preco']:.2f}")
                    c2.write(f"**Origem:** {row.get('origem_custo') or '-'}")
                    c2.write(f"**Resumo:** {row['resumo']}")
                    
                    st.divider()
                    b1, b2, b3, b4 = st.columns(4)
                    
                    if b1.button("📂 Arquivos", key=f"arq_{row['id']}"):
                        dialog_visualizar_arquivos(row['caminho_pasta'], row['nome'])
                    
                    if b2.button("✏️ Editar", key=f"edit_{row['id']}"):
                        dialog_editar_produto(row)
                        
                    if b3.button("🔄 Status", key=f"st_{row['id']}"):
                        alternar_status(row['id'], row['ativo'])
                        st.rerun()
                        
                    if b4.button("🗑️ Excluir", key=f"del_{row['id']}"):
                        excluir_produto(row['id'], row['caminho_pasta'])
                        st.rerun()
        else:
            st.info("Nenhum produto cadastrado.")

if __name__ == "__main__":
    app_produtos()