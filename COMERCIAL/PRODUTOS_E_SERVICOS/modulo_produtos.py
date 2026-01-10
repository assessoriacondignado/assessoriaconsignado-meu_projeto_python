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
        return psycopg2.connect(
            host=conexao.host,
            port=conexao.port,
            database=conexao.database,
            user=conexao.user,
            password=conexao.password
        )
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

# --- FUNÇÕES DE TEMAS (Instruções) ---
def listar_temas_disponiveis():
    """Retorna lista de dicts [{'id': 1, 'tema': 'Nome'}]"""
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT id, tema FROM admin.temas_produtos ORDER BY tema", conn)
            conn.close()
            return df.to_dict('records')
        except: conn.close()
    return []

def buscar_temas_do_produto(id_produto):
    """Retorna lista de IDs [1, 5, ...]"""
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT id_tema FROM admin.produtos_temas_vinculo WHERE id_produto = %s", (id_produto,))
            ids = [row[0] for row in cur.fetchall()]
            conn.close()
            return ids
        except: conn.close()
    return []

def buscar_texto_temas_produto(id_produto):
    """Retorna DataFrame com tema e texto para visualização"""
    conn = get_conn()
    if conn:
        try:
            query = """
                SELECT t.tema, t.texto 
                FROM admin.produtos_temas_vinculo v
                JOIN admin.temas_produtos t ON v.id_tema = t.id
                WHERE v.id_produto = %s
                ORDER BY t.tema
            """
            df = pd.read_sql(query, conn, params=(id_produto,))
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def atualizar_vinculo_temas(id_produto, lista_ids_temas):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM admin.produtos_temas_vinculo WHERE id_produto = %s", (id_produto,))
            if lista_ids_temas:
                args = [(int(id_produto), int(tid)) for tid in lista_ids_temas]
                cur.executemany("INSERT INTO admin.produtos_temas_vinculo (id_produto, id_tema) VALUES (%s, %s)", args)
            conn.commit(); conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao vincular temas: {e}"); conn.close()
    return False

# --- FUNÇÕES DE BANCO DE DADOS (CRUD PRODUTOS) ---
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

def cadastrar_produto_db(codigo, nome, tipo, resumo, preco, caminho_pasta, origem_custo, ids_temas):
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
            
            if ids_temas:
                atualizar_vinculo_temas(novo_id, ids_temas)
                
            return novo_id
        except Exception as e:
            st.error(f"Erro SQL: {e}")
            if conn: conn.close()
    return None

def atualizar_produto_db(id_prod, nome, tipo, resumo, preco, origem_custo, ids_temas):
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
            
            atualizar_vinculo_temas(id_prod, ids_temas)
            
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

# --- PAINÉIS LATERAIS (SUBSTITUEM OS DIALOGS) ---

def painel_visualizar_arquivos(caminho_pasta, nome_item):
    st.markdown(f"### 📂 Arquivos: {nome_item}")
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
                        c3.download_button("⬇️", data=f, file_name=arquivo, key=f"dl_{arquivo}_{uuid.uuid4()}", help="Baixar Arquivo")
                except:
                    c3.write("Erro")
        else:
            st.warning("Pasta vazia.")
    else:
        st.error("Pasta não encontrada.")

def painel_visualizar_instrucoes(id_prod, nome_prod):
    st.markdown(f"### 📖 Instruções: {nome_prod}")
    df_inst = buscar_texto_temas_produto(id_prod)
    
    if not df_inst.empty:
        for _, row in df_inst.iterrows():
            with st.expander(f"📌 {row['tema']}", expanded=False):
                st.markdown(row['texto'])
    else:
        st.info("Este produto não possui instruções vinculadas.")

def painel_editar_produto(dados):
    st.markdown(f"### ✏️ Editar: {dados['nome']}")
    lista_origens = listar_origens_custo()
    opcoes_origem = [""] + lista_origens
    
    # Busca temas
    todos_temas = listar_temas_disponiveis()
    temas_vinculados = buscar_temas_do_produto(dados['id'])

    with st.form("form_editar_prod_painel"):
        nome = st.text_input("Nome *", value=dados['nome'])
        
        tipos_prods = ["PRODUTO", "SERVIÇO RECORRENTE", "SERVIÇO CRÉDITO"]
        idx_tipo = 0
        if dados['tipo'] in tipos_prods: idx_tipo = tipos_prods.index(dados['tipo'])
        tipo = st.selectbox("Tipo", tipos_prods, index=idx_tipo)
        
        c_p1, c_p2 = st.columns(2)
        preco = c_p1.number_input("Preço (R$)", value=float(dados['preco'] or 0.0), format="%.2f")
        
        idx_orig = 0
        if dados.get('origem_custo') in lista_origens:
            idx_orig = lista_origens.index(dados.get('origem_custo')) + 1
        origem = c_p2.selectbox("Origem Custo", options=opcoes_origem, index=idx_orig)
        
        temas_sel = st.multiselect(
            "Instruções / Temas",
            options=[t['id'] for t in todos_temas],
            default=temas_vinculados,
            format_func=lambda x: next((t['tema'] for t in todos_temas if t['id'] == x), str(x))
        )
        
        resumo = st.text_area("Resumo", value=dados.get('resumo', ''))
        
        st.divider()
        
        if st.form_submit_button("💾 Salvar Alterações", type="primary"):
            if nome:
                if atualizar_produto_db(dados['id'], nome, tipo, resumo, preco, origem, temas_sel):
                    st.success("Atualizado!")
                    time.sleep(1)
                    st.session_state.prod_panel_active = False # Fecha painel ao salvar
                    st.rerun()
                else:
                    st.error("Erro ao atualizar.")
            else:
                st.warning("Preencha o nome.")

# --- FUNÇÃO PRINCIPAL ---
def app_produtos():
    tab_novo, tab_lista = st.tabs(["➕ Novo Produto", "📋 Lista de Produtos"])

    # ==========================
    # ABA 1: NOVO PRODUTO (Mantido Layout Original)
    # ==========================
    with tab_novo:
        lista_origens = listar_origens_custo()
        opcoes_origem = [""] + lista_origens
        todos_temas = listar_temas_disponiveis()

        with st.container(border=True):
            with st.form("form_novo_produto"):
                c1, c2 = st.columns(2)
                nome = c1.text_input("Nome *")
                tipo = c2.selectbox("Tipo", ["PRODUTO", "SERVIÇO RECORRENTE", "SERVIÇO CRÉDITO"])
                
                c3, c4 = st.columns(2)
                preco = c3.number_input("Preço (R$)", value=0.0, format="%.2f")
                origem = c4.selectbox("Origem de Custo", options=opcoes_origem)
                
                temas_sel = st.multiselect(
                    "Instruções / Temas",
                    options=[t['id'] for t in todos_temas],
                    format_func=lambda x: next((t['tema'] for t in todos_temas if t['id'] == x), str(x))
                )

                resumo = st.text_area("Resumo")
                arquivos = st.file_uploader("Arquivos Iniciais (Opcional)", accept_multiple_files=True)

                st.divider()
                
                if st.form_submit_button("✅ Cadastrar Produto", type="primary"):
                    if nome:
                        codigo = gerar_codigo_automatico()
                        caminho = criar_pasta_produto(codigo, nome)
                        if arquivos: salvar_arquivos(arquivos, caminho)
                        
                        cadastrar_produto_db(codigo, nome, tipo, resumo, preco, caminho, origem, temas_sel)
                        st.success(f"Produto {codigo} cadastrado com sucesso!")
                        time.sleep(1.5); st.rerun()
                    else: st.warning("O campo Nome é obrigatório.")

    # ==========================
    # ABA 2: LISTA DE PRODUTOS (LAYOUT COM PAINEL LATERAL)
    # ==========================
    with tab_lista:
        # Inicializa estado do painel
        if 'prod_panel_active' not in st.session_state: st.session_state.prod_panel_active = False
        if 'prod_panel_data' not in st.session_state: st.session_state.prod_panel_data = None
        if 'prod_panel_type' not in st.session_state: st.session_state.prod_panel_type = None 
        if 'prod_panel_width' not in st.session_state: st.session_state.prod_panel_width = 40 # Largura padrão

        # Define as colunas dinâmicas
        if st.session_state.prod_panel_active:
            w = st.session_state.prod_panel_width
            col_lista, col_painel = st.columns([100-w, w])
        else:
            col_lista = st.container()
            col_painel = None

        # --- COLUNA ESQUERDA: LISTA ---
        with col_lista:
            df = listar_produtos()
            if not df.empty:
                filtro = st.text_input("🔍 Buscar Produto", placeholder="Nome ou Código", key="search_prod")
                if filtro:
                    df = df[df['nome'].str.contains(filtro, case=False, na=False) | df['codigo'].str.contains(filtro, case=False, na=False)]
                
                st.markdown("---")
                
                for index, row in df.iterrows():
                    status_icon = "🟢" if row['ativo'] else "🔴"
                    # Card do produto
                    with st.container(border=True):
                        st.write(f"**{status_icon} {row['nome']}**")
                        st.caption(f"{row['codigo']} | {row['tipo']}")
                        
                        b_cols = st.columns(5)
                        
                        # Função Helper para ativar painel
                        def ativar_painel(tipo, dados):
                            st.session_state.prod_panel_active = True
                            st.session_state.prod_panel_type = tipo
                            st.session_state.prod_panel_data = dados

                        if b_cols[0].button("📂", key=f"arq_{row['id']}", help="Arquivos"):
                            ativar_painel('arquivos', row)
                            st.rerun()

                        if b_cols[1].button("📖", key=f"inst_{row['id']}", help="Instruções"):
                            ativar_painel('instrucoes', row)
                            st.rerun()
                        
                        if b_cols[2].button("✏️", key=f"edit_{row['id']}", help="Editar"):
                            ativar_painel('editar', row)
                            st.rerun()
                            
                        if b_cols[3].button("🔄", key=f"st_{row['id']}", help="Mudar Status"):
                            alternar_status(row['id'], row['ativo'])
                            st.rerun()
                            
                        if b_cols[4].button("🗑️", key=f"del_{row['id']}", help="Excluir"):
                            excluir_produto(row['id'], row['caminho_pasta'])
                            st.rerun()
            else:
                st.info("Nenhum produto cadastrado.")

        # --- COLUNA DIREITA: PAINEL LATERAL ---
        if col_painel and st.session_state.prod_panel_active:
            with col_painel:
                with st.container(border=True):
                    # Cabeçalho do Painel (Fechar + Slider)
                    cp1, cp2 = st.columns([1, 3])
                    if cp1.button("✖ Fechar", type="primary"):
                        st.session_state.prod_panel_active = False
                        st.rerun()
                    
                    st.session_state.prod_panel_width = cp2.slider(
                        "Largura da Janela (%)", 
                        min_value=20, max_value=90, 
                        value=st.session_state.prod_panel_width,
                        label_visibility="collapsed"
                    )
                    
                    st.divider()
                    
                    # Conteúdo do Painel
                    tipo = st.session_state.prod_panel_type
                    dados = st.session_state.prod_panel_data
                    
                    if tipo == 'arquivos':
                        painel_visualizar_arquivos(dados['caminho_pasta'], dados['nome'])
                    elif tipo == 'instrucoes':
                        painel_visualizar_instrucoes(dados['id'], dados['nome'])
                    elif tipo == 'editar':
                        painel_editar_produto(dados)

if __name__ == "__main__":
    app_produtos()