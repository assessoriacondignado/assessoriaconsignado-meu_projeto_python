import streamlit as st
import pandas as pd
import psycopg2
import os
import shutil
import uuid
import re
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

def sanitizar_nome_tabela(nome):
    s = str(nome).lower().strip()
    s = re.sub(r'[^a-z0-9]', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.strip('_')

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

def criar_carteira_automatica(id_prod, nome_prod, origem_custo):
    conn = get_conn()
    if not conn: return False, "Erro conexão"
    try:
        cur = conn.cursor()
        # Cria a tabela de configuração de carteiras se não existir
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cliente.carteiras_config (
                id SERIAL PRIMARY KEY,
                id_produto INTEGER,
                nome_produto VARCHAR(255),
                nome_carteira VARCHAR(255),
                nome_tabela_transacoes VARCHAR(255),
                status VARCHAR(50) DEFAULT 'ATIVO',
                data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                origem_custo VARCHAR(100)
            );
        """)
        
        nome_carteira = nome_prod
        sufixo = sanitizar_nome_tabela(nome_carteira)
        nome_tabela_dinamica = f"cliente.transacoes_{sufixo}"
        
        # Cria a tabela dinâmica de transações
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {nome_tabela_dinamica} (
                id SERIAL PRIMARY KEY,
                cpf_cliente VARCHAR(20),
                nome_cliente VARCHAR(255),
                motivo VARCHAR(255),
                origem_lancamento VARCHAR(100),
                data_transacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tipo_lancamento VARCHAR(50),
                valor NUMERIC(10, 2),
                saldo_anterior NUMERIC(10, 2),
                saldo_novo NUMERIC(10, 2)
            );
        """)
        
        cur.execute("""
            INSERT INTO cliente.carteiras_config 
            (id_produto, nome_produto, nome_carteira, nome_tabela_transacoes, status, origem_custo)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (id_prod, nome_prod, nome_carteira, nome_tabela_dinamica, 'ATIVO', origem_custo))
        
        conn.commit()
        conn.close()
        return True, "Carteira criada"
    except Exception as e:
        if conn: conn.close()
        return False, str(e)

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

# --- FUNÇÃO PRINCIPAL (CORREÇÃO DO NOME) ---
def app_produtos():
    if 'view_prod' not in st.session_state:
        st.session_state['view_prod'] = 'lista'

    # --- LISTA ---
    if st.session_state['view_prod'] == 'lista':
        c1, c2 = st.columns([6, 1])
        c1.markdown("### 📦 Produtos e Serviços")
        if c2.button("➕ Novo", type="primary"):
            st.session_state['view_prod'] = 'novo'
            st.rerun()
        
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
                        st.session_state['view_prod'] = 'editar'
                        st.session_state['prod_id'] = row['id']
                        st.rerun()
                        
                    if b3.button("🔄 Status", key=f"st_{row['id']}"):
                        alternar_status(row['id'], row['ativo'])
                        st.rerun()
                        
                    if b4.button("🗑️ Excluir", key=f"del_{row['id']}"):
                        excluir_produto(row['id'], row['caminho_pasta'])
                        st.rerun()
        else:
            st.info("Nenhum produto cadastrado.")

    # --- NOVO / EDITAR ---
    elif st.session_state['view_prod'] in ['novo', 'editar']:
        st.markdown(f"### {'📝 Novo Produto' if st.session_state['view_prod']=='novo' else '✏️ Editar Produto'}")
        
        dados = {}
        if st.session_state['view_prod'] == 'editar':
            df_atual = listar_produtos()
            if not df_atual.empty:
                filtro_id = df_atual[df_atual['id'] == st.session_state['prod_id']]
                if not filtro_id.empty: dados = filtro_id.iloc[0]

        lista_origens = listar_origens_custo()
        opcoes_origem = [""] + lista_origens

        with st.form("form_produto"):
            c1, c2 = st.columns(2)
            nome = c1.text_input("Nome *", value=dados.get('nome', ''))
            tipo = c2.selectbox("Tipo", ["PRODUTO", "SERVIÇO RECORRENTE", "SERVIÇO CRÉDITO"], index=0) # Index logic omitted for brevity
            
            c3, c4 = st.columns(2)
            preco = c3.number_input("Preço (R$)", value=float(dados.get('preco') or 0.0), format="%.2f")
            
            idx_orig = 0
            if dados.get('origem_custo') in lista_origens:
                idx_orig = lista_origens.index(dados.get('origem_custo')) + 1
            origem = c4.selectbox("Origem de Custo", options=opcoes_origem, index=idx_orig)
            
            resumo = st.text_area("Resumo", value=dados.get('resumo', ''))
            
            arquivos = None
            if st.session_state['view_prod'] == 'novo':
                arquivos = st.file_uploader("Arquivos Iniciais", accept_multiple_files=True)
                criar_cart = st.checkbox("Criar Carteira Financeira?", value=True)

            st.divider()
            col_save, col_cancel = st.columns([1, 6])
            
            if col_save.form_submit_button("💾 Salvar"):
                if nome:
                    if st.session_state['view_prod'] == 'novo':
                        codigo = gerar_codigo_automatico()
                        caminho = criar_pasta_produto(codigo, nome)
                        if arquivos: salvar_arquivos(arquivos, caminho)
                        
                        nid = cadastrar_produto_db(codigo, nome, tipo, resumo, preco, caminho, origem)
                        if nid and criar_cart:
                            criar_carteira_automatica(nid, nome, origem)
                        st.success("Cadastrado!")
                    else:
                        atualizar_produto_db(dados['id'], nome, tipo, resumo, preco, origem)
                        st.success("Atualizado!")
                    
                    time.sleep(1)
                    st.session_state['view_prod'] = 'lista'
                    st.rerun()
                else:
                    st.warning("Preencha o nome.")
            
            if col_cancel.form_submit_button("Cancelar"):
                st.session_state['view_prod'] = 'lista'
                st.rerun()

if __name__ == "__main__":
    app_produtos()