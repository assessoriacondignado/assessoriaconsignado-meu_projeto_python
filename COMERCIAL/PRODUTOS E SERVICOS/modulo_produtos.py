import streamlit as st
import pandas as pd
import psycopg2
import time  # <--- FALTAVA ESTA IMPORTAÇÃO
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

# --- CONEXÃO COM BANCO (COM AUTOCORREÇÃO) ---
def get_conn():
    try:
        conn = psycopg2.connect(
            host=conexao.host,
            port=conexao.port,
            database=conexao.database,
            user=conexao.user,
            password=conexao.password
        )
        # Autocorreção: Garante que a coluna origem_custo exista
        try:
            cur = conn.cursor()
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='produtos_servicos' AND column_name='origem_custo') THEN 
                        ALTER TABLE produtos_servicos ADD COLUMN origem_custo VARCHAR(100); 
                    END IF; 
                END $$;
            """)
            conn.commit()
            cur.close()
        except:
            conn.rollback()
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
    conn = get_conn()
    lista = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT origem FROM conexoes.fatorconferi_origem_consulta_fator ORDER BY origem ASC")
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
        for file in uploaded_files:
            file_path = os.path.join(caminho_destino, file.name)
            with open(file_path, "wb") as f:
                f.write(file.getbuffer())

# --- FUNÇÕES DE CRUD (BANCO) ---
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
            st.error(f"Erro ao salvar no banco: {e}")
            return None
    return None

def criar_carteira_automatica(id_prod, nome_prod, origem_custo):
    conn = get_conn()
    if not conn: return False, "Erro de conexão"
    try:
        cur = conn.cursor()
        
        # 1. Garante que a tabela existe
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
        
        try:
            cur.execute("ALTER TABLE cliente.carteiras_config ADD COLUMN IF NOT EXISTS origem_custo VARCHAR(100)")
            conn.commit()
        except:
            conn.rollback()

        # 3. Cria a carteira
        nome_carteira = nome_prod
        sufixo = sanitizar_nome_tabela(nome_carteira)
        nome_tabela_dinamica = f"cliente.transacoes_{sufixo}"
        
        sql_create = f"""
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
        """
        cur.execute(sql_create)
        
        sql_insert = """
            INSERT INTO cliente.carteiras_config 
            (id_produto, nome_produto, nome_carteira, nome_tabela_transacoes, status, origem_custo)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql_insert, (id_prod, nome_prod, nome_carteira, nome_tabela_dinamica, 'ATIVO', origem_custo))
        conn.commit()
        conn.close()
        return True, nome_tabela_dinamica
    except Exception as e:
        if conn: conn.close()
        return False, str(e)

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
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}"); return False
    return False

def listar_produtos():
    conn = get_conn()
    if conn:
        try:
            query = "SELECT id, codigo, nome, tipo, preco, data_criacao, caminho_pasta, ativo, resumo, origem_custo FROM produtos_servicos ORDER BY data_criacao DESC"
            df = pd.read_sql(query, conn)
            conn.close(); return df
        except: return pd.DataFrame()
    return pd.DataFrame()

def alternar_status(id_prod, status_atual):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            novo_status = not status_atual
            cur.execute("UPDATE produtos_servicos SET ativo = %s WHERE id = %s", (novo_status, id_prod))
            conn.commit(); conn.close(); return True
        except: return False
    return False

def excluir_produto(id_prod, caminho_pasta):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM produtos_servicos WHERE id = %s", (id_prod,))
            conn.commit(); conn.close()
            if caminho_pasta and os.path.exists(caminho_pasta):
                shutil.rmtree(caminho_pasta)
            return True
        except: return False
    return False

# --- POP-UPS (DIALOGS) ---

@st.dialog("📂 Arquivos do Item")
def dialog_visualizar_arquivos(caminho_pasta, nome_item):
    st.write(f"Arquivos de: **{nome_item}**")
    if caminho_pasta and os.path.exists(caminho_pasta):
        arquivos = os.listdir(caminho_pasta)
        if arquivos:
            st.markdown("---")
            for arquivo in arquivos:
                col_ico, col_nome, col_down = st.columns([0.5, 3, 1.5])
                caminho_completo = os.path.join(caminho_pasta, arquivo)
                with col_ico: st.write("📄")
                with col_nome: st.write(arquivo)
                with col_down:
                    try:
                        with open(caminho_completo, "rb") as f:
                            st.download_button("⬇️ Baixar", data=f, file_name=arquivo, key=f"d_{arquivo}_{uuid.uuid4().hex}")
                    except:
                        st.write("Indisponível")
        else:
            st.warning("Pasta vazia.")
    else:
        st.error("Pasta de arquivos não localizada no servidor.")

@st.dialog("✏️ Editar Item", width="large")
def dialog_editar_produto(dados_atuais):
    st.write(f"Editando: **{dados_atuais['codigo']}**")
    
    lista_origens = listar_origens_custo()
    opcoes_origem = [""] + lista_origens
    
    # Define índice atual da origem
    idx_origem = 0
    valor_atual_origem = dados_atuais.get('origem_custo')
    if valor_atual_origem and valor_atual_origem in lista_origens:
        idx_origem = lista_origens.index(valor_atual_origem) + 1
        
    # Busca carteira vinculada
    carteira_vinculada = None
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT nome_carteira FROM cliente.carteiras_config WHERE id_produto = %s", (dados_atuais['id'],))
            res = cur.fetchone()
            if res: carteira_vinculada = res[0]
            conn.close()
        except: conn.close()

    with st.form("form_editar", clear_on_submit=False):
        c1, c2 = st.columns(2)
        novo_nome = c1.text_input("Nome", value=dados_atuais['nome'])
        
        opcoes_tipo = ["PRODUTO", "SERVIÇO RECORRENTE", "SERVIÇO CRÉDITO"]
        idx_tipo = opcoes_tipo.index(dados_atuais['tipo']) if dados_atuais['tipo'] in opcoes_tipo else 0
        novo_tipo = c2.selectbox("Categoria", opcoes_tipo, index=idx_tipo)
        
        c3, c4, c5 = st.columns(3)
        novo_preco = c3.number_input("Preço (R$)", value=float(dados_atuais['preco'] or 0.0), format="%.2f")
        
        # Campo de Origem
        novo_origem = c4.selectbox("Origem de Custo (Fator)", options=opcoes_origem, index=idx_origem)
        
        # Exibe carteira ou opção de criar
        criar_carteira_check = False
        if carteira_vinculada:
            c5.text_input("Carteira Vinculada", value=carteira_vinculada, disabled=True)
        else:
            c5.warning("Sem Carteira")
            criar_carteira_check = st.checkbox("➕ Criar Carteira Financeira?", help="Cria a tabela de saldo para este produto ao salvar.")
        
        novo_resumo = st.text_area("Resumo", value=dados_atuais['resumo'], height=100)
        
        if st.form_submit_button("💾 Salvar Alterações"):
            # 1. Atualiza o produto
            atualizou = atualizar_produto_db(dados_atuais['id'], novo_nome, novo_tipo, novo_resumo, novo_preco, novo_origem)
            
            # 2. Cria a carteira se foi marcado e se a atualização deu certo
            msg_extra = ""
            if atualizou and criar_carteira_check:
                ok_c, msg_c = criar_carteira_automatica(dados_atuais['id'], novo_nome, novo_origem)
                if ok_c: msg_extra = " | Carteira Criada!"
                else: st.error(f"Erro ao criar carteira: {msg_c}")
            
            if atualizou:
                st.success(f"Atualizado com sucesso!{msg_extra}")
                time.sleep(1.5)
                st.rerun()

@st.dialog("📝 Novo Cadastro", width="large")
def dialog_novo_cadastro():
    st.write("Novo item")
    lista_origens = listar_origens_custo()
    opcoes_origem = [""] + lista_