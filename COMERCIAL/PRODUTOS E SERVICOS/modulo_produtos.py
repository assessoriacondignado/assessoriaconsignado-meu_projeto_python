import streamlit as st
import pandas as pd
import psycopg2
import os
import shutil
import uuid
import re
from datetime import datetime
import conexao

# --- CONFIGURAÇÕES DE DIRETÓRIO (AJUSTADO PARA SERVIDOR LINUX) ---
# Caminho absoluto baseado na localização deste arquivo
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Garante que o diretório exista (Sem fallback para /tmp)
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

def sanitizar_nome_tabela(nome):
    """Remove caracteres especiais e espaços para criar nomes de tabelas SQL seguros."""
    s = str(nome).lower().strip()
    s = re.sub(r'[^a-z0-9]', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.strip('_')

# --- FUNÇÕES DE ARQUIVO E PASTA ---
def criar_pasta_produto(codigo, nome):
    data_str = datetime.now().strftime("%Y-%m-%d")
    nome_pasta = f"{codigo} - {nome} - {data_str}"
    # Sanitização do nome da pasta
    nome_pasta = "".join(c for c in nome_pasta if c.isalnum() or c in (' ', '-', '_')).strip()
    
    # Cria subpasta para armazenar os arquivos do produto
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
def cadastrar_produto_db(codigo, nome, tipo, resumo, preco, caminho_pasta):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            # ATUALIZADO: Retorna o ID do produto criado
            query = """
                INSERT INTO produtos_servicos (codigo, nome, tipo, resumo, preco, caminho_pasta, data_criacao, ativo)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), TRUE)
                RETURNING id
            """
            cur.execute(query, (codigo, nome, tipo, resumo, preco, caminho_pasta))
            novo_id = cur.fetchone()[0]
            conn.commit()
            conn.close()
            return novo_id
        except Exception as e:
            st.error(f"Erro ao salvar no banco: {e}")
            return None
    return None

def criar_carteira_automatica(id_prod, nome_prod):
    """
    Cria automaticamente a estrutura de carteira para o novo produto.
    Réplica da lógica do modulo_cliente.py
    """
    conn = get_conn()
    if not conn: return False, "Erro de conexão"
    
    try:
        cur = conn.cursor()
        
        # 1. Garante que a tabela de configuração existe (caso seja o primeiro uso)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cliente.carteiras_config (
                id SERIAL PRIMARY KEY,
                id_produto INTEGER,
                nome_produto VARCHAR(255),
                nome_carteira VARCHAR(255),
                nome_tabela_transacoes VARCHAR(255),
                status VARCHAR(50) DEFAULT 'ATIVO',
                data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 2. Gera nome da tabela dinâmica
        # Usa o nome do produto como base para o nome da carteira e tabela
        nome_carteira = nome_prod
        sufixo = sanitizar_nome_tabela(nome_carteira)
        nome_tabela_dinamica = f"cliente.transacoes_{sufixo}"
        
        # 3. Cria a Tabela Dinâmica de Transações
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
        
        # 4. Registra na tabela de configuração
        sql_insert = """
            INSERT INTO cliente.carteiras_config 
            (id_produto, nome_produto, nome_carteira, nome_tabela_transacoes, status)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(sql_insert, (id_prod, nome_prod, nome_carteira, nome_tabela_dinamica, 'ATIVO'))
        
        conn.commit()
        conn.close()
        return True, nome_tabela_dinamica
    except Exception as e:
        if conn: conn.close()
        return False, str(e)

def atualizar_produto_db(id_prod, nome, tipo, resumo, preco):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            query = """
                UPDATE produtos_servicos 
                SET nome=%s, tipo=%s, resumo=%s, preco=%s, data_atualizacao=NOW() 
                WHERE id=%s
            """
            cur.execute(query, (nome, tipo, resumo, preco, id_prod))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}")
            return False
    return False

def listar_produtos():
    conn = get_conn()
    if conn:
        try:
            query = "SELECT id, codigo, nome, tipo, preco, data_criacao, caminho_pasta, ativo, resumo FROM produtos_servicos ORDER BY data_criacao DESC"
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except:
            return pd.DataFrame()
    return pd.DataFrame()

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
        except: return False
    return False

def excluir_produto(id_prod, caminho_pasta):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM produtos_servicos WHERE id = %s", (id_prod,))
            conn.commit()
            conn.close()
            
            # Remove a pasta física do servidor
            if caminho_pasta and os.path.exists(caminho_pasta):
                shutil.rmtree(caminho_pasta)
            return True
        except Exception as e:
            st.error(f"Erro ao excluir: {e}")
            return False
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

@st.dialog("✏️ Editar Item")
def dialog_editar_produto(dados_atuais):
    st.write(f"Editando: **{dados_atuais['codigo']}**")
    with st.form("form_editar", clear_on_submit=False):
        novo_nome = st.text_input("Nome", value=dados_atuais['nome'])
        opcoes_tipo = ["PRODUTO", "SERVIÇO RECORRENTE", "SERVIÇO CRÉDITO"]
        idx_tipo = opcoes_tipo.index(dados_atuais['tipo']) if dados_atuais['tipo'] in opcoes_tipo else 0
        novo_tipo = st.selectbox("Categoria", opcoes_tipo, index=idx_tipo)
        novo_preco = st.number_input("Preço (R$)", value=float(dados_atuais['preco'] or 0.0), format="%.2f")
        novo_resumo = st.text_area("Resumo", value=dados_atuais['resumo'], height=100)
        
        if st.form_submit_button("💾 Salvar Alterações"):
            if atualizar_produto_db(dados_atuais['id'], novo_nome, novo_tipo, novo_resumo, novo_preco):
                st.success("Atualizado com sucesso!")
                st.rerun()

@st.dialog("📝 Novo Cadastro")
def dialog_novo_cadastro():
    st.write("Novo item")
    with st.form("form_cadastro_popup", clear_on_submit=True):
        nome = st.text_input("Nome")
        tipo = st.selectbox("Categoria", ["PRODUTO", "SERVIÇO RECORRENTE", "SERVIÇO CRÉDITO"])
        preco = st.number_input("Preço (R$) (Opcional)", min_value=0.0, format="%.2f")
        arquivos = st.file_uploader("Arquivos", accept_multiple_files=True)
        resumo = st.text_area("Resumo", height=100)
        
        st.divider()
        st.markdown("##### ⚙️ Configurações Automáticas")
        # Regra 2.5: Aviso em tela para confirmar criação
        criar_cart = st.checkbox("✅ Criar Carteira Financeira Automaticamente?", value=True, 
                                 help="Se marcado, cria automaticamente a tabela de saldo para este produto.")
        if criar_cart:
            st.caption("ℹ️ Uma nova carteira será criada com o mesmo nome do produto.")
        
        if st.form_submit_button("💾 Salvar"):
            if nome:
                codigo_auto = gerar_codigo_automatico()
                caminho = criar_pasta_produto(codigo_auto, nome)
                
                # Salva arquivos antes de registrar no banco
                if arquivos: salvar_arquivos(arquivos, caminho)
                
                # Cadastra e recupera ID
                novo_id = cadastrar_produto_db(codigo_auto, nome, tipo, resumo, preco, caminho)
                
                if novo_id:
                    msg_sucesso = f"Produto criado: {codigo_auto}"
                    
                    # Criação automática de carteira (Regra 2.1)
                    if criar_cart:
                        ok_cart, msg_cart = criar_carteira_automatica(novo_id, nome)
                        if ok_cart:
                            msg_sucesso += f"\n\n + Carteira Financeira criada com sucesso!"
                        else:
                            st.error(f"Erro ao criar carteira: {msg_cart}")
                    
                    st.success(msg_sucesso)
                    time.sleep(2)
                    st.rerun()
                else: st.error("Erro ao salvar no banco.")
            else: st.warning("Nome obrigatório.")

# --- INTERFACE PRINCIPAL ---
def app_produtos():
    st.markdown("## 📦 Módulo Produtos e Serviços")
    
    col_head1, col_head2 = st.columns([6, 1])
    with col_head2:
        if st.button("➕ Novo", help="Cadastrar novo item"):
            dialog_novo_cadastro()

    st.markdown("---")
    
    df = listar_produtos()
    if not df.empty:
        col_f1, col_f2 = st.columns(2)
        with col_f1: filtro_nome = st.text_input("🔎 Pesquisar")
        with col_f2: filtro_tipo = st.multiselect("Filtrar Categoria", df['tipo'].unique())

        if filtro_nome:
            df = df[df['nome'].str.contains(filtro_nome, case=False) | df['codigo'].str.contains(filtro_nome, case=False)]
        if filtro_tipo:
            df = df[df['tipo'].isin(filtro_tipo)]

        for index, row in df.iterrows():
            status_cor = "🟢" if row['ativo'] else "🔴"
            with st.expander(f"{status_cor} {row['nome']} ({row['codigo']})"):
                st.markdown(f"**Categoria:** {row['tipo']} | **Preço:** R$ {row['preco']:.2f}")
                st.markdown(f"**Resumo:** {row['resumo']}")
                st.markdown("---")
                
                col_folder, col_actions = st.columns([1, 1])
                with col_folder:
                     if st.button(f"📂 Arquivos", key=f"f_{row['id']}"):
                        dialog_visualizar_arquivos(row['caminho_pasta'], row['nome'])

                with col_actions:
                    b1, b2, b3 = st.columns(3)
                    with b1:
                        if st.button("✏️", key=f"ed_{row['id']}", help="Editar"):
                            dialog_editar_produto(row)
                    with b2:
                        if st.button("🔄", key=f"st_{row['id']}", help="Alterar Status"):
                            if alternar_status(row['id'], row['ativo']):
                                st.rerun()
                    with b3:
                        if st.button("🗑️", key=f"del_{row['id']}", help="Excluir"):
                            if excluir_produto(row['id'], row['caminho_pasta']):
                                st.warning("Item removido.")
                                st.rerun()
    else:
        st.info("Nenhum item encontrado no banco de dados.")

if __name__ == "__main__":
    app_produtos()