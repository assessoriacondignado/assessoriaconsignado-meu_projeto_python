import streamlit as st
import pandas as pd
import psycopg2
import time
from datetime import datetime
import conexao

# --- CORREÇÃO: Importação com o nome EXATO informado ---
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

def atualizar_conexao(id_con, nome, tipo, desc, user, senha, key, status):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            # Lógica: Se a senha for vazia, não atualiza o campo senha (mantém a antiga)
            if senha:
                sql = """
                    UPDATE conexoes.relacao 
                    SET nome_conexao=%s, tipo_conexao=%s, descricao=%s, usuario_conexao=%s, senha_conexao=%s, key_conexao=%s, status=%s
                    WHERE id=%s
                """
                cur.execute(sql, (nome, tipo, desc, user, senha, key, status, id_con))
            else:
                sql = """
                    UPDATE conexoes.relacao 
                    SET nome_conexao=%s, tipo_conexao=%s, descricao=%s, usuario_conexao=%s, key_conexao=%s, status=%s
                    WHERE id=%s
                """
                cur.execute(sql, (nome, tipo, desc, user, key, status, id_con))
                
            conn.commit(); conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}"); conn.close()
    return False

def excluir_conexao_db(id_con):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM conexoes.relacao WHERE id = %s", (id_con,))
            conn.commit(); conn.close()
            return True
        except: conn.close()
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
        
        if st.form_submit_button("💾 Salvar"):
            if nome:
                if salvar_conexao(nome, tipo, desc, user, senha, key, status):
                    st.success("Salvo!"); time.sleep(1); st.rerun()
            else: st.warning("Nome obrigatório.")

@st.dialog("✏️ Editar Conexão")
def dialog_editar_conexao(dados):
    st.markdown(f"Editando: **{dados['nome_conexao']}**")
    with st.form("form_edit_con"):
        nome = st.text_input("Nome", value=dados['nome_conexao'])
        tipo = st.selectbox("Tipo", ["SAIDA", "ENTRADA", "API", "BANCO DE DADOS"], index=["SAIDA", "ENTRADA", "API", "BANCO DE DADOS"].index(dados['tipo_conexao']) if dados['tipo_conexao'] in ["SAIDA", "ENTRADA", "API", "BANCO DE DADOS"] else 0)
        desc = st.text_area("Descrição", value=dados['descricao'] or "")
        
        st.divider()
        st.markdown("🔐 **Credenciais**")
        c1, c2 = st.columns(2)
        user = c1.text_input("Usuário", value=dados['usuario_conexao'] or "")
        senha = c2.text_input("Senha (Deixe vazio para manter)", type="password", help="Preencha apenas se quiser alterar a senha.")
        key = st.text_input("Key / Token", value=dados['key_conexao'] or "")
        
        status = st.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if dados['status'] == "ATIVO" else 1)
        
        if st.form_submit_button("💾 Atualizar Dados"):
            if atualizar_conexao(dados['id'], nome, tipo, desc, user, senha, key, status):
                st.success("Atualizado!"); time.sleep(1); st.rerun()

@st.dialog("🚨 Confirmar Exclusão")
def dialog_excluir_conexao(id_con, nome):
    st.warning(f"Tem certeza que deseja excluir a conexão **{nome}**?")
    st.caption("Esta ação não pode ser desfeita.")
    
    col_sim, col_nao = st.columns(2)
    
    if col_sim.button("✅ Sim, Excluir", type="primary", use_container_width=True):
        if excluir_conexao_db(id_con):
            st.success("Conexão removida.")
            time.sleep(1)
            st.rerun()
            
    if col_nao.button("❌ Cancelar", use_container_width=True):
        st.rerun()

# --- INTERFACE PRINCIPAL ---
def app_conexoes():
    # [LÓGICA DE NAVEGAÇÃO] Verifica se deve mostrar o Painel Fator
    if st.session_state.get('navegacao_conexoes') == 'FATOR_CONFERI':
        if st.button("⬅️ Voltar para Lista de Conexões"):
            st.session_state['navegacao_conexoes'] = None
            st.rerun()
        
        # --- CORREÇÃO: Uso do nome correto do módulo ---
        if modulo_fator_conferi:
            modulo_fator_conferi.app_fator_conferi()
        else:
            st.error("Módulo 'modulo_fator_conferi.py' não encontrado. Verifique se o arquivo existe na pasta.")
        return 

    # --- TELA: LISTA DE CONEXÕES ---
    st.markdown("## 🔌 Módulo de Conexões")
    
    # Filtros e Botão Superior
    c_filtros, c_btn = st.columns([5, 1])
    with c_filtros:
        col_tipo, col_busca = st.columns([1, 2])
        filtro_tipo = col_tipo.selectbox("Filtrar Tipo", ["Todos", "SAIDA", "ENTRADA", "API", "BANCO DE DADOS"])
        busca = col_busca.text_input("Buscar Conexão", placeholder="Nome ou descrição...")
    
    with c_btn:
        st.write("") 
        if st.button("➕ Nova", type="primary", use_container_width=True):
            dialog_nova_conexao()

    st.divider()

    # Listagem
    df = listar_conexoes(filtro_tipo, busca)
    
    if not df.empty:
        # Cabeçalho Visual da Tabela
        st.markdown("""
        <div style="display: flex; font-weight: bold; color: #555; margin-bottom: 5px; padding-left: 10px; font-size: 0.9em;">
            <div style="flex: 4;">Nome da Conexão</div>
            <div style="width: 100px; text-align: center;">Status</div>
            <div style="width: 100px; text-align: center;">Painel</div>
            <div style="width: 80px; text-align: center;">Editar</div>
            <div style="width: 80px; text-align: center;">Excluir</div>
        </div>
        """, unsafe_allow_html=True)
        
        for _, row in df.iterrows():
            with st.container(border=True):
                # Layout em Colunas: Nome (Largo) | Status | Painel | Editar | Excluir
                c_nome, c_status, c_painel, c_edit, c_del = st.columns([4, 1, 1, 0.8, 0.8])
                
                # 1. Nome e Descrição
                with c_nome:
                    st.markdown(f"**{row['nome_conexao']}**")
                    if row['descricao']:
                        st.caption(row['descricao'])

                # 2. Status (Badge)
                with c_status:
                    if row['status'] == 'ATIVO':
                        st.markdown("<div style='text-align:center; color:green; font-weight:bold; font-size:0.8em;'>🟢 ATIVO</div>", unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='text-align:center; color:red; font-weight:bold; font-size:0.8em;'>🔴 INATIVO</div>", unsafe_allow_html=True)

                # 3. Botão Painel
                with c_painel:
                    if "FATOR" in row['nome_conexao'].upper():
                        if st.button("🚀", key=f"btn_p_{row['id']}", help="Acessar Painel"):
                            st.session_state['navegacao_conexoes'] = 'FATOR_CONFERI'
                            st.rerun()
                    else:
                        st.markdown("<div style='text-align:center; color:#ccc;'>-</div>", unsafe_allow_html=True)

                # 4. Botão Editar
                with c_edit:
                    if st.button("✏️", key=f"btn_e_{row['id']}", help="Editar Dados e Credenciais"):
                        dialog_editar_conexao(row)

                # 5. Botão Excluir
                with c_del:
                    if st.button("🗑️", key=f"btn_d_{row['id']}", help="Excluir Conexão"):
                        dialog_excluir_conexao(row['id'], row['nome_conexao'])

    else:
        st.info(f"Nenhuma conexão encontrada.")