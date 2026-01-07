import streamlit as st
import pandas as pd
import psycopg2
import time

# Tenta importar conexao
try:
    import conexao
except ImportError:
    st.error("Erro: conexao.py não encontrado na raiz.")

# --- CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database, 
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        print(f"Erro conexão: {e}")
        return None

# =============================================================================
# 1. FUNÇÃO CORE DE VERIFICAÇÃO (USADA POR TODO O SISTEMA)
# =============================================================================

def verificar_bloqueio_de_acesso(chave, caminho_atual="Desconhecido", parar_se_bloqueado=False, nome_regra_codigo=None):
    """
    Verifica se o usuário logado tem permissão para acessar determinado recurso (chave).
    Retorna True se estiver BLOQUEADO.
    """
    if nome_regra_codigo:
        chave = nome_regra_codigo

    # Se não estiver logado ou admin, comportamento padrão pode variar. 
    # Aqui assumimos que se não logou, bloqueia.
    if not st.session_state.get('logado'):
        return True 

    conn = get_conn()
    if not conn: return False # Se falhar conexão, libera ou bloqueia? (Aqui libera para não travar erro)
    
    try:
        cur = conn.cursor()
        
        nivel_usuario_nome = st.session_state.get('usuario_cargo', '') 
        if not nivel_usuario_nome:
            nivel_usuario_nome = 'Cliente sem permissão'

        # Busca ID do nível do usuário
        cur.execute("SELECT id FROM permissão.permissão_grupo_nivel WHERE nivel = %s", (nivel_usuario_nome,))
        res_nivel = cur.fetchone()
        
        if not res_nivel:
            conn.close(); return False 
            
        id_nivel_usuario = str(res_nivel[0])

        # Busca regras ativas
        cur.execute("""
            SELECT id, chave, nivel, status, caminho_bloqueio, nome_regra
            FROM permissão.permissão_usuario_regras_nível 
            WHERE status = 'SIM'
        """)
        regras_ativas = cur.fetchall()
        
        bloqueado = False
        regra_aplicada = None
        id_regra_aplicada = None
        caminho_registrado = None

        for row in regras_ativas:
            rid, r_chave_db, r_niveis_bloqueados, r_status, r_caminho, r_nome = row
            
            lista_chaves_db = [k.strip() for k in str(r_chave_db).split(';') if k.strip()]
            
            if chave in lista_chaves_db:
                lista_niveis = [n.strip() for n in str(r_niveis_bloqueados).split(';') if n.strip()]
                
                if id_nivel_usuario in lista_niveis:
                    bloqueado = True
                    regra_aplicada = r_nome
                    id_regra_aplicada = rid
                    caminho_registrado = r_caminho
                    break 
        
        if bloqueado:
            # Registra onde ocorreu o bloqueio se ainda não tiver registrado
            if not caminho_registrado and id_regra_aplicada:
                cur.execute("""
                    UPDATE permissão.permissão_usuario_regras_nível 
                    SET caminho_bloqueio = %s 
                    WHERE id = %s
                """, (caminho_atual, id_regra_aplicada))
                conn.commit()
            
            conn.close()
            
            if parar_se_bloqueado:
                st.error("🚫 ACESSO NEGADO / PERMISSÃO INSUFICIENTE")
                st.caption(f"Regra de bloqueio: {regra_aplicada}")
                st.stop()
                
            return True
            
        conn.close()
        return False

    except Exception as e:
        print(f"Erro verificação permissão: {e}")
        if conn: conn.close()
        return False

# =============================================================================
# 2. CRUDs DE ESTRUTURA (NÍVEIS, CHAVES, CATEGORIAS)
# =============================================================================

def listar_permissoes_nivel():
    conn = get_conn(); 
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, nivel FROM permissão.permissão_grupo_nivel ORDER BY id", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_permissao_nivel(nome):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("INSERT INTO permissão.permissão_grupo_nivel (nivel) VALUES (%s)", (nome,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def atualizar_permissao_nivel(id_reg, novo_nome):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("UPDATE permissão.permissão_grupo_nivel SET nivel = %s WHERE id = %s", (novo_nome, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_permissao_nivel(id_reg):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("DELETE FROM permissão.permissão_grupo_nivel WHERE id = %s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

# --- CHAVES ---
def listar_permissoes_chave():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, chave FROM permissão.permissão_usuario_cheve ORDER BY id", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_permissao_chave(nome):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("INSERT INTO permissão.permissão_usuario_cheve (chave) VALUES (%s)", (nome,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def atualizar_permissao_chave(id_reg, novo_nome):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("UPDATE permissão.permissão_usuario_cheve SET chave = %s WHERE id = %s", (novo_nome, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_permissao_chave(id_reg):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("DELETE FROM permissão.permissão_usuario_cheve WHERE id = %s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

# --- CATEGORIAS ---
def listar_permissoes_categoria():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, categoria FROM permissão.permissão_usuario_categoria ORDER BY id", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_permissao_categoria(nome):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("INSERT INTO permissão.permissão_usuario_categoria (categoria) VALUES (%s)", (nome,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def atualizar_permissao_categoria(id_reg, novo_nome):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("UPDATE permissão.permissão_usuario_categoria SET categoria = %s WHERE id = %s", (novo_nome, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_permissao_categoria(id_reg):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor(); cur.execute("DELETE FROM permissão.permissão_usuario_categoria WHERE id = %s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

# =============================================================================
# 3. CRUDs DE REGRAS DE BLOQUEIO
# =============================================================================

def listar_regras_bloqueio():
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT * FROM permissão.permissão_usuario_regras_nível ORDER BY id", conn)
        conn.close(); return df
    except: conn.close(); return pd.DataFrame()

def salvar_regra_bloqueio(nome, chave, niveis_ids_str, categoria, status, descricao):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO permissão.permissão_usuario_regras_nível 
            (nome_regra, chave, nivel, categoria, status, descricao) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (nome, chave, niveis_ids_str, categoria, status, descricao))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def atualizar_regra_bloqueio(id_reg, nome, chave, niveis_ids_str, categoria, status, descricao):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE permissão.permissão_usuario_regras_nível 
            SET nome_regra=%s, chave=%s, nivel=%s, categoria=%s, status=%s, descricao=%s
            WHERE id=%s
        """, (nome, chave, niveis_ids_str, categoria, status, descricao, id_reg))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

def excluir_regra_bloqueio(id_reg):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM permissão.permissão_usuario_regras_nível WHERE id=%s", (id_reg,))
        conn.commit(); conn.close(); return True
    except: conn.close(); return False

# --- DIALOGS DE EDIÇÃO ---

@st.dialog("✏️ Editar Regra")
def dialog_editar_regra_bloqueio(regra):
    st.caption(f"ID: {regra['id']}")
    df_niveis = listar_permissoes_nivel()
    
    with st.form("form_edit_regra"):
        n_nome = st.text_input("Nome Regra", value=regra['nome_regra'])
        n_chave = st.text_input("Chave(s)", value=regra['chave'], help="Separe por ;")
        n_cat = st.text_input("Categoria", value=regra['categoria'])
        n_desc = st.text_area("Descrição", value=regra['descricao'])
        n_status = st.selectbox("Status", ["SIM", "NÃO"], index=0 if regra['status'] == "SIM" else 1)
        
        # Lógica para multiselect dos níveis
        ids_salvos = [int(x) for x in str(regra['nivel']).split(';') if x.strip().isdigit()]
        opcoes_nomes = df_niveis['nivel'].tolist() if not df_niveis.empty else []
        mapa_id_nome = dict(zip(df_niveis['id'], df_niveis['nivel'])) if not df_niveis.empty else {}
        mapa_nome_id = dict(zip(df_niveis['nivel'], df_niveis['id'])) if not df_niveis.empty else {}
        
        nomes_selecionados = [mapa_id_nome[i] for i in ids_salvos if i in mapa_id_nome]
        sel_niveis = st.multiselect("Bloquear Níveis:", options=opcoes_nomes, default=nomes_selecionados)
        
        if st.form_submit_button("Salvar"):
            ids_finais = [str(mapa_nome_id[n]) for n in sel_niveis]
            str_ids_finais = ";".join(ids_finais)
            if atualizar_regra_bloqueio(regra['id'], n_nome, n_chave, str_ids_finais, n_cat, n_status, n_desc):
                st.success("Salvo!"); time.sleep(0.5); st.rerun()
            else: st.error("Erro.")

@st.dialog("✏️ Editar Item")
def dialog_editar_generico(tipo, id_reg, valor_atual):
    with st.form(f"form_ed_{tipo}"):
        novo = st.text_input(f"Novo Nome ({tipo})", value=valor_atual)
        if st.form_submit_button("Salvar"):
            res = False
            if tipo == "Nível": res = atualizar_permissao_nivel(id_reg, novo)
            elif tipo == "Chave": res = atualizar_permissao_chave(id_reg, novo)
            elif tipo == "Categoria": res = atualizar_permissao_categoria(id_reg, novo)
            
            if res: st.success("Ok!"); time.sleep(0.5); st.rerun()
            else: st.error("Erro.")

# =============================================================================
# 4. APP PRINCIPAL DO MÓDULO
# =============================================================================

def app_permissoes():
    st.markdown("## 🛡️ Central de Permissões")
    
    tab_regras, tab_estrut = st.tabs(["🚫 Regras de Bloqueio", "🏗️ Estrutura (Níveis/Chaves)"])

    with tab_regras:
        st.info("Defina aqui quem **NÃO** pode acessar o quê. Se o nível do usuário estiver na lista, ele será bloqueado.")
        
        if st.button("➕ Nova Regra", type="primary"):
            salvar_regra_bloqueio("Nova Regra", "", "", "", "SIM", "")
            st.rerun()

        df_regras = listar_regras_bloqueio()
        if not df_regras.empty:
            st.markdown("""<div style="display: flex; font-weight: bold; background: #e9ecef; padding: 5px; font-size:0.9em;">
            <div style="flex:2;">Regra</div><div style="flex:2;">Chave</div><div style="flex:2;">Bloqueados (IDs)</div>
            <div style="flex:1;">Status</div><div style="flex:1;">Ações</div></div>""", unsafe_allow_html=True)
            
            for _, r in df_regras.iterrows():
                with st.container():
                    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 1, 1])
                    c1.write(r['nome_regra'])
                    c2.code(r['chave'])
                    c3.write(r['nivel']) 
                    c4.write(r['status'])
                    with c5:
                        if st.button("✏️", key=f"ed_rg_{r['id']}"): dialog_editar_regra_bloqueio(r)
                        if st.button("🗑️", key=f"del_rg_{r['id']}"): 
                            excluir_regra_bloqueio(r['id']); st.rerun()
                st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
        else:
            st.warning("Nenhuma regra cadastrada.")

    with tab_estrut:
        c1, c2, c3 = st.columns(3)
        
        with c1:
            st.markdown("#### 📶 Níveis")
            with st.expander("Gerenciar Níveis", expanded=True):
                n_n = st.text_input("Novo Nível", key="n_n_in")
                if st.button("Add Nível"): 
                    if n_n: salvar_permissao_nivel(n_n); st.rerun()
                
                df_n = listar_permissoes_nivel()
                if not df_n.empty:
                    for _, r in df_n.iterrows():
                        xc1, xc2, xc3 = st.columns([6, 1, 1])
                        xc1.write(f"{r['id']} - {r['nivel']}")
                        if xc2.button("✎", key=f"e_n_{r['id']}"): dialog_editar_generico("Nível", r['id'], r['nivel'])
                        if xc3.button("x", key=f"d_n_{r['id']}"): excluir_permissao_nivel(r['id']); st.rerun()

        with c2:
            st.markdown("#### 🔑 Chaves")
            with st.expander("Gerenciar Chaves", expanded=True):
                n_c = st.text_input("Nova Chave", key="n_c_in")
                if st.button("Add Chave"): 
                    if n_c: salvar_permissao_chave(n_c); st.rerun()
                
                df_c = listar_permissoes_chave()
                if not df_c.empty:
                    for _, r in df_c.iterrows():
                        xc1, xc2, xc3 = st.columns([6, 1, 1])
                        xc1.write(f"{r['chave']}")
                        if xc2.button("✎", key=f"e_c_{r['id']}"): dialog_editar_generico("Chave", r['id'], r['chave'])
                        if xc3.button("x", key=f"d_c_{r['id']}"): excluir_permissao_chave(r['id']); st.rerun()

        with c3:
            st.markdown("#### 🗂️ Categorias")
            with st.expander("Gerenciar Cat.", expanded=True):
                n_ct = st.text_input("Nova Categoria", key="n_ct_in")
                if st.button("Add Categoria"): 
                    if n_ct: salvar_permissao_categoria(n_ct); st.rerun()
                
                df_ct = listar_permissoes_categoria()
                if not df_ct.empty:
                    for _, r in df_ct.iterrows():
                        xc1, xc2, xc3 = st.columns([6, 1, 1])
                        xc1.write(f"{r['categoria']}")
                        if xc2.button("✎", key=f"e_ct_{r['id']}"): dialog_editar_generico("Categoria", r['id'], r['categoria'])
                        if xc3.button("x", key=f"d_ct_{r['id']}"): excluir_permissao_categoria(r['id']); st.rerun()