import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import pool
import time
import re
import bcrypt
import sys
import os
import contextlib

# ==============================================================================
# 0. CONFIGURAÇÃO DE CAMINHOS
# ==============================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    import conexao
except ImportError:
    st.error("Erro: conexao.py não encontrado na raiz.")
    conexao = None

try:
    import modulo_validadores as v
except ImportError:
    st.error("Erro: modulo_validadores.py não encontrado.")
    v = None

# ==============================================================================
# 1. CONEXÃO BLINDADA (Connection Pool + Retry Logic)
# ==============================================================================

@st.cache_resource
def get_pool():
    if not conexao: return None
    try:
        # [ALTERAÇÃO AQUI] Mudança para ThreadedConnectionPool e aumento de capacidade
        return psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=30, 
            host=conexao.host, port=conexao.port,
            database=conexao.database, user=conexao.user, password=conexao.password,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5
        )
    except Exception as e:
        st.error(f"Erro fatal no Pool de Conexão: {e}")
        return None

@contextlib.contextmanager
def get_db_connection():
    """
    Gerenciador de contexto robusto com AUTO-RECOVERY para Pool Esgotado.
    """
    pool_obj = get_pool()
    if not pool_obj:
        yield None
        return
    
    conn = None
    try:
        # --- ETAPA 1: TENTATIVA DE OBTENÇÃO DA CONEXÃO ---
        try:
            conn = pool_obj.getconn()
        except (psycopg2.pool.PoolError, IndexError):
            # [ALTERAÇÃO AQUI] SE O POOL ESTIVER CHEIO/ESGOTADO:
            # Força a limpeza do cache e cria um novo pool imediatamente
            st.warning("⚠️ Pool de conexões esgotado. Reiniciando pool...")
            get_pool.clear() # Limpa o cache do Streamlit
            pool_obj = get_pool() # Cria novo pool
            conn = pool_obj.getconn() # Tenta pegar conexão do novo pool

        # HEALTH CHECK ATIVO
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        except (psycopg2.InterfaceError, psycopg2.OperationalError, psycopg2.DatabaseError):
            # Se a conexão veio morta, descarta e pega outra
            if conn:
                try: pool_obj.putconn(conn, close=True)
                except: pass
            conn = pool_obj.getconn()

        # --- ETAPA 2: ENTREGA DA CONEXÃO ---
        try:
            yield conn
        finally:
            # BLOCO FINALLY: Executa SEMPRE
            if conn:
                try:
                    pool_obj.putconn(conn)
                except Exception as e_put:
                    # Se der erro ao devolver, não trava o app, apenas loga
                    # st.warning(f"Aviso: Erro ao devolver conexão: {e_put}")
                    pass

    except Exception as e:
        st.error(f"Falha na comunicação com o banco: {e}")
        if conn:
            try: pool_obj.putconn(conn, close=True)
            except: pass
        yield None

def ler_dados_seguro(query, params=None):
    """
    Executa pd.read_sql com sistema de retentativa automática (Retry)
    """
    max_tentativas = 3
    for i in range(max_tentativas):
        try:
            with get_db_connection() as conn:
                if not conn: return pd.DataFrame()
                if params:
                    return pd.read_sql(query, conn, params=tuple(params))
                else:
                    return pd.read_sql(query, conn)
        except Exception as e:
            msg = str(e)
            if "SSL" in msg or "EOF" in msg or "terminating" in msg or "closed" in msg or "pool" in msg:
                time.sleep(0.5)
                if i == max_tentativas - 1:
                    st.error(f"Erro de conexão persistente: {e}")
                    return pd.DataFrame()
                continue
            else:
                st.error(f"Erro SQL: {e}")
                return pd.DataFrame()
    return pd.DataFrame()

# --- FUNÇÕES AUXILIARES ---

def limpar_formatacao_texto(texto):
    if not texto: return ""
    return str(texto).replace('*', '').strip()

def hash_senha(senha):
    if senha.startswith('$2b$'): return senha
    return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# --- FUNÇÕES DE BANCO DE DADOS (CRUD) ---

def listar_agrupamentos(tipo):
    tabela = "admin.agrupamento_clientes" if tipo == "cliente" else "admin.agrupamento_empresas"
    return ler_dados_seguro(f"SELECT id, nome_agrupamento FROM {tabela} ORDER BY id")

def listar_cliente_cnpj():
    return ler_dados_seguro("SELECT id, cnpj, nome_empresa FROM admin.cliente_cnpj ORDER BY nome_empresa")

def excluir_cliente_db(id_cliente):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM admin.clientes WHERE id = %s", (id_cliente,))
            conn.commit()
            return True
        except: return False

def buscar_usuarios_disponiveis():
    query = """
        SELECT id, nome, email, cpf 
        FROM admin.clientes_usuarios 
        WHERE id NOT IN (SELECT id_usuario_vinculo FROM admin.clientes WHERE id_usuario_vinculo IS NOT NULL) 
        ORDER BY nome
    """
    return ler_dados_seguro(query)

def vincular_usuario_cliente(id_cliente, id_usuario):
    with get_db_connection() as conn:
        if not conn: return False, "Erro Conexão"
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = %s WHERE id = %s", (int(id_usuario), int(id_cliente)))
            conn.commit()
            return True, "Vinculado!"
        except Exception as e: return False, str(e)

def desvincular_usuario_cliente(id_cliente):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE admin.clientes SET id_usuario_vinculo = NULL WHERE id = %s", (id_cliente,))
            conn.commit()
            return True
        except: return False

def salvar_usuario_novo(nome, email, cpf, tel, senha, nivel, ativo, dados_bancarios, observacao, pasta_caminho):
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(cpf) if v else 0
    if not cpf_val: cpf_val = 0

    with get_db_connection() as conn:
        if not conn: return None
        try:
            with conn.cursor() as cur:
                senha_f = hash_senha(senha)
                if not nivel: nivel = 'Cliente sem permissão'
                cur.execute("""
                    INSERT INTO admin.clientes_usuarios 
                    (nome, email, cpf, telefone, senha, nivel, ativo, dados_bancarios, observacao, pasta_caminho, data_cadastro) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()) RETURNING id
                """, (nome, email, cpf_val, tel, senha_f, nivel, ativo, dados_bancarios, observacao, pasta_caminho))
                res = cur.fetchone()
                nid = res[0] if res else None
            conn.commit()
            return nid
        except Exception as e: 
            st.error(f"Erro ao salvar usuário: {e}")
            return None

# --- DIALOGS (MODAIS) ---

@st.dialog("🔗 Gestão de Acesso do Cliente")
def dialog_gestao_usuario_vinculo(dados_cliente):
    raw_id = dados_cliente.get('id_vinculo') or dados_cliente.get('id_usuario_vinculo')
    id_vinculo = None
    if pd.notna(raw_id) and raw_id is not None:
        try: id_vinculo = int(float(raw_id))
        except: id_vinculo = None

    if id_vinculo:
        st.success("✅ Este cliente já possui um usuário vinculado.")
        df_u = ler_dados_seguro(f"SELECT nome, email, telefone, cpf FROM admin.clientes_usuarios WHERE id = {id_vinculo}")
        
        if not df_u.empty:
            usr = df_u.iloc[0]
            cpf_tela = v.ValidadorDocumentos.cpf_para_tela(usr['cpf']) if v else str(usr['cpf'])
            
            st.write(f"**Nome:** {usr['nome']}")
            st.write(f"**Login:** {usr['email']}")
            st.write(f"**CPF:** {cpf_tela}")
            st.markdown("---")
            if st.button("🔓 Desvincular Usuário", type="primary"):
                if desvincular_usuario_cliente(dados_cliente['id']): 
                    st.success("Desvinculado!"); time.sleep(1.5); st.rerun()
                else: st.error("Erro.")
        else:
            st.warning("Usuário vinculado não encontrado.")
            if st.button("Forçar Desvinculo"): 
                desvincular_usuario_cliente(dados_cliente['id']); st.rerun()
    else:
        st.warning("⚠️ Este cliente não tem acesso ao sistema.")
        tab_novo, tab_existente = st.tabs(["✨ Criar Novo", "🔍 Vincular Existente"])
        with tab_novo:
            with st.form("form_cria_vincula"):
                c1, c2 = st.columns(2)
                u_email = c1.text_input("Login (Email)", value=dados_cliente['email'])
                u_senha = c2.text_input("Senha Inicial", value="1234")
                
                cpf_origem = dados_cliente.get('cpf')
                val_cpf_form = v.ValidadorDocumentos.cpf_para_tela(cpf_origem) if v else str(cpf_origem)
                
                c3, c4 = st.columns(2)
                u_cpf = c3.text_input("CPF", value=val_cpf_form)
                u_nome = c4.text_input("Nome", value=limpar_formatacao_texto(dados_cliente['nome']))
                
                st.markdown("---")
                st.markdown("###### 📂 Dados Adicionais")
                u_pasta = st.text_input("Caminho da Pasta (Servidor)", placeholder="Ex: Z:/CLIENTES/NOME_CLIENTE")
                
                cc1, cc2 = st.columns(2)
                u_dados_bancarios = cc1.text_area("Dados Bancários", height=100, placeholder="Pix, Conta, etc...")
                u_observacao = cc2.text_area("Observação Interna", height=100)

                if st.form_submit_button("Criar e Vincular"):
                    novo_id = salvar_usuario_novo(
                        u_nome, u_email, u_cpf, dados_cliente['telefone'], u_senha, 
                        'Cliente sem permissão', True, u_dados_bancarios, u_observacao, u_pasta
                    )
                    
                    if novo_id: 
                        ok, msg = vincular_usuario_cliente(dados_cliente['id'], novo_id)
                        if ok: st.success("Criado e vinculado!"); time.sleep(1); st.rerun()
                        else: st.error(f"Erro ao vincular: {msg}")
                    else: st.error("Erro ao criar usuário.")
        with tab_existente:
            df_livres = buscar_usuarios_disponiveis()
            if not df_livres.empty:
                opcoes = df_livres.apply(lambda x: f"{x['nome']} ({x['email']})", axis=1)
                idx_sel = st.selectbox("Selecione o Usuário", range(len(df_livres)), format_func=lambda x: opcoes[x])
                if st.button("Vincular Selecionado"):
                    ok, msg = vincular_usuario_cliente(dados_cliente['id'], df_livres.iloc[idx_sel]['id'])
                    if ok: st.success("Vinculado com sucesso!"); time.sleep(1); st.rerun()
                    else: st.error(f"Erro ao vincular: {msg}")
            else: st.info("Sem usuários livres.")

@st.dialog("🚨 Excluir Cliente")
def dialog_excluir_cliente(id_cli, nome):
    st.error(f"Excluir **{nome}**?"); st.warning("Apenas a ficha cadastral será apagada.")
    c1, c2 = st.columns(2)
    if c1.button("Sim, Excluir"):
        if excluir_cliente_db(id_cli): st.success("Removido."); time.sleep(1); st.session_state['view_cliente'] = 'lista'; st.rerun()
    if c2.button("Cancelar"): st.rerun()

# --- FUNÇÃO PRINCIPAL DO MÓDULO ---

def app_cadastro_cliente():
    c1, c2 = st.columns([6, 1])
    filtro = c1.text_input("🔍 Buscar Cliente", placeholder="Nome, CPF ou Nome Empresa")
    if c2.button("➕ Novo", type="primary"): st.session_state['view_cliente'] = 'novo'; st.rerun()

    if st.session_state.get('view_cliente', 'lista') == 'lista':
        # Monta a Query
        sql = """
            SELECT c.*, c.id_usuario_vinculo as id_vinculo, u.nome as nome_usuario_vinculado
            FROM admin.clientes c
            LEFT JOIN admin.clientes_usuarios u ON c.id_usuario_vinculo = u.id
        """
        
        params = []
        if filtro:
            filtro_limpo = v.ValidadorDocumentos.limpar_numero(filtro) if v else filtro
            
            # Se for numérico e parecer CPF, busca por BigInt
            if filtro_limpo and len(filtro_limpo) >= 3 and filtro_limpo.isdigit():
                sql += " WHERE c.cpf = %s OR CAST(c.cpf AS TEXT) ILIKE %s OR c.nome ILIKE %s"
                params = [int(filtro_limpo), f"%{filtro_limpo}%", f"%{filtro}%"]
            else:
                sql += " WHERE c.nome ILIKE %s OR c.nome_empresa ILIKE %s"
                params = [f"%{filtro}%", f"%{filtro}%"]
        
        sql += " ORDER BY c.id DESC LIMIT 50"
        
        # EXECUÇÃO BLINDADA COM RETRY
        df_cli = ler_dados_seguro(sql, params)

        if not df_cli.empty:
            st.markdown("""
            <div style="display:flex; font-weight:bold; color:#555; padding:8px; border-bottom:2px solid #ddd; margin-bottom:10px; background-color:#f8f9fa;">
                <div style="flex:3;">Nome</div>
                <div style="flex:2;">CPF</div>
                <div style="flex:2;">Empresa</div>
                <div style="flex:2;">Usuário</div>
                <div style="flex:1;">Status</div>
                <div style="flex:2; text-align:center;">Ações</div>
            </div>
            """, unsafe_allow_html=True)
            
            for _, row in df_cli.iterrows():
                with st.container():
                    c1, c2, c3, c4, c5, c6 = st.columns([3, 2, 2, 2, 1, 2])
                    c1.write(f"**{limpar_formatacao_texto(row['nome'])}**")
                    
                    cpf_view = v.ValidadorDocumentos.cpf_para_tela(row['cpf']) if v else str(row['cpf'])
                    c2.write(cpf_view or "-")
                    c3.write(row['nome_empresa'] or "-")
                    
                    nome_vinculo = row['nome_usuario_vinculado']
                    c4.write(limpar_formatacao_texto(nome_vinculo) if nome_vinculo else "-")

                    cor_st = 'green' if row.get('status','ATIVO')=='ATIVO' else 'red'
                    c5.markdown(f":{cor_st}[{row.get('status','ATIVO')}]")
                    
                    with c6:
                        b1, b3, b4 = st.columns(3)
                        if b1.button("✏️", key=f"e_{row['id']}", help="Editar Cadastro"): 
                            st.session_state.update({'view_cliente': 'editar', 'cli_id': row['id']}); st.rerun()
                        
                        if b3.button("🔗" if row['id_vinculo'] else "👤", key=f"u_{row['id']}", help="Acesso Usuário"): 
                            dialog_gestao_usuario_vinculo(row)
                            
                        if b4.button("🗑️", key=f"d_{row['id']}", help="Excluir"):
                            dialog_excluir_cliente(row['id'], row['nome'])
                    
                    st.markdown("<hr style='margin: 5px 0; border-color: #eee;'>", unsafe_allow_html=True)
        else: st.info("Nenhum cliente encontrado.")

    elif st.session_state['view_cliente'] in ['novo', 'editar']:
        st.markdown(f"### {'📝 Novo' if st.session_state['view_cliente']=='novo' else '✏️ Editar'}")
        
        dados = {}
        if st.session_state['view_cliente'] == 'editar':
            df = ler_dados_seguro(f"SELECT * FROM admin.clientes WHERE id = {st.session_state['cli_id']}")
            if not df.empty: dados = df.iloc[0]

        df_empresas = listar_cliente_cnpj() 
        df_ag_cli = listar_agrupamentos("cliente")
        df_ag_emp = listar_agrupamentos("empresa")

        with st.form("form_cliente"):
            c1, c2, c3 = st.columns(3)
            nome = c1.text_input("Nome Completo *", value=limpar_formatacao_texto(dados.get('nome', '')))
            
            lista_empresas = df_empresas['nome_empresa'].unique().tolist()
            idx_emp = 0
            val_emp_atual = dados.get('nome_empresa', '')
            if val_emp_atual in lista_empresas: idx_emp = lista_empresas.index(val_emp_atual)
            
            nome_emp = c2.selectbox("Empresa (Selecionar)", options=[""] + lista_empresas, index=idx_emp + 1 if val_emp_atual else 0)
            cnpj_display = dados.get('cnpj_empresa', '')
            c3.text_input("CNPJ (Vinculado)", value=cnpj_display, disabled=True)

            c4, c5, c6, c7 = st.columns(4)
            email = c4.text_input("E-mail *", value=dados.get('email', ''))
            
            val_cpf_ini = v.ValidadorDocumentos.cpf_para_tela(dados.get('cpf')) if v else dados.get('cpf', '')
            cpf = c5.text_input("CPF *", value=val_cpf_ini)
            
            tel1 = c6.text_input("Telefone 1", value=dados.get('telefone', ''))
            tel2 = c7.text_input("Telefone 2", value=dados.get('telefone2', ''))
            
            c8, c9, c10 = st.columns([1, 1, 1])
            id_gp = c8.text_input("ID Grupo WhatsApp", value=dados.get('id_grupo_whats', ''))
            
            padrao_cli = []
            if dados.get('ids_agrupamento_cliente'):
                try: padrao_cli = [int(x.strip()) for x in str(dados.get('ids_agrupamento_cliente')).split(',') if x.strip().isdigit()]
                except: pass
            sel_ag_cli = c9.multiselect("Agrupamento Cliente", options=df_ag_cli['id'], format_func=lambda x: df_ag_cli[df_ag_cli['id']==x]['nome_agrupamento'].values[0] if not df_ag_cli[df_ag_cli['id']==x].empty else x, default=[x for x in padrao_cli if x in df_ag_cli['id'].values])

            padrao_emp = []
            if dados.get('ids_agrupamento_empresa'):
                try: padrao_emp = [int(x.strip()) for x in str(dados.get('ids_agrupamento_empresa')).split(',') if x.strip().isdigit()]
                except: pass
            sel_ag_emp = c10.multiselect("Agrupamento Empresa", options=df_ag_emp['id'], format_func=lambda x: df_ag_emp[df_ag_emp['id']==x]['nome_agrupamento'].values[0] if not df_ag_emp[df_ag_emp['id']==x].empty else x, default=[x for x in padrao_emp if x in df_ag_emp['id'].values])
            
            status_final = "ATIVO"
            if st.session_state['view_cliente'] == 'editar':
                st.divider(); cs1, _ = st.columns([1, 4])
                status_final = cs1.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if dados.get('status','ATIVO')=="ATIVO" else 1)

            st.markdown("<br>", unsafe_allow_html=True); ca = st.columns([1, 1, 4])
            
            if ca[0].form_submit_button("💾 Salvar"):
                cpf_limpo = v.ValidadorDocumentos.cpf_para_bigint(cpf) if v else cpf
                if not cpf_limpo: cpf_limpo = 0

                cnpj_final = ""
                if nome_emp:
                    filtro_cnpj = df_empresas[df_empresas['nome_empresa'] == nome_emp]
                    if not filtro_cnpj.empty: cnpj_final = filtro_cnpj.iloc[0]['cnpj']
                
                str_ag_cli = ",".join(map(str, sel_ag_cli))
                str_ag_emp = ",".join(map(str, sel_ag_emp))

                with get_db_connection() as conn:
                    if conn:
                        with conn.cursor() as cur:
                            if st.session_state['view_cliente'] == 'novo':
                                cur.execute("INSERT INTO admin.clientes (nome, nome_empresa, cnpj_empresa, email, cpf, telefone, telefone2, id_grupo_whats, ids_agrupamento_cliente, ids_agrupamento_empresa, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ATIVO')", (nome, nome_emp, cnpj_final, email, cpf_limpo, tel1, tel2, id_gp, str_ag_cli, str_ag_emp))
                            else:
                                cur.execute("UPDATE admin.clientes SET nome=%s, nome_empresa=%s, cnpj_empresa=%s, email=%s, cpf=%s, telefone=%s, telefone2=%s, id_grupo_whats=%s, ids_agrupamento_cliente=%s, ids_agrupamento_empresa=%s, status=%s WHERE id=%s", (nome, nome_emp, cnpj_final, email, cpf_limpo, tel1, tel2, id_gp, str_ag_cli, str_ag_emp, status_final, st.session_state['cli_id']))
                        conn.commit()
                        st.success("Salvo!"); time.sleep(1); st.session_state['view_cliente'] = 'lista'; st.rerun()
            
            if ca[1].form_submit_button("Cancelar"): st.session_state['view_cliente'] = 'lista'; st.rerun()

        if st.session_state['view_cliente'] == 'editar':
            st.markdown("---")
            if st.button("🗑️ Excluir Cliente", type="primary"): dialog_excluir_cliente(st.session_state['cli_id'], nome)

if __name__ == "__main__":
    if get_pool():
        app_cadastro_cliente()
    else:
        st.error("Erro crítico de conexão.")