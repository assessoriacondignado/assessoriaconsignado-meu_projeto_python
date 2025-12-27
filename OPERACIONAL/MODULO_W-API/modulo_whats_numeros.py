import streamlit as st
import pandas as pd
import psycopg2
import time
import conexao

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

# --- DIALOGS (POP-UPS) ---

@st.dialog("✏️ Editar Vínculo")
def dialog_editar_vinculo(id_registro, telefone_atual):
    st.write(f"Vinculando número: **{telefone_atual}**")
    
    conn = get_conn()
    if not conn: st.error("Erro conexão"); return

    # Busca clientes para o selectbox
    try:
        df_cli = pd.read_sql("SELECT id, nome, telefone FROM admin.clientes ORDER BY nome", conn)
    except: df_cli = pd.DataFrame()
    conn.close()

    if df_cli.empty:
        st.warning("Nenhum cliente cadastrado no módulo admin.")
        return

    # Cria lista de opções formatada
    opcoes = df_cli.apply(lambda x: f"{x['nome']} | Tel: {x['telefone'] or 'S/N'} (ID: {x['id']})", axis=1)
    
    idx_cli = st.selectbox("Escolha o Cliente", range(len(df_cli)), format_func=lambda x: opcoes[x], index=None, placeholder="Digite para buscar...")

    if st.button("💾 Salvar Vínculo"):
        if idx_cli is not None:
            cli_sel = df_cli.iloc[idx_cli]
            try:
                conn = get_conn()
                cur = conn.cursor()
                # Atualiza tabela de números
                cur.execute("""
                    UPDATE wapi_numeros 
                    SET id_cliente = %s, nome_cliente = %s 
                    WHERE id = %s
                """, (int(cli_sel['id']), cli_sel['nome'], id_registro))
                
                # Opcional: Atualizar logs antigos deste número
                cur.execute("""
                    UPDATE wapi_logs 
                    SET id_cliente = %s, nome_cliente = %s 
                    WHERE telefone = %s
                """, (int(cli_sel['id']), cli_sel['nome'], telefone_atual))
                
                conn.commit()
                conn.close()
                st.success(f"Vinculado a {cli_sel['nome']}!")
                time.sleep(1)
                st.rerun()
            except Exception as e: st.error(f"Erro: {e}")
        else:
            st.warning("Selecione um cliente.")

@st.dialog("👤 Visualizar Cliente")
def dialog_ver_cliente(id_cliente):
    conn = get_conn()
    try:
        df = pd.read_sql(f"SELECT * FROM admin.clientes WHERE id = {id_cliente}", conn)
        conn.close()
        if not df.empty:
            row = df.iloc[0]
            st.write(f"**Nome:** {row['nome']}")
            st.write(f"**E-mail:** {row['email']}")
            st.write(f"**Telefone:** {row['telefone']}")
            st.write(f"**CPF:** {row.get('cpf', '-')}")
        else:
            st.warning("Cliente não encontrado (talvez excluído).")
    except: st.error("Erro ao buscar dados.")

@st.dialog("⚠️ Excluir Registro")
def dialog_excluir_numero(id_registro, telefone):
    st.error(f"Tem certeza que deseja esquecer o número {telefone}?")
    st.warning("Isso não apaga os logs de mensagens, apenas o vínculo atual.")
    
    col1, col2 = st.columns(2)
    if col1.button("Sim, Excluir", type="primary"):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM wapi_numeros WHERE id = %s", (id_registro,))
            conn.commit()
            conn.close()
            st.success("Registro apagado!")
            time.sleep(1)
            st.rerun()
        except: st.error("Erro ao excluir.")
    
    if col2.button("Cancelar"):
        st.rerun()

# --- APP PRINCIPAL ---
def app_numeros():
    st.markdown("### 📒 Registro de Números (Conciliação)")
    
    conn = get_conn()
    if not conn: return

    # Filtros
    c1, c2 = st.columns([3, 1])
    busca = c1.text_input("🔍 Buscar (Telefone ou Nome)", placeholder="Digite para filtrar...")
    filtro_tipo = c2.selectbox("Filtrar por", ["Todos", "Vinculados", "Sem Vínculo"])

    # Query Base
    sql = "SELECT id, telefone, id_cliente, nome_cliente, data_ultima_interacao FROM wapi_numeros WHERE 1=1"
    
    if filtro_tipo == "Vinculados": sql += " AND id_cliente IS NOT NULL"
    if filtro_tipo == "Sem Vínculo": sql += " AND id_cliente IS NULL"
    if busca: sql += f" AND (telefone ILIKE '%%{busca}%%' OR nome_cliente ILIKE '%%{busca}%%')"
    
    sql += " ORDER BY data_ultima_interacao DESC LIMIT 50"

    df = pd.read_sql(sql, conn)
    conn.close()

    if not df.empty:
        # Cabeçalho
        c_h1, c_h2, c_h3, c_h4 = st.columns([2, 3, 2, 2])
        c_h1.markdown("**Telefone**")
        c_h2.markdown("**Cliente Vinculado**")
        c_h3.markdown("**Última Interação**")
        c_h4.markdown("**Ações**")
        st.divider()

        for _, row in df.iterrows():
            with st.container():
                c1, c2, c3, c4 = st.columns([2, 3, 2, 2])
                
                c1.write(row['telefone'])
                
                # Coluna Cliente
                if row['id_cliente']:
                    c2.success(f"🆔 {row['id_cliente']} - {row['nome_cliente']}")
                else:
                    c2.warning("⚠️ Sem Vínculo")
                
                # Data
                data_fmt = pd.to_datetime(row['data_ultima_interacao']).strftime('%d/%m %H:%M')
                c3.write(data_fmt)
                
                # Botões de Ação
                b1, b2, b3 = c4.columns(3)
                
                # Botão Editar (Lápis)
                if b1.button("✏️", key=f"ed_{row['id']}", help="Editar / Vincular"):
                    dialog_editar_vinculo(row['id'], row['telefone'])
                
                # Botão Ver (Olho) - Só habilita se tiver cliente vinculado
                if row['id_cliente']:
                    if b2.button("👁️", key=f"ver_{row['id']}", help="Ver Cliente"):
                        dialog_ver_cliente(row['id_cliente'])
                else:
                    b2.write("-")

                # Botão Excluir (Lixeira)
                if b3.button("🗑️", key=f"del_{row['id']}", help="Excluir Registro"):
                    dialog_excluir_numero(row['id'], row['telefone'])
                
                st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)
    else:
        st.info("Nenhum registro encontrado.")