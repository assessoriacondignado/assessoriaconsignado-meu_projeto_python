import streamlit as st
import pandas as pd
import psycopg2
import time
from datetime import datetime
import conexao

# --- FUNÇÃO DE CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

# =============================================================================
# 1. FUNÇÕES DE BANCO DE DADOS (CRUD CLIENTES)
# =============================================================================

def listar_clientes(termo_busca=None):
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        # Busca os campos padrão da tabela admin.clientes
        sql = "SELECT id, nome, cpf_cnpj, telefone, email, status FROM admin.clientes WHERE 1=1"
        if termo_busca:
            sql += f" AND (nome ILIKE '%{termo_busca}%' OR cpf_cnpj ILIKE '%{termo_busca}%')"
        sql += " ORDER BY nome ASC"
        df = pd.read_sql(sql, conn)
        return df
    except Exception as e:
        st.error(f"Erro ao listar clientes: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def salvar_novo_cliente(nome, cpf, tel, email, status):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO admin.clientes (nome, cpf_cnpj, telefone, email, status, data_criacao)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """, (nome, cpf, tel, email, status))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False
    finally:
        conn.close()

def atualizar_cliente(id_cli, nome, cpf, tel, email, status):
    conn = get_conn()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE admin.clientes 
            SET nome=%s, cpf_cnpj=%s, telefone=%s, email=%s, status=%s
            WHERE id=%s
        """, (nome, cpf, tel, email, status, id_cli))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Erro ao atualizar: {e}")
        return False
    finally:
        conn.close()

# =============================================================================
# 2. FUNÇÃO ESPECÍFICA DE RELATÓRIO (CORRIGIDA)
# =============================================================================

def buscar_extrato_cliente(id_cliente):
    """
    Busca o extrato financeiro EXCLUSIVAMENTE na tabela cliente.extrato_carteira_por_produto
    conforme solicitado.
    """
    conn = get_conn()
    if not conn: return pd.DataFrame()
    try:
        # Query ajustada para a tabela solicitada
        sql = """
            SELECT 
                id,
                data_lancamento,
                tipo_lancamento,
                produto_vinculado,
                origem_lancamento,
                valor_lancado,
                saldo_anterior,
                saldo_novo,
                nome_usuario as usuario_responsavel
            FROM cliente.extrato_carteira_por_produto
            WHERE id_cliente = %s
            ORDER BY id DESC
        """
        df = pd.read_sql(sql, conn, params=(id_cliente,))
        return df
    except Exception as e:
        st.error(f"Erro ao gerar extrato: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# =============================================================================
# 3. INTERFACE PRINCIPAL (LAYOUT)
# =============================================================================

def app_clientes():
    st.markdown("## 👥 Gestão de Clientes")
    
    # Abas para manter a organização do layout
    tab_lista, tab_novo, tab_relatorio = st.tabs(["📂 Consultar / Editar", "➕ Novo Cliente", "💰 Extrato & Relatórios"])

    # --- ABA 1: LISTAGEM E EDIÇÃO ---
    with tab_lista:
        col_busca, col_btn = st.columns([3, 1])
        busca = col_busca.text_input("Buscar Cliente", placeholder="Nome ou CPF...")
        if col_btn.button("🔍 Buscar"):
            pass # Apenas recarrega a página

        df_clientes = listar_clientes(busca)
        
        if not df_clientes.empty:
            # Dropdown para selecionar cliente para edição
            opcoes = df_clientes['nome'].tolist()
            cliente_selecionado = st.selectbox("Selecione um Cliente para Editar", ["Selecione..."] + opcoes, index=0)
            
            if cliente_selecionado and cliente_selecionado != "Selecione...":
                dados_cli = df_clientes[df_clientes['nome'] == cliente_selecionado].iloc[0]
                
                with st.expander(f"✏️ Editar: {dados_cli['nome']}", expanded=True):
                    with st.form(key=f"form_edit_{dados_cli['id']}"):
                        c1, c2 = st.columns(2)
                        n_nome = c1.text_input("Nome", value=dados_cli['nome'])
                        n_cpf = c2.text_input("CPF/CNPJ", value=dados_cli['cpf_cnpj'])
                        
                        c3, c4 = st.columns(2)
                        n_tel = c3.text_input("Telefone", value=dados_cli['telefone'])
                        n_email = c4.text_input("Email", value=dados_cli['email'])
                        
                        n_status = st.selectbox("Status", ["ATIVO", "INATIVO"], index=0 if dados_cli['status'] == "ATIVO" else 1)
                        
                        if st.form_submit_button("💾 Salvar Alterações"):
                            if atualizar_cliente(int(dados_cli['id']), n_nome, n_cpf, n_tel, n_email, n_status):
                                st.success("Cliente atualizado com sucesso!")
                                time.sleep(1)
                                st.rerun()
            
            st.divider()
            # Exibe a tabela completa
            st.dataframe(
                df_clientes, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "id": st.column_config.NumberColumn("ID", width="small"),
                    "nome": "Nome Completo",
                    "cpf_cnpj": "CPF/CNPJ",
                    "email": "E-mail",
                    "status": st.column_config.TextColumn("Status", width="small")
                }
            )
        else:
            st.info("Nenhum cliente encontrado com os critérios de busca.")

    # --- ABA 2: NOVO CLIENTE ---
    with tab_novo:
        st.markdown("#### Cadastro de Novo Cliente")
        with st.form("form_novo_cli"):
            c1, c2 = st.columns(2)
            novo_nome = c1.text_input("Nome Completo")
            novo_cpf = c2.text_input("CPF ou CNPJ")
            
            c3, c4 = st.columns(2)
            novo_tel = c3.text_input("Telefone")
            novo_email = c4.text_input("Email")
            
            status_ini = st.selectbox("Status Inicial", ["ATIVO", "INATIVO"])
            
            if st.form_submit_button("✅ Cadastrar Cliente", type="primary"):
                if novo_nome and novo_cpf:
                    if salvar_novo_cliente(novo_nome, novo_cpf, novo_tel, novo_email, status_ini):
                        st.success(f"Cliente {novo_nome} cadastrado com sucesso!")
                        time.sleep(1)
                        st.rerun()
                else:
                    st.warning("Os campos Nome e CPF são obrigatórios.")

    # --- ABA 3: RELATÓRIOS (USANDO APENAS A TABELA SOLICITADA) ---
    with tab_relatorio:
        st.markdown("#### 📊 Extrato Financeiro por Cliente")
        st.caption("Fonte de dados: Tabela `cliente.extrato_carteira_por_produto`")
        
        # Recarrega lista completa para o relatório
        df_para_relatorio = listar_clientes()
        
        if not df_para_relatorio.empty:
            cli_rel_nome = st.selectbox("Selecione o Cliente:", df_para_relatorio['nome'].unique(), key="sb_rel_cli")
            
            if cli_rel_nome:
                # Pega o ID do cliente selecionado
                id_cli_rel = df_para_relatorio[df_para_relatorio['nome'] == cli_rel_nome].iloc[0]['id']
                
                # --- AQUI ESTÁ A ALTERAÇÃO PRINCIPAL ---
                df_extrato = buscar_extrato_cliente(int(id_cli_rel))
                
                if not df_extrato.empty:
                    # Métricas Rápidas
                    # O saldo mais atual geralmente é o do último registro (ordenado por ID DESC na query, então é o primeiro do DF)
                    saldo_atual = df_extrato.iloc[0]['saldo_novo'] 
                    
                    # Soma apenas os débitos para exibir total gasto
                    total_debitos = df_extrato[df_extrato['tipo_lancamento'] == 'DEBITO']['valor_lancado'].sum()
                    
                    col_metric1, col_metric2 = st.columns(2)
                    col_metric1.metric("💰 Saldo Atual (Carteira)", f"R$ {saldo_atual:,.2f}")
                    col_metric2.metric("📉 Total Utilizado (Débitos)", f"R$ {total_debitos:,.2f}")
                    
                    st.divider()
                    st.markdown("##### Histórico de Movimentações")
                    
                    st.dataframe(
                        df_extrato, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config={
                            "data_lancamento": st.column_config.DatetimeColumn("Data/Hora", format="DD/MM/YYYY HH:mm"),
                            "valor_lancado": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
                            "saldo_anterior": st.column_config.NumberColumn("Saldo Ant.", format="R$ %.2f"),
                            "saldo_novo": st.column_config.NumberColumn("Saldo Novo", format="R$ %.2f"),
                            "tipo_lancamento": "Tipo",
                            "produto_vinculado": "Produto",
                            "origem_lancamento": "Origem",
                            "usuario_responsavel": "Usuário"
                        }
                    )
                else:
                    st.warning(f"Nenhum registro financeiro encontrado para {cli_rel_nome}.")
        else:
            st.info("Cadastre clientes para visualizar os relatórios.")