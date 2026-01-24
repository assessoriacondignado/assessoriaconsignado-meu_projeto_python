import streamlit as st
import pandas as pd
import psycopg2
import time
import re
import os
import sys
from datetime import datetime, date
import modulo_wapi

# Ajuste de path para importar módulos da raiz e de COMERCIAL
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
diretorio_comercial = os.path.dirname(diretorio_atual) # Pasta COMERCIAL
raiz_projeto = os.path.dirname(diretorio_comercial)    # Raiz do Projeto

if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)

try:
    import conexao
except ImportError:
    st.error("Erro crítico: Arquivo conexao.py não localizado.")

# Importação do módulo de configurações e Módulos Satélites (Tarefas/Renovação)
try:
    from COMERCIAL import modulo_comercial_configuracoes
    from COMERCIAL.TAREFAS import modulo_tarefas
    from COMERCIAL.RENOVACAO_E_FEEDBACK import modulo_renovacao_feedback
except ImportError:
    modulo_comercial_configuracoes = None
    modulo_tarefas = None
    modulo_renovacao_feedback = None

# --- CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")
        return None

# =============================================================================
# 1. FUNÇÕES AUXILIARES E DE NEGÓCIO
# =============================================================================

def listar_modelos_mensagens():
    """Busca os modelos de mensagem cadastrados no Config para este módulo"""
    if modulo_comercial_configuracoes:
        return modulo_comercial_configuracoes.listar_chaves_config("PEDIDOS")
    return []

def registrar_movimentacao_financeira(conn, dados_pedido, tipo_lancamento, valor):
    # Esta função usa o cursor da conexão pai, não comita e deixa a transação fluir
    try:
        cur = conn.cursor()
        id_cliente = dados_pedido['id_cliente']
        
        cur.execute("""
            SELECT saldo_novo 
            FROM cliente.extrato_carteira_por_produto 
            WHERE id_cliente = %s 
            ORDER BY id DESC LIMIT 1
        """, (str(id_cliente),))
        res = cur.fetchone()
        saldo_anterior = float(res[0]) if res else 0.0
        
        valor_float = float(valor)
        if tipo_lancamento == 'CREDITO':
            saldo_novo = saldo_anterior + valor_float
        else: # DEBITO
            saldo_novo = saldo_anterior - valor_float
            
        cur.execute("""
            INSERT INTO cliente.extrato_carteira_por_produto (
                id_cliente, data_lancamento, tipo_lancamento, 
                produto_vinculado, origem_lancamento, 
                valor_lancado, saldo_anterior, saldo_novo, nome_usuario
            ) VALUES (%s, NOW(), %s, %s, 'PEDIDOS', %s, %s, %s, 'Sistema')
        """, (
            str(id_cliente), 
            tipo_lancamento, 
            f"Pedido {dados_pedido['codigo']} - {dados_pedido['nome_produto']}", 
            valor_float, 
            saldo_anterior, 
            saldo_novo
        ))
        return True
    except Exception as e:
        print(f"Erro ao registrar financeiro: {e}")
        return False

def registrar_custo_carteira_upsert(cur, dados_cliente, dados_produto, valor_custo, origem_custo_txt):
    """
    CORREÇÃO APLICADA: Verifica existência por CLIENTE + PRODUTO (e não origem).
    Isso evita o erro de duplicidade de chave unique no banco.
    """
    # Tratamento de Nulos
    id_user = str(dados_cliente.get('id_usuario_vinculo', ''))
    nome_user = str(dados_cliente.get('nome_usuario_vinculo', ''))
    
    if id_user == 'None' or not id_user: 
        id_user = '0'
        nome_user = 'Sem Vínculo'

    # Converte IDs para string pois schema cliente.valor_custo... usa TEXT
    id_cli_str = str(dados_cliente['id'])
    id_prod_str = str(dados_produto['id'])
    nm_cli_str = str(dados_cliente['nome'])
    nm_prod_str = str(dados_produto['nome'])

    # --- LÓGICA DE CORREÇÃO ---
    # Verifica se esse cliente JÁ tem esse produto registrado, independente da origem
    sql_check = """
        SELECT id FROM cliente.valor_custo_carteira_cliente 
        WHERE id_cliente = %s AND id_produto = %s
    """
    cur.execute(sql_check, (id_cli_str, id_prod_str))
    resultado = cur.fetchone()

    if resultado:
        # SE JÁ EXISTE: Faz UPDATE atualizando o custo e a origem para a nova
        id_existente = resultado[0]
        sql_update = """
            UPDATE cliente.valor_custo_carteira_cliente SET
                valor_custo = %s,
                origem_custo = %s,
                nome_usuario = %s,
                id_usuario = %s,
                data_criacao = NOW()
            WHERE id = %s
        """
        cur.execute(sql_update, (float(valor_custo), str(origem_custo_txt), nome_user, id_user, id_existente))
    else:
        # SE NÃO EXISTE: Faz INSERT
        sql_insert = """
            INSERT INTO cliente.valor_custo_carteira_cliente (
                id_cliente, nome_cliente,
                id_usuario, nome_usuario,
                id_produto, nome_produto,
                origem_custo, valor_custo,
                data_criacao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        cur.execute(sql_insert, (
            id_cli_str, nm_cli_str,
            id_user, nome_user,
            id_prod_str, nm_prod_str,
            str(origem_custo_txt), float(valor_custo)
        ))

def criar_pedido_novo_fluxo(cliente, produto, qtd, valor_unitario, valor_total, valor_custo_informado, origem_custo_txt, avisar_cliente, observacao):
    codigo = f"PED-{datetime.now().strftime('%y%m%d%H%M')}"
    conn = get_conn()
    
    if not conn:
        return False, "Falha na conexão com o Banco de Dados", None

    try:
        cur = conn.cursor()
        
        # --- 1. PREPARAÇÃO E LIMPEZA DOS DADOS (CASTING EXPLÍCITO) ---
        p_codigo = str(codigo)
        
        # IDs (Schema admin.pedidos usa Integer)
        p_id_cliente = int(cliente['id']) 
        p_id_produto = int(produto['id'])
        
        # Strings Básicas
        p_nome_cliente = str(cliente['nome']).strip()
        p_nome_produto = str(produto['nome']).strip()
        p_cat_produto = str(produto['tipo'])
        p_origem = str(origem_custo_txt)
        p_obs = str(observacao) if observacao else ""
        
        # CPF (Schema admin.pedidos usa BIGINT)
        raw_cpf = str(cliente['cpf']) if cliente['cpf'] else ""
        cpf_limpo = re.sub(r'\D', '', raw_cpf)
        p_cpf_cliente = int(cpf_limpo) if cpf_limpo else None
        
        # Telefone (Schema admin.pedidos usa VARCHAR)
        p_tel_cliente = str(cliente['telefone']).strip() if cliente['telefone'] else None
        
        # Números
        p_qtd = int(qtd)
        p_val_unit = float(valor_unitario)
        p_val_total = float(valor_total)
        p_custo = float(valor_custo_informado)

        # --- 2. INSERT PEDIDO ---
        cur.execute("""
            INSERT INTO admin.pedidos (
                codigo, id_cliente, nome_cliente, cpf_cliente, telefone_cliente,
                id_produto, nome_produto, categoria_produto, quantidade, 
                valor_unitario, valor_total, custo_carteira, origem_custo, 
                data_solicitacao, observacao, data_criacao, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, NOW(), 'Solicitado') 
            RETURNING id
        """, (
            p_codigo, p_id_cliente, p_nome_cliente, p_cpf_cliente, p_tel_cliente,
            p_id_produto, p_nome_produto, p_cat_produto, p_qtd, 
            p_val_unit, p_val_total, p_custo, p_origem, p_obs
        ))
        
        id_novo = cur.fetchone()[0]
        
        # --- 3. INSERT HISTÓRICO ---
        cur.execute("""
            INSERT INTO admin.pedidos_historico (id_pedido, status_novo, observacao, data_mudanca) 
            VALUES (%s, 'Solicitado', 'Criado via Novo Fluxo', NOW())
        """, (id_novo,))
        
        # --- 4. ATUALIZA CUSTO (UPSERT CORRIGIDO) ---
        registrar_custo_carteira_upsert(cur, cliente, produto, p_custo, p_origem)
        
        # --- 5. COMMIT FINAL ---
        conn.commit()
        
        # --- 6. ENVIO WHATSAPP (FORA DA TRANSAÇÃO DE BANCO) ---
        msg_whats = ""
        if avisar_cliente and p_tel_cliente and modulo_comercial_configuracoes:
            try:
                inst = modulo_wapi.buscar_instancia_ativa()
                if inst:
                    tpl = modulo_comercial_configuracoes.buscar_template_config("PEDIDOS", "criacao")
                    if tpl:
                        primeiro_nome = p_nome_cliente.split()[0].title()
                        msg = tpl.replace("{nome}", primeiro_nome) \
                                 .replace("{pedido}", p_codigo) \
                                 .replace("{produto}", p_nome_produto)
                        
                        modulo_wapi.enviar_msg_api(inst[0], inst[1], p_tel_cliente, msg)
                        msg_whats = " (WhatsApp Enviado)"
            except Exception as e_w:
                print(f"Erro envio whats: {e_w}")

        return True, f"Pedido {codigo} criado com sucesso!{msg_whats}", id_novo

    except psycopg2.Error as e_db:
        conn.rollback()
        # Tratamento específico para erro de duplicidade (caso ainda ocorra em outra tabela)
        if e_db.pgcode == '23505':
            return False, f"Erro: Registro duplicado detectado. Detalhes: {e_db.pgerror}", None
        return False, f"Erro de Banco de Dados: {e_db.pgcode} - {e_db.pgerror}", None
    except Exception as e: 
        conn.rollback()
        return False, f"Erro Geral ao criar pedido: {str(e)}", None
    finally:
        conn.close()

# =============================================================================
# 3. CRUD E FUNÇÕES GERAIS
# =============================================================================

def buscar_clientes():
    conn = get_conn()
    if conn:
        query = """
            SELECT c.id, c.nome, c.cpf, c.telefone, c.email, c.id_usuario_vinculo, u.nome as nome_usuario_vinculo
            FROM admin.clientes c
            LEFT JOIN admin.clientes_usuarios u ON c.id_usuario_vinculo = u.id
            ORDER BY c.nome
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def buscar_produtos():
    conn = get_conn()
    if conn:
        df = pd.read_sql("SELECT id, codigo, nome, tipo, preco, origem_custo FROM admin.produtos_servicos WHERE ativo = TRUE ORDER BY nome", conn)
        conn.close()
        return df
    return pd.DataFrame()

def buscar_historico_pedido(id_pedido):
    conn = get_conn()
    if conn:
        query = "SELECT data_mudanca, status_novo, observacao FROM admin.pedidos_historico WHERE id_pedido = %s ORDER BY data_mudanca DESC"
        df = pd.read_sql(query, conn, params=(int(id_pedido),))
        conn.close()
        return df
    return pd.DataFrame()

def atualizar_status_pedido(id_pedido, novo_status, dados_pedido, avisar, obs):
    if dados_pedido['status'] == novo_status:
        return False, f"⚠️ O pedido já está com o status '{novo_status}'. Nenhuma alteração realizada."
    
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            obs_hist = obs
            coluna_data = ""
            if novo_status == "Solicitado": coluna_data = ", data_solicitacao = NOW()"
            elif novo_status == "Pago": coluna_data = ", data_pago = NOW()"
            elif novo_status == "Pendente": coluna_data = ", data_pendente = NOW()"
            elif novo_status == "Cancelado": coluna_data = ", data_cancelado = NOW()"

            sql_update = f"UPDATE admin.pedidos SET status=%s, observacao=%s, data_atualizacao=NOW(){coluna_data} WHERE id=%s"
            cur.execute(sql_update, (novo_status, obs, id_pedido))
            
            cur.execute("INSERT INTO admin.pedidos_historico (id_pedido, status_novo, observacao, data_mudanca) VALUES (%s, %s, %s, NOW())", (id_pedido, novo_status, obs_hist))
            
            if novo_status == "Pago":
                registrar_movimentacao_financeira(conn, dados_pedido, "CREDITO", dados_pedido['valor_total'])
            elif novo_status == "Cancelado":
                registrar_movimentacao_financeira(conn, dados_pedido, "DEBITO", dados_pedido['valor_total'])
            
            # --- ENVIO DE MENSAGEM ---
            if avisar and dados_pedido['telefone_cliente']:
                cur.execute("SELECT mensagem_padrao FROM admin.status WHERE modulo='PEDIDOS' AND status_relacionado=%s", (novo_status,))
                res_msg = cur.fetchone()
                
                if res_msg and res_msg[0]:
                    template = res_msg[0]
                    msg_final = template.replace("{nome}", str(dados_pedido['nome_cliente']).split()[0]) \
                                        .replace("{nome_completo}", str(dados_pedido['nome_cliente'])) \
                                        .replace("{pedido}", str(dados_pedido['codigo'])) \
                                        .replace("{status}", novo_status) \
                                        .replace("{produto}", str(dados_pedido['nome_produto'])) \
                                        .replace("{obs_status}", obs)
                    
                    inst = modulo_wapi.buscar_instancia_ativa()
                    if inst:
                        modulo_wapi.enviar_msg_api(inst[0], inst[1], dados_pedido['telefone_cliente'], msg_final)
            
            conn.commit(); conn.close()
            return True, "Status atualizado com sucesso!"
        except Exception as e:
            print(e); return False, str(e)
    return False, "Erro conexão"

def excluir_pedido_db(id_pedido):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM admin.pedidos WHERE id=%s", (id_pedido,))
            conn.commit(); conn.close()
            return True
        except: return False
    return False

def editar_dados_pedido_completo(id_pedido, dados_novos):
    try:
        conn = get_conn()
        if not conn: return False, "Erro de conexão"
        cur = conn.cursor()
        total = float(dados_novos['qtd']) * float(dados_novos['valor'])
        
        raw_cpf = str(dados_novos['cliente']['cpf']) if dados_novos['cliente']['cpf'] else ""
        cpf_limpo = re.sub(r'\D', '', raw_cpf)
        p_cpf = int(cpf_limpo) if cpf_limpo else None
        
        sql = """
            UPDATE admin.pedidos SET 
                id_cliente=%s, nome_cliente=%s, cpf_cliente=%s, telefone_cliente=%s,
                id_produto=%s, nome_produto=%s, categoria_produto=%s,
                quantidade=%s, valor_unitario=%s, valor_total=%s,
                custo_carteira=%s, origem_custo=%s, observacao=%s,
                data_atualizacao=NOW()
            WHERE id=%s
        """
        cur.execute(sql, (
            int(dados_novos['cliente']['id']), str(dados_novos['cliente']['nome']), p_cpf, str(dados_novos['cliente']['telefone']),
            int(dados_novos['produto']['id']), str(dados_novos['produto']['nome']), str(dados_novos['produto']['tipo']),
            int(dados_novos['qtd']), float(dados_novos['valor']), float(total),
            float(dados_novos['custo']), str(dados_novos['origem']), str(dados_novos['obs']),
            int(id_pedido)
        ))
        conn.commit(); conn.close()
        return True, "Pedido atualizado completo!"
    except Exception as e: return False, str(e)

# =============================================================================
# 4. COMPONENTE DE NOVO PEDIDO (COM FLUXO PÓS-VENDA)
# =============================================================================

def renderizar_fluxo_pos_venda():
    st.markdown("### 🚀 Fluxo de Pós-Venda")
    st.info("O pedido foi criado! Agora vamos definir as próximas ações.")
    
    etapa = st.session_state.get('pos_venda_etapa', 'tarefa')
    dados = st.session_state.get('pos_venda_dados', {})
    id_ped = st.session_state.get('pos_venda_ped_id')

    if not dados or not id_ped:
        st.error("Erro ao recuperar dados do pedido.")
        if st.button("Sair"): 
            st.session_state.pos_venda_ativo = False
            st.rerun()
        return

    # --- ETAPA 1: TAREFA ---
    if etapa == 'tarefa':
        st.markdown("---")
        st.markdown("#### 1️⃣ Deseja criar uma **TAREFA** para este pedido?")
        
        if st.session_state.get('show_task_form', False):
            with st.container(border=True):
                st.write(f"Nova Tarefa para: **{dados['nome_cliente']}**")
                dt_prev = st.date_input("Data Previsão", value=date.today())
                obs_tar = st.text_area("Descrição da Tarefa", value=f"Acompanhar pedido do produto {dados['nome_produto']}")
                
                avisar_task = st.checkbox("📱 Avisar cliente via WhatsApp?", value=True, key="chk_aviso_tarefa_pv")
                
                c_btn1, c_btn2 = st.columns(2)
                if c_btn1.button("✅ Confirmar Tarefa", type="primary"):
                    if modulo_tarefas:
                        dados_msg = {
                            'codigo_pedido': 'Novo', 
                            'nome_cliente': dados['nome_cliente'], 
                            'telefone_cliente': dados['telefone'], 
                            'nome_produto': dados['nome_produto']
                        }
                        ok = modulo_tarefas.criar_tarefa(
                            id_pedido=id_ped,
                            id_cliente=dados['id_cliente'],
                            id_produto=dados['id_produto'],
                            data_prev=dt_prev,
                            obs_tarefa=obs_tar,
                            dados_pedido=dados_msg,
                            avisar_cli=avisar_task
                        )
                        if ok: st.success("Tarefa Criada!")
                    
                    st.session_state.show_task_form = False
                    st.session_state.pos_venda_etapa = 'renovacao'
                    st.rerun()
                
                if c_btn2.button("Cancelar Criação"):
                      st.session_state.show_task_form = False
                      st.rerun()

        else:
            c1, c2 = st.columns([1, 4])
            if c1.button("Sim, criar Tarefa", key="btn_pv_task_sim"):
                st.session_state.show_task_form = True
                st.rerun()
            if c2.button("Não, pular", key="btn_pv_task_nao"):
                st.session_state.pos_venda_etapa = 'renovacao'
                st.rerun()

    # --- ETAPA 2: RENOVAÇÃO ---
    elif etapa == 'renovacao':
        st.markdown("---")
        st.markdown("#### 2️⃣ Deseja agendar uma **RENOVAÇÃO/FEEDBACK**?")
        
        if st.session_state.get('show_rf_form', False):
            with st.container(border=True):
                st.write(f"Agendar Renovação para: **{dados['nome_produto']}**")
                dt_ren = st.date_input("Data para contato", value=date.today())
                obs_ren = st.text_area("Observação", value="Entrar em contato para renovação.")
                
                avisar_ren = st.checkbox("📱 Avisar cliente via WhatsApp?", value=True, key="chk_aviso_renovacao_pv")
                
                c_btn1, c_btn2 = st.columns(2)
                if c_btn1.button("✅ Confirmar Agendamento", type="primary"):
                    if modulo_renovacao_feedback:
                        dados_msg = {
                            'codigo_pedido': 'Novo',
                            'nome_cliente': dados['nome_cliente'],
                            'telefone_cliente': dados['telefone'],
                            'nome_produto': dados['nome_produto']
                        }
                        
                        ok = modulo_renovacao_feedback.criar_registro_rf(
                            id_pedido=id_ped,
                            data_prev=dt_ren,
                            obs=obs_ren,
                            dados_pedido=dados_msg,
                            avisar=avisar_ren
                        )
                        if ok: st.success("Renovação Agendada!")
                    
                    st.session_state.pos_venda_ativo = False
                    st.session_state.pos_venda_dados = None
                    st.success("Fluxo finalizado com sucesso!")
                    time.sleep(1)
                    st.rerun()
                
                if c_btn2.button("Cancelar"):
                    st.session_state.pos_venda_ativo = False
                    st.session_state.pos_venda_dados = None
                    st.rerun()
        else:
            c1, c2 = st.columns([1, 4])
            if c1.button("Sim, agendar", key="btn_pv_rf_sim"):
                st.session_state.show_rf_form = True
                st.rerun()
            if c2.button("Não, finalizar", key="btn_pv_rf_nao"):
                st.session_state.pos_venda_ativo = False
                st.session_state.pos_venda_dados = None
                st.success("Finalizado!")
                time.sleep(1)
                st.rerun()

def renderizar_novo_pedido_tab():
    if st.session_state.get('pos_venda_ativo', False):
        renderizar_fluxo_pos_venda()
        return

    df_c = buscar_clientes()
    df_p = buscar_produtos()
    
    if df_c.empty or df_p.empty: 
        st.warning("Cadastre clientes e produtos antes.")
        return

    if 'np_cli_idx' not in st.session_state: st.session_state.np_cli_idx = 0
    if 'np_prod_idx' not in st.session_state: st.session_state.np_prod_idx = 0
    
    try:
        prod_inicial = df_p.iloc[st.session_state.np_prod_idx]
    except: prod_inicial = df_p.iloc[0]

    if 'np_val' not in st.session_state: st.session_state.np_val = float(prod_inicial['preco'] or 0.0)
    if 'np_qtd' not in st.session_state: st.session_state.np_qtd = 1
    if 'np_origem' not in st.session_state: st.session_state.np_origem = prod_inicial.get('origem_custo', 'Geral') or 'Geral'
    if 'np_custo' not in st.session_state: st.session_state.np_custo = 0.0
    
    def buscar_custo_referencia(id_cliente, id_produto):
        custo = 0.0
        conn_chk = get_conn()
        if conn_chk:
            try:
                cur = conn_chk.cursor()
                # Verifica custo por cliente+produto (não por origem) para exibição correta
                cur.execute("SELECT valor_custo FROM cliente.valor_custo_carteira_cliente WHERE id_cliente = %s AND id_produto = %s", (str(id_cliente), str(id_produto)))
                chk = cur.fetchone()
                if chk: custo = float(chk[0])
                conn_chk.close()
            except: conn_chk.close()
        return custo

    def on_change_produto():
        idx = st.session_state.np_prod_idx 
        prod = df_p.iloc[idx]
        st.session_state.np_val = float(prod['preco'] or 0.0)
        origem = prod.get('origem_custo', 'Geral')
        st.session_state.np_origem = origem if origem else 'Geral'
        idx_c = st.session_state.np_cli_idx
        cli = df_c.iloc[idx_c]
        st.session_state.np_custo = buscar_custo_referencia(cli['id'], prod['id'])

    def on_change_cliente():
        idx_c = st.session_state.np_cli_idx
        idx_p = st.session_state.np_prod_idx
        cli = df_c.iloc[idx_c]
        prod = df_p.iloc[idx_p]
        st.session_state.np_custo = buscar_custo_referencia(cli['id'], prod['id'])

    def atualizar_calculo(): pass

    if st.session_state.np_custo == 0.0:
        cli_atual = df_c.iloc[st.session_state.np_cli_idx]
        prod_atual = df_p.iloc[st.session_state.np_prod_idx]
        st.session_state.np_custo = buscar_custo_referencia(cli_atual['id'], prod_atual['id'])

    with st.container(border=True):
        c1, c2 = st.columns(2)
        st.session_state.np_cli_idx = c1.selectbox(
            "1. Cliente", range(len(df_c)), index=st.session_state.np_cli_idx,
            format_func=lambda x: f"{df_c.iloc[x]['nome']} / {df_c.iloc[x]['cpf']}", 
            key="np_cli_selector", on_change=lambda: st.session_state.update({'np_cli_idx': st.session_state.np_cli_selector}) or on_change_cliente()
        )
        st.session_state.np_prod_idx = c2.selectbox(
            "3. Produto", range(len(df_p)), index=st.session_state.np_prod_idx,
            format_func=lambda x: df_p.iloc[x]['nome'], 
            key="np_prod_selector", on_change=lambda: st.session_state.update({'np_prod_idx': st.session_state.np_prod_selector}) or on_change_produto()
        )
        c2.info(f"📍 **Origem:** {st.session_state.np_origem}")
        st.divider()
        c3, c4, c5 = st.columns(3)
        qtd = c3.number_input("Qtd", min_value=1, key="np_qtd", on_change=atualizar_calculo)
        val = c4.number_input("Valor Unit.", min_value=0.0, format="%.2f", step=1.0, key="np_val", on_change=atualizar_calculo)
        total = st.session_state.np_qtd * st.session_state.np_val
        c5.metric("Total", f"R$ {total:.2f}")
        st.divider()
        c_custo = st.number_input("Valor de Custo (Referência)", step=1.0, key="np_custo")
        obs = st.text_area("Observação", placeholder="Detalhes...")
        avisar = st.checkbox("Avisar WhatsApp?", value=True)
        
        if st.button("✅ Criar Pedido", type="primary", use_container_width=True):
            cli_final = df_c.iloc[st.session_state.np_cli_idx]
            prod_final = df_p.iloc[st.session_state.np_prod_idx]
            
            with st.spinner("Processando pedido..."):
                ok, res, id_novo_ped = criar_pedido_novo_fluxo(
                    cli_final, prod_final, st.session_state.np_qtd, st.session_state.np_val, total, 
                    st.session_state.np_custo, st.session_state.np_origem, avisar, obs
                )
                if ok: 
                    st.toast(res)
                    st.session_state.pos_venda_ativo = True
                    st.session_state.pos_venda_etapa = 'tarefa'
                    st.session_state.pos_venda_ped_id = id_novo_ped
                    st.session_state.pos_venda_dados = {
                        'nome_cliente': cli_final['nome'],
                        'id_cliente': cli_final['id'],
                        'telefone': cli_final['telefone'],
                        'nome_produto': prod_final['nome'],
                        'id_produto': prod_final['id']
                    }
                    time.sleep(0.5)
                    st.rerun()
                else: 
                    st.error(f"Falha ao criar pedido: {res}")

# =============================================================================
# 5. PAINÉIS DE RENDERIZAÇÃO (Layout Gaveta)
# =============================================================================

def renderizar_dados_cliente(ped):
    st.markdown(f"#### 👤 Dados do Cliente")
    st.write(f"**Nome:** {ped['nome_cliente']}")
    st.write(f"**CPF:** {ped['cpf_cliente']}")
    st.write(f"**Telefone:** {ped['telefone_cliente']}")
    st.write(f"**E-mail:** {ped.get('email_cliente', '-')}")
    if ped.get('nome_empresa'):
        st.write(f"**Empresa:** {ped['nome_empresa']}")

def renderizar_editar_pedido(ped):
    st.markdown(f"#### ✏️ Editar Dados")
    df_c = buscar_clientes()
    df_p = buscar_produtos()
    
    if df_c.empty or df_p.empty: st.error("Dados base vazios."); return

    with st.form("form_gaveta_editar_ped"):
        try: curr_cli_idx = df_c[df_c['id'] == ped['id_cliente']].index[0]
        except: curr_cli_idx = 0
        idx_cli = st.selectbox("Cliente", range(len(df_c)), index=int(curr_cli_idx), format_func=lambda x: f"{df_c.iloc[x]['nome']} ({df_c.iloc[x]['cpf']})")
        sel_cli = df_c.iloc[idx_cli]
        
        try: curr_prod_idx = df_p[df_p['id'] == ped['id_produto']].index[0]
        except: curr_prod_idx = 0
        idx_prod = st.selectbox("Produto", range(len(df_p)), index=int(curr_prod_idx), format_func=lambda x: df_p.iloc[x]['nome'])
        sel_prod = df_p.iloc[idx_prod]

        st.divider()
        c_v1, c_v2 = st.columns(2)
        nq = c_v1.number_input("Qtd", min_value=1, value=int(ped['quantidade']))
        nv = c_v2.number_input("Valor Unit.", min_value=0.0, value=float(ped['valor_unitario']), format="%.2f")
        
        c_c1, c_c2 = st.columns(2)
        ncusto = c_c1.number_input("Custo Carteira (R$)", value=float(ped['custo_carteira'] or 0.0), step=0.01)
        norigem = c_c2.text_input("Origem Custo", value=ped.get('origem_custo', ''))
        
        nobs = st.text_area("Observação", value=ped['observacao'] or "")
        st.info(f"💰 Novo Total: R$ {nq*nv:.2f}")

        if st.form_submit_button("💾 Salvar Alterações", type="primary"):
            dados_novos = {
                'cliente': sel_cli, 'produto': sel_prod, 'qtd': nq, 'valor': nv,
                'custo': ncusto, 'origem': norigem, 'obs': nobs
            }
            ok, msg = editar_dados_pedido_completo(ped['id'], dados_novos)
            if ok: 
                st.success("Salvo!"); time.sleep(1)
                st.session_state.ped_selecionado = None
                st.session_state.ped_aba_ativa = None
                st.rerun()
            else: st.error(f"Erro: {msg}")

def renderizar_status_pedido(ped):
    st.markdown(f"#### 📜 Histórico & Status")
    
    # 1. LISTAGEM DO HISTÓRICO
    df = buscar_historico_pedido(ped['id'])
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else: 
        st.info("Sem histórico registrado.")

    st.markdown("---")
    
    # 2. FORMULÁRIO DE ATUALIZAÇÃO
    with st.expander("🔄 Registrar Nova Atualização", expanded=False):
        lst = ["Solicitado", "Pago", "Registro", "Pendente", "Cancelado"]
        try: idx = lst.index(ped['status']) 
        except: idx = 0
        
        with st.form("form_gaveta_status_ped"):
            ns = st.selectbox("Novo Status", lst, index=idx)
            obs = st.text_area("Observação da Mudança")
            av = st.checkbox("Avisar Cliente (WhatsApp)?", value=True)
            
            if st.form_submit_button("✅ Confirmar Novo Status", type="primary"):
                ok, msg = atualizar_status_pedido(ped['id'], ns, ped, av, obs)
                if ok:
                    st.success(msg); time.sleep(1)
                    st.session_state.ped_selecionado = None
                    st.session_state.ped_aba_ativa = None
                    st.rerun()
                else: st.warning(msg)

def renderizar_excluir_pedido(ped):
    st.markdown(f"#### 🗑️ Excluir Pedido: {ped['codigo']}")
    st.warning("⚠️ Esta ação é irreversível e removerá o histórico financeiro associado.")
    
    col_del = st.columns([1, 1])
    if col_del[0].button("Sim, Excluir Permanentemente", type="primary", use_container_width=True):
        if excluir_pedido_db(ped['id']): 
            st.success("Pedido excluído com sucesso!"); time.sleep(1)
            st.session_state.ped_selecionado = None
            st.session_state.ped_aba_ativa = None
            st.rerun()

def renderizar_tarefa_pedido(ped):
    st.markdown("#### 📋 Tarefas Vinculadas")
    conn = get_conn()
    if conn:
        try:
            query = """
                SELECT id, status, data_previsao, observacao_tarefa as observacao 
                FROM admin.tarefas 
                WHERE id_pedido = %s 
                ORDER BY data_criacao DESC
            """
            df_tar = pd.read_sql(query, conn, params=(ped['id'],))
            conn.close()
            
            if not df_tar.empty:
                df_tar['data_previsao'] = pd.to_datetime(df_tar['data_previsao']).dt.strftime('%d/%m/%Y')
                st.dataframe(df_tar, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhuma tarefa vinculada a este pedido.")
        except Exception as e:
            st.error(f"Erro ao buscar tarefas: {e}")
            if conn: conn.close()

    st.markdown("---")
    
    with st.expander("➕ Criar Nova Tarefa", expanded=False):
        with st.form("form_gaveta_tarefa_ped"):
            dt = st.date_input("Data de Previsão", datetime.now())
            obs = st.text_area("Descrição da Tarefa")
            avisar_task_drawer = st.checkbox("📱 Avisar cliente via WhatsApp?", value=True, key="chk_aviso_tarefa_drawer")
            
            if st.form_submit_button("Criar Tarefa", type="primary"):
                if modulo_tarefas:
                    dados_msg = {
                        'codigo_pedido': ped['codigo'],
                        'nome_cliente': ped['nome_cliente'],
                        'telefone_cliente': ped['telefone_cliente'],
                        'nome_produto': ped['nome_produto']
                    }
                    
                    ok = modulo_tarefas.criar_tarefa(
                        id_pedido=ped['id'], 
                        id_cliente=ped['id_cliente'], 
                        id_produto=ped['id_produto'], 
                        data_prev=dt, 
                        obs_tarefa=obs, 
                        dados_pedido=dados_msg, 
                        avisar_cli=avisar_task_drawer
                    )
                    
                    if ok:
                        st.success("Tarefa criada!"); time.sleep(1)
                        st.rerun()
                else:
                    st.error("Módulo de Tarefas não disponível.")

# --- NOVA FUNÇÃO: RENOVAÇÃO ---
def renderizar_renovacao_pedido(ped):
    st.markdown("#### 📅 Renovações / Feedback")
    
    conn = get_conn()
    if conn:
        try:
            query = """
                SELECT id, status, data_previsao, observacao 
                FROM admin.renovacao_feedback 
                WHERE id_pedido = %s 
                ORDER BY data_criacao DESC
            """
            df = pd.read_sql(query, conn, params=(ped['id'],))
            conn.close()
            
            if not df.empty:
                df['data_previsao'] = pd.to_datetime(df['data_previsao']).dt.strftime('%d/%m/%Y')
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhuma renovação agendada para este pedido.")
        except Exception as e:
            st.error(f"Erro ao buscar renovações: {e}")
            if conn: conn.close()

    st.markdown("---")
    
    with st.expander("➕ Agendar Nova Renovação", expanded=False):
        with st.form("form_gaveta_renovacao"):
            dt = st.date_input("Data Previsão", value=date.today())
            obs = st.text_area("Observação")
            avisar_ren_drawer = st.checkbox("📱 Avisar cliente via WhatsApp?", value=True, key="chk_aviso_renovacao_drawer")
            
            if st.form_submit_button("Agendar", type="primary"):
                if modulo_renovacao_feedback:
                    dados_msg = {
                        'codigo_pedido': ped['codigo'],
                        'nome_cliente': ped['nome_cliente'],
                        'telefone_cliente': ped['telefone_cliente'],
                        'nome_produto': ped['nome_produto']
                    }
                    
                    ok = modulo_renovacao_feedback.criar_registro_rf(
                        id_pedido=ped['id'],
                        data_prev=dt,
                        obs=obs,
                        dados_pedido=dados_msg,
                        avisar=avisar_ren_drawer
                    )
                    if ok: 
                        st.success("Renovação Agendada!"); time.sleep(1)
                        st.rerun()
                else:
                    st.error("Módulo de Renovação não carregado.")

# =============================================================================
# 6. APP PRINCIPAL
# =============================================================================

def app_pedidos():
    # --- CORREÇÃO DE ESTILO: APLICAR APENAS AO BLOCO PRINCIPAL ---
    # Usando o seletor 'section[data-testid="stMainBlock"]' para não vazar para a sidebar
    st.markdown("""
        <style>
        section[data-testid="stMainBlock"] div.stButton > button {
            background-color: #FF4B4B !important;
            color: white !important;
            border-color: #FF4B4B !important;
        }
        section[data-testid="stMainBlock"] div.stButton > button:hover {
            background-color: #FF0000 !important;
            border-color: #FF0000 !important;
            color: white !important;
        }
        section[data-testid="stMainBlock"] div.stButton > button:active {
            background-color: #CC0000 !important;
            border-color: #CC0000 !important;
            color: white !important;
        }
        </style>
    """, unsafe_allow_html=True)

    tab_novo, tab_lista, tab_param = st.tabs(["➕ Novo Pedido", "📋 Lista de Pedidos", "⚙️ Parâmetros"])

    # ABA 1: NOVO PEDIDO (COM WIZARD DE PÓS-VENDA)
    with tab_novo:
        renderizar_novo_pedido_tab()

    # ABA 2: LISTA DE PEDIDOS (LAYOUT MASTER-DETAIL 30/70)
    with tab_lista:
        if 'ped_selecionado' not in st.session_state: st.session_state.ped_selecionado = None
        if 'ped_aba_ativa' not in st.session_state: st.session_state.ped_aba_ativa = None

        col_lista, col_detalhe = st.columns([0.3, 0.7])

        with col_lista:
            st.markdown("##### 🔍 Filtros & Lista")
            f1, f2 = st.columns(2)
            filtro_txt = f1.text_input("Busca", placeholder="Nome/Cod", label_visibility="collapsed")
            filtro_stt = f2.selectbox("St", ["Todos", "Solicitado", "Pendente", "Pago"], label_visibility="collapsed")
            
            conn = get_conn()
            if conn:
                query_base = """
                    SELECT p.*, c.nome_empresa, c.email as email_cliente 
                    FROM admin.pedidos p 
                    LEFT JOIN admin.clientes c ON p.id_cliente = c.id 
                    WHERE 1=1
                """
                params = []
                if filtro_txt:
                    query_base += " AND (p.nome_cliente ILIKE %s OR p.codigo ILIKE %s)"
                    params.extend([f"%{filtro_txt}%", f"%{filtro_txt}%"])
                
                if filtro_stt != "Todos":
                    query_base += " AND p.status = %s"
                    params.append(filtro_stt)

                query_base += " ORDER BY p.data_criacao DESC LIMIT 20"
                df = pd.read_sql(query_base, conn, params=params)
                conn.close()

                if not df.empty:
                    for i, row in df.iterrows():
                        is_selected = (st.session_state.ped_selecionado is not None and 
                                       st.session_state.ped_selecionado['id'] == row['id'])
                        
                        cor = "🔴"
                        if row['status'] == 'Pago': cor = "🟢"
                        elif row['status'] == 'Pendente': cor = "🟠"
                        elif row['status'] == 'Solicitado': cor = "🔵"
                        
                        border_style = True 
                        
                        with st.container(border=border_style):
                            st.write(f"{row['nome_cliente']}")
                            st.caption(f"{cor} {row['codigo']} | R$ {row['valor_total']:.2f}")
                            
                            if st.button("Ver Detalhes >", key=f"sel_ped_{row['id']}", use_container_width=True):
                                st.session_state.ped_selecionado = row.to_dict()
                                st.session_state.ped_aba_ativa = None
                                st.rerun()
                else:
                    st.info("Nenhum pedido.")
            else:
                st.error("Sem conexão.")

        with col_detalhe:
            ped = st.session_state.ped_selecionado
            
            if ped:
                with st.container(border=True):
                    st.title(f"{ped['nome_cliente']}")
                    st.caption(f"Pedido: {ped['codigo']} | Status: {ped['status']} | Data: {ped['data_criacao']}")
                    
                    st.divider()
                    
                    def selecionar_aba_callback(nome_aba):
                        st.session_state.ped_aba_ativa = nome_aba

                    # --- MENU DE OPÇÕES ATUALIZADO (RENOVACAO INCLUÍDA) ---
                    opcoes_menu = [
                        ("👤 Cliente", "cliente"),
                        ("✏️ Editar", "editar"),
                        ("🔄 Status", "status"),
                        ("📝 Tarefa", "tarefa"),
                        ("📅 Renovação", "renovacao"), 
                        ("🗑️ Excluir", "excluir")
                    ]
                    
                    cols_menu = st.columns(6, gap="small")
                    
                    for col, (label, key_aba) in zip(cols_menu, opcoes_menu):
                        tipo_btn = "primary" if st.session_state.ped_aba_ativa == key_aba else "secondary"
                        col.button(
                            label, 
                            key=f"btn_topo_{key_aba}", 
                            type=tipo_btn, 
                            use_container_width=True, 
                            on_click=selecionar_aba_callback, 
                            args=(key_aba,)
                        )

                aba = st.session_state.ped_aba_ativa
                
                if aba:
                    with st.container(border=True):
                        if aba == 'cliente': renderizar_dados_cliente(ped)
                        elif aba == 'editar': renderizar_editar_pedido(ped)
                        elif aba == 'status': renderizar_status_pedido(ped) # Contém histórico
                        elif aba == 'tarefa': renderizar_tarefa_pedido(ped)
                        elif aba == 'renovacao': renderizar_renovacao_pedido(ped) # Nova Aba
                        elif aba == 'excluir': renderizar_excluir_pedido(ped)
                else:
                    st.info("👆 Selecione uma opção acima para gerenciar o pedido.")

            else:
                st.container(border=True).markdown(
                    """
                    <div style='text-align: center; padding: 50px;'>
                        <h3>⬅️ Selecione um pedido na lista</h3>
                        <p>Os detalhes, financeiro e opções de gerenciamento aparecerão aqui.</p>
                    </div>
                    """, 
                    unsafe_allow_html=True
                )
    
    with tab_param:
        conn = get_conn()
        if conn:
            df_pedidos_raw = pd.read_sql("SELECT * FROM admin.pedidos ORDER BY id DESC LIMIT 50", conn)
            st.markdown("**Tabela Pedidos:**"); st.dataframe(df_pedidos_raw, height=200)
            st.markdown("---")
            st.markdown("**Tabela Custos:**")
            try:
                df_custos = pd.read_sql("SELECT * FROM cliente.valor_custo_carteira_cliente ORDER BY id DESC LIMIT 50", conn)
                st.dataframe(df_custos, height=200)
            except: st.warning("Tabela custos não existe.")
            conn.close()

if __name__ == "__main__":
    app_pedidos()