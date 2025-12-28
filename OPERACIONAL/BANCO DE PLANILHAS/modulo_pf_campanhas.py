import streamlit as st
import pandas as pd
import json
import time
from datetime import date
# Importações dos módulos que estão na mesma pasta
import modulo_pf_cadastro as pf_core
import modulo_pf_pesquisa as pf_pesquisa

# =============================================================================
# 1. FUNÇÕES DE BANCO DE DADOS (CRUD)
# =============================================================================

def salvar_campanha(nome, objetivo, status, filtros_lista):
    conn = pf_core.get_conn()
    if conn:
        try:
            filtros_json = json.dumps(filtros_lista, default=str)
            txt_visual = "; ".join([f.get('descricao_visual', '') for f in filtros_lista])

            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pf_campanhas (nome_campanha, objetivo, status, filtros_config, filtros_aplicaveis, data_criacao)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (nome, objetivo, status, filtros_json, txt_visual, date.today()))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao salvar campanha: {e}")
            conn.close()
    return False

def atualizar_campanha_db(id_campanha, nome, objetivo, status, filtros_lista):
    conn = pf_core.get_conn()
    if conn:
        try:
            filtros_json = json.dumps(filtros_lista, default=str)
            txt_visual = "; ".join([f.get('descricao_visual', '') for f in filtros_lista])

            cur = conn.cursor()
            cur.execute("""
                UPDATE pf_campanhas 
                SET nome_campanha=%s, objetivo=%s, status=%s, filtros_config=%s, filtros_aplicaveis=%s 
                WHERE id=%s
            """, (nome, objetivo, status, filtros_json, txt_visual, id_campanha))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}")
            conn.close()
    return False

def excluir_campanha_db(id_campanha):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM pf_campanhas WHERE id = %s", (id_campanha,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao excluir: {e}")
            conn.close()
    return False

def listar_campanhas_ativas():
    conn = pf_core.get_conn()
    if conn:
        try:
            # Trazemos todas (ativas e inativas) para permitir gestão, ou filtramos na tela
            query = """
                SELECT id, nome_campanha, filtros_config, filtros_aplicaveis, objetivo, data_criacao, status 
                FROM pf_campanhas 
                ORDER BY id DESC
            """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except: 
            conn.close()
    return pd.DataFrame()

def vincular_campanha_aos_clientes(id_campanha, nome_campanha, lista_ids_clientes):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            if not lista_ids_clientes: return 0
            ids_tuple = tuple(lista_ids_clientes)
            query = f"UPDATE pf_dados SET id_campanha = %s WHERE id IN %s"
            cur.execute(query, (str(id_campanha), ids_tuple))
            afetados = cur.rowcount
            conn.commit()
            conn.close()
            return afetados
        except Exception as e:
            st.error(f"Erro ao vincular: {e}")
            conn.close()
    return 0

# =============================================================================
# 2. DIALOGS (POP-UPS) DE EDIÇÃO E EXCLUSÃO
# =============================================================================

@st.dialog("✏️ Editar Campanha", width="large")
def dialog_editar_campanha(dados_atuais):
    # Inicializa estado local do dialog para os filtros
    if 'edit_filtros' not in st.session_state:
        try:
            st.session_state['edit_filtros'] = json.loads(dados_atuais['filtros_config'])
        except:
            st.session_state['edit_filtros'] = []

    with st.form("form_editar_campanha"):
        c1, c2 = st.columns([3, 1])
        novo_nome = c1.text_input("Nome", value=dados_atuais['nome_campanha'])
        status_opts = ["ATIVO", "INATIVO"]
        idx_st = status_opts.index(dados_atuais['status']) if dados_atuais['status'] in status_opts else 0
        novo_status = c2.selectbox("Status", status_opts, index=idx_st)
        
        novo_obj = st.text_area("Objetivo", value=dados_atuais['objetivo'])
        
        st.divider()
        st.markdown("#### 🛠️ Reconfigurar Filtros")
        
        # --- Lógica de Adicionar Filtro (Igual ao Cadastro) ---
        opcoes_campos = []
        mapa_campos = {}
        for grupo, lista in pf_pesquisa.CAMPOS_CONFIG.items():
            for item in lista:
                chave = f"{grupo} -> {item['label']}"
                opcoes_campos.append(chave)
                mapa_campos[chave] = item

        ec1, ec2, ec3, ec4 = st.columns([2, 1.5, 2, 1])
        cp_sel = ec1.selectbox("Campo", opcoes_campos, key="ed_cp")
        op_sel = ec2.selectbox("Op.", ["=", ">", "<", "≥", "≤", "≠", "Contém"], key="ed_op")
        val_sel = ec3.text_input("Valor", key="ed_val")
        
        if ec4.form_submit_button("➕ Add"):
            dado = mapa_campos[cp_sel]
            st.session_state['edit_filtros'].append({
                'label': dado['label'], 'coluna': dado['coluna'], 'tabela': dado['tabela'],
                'tipo': dado['tipo'], 'operador': op_sel, 'valor': val_sel,
                'descricao_visual': f"({dado['tabela']}, {dado['label']}, {op_sel}, {val_sel})"
            })
            st.rerun()

        # Lista e Remoção
        if st.session_state['edit_filtros']:
            st.write("Filtros Atuais:")
            filtros_para_manter = []
            for idx, f in enumerate(st.session_state['edit_filtros']):
                cols = st.columns([6, 1])
                cols[0].code(f.get('descricao_visual', 'Regra'), language="sql")
                if not cols[1].checkbox("❌", key=f"del_f_{idx}"):
                    filtros_para_manter.append(f)
            # Atualiza lista se houver remoção (gambiarra visual pois form não atualiza na hora sem rerun)
            st.session_state['edit_filtros'] = filtros_para_manter

        st.markdown("---")
        
        if st.form_submit_button("💾 SALVAR ALTERAÇÕES", type="primary"):
            if atualizar_campanha_db(dados_atuais['id'], novo_nome, novo_obj, novo_status, st.session_state['edit_filtros']):
                st.success("Campanha atualizada!")
                del st.session_state['edit_filtros'] # Limpa memória
                time.sleep(1)
                st.rerun()

@st.dialog("⚠️ Excluir Campanha")
def dialog_excluir_campanha(id_campanha, nome):
    st.error(f"Tem certeza que deseja excluir a campanha: **{nome}**?")
    st.warning("Esta ação é irreversível. O histórico de vínculo nos clientes, porém, permanecerá até ser substituído.")
    
    col_sim, col_nao = st.columns(2)
    
    if col_sim.button("🚨 SIM, EXCLUIR DEFINITIVAMENTE"):
        if excluir_campanha_db(id_campanha):
            st.success("Campanha removida.")
            time.sleep(1)
            st.rerun()
            
    if col_nao.button("Cancelar"):
        st.rerun()

# =============================================================================
# 3. INTERFACE PRINCIPAL
# =============================================================================

def app_campanhas():
    st.markdown("## 📢 Gestão de Campanhas e Perfilamento")
    
    tab_config, tab_aplicar = st.tabs(["⚙️ Configurar Campanha", "🚀 Executar Campanha"])

    # ---------------------------------------------------------
    # ABA 1: CONFIGURAÇÃO (CRIAR)
    # ---------------------------------------------------------
    with tab_config:
        st.markdown("### 📝 Nova Campanha")
        
        with st.form("form_create_campanha"):
            c1, c2, c3 = st.columns([3, 1.5, 1.5])
            nome = c1.text_input("Nome da Campanha")
            data_criacao = c2.date_input("Data Criação", value=date.today(), disabled=True)
            status = c3.selectbox("Status", ["ATIVO", "INATIVO"])
            objetivo = st.text_area("Objetivo da Campanha")
            
            st.divider()
            st.markdown("#### 🎯 Configuração de Filtros Padrão")
            
            if 'campanha_filtros_temp' not in st.session_state:
                st.session_state['campanha_filtros_temp'] = []

            # Opções de campos
            opcoes_campos = []
            mapa_campos = {}
            for grupo, lista in pf_pesquisa.CAMPOS_CONFIG.items():
                for item in lista:
                    chave = f"{grupo} -> {item['label']}"
                    opcoes_campos.append(chave)
                    mapa_campos[chave] = item

            rc1, rc2, rc3, rc4 = st.columns([2, 1.5, 2, 1])
            campo_sel = rc1.selectbox("Campo", opcoes_campos, key="cp_new_camp")
            op_sel = rc2.selectbox("Operador", ["=", ">", "<", "≥", "≤", "≠", "Contém", "Começa com"], key="op_new_camp")
            valor_sel = rc3.text_input("Valor", key="val_new_camp")
            
            if rc4.form_submit_button("➕ Incluir"):
                if valor_sel:
                    dado = mapa_campos[campo_sel]
                    novo_filtro = {
                        'label': dado['label'], 'coluna': dado['coluna'], 'tabela': dado['tabela'],
                        'tipo': dado['tipo'], 'operador': op_sel, 'valor': valor_sel,
                        'descricao_visual': f"({dado['tabela']}, {dado['label']}, {op_sel}, {valor_sel})"
                    }
                    st.session_state['campanha_filtros_temp'].append(novo_filtro)
                    st.rerun()

            if st.session_state['campanha_filtros_temp']:
                st.markdown("**Filtros Selecionados:**")
                txt_display = "\n".join([f"- {f['descricao_visual']}" for f in st.session_state['campanha_filtros_temp']])
                st.code(txt_display, language="text")
                if st.form_submit_button("🗑️ Limpar Lista"):
                    st.session_state['campanha_filtros_temp'] = []; st.rerun()

            st.markdown("---")
            if st.form_submit_button("💾 SALVAR CAMPANHA"):
                if nome and st.session_state['campanha_filtros_temp']:
                    if salvar_campanha(nome, objetivo, status, st.session_state['campanha_filtros_temp']):
                        st.success(f"Campanha '{nome}' criada!"); st.session_state['campanha_filtros_temp'] = []; time.sleep(1.5); st.rerun()
                else: st.warning("Preencha nome e filtros.")

    # ---------------------------------------------------------
    # ABA 2: APLICAR (EXECUTAR / EDITAR / EXCLUIR)
    # ---------------------------------------------------------
    with tab_aplicar:
        st.markdown("### 🚀 Executar Campanha")
        
        df_todas = listar_campanhas_ativas()
        
        if df_todas.empty:
            st.info("Nenhuma campanha cadastrada.")
        else:
            # Filtro para selecionar apenas ativas se quiser, mas aqui mostra tudo com status no nome
            sel_camp_fmt = df_todas.apply(lambda x: f"#{x['id']} - {x['nome_campanha']} ({x['status']})", axis=1)
            idx = st.selectbox("Selecione a Campanha", range(len(df_todas)), format_func=lambda x: sel_camp_fmt[x])
            
            campanha = df_todas.iloc[idx]
            filtros_db = json.loads(campanha['filtros_config'])
            
            # --- CARD DE DETALHES + BOTÕES DE AÇÃO ---
            with st.container(border=True):
                # Cabeçalho do Card com Botões na direita
                c_info, c_acts = st.columns([4, 1.5])
                
                with c_info:
                    st.markdown(f"**Campanha:** {campanha['nome_campanha']}")
                    st.caption(f"ID: {campanha['id']} | Status: {campanha['status']} | Criada em: {campanha['data_criacao']}")
                    st.markdown(f"**Objetivo:** {campanha['objetivo']}")
                    st.markdown("**Filtros Automáticos:**")
                    st.info(campanha['filtros_aplicaveis']) 

                with c_acts:
                    st.markdown("<br>", unsafe_allow_html=True) # Espaçamento
                    # Botão Editar
                    if st.button("✏️ Editar", key="btn_edit_camp", use_container_width=True):
                        dialog_editar_campanha(campanha)
                    
                    # Botão Excluir
                    if st.button("🗑️ Excluir", key="btn_del_camp", type="primary", use_container_width=True):
                        dialog_excluir_campanha(campanha['id'], campanha['nome_campanha'])

            # --- ÁREA DE EXECUÇÃO DA PESQUISA ---
            st.markdown("#### 🔎 Filtros Adicionais (Opcional)")
            st.caption("Refine a busca dentro desta campanha.")
            
            if 'filtros_extras' not in st.session_state: st.session_state['filtros_extras'] = []
            
            # Recarrega opções de campos para o filtro extra
            opcoes_campos = []
            mapa_campos = {}
            for grupo, lista in pf_pesquisa.CAMPOS_CONFIG.items():
                for item in lista:
                    chave = f"{grupo} -> {item['label']}"
                    opcoes_campos.append(chave)
                    mapa_campos[chave] = item

            fe1, fe2, fe3, fe4 = st.columns([2, 1.5, 2, 1])
            ex_campo = fe1.selectbox("Campo Extra", opcoes_campos, key="cp_ex")
            ex_op = fe2.selectbox("Operador", ["=", ">", "<", "Contém"], key="op_ex")
            ex_val = fe3.text_input("Valor", key="val_ex")
            
            if fe4.button("➕ Add", key="add_ex"):
                dado_ex = mapa_campos[ex_campo]
                st.session_state['filtros_extras'].append({
                    'label': dado_ex['label'], 'coluna': dado_ex['coluna'], 'tabela': dado_ex['tabela'],
                    'tipo': dado_ex['tipo'], 'operador': ex_op, 'valor': ex_val
                })
            
            if st.session_state['filtros_extras']:
                st.write("Extras:")
                for i, fx in enumerate(st.session_state['filtros_extras']):
                    st.text(f"{i+1}. {fx['label']} {fx['operador']} {fx['valor']}")
                if st.button("Limpar Extras"): st.session_state['filtros_extras'] = []

            st.divider()

            # Botão Principal de Busca
            if st.button("🔎 VISUALIZAR PÚBLICO ALVO", type="primary", use_container_width=True):
                todos_filtros = filtros_db + st.session_state['filtros_extras']
                with st.spinner("Analisando base de dados..."):
                    df_res, total = pf_pesquisa.executar_pesquisa_ampla(todos_filtros, pagina=1, itens_por_pagina=5000)
                st.session_state['resultado_campanha_df'] = df_res
                st.session_state['resultado_campanha_total'] = total

            # Resultados e Vinculação
            if 'resultado_campanha_df' in st.session_state and st.session_state['resultado_campanha_df'] is not None:
                df_r = st.session_state['resultado_campanha_df']
                tot = st.session_state['resultado_campanha_total']
                
                st.success(f"Público encontrado: **{tot}** clientes.")
                st.dataframe(df_r[['id', 'nome', 'cpf', 'data_nascimento']], use_container_width=True)
                
                st.info(f"Ao confirmar, o ID **{campanha['id']}** será aplicado no cadastro desses clientes.")
                
                if st.button(f"✅ CONFIRMAR VÍNCULO ({tot} CLIENTES)"):
                    ids = df_r['id'].tolist()
                    if ids:
                        qtd = vincular_campanha_aos_clientes(campanha['id'], campanha['nome_campanha'], ids)
                        st.balloons()
                        st.success(f"{qtd} clientes atualizados com a campanha '{campanha['nome_campanha']}'.")
                        st.session_state['resultado_campanha_df'] = None
                    else:
                        st.error("Lista vazia.")