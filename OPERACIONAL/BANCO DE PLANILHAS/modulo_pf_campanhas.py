import streamlit as st
import pandas as pd
import json
import time
from datetime import date
# Importações dos módulos que estão na mesma pasta
import modulo_pf_cadastro as pf_core
import modulo_pf_pesquisa as pf_pesquisa

# --- FUNÇÕES DE BANCO DE DADOS ---

def salvar_campanha(nome, objetivo, status, filtros_lista):
    conn = pf_core.get_conn()
    if conn:
        try:
            # Prepara os dados
            filtros_json = json.dumps(filtros_lista, default=str)
            
            # Cria o texto legível para a coluna 'filtros_aplicaveis'
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

def listar_campanhas_ativas():
    conn = pf_core.get_conn()
    if conn:
        try:
            # Busca apenas as ativas
            query = """
                SELECT id, nome_campanha, filtros_config, filtros_aplicaveis, objetivo, data_criacao 
                FROM pf_campanhas 
                WHERE status = 'ATIVO' 
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

            # Atualiza o campo id_campanha na tabela pf_dados para os IDs filtrados
            # Concatena o ID novo caso já exista algum? 
            # Neste modelo simplificado, vamos substituir ou definir o ID da campanha atual.
            # Convertendo lista para tupla SQL
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

# --- INTERFACE PRINCIPAL ---

def app_campanhas():
    st.markdown("## 📢 Gestão de Campanhas e Perfilamento")
    
    # Abas conforme solicitado
    tab_config, tab_aplicar = st.tabs(["⚙️ Configurar Campanha", "🚀 Executar Campanha"])

    # =========================================================================
    # ABA 1: CONFIGURAÇÃO (CRIAR)
    # =========================================================================
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
            
            # --- Gerenciador de Filtros (Memória Temporária) ---
            if 'campanha_filtros_temp' not in st.session_state:
                st.session_state['campanha_filtros_temp'] = []

            # Seletor de Campos (Reutilizando a config do modulo_pf_pesquisa)
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
            
            # Botão para adicionar filtro à lista visual
            if rc4.form_submit_button("➕ Incluir"):
                if valor_sel:
                    dado = mapa_campos[campo_sel]
                    novo_filtro = {
                        'label': dado['label'],
                        'coluna': dado['coluna'],
                        'tabela': dado['tabela'],
                        'tipo': dado['tipo'],
                        'operador': op_sel,
                        'valor': valor_sel,
                        'descricao_visual': f"({dado['tabela']}, {dado['label']}, {op_sel}, {valor_sel})"
                    }
                    st.session_state['campanha_filtros_temp'].append(novo_filtro)
                    st.rerun()

            # Lista os filtros adicionados
            if st.session_state['campanha_filtros_temp']:
                st.markdown("**Filtros Aplicáveis Selecionados:**")
                txt_display = ""
                for f in st.session_state['campanha_filtros_temp']:
                    txt_display += f"- {f['descricao_visual']}\n"
                st.code(txt_display, language="text")
                
                if st.form_submit_button("🗑️ Limpar Lista"):
                    st.session_state['campanha_filtros_temp'] = []
                    st.rerun()

            st.markdown("---")
            
            # Botão Final de Salvar
            if st.form_submit_button("💾 SALVAR CAMPANHA"):
                if nome and st.session_state['campanha_filtros_temp']:
                    if salvar_campanha(nome, objetivo, status, st.session_state['campanha_filtros_temp']):
                        st.success(f"Campanha '{nome}' criada com sucesso!")
                        st.session_state['campanha_filtros_temp'] = [] # Limpa memória
                        time.sleep(1.5)
                        st.rerun()
                else:
                    st.warning("Preencha o nome e adicione pelo menos um filtro.")

    # =========================================================================
    # ABA 2: APLICAR (EXECUTAR)
    # =========================================================================
    with tab_aplicar:
        st.markdown("### 🚀 Executar Campanha")
        
        df_ativas = listar_campanhas_ativas()
        
        if df_ativas.empty:
            st.info("Nenhuma campanha ativa no momento.")
        else:
            # 1. Seleção da Campanha
            sel_camp_fmt = df_ativas.apply(lambda x: f"#{x['id']} - {x['nome_campanha']}", axis=1)
            idx = st.selectbox("Selecione a Campanha", range(len(df_ativas)), format_func=lambda x: sel_camp_fmt[x])
            
            campanha = df_ativas.iloc[idx]
            filtros_db = json.loads(campanha['filtros_config'])
            
            # 2. Visualização Fixa
            with st.container(border=True):
                st.markdown(f"**Campanha:** {campanha['nome_campanha']} (ID: {campanha['id']})")
                st.markdown(f"**Objetivo:** {campanha['objetivo']}")
                st.markdown("**Filtros Pré-Definidos (Automáticos):**")
                st.info(campanha['filtros_aplicaveis']) # Mostra o texto visual salvo
            
            st.markdown("#### 🔎 Filtros Adicionais (Opcional)")
            st.caption("Você pode refinar ainda mais a busca acima adicionando novos filtros aqui (Ex: Filtrar por Idade específica dentro da campanha).")
            
            # --- Filtros Extras (Opcionais) ---
            if 'filtros_extras' not in st.session_state: st.session_state['filtros_extras'] = []
            
            fe1, fe2, fe3, fe4 = st.columns([2, 1.5, 2, 1])
            ex_campo = fe1.selectbox("Campo Extra", opcoes_campos, key="cp_ex")
            ex_op = fe2.selectbox("Operador", ["=", ">", "<", "Contém"], key="op_ex")
            ex_val = fe3.text_input("Valor", key="val_ex")
            
            if fe4.button("➕ Add"):
                dado_ex = mapa_campos[ex_campo]
                st.session_state['filtros_extras'].append({
                    'label': dado_ex['label'], 'coluna': dado_ex['coluna'], 'tabela': dado_ex['tabela'],
                    'tipo': dado_ex['tipo'], 'operador': ex_op, 'valor': ex_val
                })
            
            # Mostra filtros extras se houver
            if st.session_state['filtros_extras']:
                st.write("Filtros Extras:")
                for i, fx in enumerate(st.session_state['filtros_extras']):
                    st.text(f"{i+1}. {fx['label']} {fx['operador']} {fx['valor']}")
                if st.button("Limpar Extras"):
                    st.session_state['filtros_extras'] = []

            st.divider()

            # 3. Botão de Pesquisa
            col_search, col_action = st.columns([1, 2])
            
            if col_search.button("🔎 VISUALIZAR RESULTADOS", type="primary"):
                # Combina filtros da campanha + filtros extras
                todos_filtros = filtros_db + st.session_state['filtros_extras']
                
                # Executa busca usando o módulo de pesquisa
                with st.spinner("Buscando clientes..."):
                    df_res, total = pf_pesquisa.executar_pesquisa_ampla(todos_filtros, pagina=1, itens_por_pagina=5000)
                    
                st.session_state['resultado_campanha_df'] = df_res
                st.session_state['resultado_campanha_total'] = total

            # 4. Resultados e Vinculação
            if 'resultado_campanha_df' in st.session_state and st.session_state['resultado_campanha_df'] is not None:
                df_r = st.session_state['resultado_campanha_df']
                tot = st.session_state['resultado_campanha_total']
                
                st.success(f"Foram encontrados **{tot}** clientes compatíveis.")
                st.dataframe(df_r[['id', 'nome', 'cpf', 'data_nascimento']], use_container_width=True)
                
                st.warning(f"⚠️ Atenção: Ao confirmar, o ID da campanha **{campanha['id']}** será gravado no cadastro desses clientes.")
                
                if st.button(f"✅ VINCULAR {tot} CLIENTES À CAMPANHA"):
                    ids = df_r['id'].tolist()
                    if ids:
                        qtd = vincular_campanha_aos_clientes(campanha['id'], campanha['nome_campanha'], ids)
                        st.balloons()
                        st.success(f"Processo concluído! {qtd} clientes foram atualizados com a campanha '{campanha['nome_campanha']}'.")
                        st.session_state['resultado_campanha_df'] = None # Limpa tela
                    else:
                        st.error("Lista vazia.")