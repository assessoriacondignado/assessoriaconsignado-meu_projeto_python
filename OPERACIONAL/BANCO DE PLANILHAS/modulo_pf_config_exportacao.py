import streamlit as st
import pandas as pd
import psycopg2
import json
import time
from datetime import datetime

# Importa as configurações de conexão e pesquisa (para reaproveitar o mapeamento de campos)
try:
    import conexao
    import modulo_pf_pesquisa as pf_pesquisa
except ImportError:
    st.error("Erro: Dependências (conexao.py ou modulo_pf_pesquisa.py) não encontradas.")

# --- CONEXÃO ---
def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

# --- FUNÇÕES DE BANCO (CRUD) ---
def listar_configuracoes():
    conn = get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT * FROM banco_pf.pf_tipos_exportacao ORDER BY id DESC", conn)
            conn.close()
            return df
        except: conn.close()
    return pd.DataFrame()

def salvar_configuracao(nome, colunas_json, modulo, desc, status, id_edit=None):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            if id_edit:
                # Atualizar
                sql = """
                    UPDATE banco_pf.pf_tipos_exportacao 
                    SET nome_exportacao=%s, colunas_exportacao=%s, modulo_vinculado=%s, 
                        descricao=%s, status=%s, data_atualizacao=NOW() 
                    WHERE id=%s
                """
                cur.execute(sql, (nome, colunas_json, modulo, desc, status, id_edit))
            else:
                # Inserir
                sql = """
                    INSERT INTO banco_pf.pf_tipos_exportacao 
                    (nome_exportacao, colunas_exportacao, modulo_vinculado, descricao, status) 
                    VALUES (%s, %s, %s, %s, %s)
                """
                cur.execute(sql, (nome, colunas_json, modulo, desc, status))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")
            conn.close()
    return False

def excluir_configuracao(id_config):
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM banco_pf.pf_tipos_exportacao WHERE id=%s", (id_config,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao excluir: {e}")
            conn.close()
    return False

# --- UTILITÁRIOS ---
def carregar_opcoes_campos():
    """Transforma o dicionário CAMPOS_CONFIG do módulo pesquisa em uma lista plana para seleção."""
    opcoes = []
    mapa_detalhado = {}
    
    # Adiciona campos especiais que podem não estar na pesquisa mas são úteis na exportação
    # Ex: Matricula e Convenio sempre disponíveis se não estiverem no config
    
    for grupo, campos in pf_pesquisa.CAMPOS_CONFIG.items():
        for campo in campos:
            # Cria uma chave única legível: "Dados Pessoais > Nome"
            chave_visual = f"{grupo} > {campo['label']}"
            opcoes.append(chave_visual)
            mapa_detalhado[chave_visual] = {
                'tabela': campo['tabela'],
                'coluna': campo['coluna'],
                'label': campo['label'],
                'tipo': campo['tipo']
            }
            
    return sorted(opcoes), mapa_detalhado

# --- INTERFACE ---
def app_config_exportacao():
    st.markdown("## ⚙️ Configuração de Exportação")
    st.caption("Defina os layouts de colunas para exportação dos módulos de Campanha e Pesquisa.")

    # Estado para controle de edição
    if 'export_edit_id' not in st.session_state: st.session_state['export_edit_id'] = None
    if 'export_colunas_sel' not in st.session_state: st.session_state['export_colunas_sel'] = []

    # Carrega campos disponíveis do módulo de pesquisa
    lista_opcoes, mapa_campos = carregar_opcoes_campos()

    # --- FORMULÁRIO (NOVO / EDITAR) ---
    with st.expander("📝 Nova / Editar Configuração", expanded=(st.session_state['export_edit_id'] is not None)):
        
        # Recupera dados se estiver editando
        dados_edit = {}
        if st.session_state['export_edit_id']:
            conn = get_conn()
            if conn:
                df_edit = pd.read_sql(f"SELECT * FROM banco_pf.pf_tipos_exportacao WHERE id = {st.session_state['export_edit_id']}", conn)
                conn.close()
                if not df_edit.empty:
                    dados_edit = df_edit.iloc[0]
                    # Carrega as colunas já salvas para o multiselect
                    try:
                        cols_salvas = json.loads(dados_edit['colunas_exportacao'])
                        # Reconstrói a lista visual baseada no JSON salvo
                        st.session_state['export_colunas_sel'] = [c['chave_visual'] for c in cols_salvas if 'chave_visual' in c]
                    except: st.session_state['export_colunas_sel'] = []

        with st.form("form_config_export"):
            c1, c2, c3 = st.columns([3, 1.5, 1])
            nome = c1.text_input("Nome da Exportação", value=dados_edit.get('nome_exportacao', ''))
            modulo = c2.selectbox("Módulo Vinculado", ["PESQUISA_AMPLA", "CAMPANHA"], 
                                  index=0 if dados_edit.get('modulo_vinculado') == 'PESQUISA_AMPLA' else 1)
            status = c3.selectbox("Status", ["ATIVO", "INATIVO"], 
                                  index=0 if dados_edit.get('status', 'ATIVO') == 'ATIVO' else 1)
            
            descricao = st.text_area("Descrição", value=dados_edit.get('descricao', ''), height=70)
            
            st.markdown("#### 🏗️ Seleção e Ordem das Colunas")
            st.caption("Selecione os campos na ordem que devem aparecer no Excel/CSV.")
            
            # O multiselect do Streamlit respeita a ordem de seleção visual
            colunas_selecionadas = st.multiselect(
                "Colunas Disponíveis (Selecione na ordem desejada):", 
                options=lista_opcoes,
                default=st.session_state['export_colunas_sel']
            )

            c_btn1, c_btn2 = st.columns([1, 5])
            if c_btn1.form_submit_button("💾 Salvar Configuração"):
                if nome and colunas_selecionadas:
                    # Monta o JSON técnico para salvar no banco
                    lista_final_json = []
                    for item_visual in colunas_selecionadas:
                        dados_tecnicos = mapa_campos.get(item_visual)
                        if dados_tecnicos:
                            dados_tecnicos['chave_visual'] = item_visual # Guarda a chave para recarregar depois
                            lista_final_json.append(dados_tecnicos)
                    
                    json_str = json.dumps(lista_final_json)
                    
                    if salvar_configuracao(nome, json_str, modulo, descricao, status, st.session_state['export_edit_id']):
                        st.success("Salvo com sucesso!")
                        # Limpa estado
                        st.session_state['export_edit_id'] = None
                        st.session_state['export_colunas_sel'] = []
                        time.sleep(1)
                        st.rerun()
                else:
                    st.warning("Nome e pelo menos uma coluna são obrigatórios.")
            
            if c_btn2.form_submit_button("Cancelar"):
                st.session_state['export_edit_id'] = None
                st.session_state['export_colunas_sel'] = []
                st.rerun()

    # --- LISTAGEM ---
    st.divider()
    st.markdown("### 📋 Configurações Existentes")
    
    df_configs = listar_configuracoes()
    
    if not df_configs.empty:
        for _, row in df_configs.iterrows():
            cor_status = "🟢" if row['status'] == 'ATIVO' else "🔴"
            with st.expander(f"{cor_status} {row['nome_exportacao']} ({row['modulo_vinculado']})"):
                st.write(f"**Descrição:** {row['descricao']}")
                
                # Mostra prévia das colunas
                try:
                    cols_json = json.loads(row['colunas_exportacao'])
                    labels = [c['label'] for c in cols_json]
                    st.caption(f"**Colunas ({len(labels)}):** {', '.join(labels)}")
                except: st.caption("Erro ao ler colunas.")
                
                c_act1, c_act2 = st.columns([1, 5])
                if c_act1.button("✏️ Editar", key=f"ed_{row['id']}"):
                    st.session_state['export_edit_id'] = row['id']
                    st.rerun()
                
                if c_act2.button("🗑️ Excluir", key=f"del_{row['id']}"):
                    dialog_confirmar_exclusao(row['id'], row['nome_exportacao'])
    else:
        st.info("Nenhuma configuração de exportação criada.")

@st.dialog("⚠️ Confirmar Exclusão")
def dialog_confirmar_exclusao(id_config, nome):
    st.error(f"Tem certeza que deseja excluir o modelo **{nome}**?")
    st.warning("Esta ação é irreversível.")
    
    c1, c2 = st.columns(2)
    if c1.button("Sim, Excluir Definitivamente"):
        if excluir_configuracao(id_config):
            st.success("Excluído com sucesso!")
            time.sleep(1)
            st.rerun()
    
    if c2.button("Cancelar"):
        st.rerun()

if __name__ == "__main__":
    app_config_exportacao()