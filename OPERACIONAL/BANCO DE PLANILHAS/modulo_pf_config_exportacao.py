import streamlit as st
import pandas as pd
import time
import modulo_pf_exportacao as pf_export

def app_config_exportacao():
    st.markdown("## ⚙️ Configuração de Modelos de Exportação")
    st.caption("Gerencie as chaves que conectam os modelos de tela às regras de código (motor fixo).")

    # --- BLOCO DE CRIAÇÃO (NOVO MODELO) ---
    with st.expander("➕ Criar Novo Modelo de Exportação", expanded=False):
        with st.form("form_novo_modelo"):
            nome = st.text_input("Nome Comercial do Modelo", placeholder="Ex: Dados Cadastrais Simples")
            
            # Campo fundamental para conectar com o código Python
            chave_motor = st.text_input("Chave do Motor (Código de Consulta)", 
                                        help="Esta chave deve ser IGUAL à definida no roteamento do arquivo modulo_pf_exportacao.py")
            
            desc = st.text_area("Descrição / Observações")
            
            if st.form_submit_button("💾 Salvar Modelo"):
                if nome and chave_motor:
                    # Chama a função de salvar do motor, passando a chave técnica
                    if pf_export.salvar_modelo(nome, chave_motor, desc):
                        st.success(f"Modelo '{nome}' criado com sucesso!")
                        time.sleep(1)
                        st.rerun()
                else:
                    st.warning("Os campos Nome e Chave do Motor são obrigatórios.")

    st.divider()
    st.subheader("📋 Modelos Cadastrados")

    # --- LISTAGEM DOS MODELOS ---
    df_modelos = pf_export.listar_modelos_ativos()
    
    if not df_modelos.empty:
        for _, row in df_modelos.iterrows():
            # Exibe o modelo e sua chave técnica
            label_expander = f"📦 {row['nome_modelo']} (Chave: {row.get('codigo_de_consulta', 'Sem Chave')})"
            
            with st.expander(label_expander):
                st.write(f"**Descrição:** {row['descricao']}")
                st.caption(f"Criado em: {row['data_criacao']} | Status: {row['status']}")
                
                # Botões de Ação
                c1, c2 = st.columns([1, 1])
                
                with c1:
                    if st.button(f"✏️ Editar", key=f"edit_{row['id']}", use_container_width=True):
                        dialog_editar_modelo(row)
                
                with c2:
                    if st.button(f"🗑️ Excluir", key=f"del_{row['id']}", use_container_width=True):
                        dialog_excluir_modelo(row['id'], row['nome_modelo'])
    else:
        st.info("Nenhum modelo configurado no momento.")

# --- DIÁLOGOS (POP-UPS) ---

@st.dialog("✏️ Editar Modelo")
def dialog_editar_modelo(modelo):
    """Pop-up para editar dados do modelo e sua chave técnica"""
    with st.form("form_edit_modelo"):
        novo_nome = st.text_input("Nome do Modelo", value=modelo['nome_modelo'])
        # Permite alterar a chave caso tenha sido cadastrada errada
        nova_chave = st.text_input("Chave do Motor (Código de Consulta)", value=modelo.get('codigo_de_consulta', ''))
        nova_desc = st.text_area("Descrição", value=modelo['descricao'])
        
        c1, c2 = st.columns(2)
        if c1.form_submit_button("💾 Salvar Alterações"):
            if pf_export.atualizar_modelo(modelo['id'], novo_nome, nova_chave, nova_desc):
                st.success("Modelo atualizado!")
                time.sleep(1)
                st.rerun()
        
        if c2.form_submit_button("Cancelar"):
            st.rerun()

@st.dialog("⚠️ Confirmar Exclusão")
def dialog_excluir_modelo(id_modelo, nome_modelo):
    """Pop-up de segurança para confirmar a remoção"""
    st.warning(f"Tem certeza que deseja excluir o modelo: **{nome_modelo}**?")
    st.error("Esta ação removerá a opção de exportação da tela de pesquisa.")
    
    # Trava de segurança simples
    confirmar = st.checkbox("Estou ciente e quero excluir.")
    
    if st.button("🚨 CONFIRMAR EXCLUSÃO", use_container_width=True, disabled=not confirmar):
        if pf_export.excluir_modelo(id_modelo):
            st.success("Modelo removido!")
            time.sleep(1)
            st.rerun()
    
    if st.button("Cancelar", use_container_width=True):
        st.rerun()