import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime
import modulo_pf_cadastro as pf_core
import modulo_pf_exportacao as pf_export

def app_config_exportacao():
    st.markdown("## ⚙️ Configuração de Exportação")
    
    # Seleção do Tipo (Simples ou Ampla)
    escolha_tipo = st.radio(
        "Selecione o Tipo de Exportação:",
        ["Exportação Simples", "Exportação Ampla"],
        horizontal=True
    )

    st.divider()

    # --- FLUXO 1: EXPORTAÇÃO SIMPLES ---
    if escolha_tipo == "Exportação Simples":
        st.subheader("📄 Modelos de Exportação Simples")
        
        # Bloco para Criar Novo (Expander)
        with st.expander("➕ Criar Novo Modelo Simples"):
            with st.form("form_novo_simples"):
                nome = st.text_input("Nome do Modelo")
                desc = st.text_area("Descrição (Listagem de campos)")
                if st.form_submit_button("Salvar Modelo"):
                    if pf_export.salvar_modelo(nome, "SIMPLES", desc):
                        st.success("Modelo criado com sucesso!")
                        st.rerun()

        # Listagem de modelos existentes
        df_modelos = pf_export.listar_modelos_ativos()
        if not df_modelos.empty:
            for _, row in df_modelos.iterrows():
                with st.expander(f"📋 {row['nome_modelo']}"):
                    st.write(f"**Descrição:** {row['descricao']}")
                    
                    # Colunas para os botões de ação
                    c1, c2, c3 = st.columns([2, 1, 1])
                    
                    with c1:
                        if st.button(f"🚀 Executar Exportação: {row['nome_modelo']}", key=f"exec_{row['id']}"):
                            st.info("Iniciando processamento...")
                            # Lógica de exportação aqui
                    
                    with c2:
                        # Botão EDITAR (Abre Diálogo)
                        if st.button(f"✏️ Editar", key=f"edit_{row['id']}", use_container_width=True):
                            dialog_editar_modelo(row)
                    
                    with c3:
                        # Botão EXCLUIR (Abre Diálogo de Dupla Confirmação)
                        if st.button(f"🗑️ Excluir", key=f"del_{row['id']}", use_container_width=True, type="secondary"):
                            dialog_excluir_modelo(row['id'], row['nome_modelo'])

    # --- FLUXO 2: EXPORTAÇÃO AMPLA ---
    else:
        st.subheader("🚀 Exportação Ampla (Processos Complexos)")
        # ... (Mantém a lógica de listagem da tabela banco_pf.pf_campanhas_exportacao)
        # Recomenda-se aplicar os mesmos botões c2 e c3 aqui para as campanhas.

# --- DIÁLOGOS (POP-UPS) ---

@st.dialog("✏️ Editar Modelo")
def dialog_editar_modelo(modelo):
    """Pop-up para editar nome e descrição do modelo simples"""
    with st.form("form_edit_modelo"):
        novo_nome = st.text_input("Nome do Modelo", value=modelo['nome_modelo'])
        nova_desc = st.text_area("Descrição", value=modelo['descricao'])
        
        c1, c2 = st.columns(2)
        if c1.form_submit_button("💾 Salvar Alterações"):
            if pf_export.atualizar_modelo(modelo['id'], novo_nome, "SIMPLES", nova_desc):
                st.success("Atualizado!")
                time.sleep(1)
                st.rerun()
        if c2.form_submit_button("Cancelar"):
            st.rerun()

@st.dialog("⚠️ Confirmar Exclusão")
def dialog_excluir_modelo(id_modelo, nome_modelo):
    """Pop-up de Dupla Confirmação para exclusão"""
    st.warning(f"Você tem certeza que deseja excluir o modelo: **{nome_modelo}**?")
    st.error("Esta ação não pode ser desfeita.")
    
    # Campo de texto ou checkbox para garantir que o usuário sabe o que está fazendo
    confirmar = st.checkbox("Eu entendo que os dados deste modelo serão apagados permanentemente.")
    
    c1, c2 = st.columns(2)
    if c1.button("🚨 SIM, EXCLUIR", use_container_width=True, disabled=not confirmar):
        if pf_export.excluir_modelo(id_modelo):
            st.success("Modelo removido!")
            time.sleep(1)
            st.rerun()
    
    if c2.button("Cancelar", use_container_width=True):
        st.rerun()