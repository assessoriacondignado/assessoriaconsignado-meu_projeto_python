import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime
import modulo_pf_cadastro as pf_core
import modulo_pf_exportacao as pf_export # Onde ficarão as funções de processamento

def app_config_exportacao():
    st.markdown("## ⚙️ Configuração de Exportação")
    
    # 2. SISTEMA APRESENTA AS DUAS OPÇÕES (COMO UM MENU)
    escolha_tipo = st.radio(
        "Selecione o Tipo de Exportação:",
        ["Exportação Simples", "Exportação Ampla"],
        horizontal=True
    )

    st.divider()

    # --- FLUXO 1: EXPORTAÇÃO SIMPLES ---
    if escolha_tipo == "Exportação Simples":
        st.subheader("📄 Modelos de Exportação Simples")
        st.caption("Baseado na tabela pf_modelos_exportacao")

        # Opção de criar novo (Bloco Retrátil)
        with st.expander("➕ Criar Novo Modelo Simples"):
            with st.form("form_novo_simples"):
                nome = st.text_input("Nome do Modelo")
                desc = st.text_area("Descrição")
                if st.form_submit_button("Salvar Modelo"):
                    if pf_export.salvar_modelo(nome, "SIMPLES", desc):
                        st.success("Modelo criado!")
                        st.rerun()

        # Listagem de modelos existentes em Blocos (Retrátil)
        df_modelos = pf_export.listar_modelos_ativos()
        if not df_modelos.empty:
            for _, row in df_modelos.iterrows():
                # 4. APRESENTA OPÇÃO EM BLOCO (NOME E CONTEÚDO RETRÁTIL)
                with st.expander(f"📋 {row['nome_modelo']}"):
                    st.write(f"**Descrição:** {row['descricao']}")
                    st.caption(f"Tipo: {row['tipo_processamento']}")
                    
                    # 5. CLIENTE FAZ A EXPORTAÇÃO
                    if st.button(f"🚀 Executar Exportação: {row['nome_modelo']}", key=f"btn_s_{row['id']}"):
                        st.info("Processando exportação completa da planilha...")
                        # Aqui chamaria a lógica de exportação massiva (até 1M linhas)
                        # O pf_export.gerar_arquivo_massivo() deve ser implementado no outro módulo
                        pass

    # --- FLUXO 2: EXPORTAÇÃO AMPLA ---
    else:
        st.subheader("🚀 Exportação Ampla (Processos Complexos)")
        st.caption("Executa funções específicas descritas no código Python")

        # Opção de criar nova campanha (Bloco Retrátil)
        with st.expander("➕ Configurar Nova Campanha de Exportação"):
            with st.form("form_nova_ampla"):
                c1, c2 = st.columns(2)
                nome_camp = c1.text_input("Nome da Campanha")
                funcao = c2.selectbox("Função no Código", ["proc_financeiro_complexo", "cruzamento_satelite_full"])
                objetivo = st.text_area("Objetivo")
                
                if st.form_submit_button("Cadastrar Campanha"):
                    # Lógica para salvar na nova tabela banco_pf.pf_campanhas_exportacao
                    conn = pf_core.get_conn()
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO banco_pf.pf_campanhas_exportacao (nome_campanha, objetivo, funcao_codigo, status)
                        VALUES (%s, %s, %s, 'ATIVO')
                    """, (nome_camp, objetivo, funcao))
                    conn.commit()
                    st.success("Campanha de exportação cadastrada!")
                    st.rerun()

        # Listagem das Campanhas Amplas em Blocos (Retrátil)
        try:
            conn = pf_core.get_conn()
            df_ampla = pd.read_sql("SELECT * FROM banco_pf.pf_campanhas_exportacao WHERE status='ATIVO'", conn)
            conn.close()
        except: df_ampla = pd.DataFrame()

        if not df_ampla.empty:
            for _, row in df_ampla.iterrows():
                # 4. APRESENTA OPÇÃO EM BLOCO
                with st.expander(f"🔥 {row['nome_campanha']}"):
                    st.write(f"**Objetivo:** {row['objetivo']}")
                    st.write(f"**Função Interna:** `{row['funcao_codigo']}`")
                    st.caption(f"Criado em: {row['data_criacao']}")
                    
                    # 5. CLIENTE FAZ A EXPORTAÇÃO
                    if st.button(f"⚡ Iniciar Processamento Complexo", key=f"btn_a_{row['id']}"):
                        # Chama a lógica no modulo_pf_exportacao enviando o nome da função
                        st.warning(f"Executando lógica complexa: {row['funcao_codigo']}...")
                        pass