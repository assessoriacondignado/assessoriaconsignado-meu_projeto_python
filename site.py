import streamlit as st
import pandas as pd
from sqlalchemy import text
from conexao import criar_conexao

# Configuração da Página
st.set_page_config(page_title="Sistema Absam", page_icon="🚀")

st.title("🚀 Sistema de Gestão - Absam")

# Criando abas para organizar (Cadastro vs Lista)
aba_cadastro, aba_listagem = st.tabs(["📝 Novo Cadastro", "📋 Ver Clientes"])

# --- ABA 1: CADASTRO ---
with aba_cadastro:
    st.header("Cadastrar Cliente")
    
    # Criamos um formulário para agrupar os campos
    with st.form("meu_formulario"):
        nome = st.text_input("Nome Completo")
        email = st.text_input("E-mail")
        telefone = st.text_input("Telefone")
        
        # O botão que envia o formulário
        botao_salvar = st.form_submit_button("Salvar no Banco")

        if botao_salvar:
            if not nome:
                st.error("Por favor, digite o nome!")
            else:
                try:
                    # Conecta e Salva
                    engine = criar_conexao()
                    novo_df = pd.DataFrame([{
                        'nome': nome,
                        'email': email,
                        'telefone': telefone
                    }])
                    # Salva no esquema 'admin'
                    novo_df.to_sql('clientes', engine, if_exists='append', index=False, schema='admin')
                    st.success(f"✅ Cliente **{nome}** salvo com sucesso!")
                except Exception as e:
                    st.error(f"Erro ao salvar: {e}")

# --- ABA 2: LISTAGEM ---
with aba_listagem:
    st.header("Base de Dados Atual")
    
    # Botão para carregar os dados
    if st.button("🔄 Atualizar Tabela"):
        try:
            engine = criar_conexao()
            # Pega os dados do banco
            df = pd.read_sql(text("SELECT * FROM admin.clientes ORDER BY id DESC"), engine)
            
            # Mostra a tabela interativa na tela
            st.dataframe(df, use_container_width=True)
            st.info(f"Total de clientes: {len(df)}")
            
        except Exception as e:
            st.error(f"Erro ao ler banco: {e}")