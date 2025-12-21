import streamlit as st
import pandas as pd
from sqlalchemy import text
import os

# Importação segura da conexão via SQLAlchemy
try:
    from conexao import criar_conexao
except ImportError:
    st.error("Erro: Arquivo conexao.py não localizado.")

def listar_clientes(engine):
    st.subheader("📋 Lista de Clientes (Base Admin)")
    try:
        # Consulta ao banco usando SQLAlchemy
        with engine.connect() as conn:
            query = text("SELECT id, nome, email, telefone FROM admin.clientes ORDER BY id")
            df = pd.read_sql(query, conn)
        
        if df.empty:
            st.info("Nenhum registro encontrado na tabela admin.clientes.")
        else:
            # Exibe a tabela de forma interativa no navegador
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Erro ao acessar a tabela: {e}")
        st.info("Certifique-se de que o schema 'admin' e a tabela 'clientes' existem no seu banco PostgreSQL.")

def cadastrar_cliente(engine):
    st.subheader("➕ Novo Cadastro Administrativo")
    
    # Substituímos o input() por um formulário visual
    with st.form("form_admin_novo_cli", clear_on_submit=True):
        nome = st.text_input("Nome do Cliente")
        email = st.text_input("E-mail")
        tel = st.text_input("Telefone")
        
        if st.form_submit_button("💾 Salvar no Banco"):
            if nome and email:
                try:
                    novo_df = pd.DataFrame({
                        'nome': [nome],
                        'email': [email],
                        'telefone': [tel]
                    })
                    
                    # Salva os dados no banco de dados
                    novo_df.to_sql('clientes', engine, if_exists='append', index=False, schema='admin')
                    st.success(f"Cliente '{nome}' salvo com sucesso!")
                except Exception as e:
                    st.error(f"Erro ao salvar: {e}")
            else:
                st.warning("Os campos Nome e E-mail são obrigatórios.")

def app_admin_clientes():
    # Função principal que será chamada pelo sistema.py
    try:
        engine = criar_conexao()
        
        # Menu de navegação interno
        escolha = st.radio("O que deseja fazer?", ["Listar Registros", "Cadastrar Novo"], horizontal=True)
        st.divider()
        
        if escolha == "Listar Registros":
            listar_clientes(engine)
        else:
            cadastrar_cliente(engine)
            
    except Exception as e:
        st.error(f"Falha na conexão com o banco: {e}")

if __name__ == "__main__":
    app_admin_clientes()