import streamlit as st
import pandas as pd
from sqlalchemy import text
import os

# Importação da conexão customizada
try:
    from conexao import criar_conexao
except ImportError:
    st.error("Erro: Módulo de conexão não encontrado.")

def listar_clientes(engine):
    st.subheader("📋 Lista de Clientes")
    try:
        # Lê a tabela do banco usando SQLAlchemy
        query = text("SELECT id, nome, email, telefone FROM admin.clientes ORDER BY id")
        df = pd.read_sql(query, engine)
        
        if df.empty:
            st.info("Nenhum cliente cadastrado no momento.")
        else:
            # Exibe de forma elegante no Streamlit
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Erro ao ler banco de dados: {e}")

def cadastrar_cliente(engine):
    st.subheader("➕ Novo Cadastro")
    
    # O Streamlit usa formulários em vez de input() sequencial
    with st.form("form_novo_cliente", clear_on_submit=True):
        nome = st.text_input("Nome Completo")
        email = st.text_input("E-mail de Contato")
        tel = st.text_input("Telefone")
        
        btn_salvar = st.form_submit_button("Salvar no Banco")
        
        if btn_salvar:
            if nome and email:
                try:
                    # Cria o dataframe com os dados digitados
                    novo_cliente = pd.DataFrame({
                        'nome': [nome],
                        'email': [email],
                        'telefone': [tel]
                    })
                    
                    # Salva no banco (schema admin)
                    novo_cliente.to_sql('clientes', engine, if_exists='append', index=False, schema='admin')
                    st.success(f"✅ Cliente {nome} cadastrado com sucesso!")
                except Exception as e:
                    st.error(f"Erro ao salvar: {e}")
            else:
                st.warning("Por favor, preencha nome e e-mail.")

def app_admin_clientes():
    st.markdown("## Gestão Administrativa de Clientes")
    
    try:
        # Inicia a conexão via SQLAlchemy
        engine = criar_conexao()
        
        menu = st.sidebar.radio("Navegação", ["Listar Clientes", "Cadastrar Novo"])
        
        if menu == "Listar Clientes":
            listar_clientes(engine)
        else:
            cadastrar_cliente(engine)
            
    except Exception as e:
        st.error(f"Falha na conexão com o banco de dados: {e}")

if __name__ == "__main__":
    app_admin_clientes()
    