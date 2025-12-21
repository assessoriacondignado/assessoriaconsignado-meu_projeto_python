import psycopg2
import streamlit as st

# Configurações do Banco de Dados Absam via Streamlit Secrets
# O sistema agora busca os dados diretamente da aba 'Secrets' do Streamlit Cloud
host = st.secrets["DB_HOST"]
port = st.secrets["DB_PORT"]
database = st.secrets["DB_NAME"]
user = st.secrets["DB_USER"]
password = st.secrets["DB_PASS"]

# Função de teste (opcional)
def testar_conexao():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print("Conexão OK!")
        conn.close()
        return True
    except Exception as e:
        print(f"Erro: {e}")
        return False

# Função auxiliar para módulos que usam SQLAlchemy (como o modulo_admin_clientes.py)
def criar_conexao():
    from sqlalchemy import create_engine
    # Constrói a URL de conexão para o SQLAlchemy
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)