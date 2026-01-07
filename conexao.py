import psycopg2
import streamlit as st
from sqlalchemy import create_engine

# =============================================================================
# 1. CARREGAMENTO DE CREDENCIAIS (Via Streamlit Secrets)
# =============================================================================
# Certifique-se de que o arquivo .streamlit/secrets.toml existe e tem essas chaves.
try:
    host = st.secrets["DB_HOST"]
    port = st.secrets["DB_PORT"]
    database = st.secrets["DB_NAME"]
    user = st.secrets["DB_USER"]
    password = st.secrets["DB_PASS"]
except Exception as e:
    st.error("Erro ao carregar secrets. Verifique o arquivo .streamlit/secrets.toml")
    st.stop()

# =============================================================================
# 2. FUNÇÃO DE CONEXÃO PADRÃO (PSYCOPG2)
# =============================================================================
# Usada para conexões diretas e rápidas (padrão dos novos módulos)
def get_conn():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        print(f"Erro de conexão (Psycopg2): {e}")
        return None

# =============================================================================
# 3. FUNÇÃO DE CONEXÃO ORM (SQLALCHEMY)
# =============================================================================
# Usada por scripts de Pandas (to_sql, read_sql com engine) e legados
def criar_conexao():
    try:
        url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        return create_engine(url)
    except Exception as e:
        print(f"Erro de conexão (SQLAlchemy): {e}")
        return None

# Teste simples se rodar direto
if __name__ == "__main__":
    if get_conn():
        print("Conexão bem-sucedida!")
    else:
        print("Falha na conexão.")