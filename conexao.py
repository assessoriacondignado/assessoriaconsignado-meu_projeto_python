import psycopg2
import streamlit as st
from sqlalchemy import create_engine
import os
import toml

# =============================================================================
# 1. CARREGAMENTO DE CREDENCIAIS (Híbrido)
# =============================================================================
host = None
port = None
database = None
user = None
password = None

def carregar_secrets_manualmente():
    """Tenta localizar e ler o secrets.toml manualmente"""
    caminhos_possiveis = [
        ".streamlit/secrets.toml",
        os.path.join(os.getcwd(), ".streamlit/secrets.toml"),
        "/root/meu_sistema/.streamlit/secrets.toml",
        "/root/.streamlit/secrets.toml"
    ]
    
    for caminho in caminhos_possiveis:
        if os.path.exists(caminho):
            try:
                dados = toml.load(caminho)
                return dados
            except Exception as e:
                print(f"Erro ao ler arquivo {caminho}: {e}")
    return None

try:
    # Tentativa 1: Via Streamlit
    host = st.secrets["DB_HOST"]
    port = st.secrets["DB_PORT"]
    database = st.secrets["DB_NAME"]
    user = st.secrets["DB_USER"]
    password = st.secrets["DB_PASS"]
    
except (FileNotFoundError, AttributeError, KeyError):
    # Tentativa 2: Via arquivo direto (Fallback)
    print("⚠️  Modo Streamlit nao detectado. Tentando leitura manual...", flush=True)
    secrets_dict = carregar_secrets_manualmente()
    
    if secrets_dict:
        try:
            host = secrets_dict["DB_HOST"]
            port = secrets_dict["DB_PORT"]
            database = secrets_dict["DB_NAME"]
            user = secrets_dict["DB_USER"]
            password = secrets_dict["DB_PASS"]
            print("✅ Secrets carregados manualmente com sucesso!", flush=True)
        except KeyError as e:
            print(f"❌ Erro: Chave {e} nao encontrada no secrets.toml manual.")
    else:
        print("❌ CRITICO: Nao foi possivel carregar as credenciais.", flush=True)

# =============================================================================
# 2. FUNCOES DE CONEXAO
# =============================================================================
def get_conn():
    try:
        conn = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        return conn
    except Exception as e:
        print(f"Erro de conexão (Psycopg2): {e}")
        return None

def criar_conexao():
    try:
        url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        return create_engine(url)
    except Exception as e:
        print(f"Erro de conexão (SQLAlchemy): {e}")
        return None
