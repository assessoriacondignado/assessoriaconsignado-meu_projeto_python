import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURAÇÕES ---
# Substitua 'SUA_SENHA' pela senha real do banco
SENHA = 'jdchfbma5b5d' 

# Dados da Absam
USUARIO = 'admin'
HOST = 'db-60137.dc-us-1.absamcloud.com'
PORTA = '27355'
BANCO = 'assessoria'

# Criando a conexão
url_conexao = f"postgresql://{USUARIO}:{SENHA}@{HOST}:{PORTA}/{BANCO}"
engine = create_engine(url_conexao)

print("1. Conectando ao banco...")

# --- DADOS PARA SALVAR ---
novo_cliente = pd.DataFrame({
    'nome': ['Robô Python'],
    'email': ['robo@python.com'],
    'telefone': ['11 99999-0000']
})

try:
    # Salvando na tabela 'clientes' dentro do esquema 'admin'
    novo_cliente.to_sql('clientes', engine, if_exists='append', index=False, schema='admin')
    print("✅ Sucesso! O Python salvou o cliente no banco.")
    
    # Lendo para confirmar
    print("2. Lendo os dados do banco:")
    with engine.connect() as conn:
        consulta = pd.read_sql(text("SELECT * FROM admin.clientes"), conn)
        print(consulta)

except Exception as erro:
    print("❌ Deu erro:", erro)