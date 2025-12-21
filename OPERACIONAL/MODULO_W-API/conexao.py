import psycopg2

# Configurações do Banco de Dados Absam
host = "db-60137.dc-us-1.absamcloud.com"
port = "27355"
database = "assessoria"
user = "admin"
password = "jdchfbma5b5d"  # <--- Coloque sua senha real do banco aqui

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