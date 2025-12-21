import pandas as pd
from sqlalchemy import text
from conexao import criar_conexao

def listar_clientes(engine):
    print("\n--- LISTA DE CLIENTES ---")
    # Lê a tabela do banco
    df = pd.read_sql(text("SELECT * FROM admin.clientes ORDER BY id"), engine)
    if df.empty:
        print("Nenhum cliente cadastrado.")
    else:
        # Mostra apenas as colunas principais
        print(df[['id', 'nome', 'email', 'telefone']])
    print("-------------------------\n")

def cadastrar_cliente(engine):
    print("\n--- NOVO CADASTRO ---")
    # O Python pede para você digitar
    nome = input("Digite o Nome: ")
    email = input("Digite o Email: ")
    tel = input("Digite o Telefone: ")
    
    # Cria o dataframe com os dados digitados
    novo_cliente = pd.DataFrame({
        'nome': [nome],
        'email': [email],
        'telefone': [tel]
    })
    
    # Salva no banco
    novo_cliente.to_sql('clientes', engine, if_exists='append', index=False, schema='admin')
    print("✅ Cliente cadastrado com sucesso!\n")

# --- PROGRAMA PRINCIPAL ---
def iniciar_sistema():
    try:
        engine = criar_conexao()
        print("✅ Conectado ao Banco de Dados!")

        while True:
            print("1 - Listar Clientes")
            print("2 - Cadastrar Novo Cliente")
            print("3 - Sair")
            opcao = input("Escolha uma opção: ")

            if opcao == '1':
                listar_clientes(engine)
            elif opcao == '2':
                cadastrar_cliente(engine)
            elif opcao == '3':
                print("Saindo...")
                break
            else:
                print("Opção inválida!")

    except Exception as e:
        print(f"Erro crítico: {e}")

# Roda o sistema
if __name__ == "__main__":
    iniciar_sistema()