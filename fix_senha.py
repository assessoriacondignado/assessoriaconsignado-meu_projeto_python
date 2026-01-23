import psycopg2
import bcrypt
import os
import sys

# Garante que encontra o arquivo conexao.py
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

try:
    import conexao
except ImportError:
    print("❌ Erro: Arquivo 'conexao.py' não encontrado.")
    sys.exit()

def resetar_senha_admin():
    # 1. Configurações
    EMAIL_ALVO = "alexbarbosaleal@gmail.com"
    NOVA_SENHA = "810401" # <-- A senha que você quer usar
    
    print(f"🔄 Iniciando reset de senha para: {EMAIL_ALVO}")

    # 2. Conectar ao Banco
    try:
        conn = psycopg2.connect(
            host=conexao.host,
            port=conexao.port,
            database=conexao.database,
            user=conexao.user,
            password=conexao.password
        )
        cur = conn.cursor()
    except Exception as e:
        print(f"❌ Erro de conexão com o Banco: {e}")
        return

    # 3. Gerar Hash Seguro (Bcrypt)
    # Gera um salt e o hash da senha
    senha_bytes = NOVA_SENHA.encode('utf-8')
    salt = bcrypt.gensalt()
    senha_hash = bcrypt.hashpw(senha_bytes, salt).decode('utf-8')

    print(f"🔐 Hash gerado: {senha_hash[:15]}...")

    # 4. Atualizar no Banco
    try:
        # Verifica se o usuário existe antes
        cur.execute("SELECT id FROM clientes_usuarios WHERE email = %s", (EMAIL_ALVO,))
        if not cur.fetchone():
            print("❌ Usuário não encontrado no banco de dados!")
        else:
            # Atualiza a senha e zera as tentativas de falha
            cur.execute("""
                UPDATE clientes_usuarios 
                SET senha = %s, tentativas_falhas = 0 
                WHERE email = %s
            """, (senha_hash, EMAIL_ALVO))
            conn.commit()
            print("✅ SUCESSO! Senha atualizada.")
            print(f"👉 Tente logar agora com: {NOVA_SENHA}")

    except Exception as e:
        print(f"❌ Erro ao atualizar: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    resetar_senha_admin()