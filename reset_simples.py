import psycopg2
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

def resetar_para_texto_simples():
    EMAIL_ALVO = "alexbarbosaleal@gmail.com"
    SENHA_TEXTO = "810401" 
    
    print(f"🔄 Resetando senha para TEXTO SIMPLES: {EMAIL_ALVO}")

    try:
        conn = psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
        cur = conn.cursor()
        
        # ATUALIZA PARA A SENHA PURA (SEM CRIPTOGRAFIA)
        cur.execute("UPDATE clientes_usuarios SET senha = %s, tentativas_falhas = 0 WHERE email = %s", (SENHA_TEXTO, EMAIL_ALVO))
        conn.commit()
        
        print("✅ SUCESSO! Senha agora é texto puro no banco.")
        print("👉 Agora atualize o sistema.py e tente logar.")

    except Exception as e:
        print(f"❌ Erro: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    resetar_para_texto_simples()