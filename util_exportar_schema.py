import pandas as pd
import psycopg2
import conexao  # Usa sua configuração atual de conexao.py

def exportar_schema_banco():
    print("🔄 Iniciando mapeamento do banco de dados...")
    conn = None
    try:
        conn = psycopg2.connect(
            host=conexao.host, 
            port=conexao.port, 
            database=conexao.database, 
            user=conexao.user, 
            password=conexao.password
        )
        
        # 1. Busca TODOS os schemas criados pelo usuário (exclui os de sistema do Postgres)
        query_schemas = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND schema_name NOT LIKE 'pg_%%'
            ORDER BY schema_name
        """
        df_schemas = pd.read_sql(query_schemas, conn)
        lista_schemas = df_schemas['schema_name'].tolist()
        
        print(f"📂 Schemas encontrados: {lista_schemas}")
        
        relatorio = "=== ESTRUTURA COMPLETA DO BANCO DE DADOS ===\n"
        relatorio += f"Schemas mapeados: {', '.join(lista_schemas)}\n\n"
        
        for schema in lista_schemas:
            # 2. Busca tabelas dentro de cada schema
            query_tabelas = f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
            df_tabelas = pd.read_sql(query_tabelas, conn)
            
            if not df_tabelas.empty:
                relatorio += f"--- SCHEMA: {schema.upper()} ---\n"
                
                for _, row in df_tabelas.iterrows():
                    tb = row['table_name']
                    relatorio += f"\n[TABELA: {schema}.{tb}]\n"
                    
                    # 3. Busca colunas e tipos
                    query_cols = f"""
                        SELECT column_name, data_type, is_nullable, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}' AND table_name = '{tb}'
                        ORDER BY ordinal_position
                    """
                    df_cols = pd.read_sql(query_cols, conn)
                    
                    for _, col in df_cols.iterrows():
                        nulo = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                        tipo = col['data_type']
                        if col['character_maximum_length'] and col['character_maximum_length'] > 0:
                            tipo += f"({int(col['character_maximum_length'])})"
                            
                        relatorio += f"   - {col['column_name']} ({tipo}) {nulo}\n"
                
                relatorio += "\n" + "="*40 + "\n\n"

        # Salva em arquivo
        nome_arquivo = "schema_banco_atual.txt"
        with open(nome_arquivo, "w", encoding="utf-8") as f:
            f.write(relatorio)
            
        print(f"✅ Sucesso! Arquivo '{nome_arquivo}' gerado/atualizado na raiz.")
        print("📎 Anexe este arquivo no chat quando precisar de ajuda com SQL.")

    except Exception as e:
        print(f"❌ Erro ao exportar schema: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    exportar_schema_banco()