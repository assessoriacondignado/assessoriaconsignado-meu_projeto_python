import streamlit as st
import pandas as pd
import psycopg2
import conexao
# Importa o módulo central para usar a função de limpeza na exibição
import modulo_wapi 

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

def app_registros():
    """
    Exibe o histórico de logs do banco de dados na interface Streamlit.
    """
    st.markdown("### 📋 Histórico de Logs (Webhook)")
    st.markdown("---")

    conn = get_conn()
    if not conn:
        st.error("Erro ao conectar ao banco de dados.")
        return

    try:
        # Busca os últimos 500 registros
        query = """
            SELECT 
                data_hora, 
                tipo, 
                telefone, 
                nome_contato, 
                grupo, 
                mensagem, 
                status 
            FROM admin.wapi_logs 
            ORDER BY data_hora DESC 
            LIMIT 500
        """
        
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            
            # --- PADRONIZAÇÃO VISUAL ---
            # Aplica a função de limpeza na coluna telefone para garantir 
            # que, mesmo dados antigos, apareçam sem o 55 na tela.
            if 'telefone' in df.columns:
                df['telefone'] = df['telefone'].apply(lambda x: modulo_wapi.limpar_telefone(x) if x else x)

            # Exibe a tabela formatada
            st.dataframe(
                df, 
                use_container_width=True,
                hide_index=True,
                column_config={
                    "data_hora": st.column_config.DatetimeColumn("Data/Hora", format="DD/MM/YYYY HH:mm:ss"),
                    "tipo": "Tipo",
                    "telefone": "Telefone",
                    "nome_contato": "Contato",
                    "grupo": "Grupo / Cliente",
                    "mensagem": "Conteúdo",
                    "status": "Status"
                }
            )
            
            if st.button("🔄 Atualizar Lista"):
                st.rerun()
        else:
            st.info("Nenhum registro encontrado.")

    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
    finally:
        if conn: conn.close()