import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime
import conexao

def get_conn():
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except: return None

def app_registros():
    st.markdown("### 📋 Histórico de Mensagens (Webhook)")
    try:
        conn = get_conn()
        query = """
            SELECT data_hora, instance_id as "Instância", nome_contato as "Contato", 
                   tipo as "Fluxo", telefone, mensagem, status 
            FROM wapi_logs 
            ORDER BY data_hora DESC 
            LIMIT 50
        """
        df_logs = pd.read_sql(query, conn)
        conn.close()
        if not df_logs.empty:
            df_logs['data_hora'] = pd.to_datetime(df_logs['data_hora']).dt.strftime('%d/%m/%Y %H:%M')
            st.dataframe(df_logs, use_container_width=True, hide_index=True)
        else: st.info("Histórico vazio.")
    except Exception as e: st.error(f"Erro ao carregar logs: {e}")
    