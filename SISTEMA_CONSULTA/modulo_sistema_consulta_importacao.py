import streamlit as st
import pandas as pd
import psycopg2
import os
from datetime import datetime
import time
import uuid
import io

# Tenta importar a conexão do sistema principal
try:
    import conexao
except ImportError:
    conexao = None

# --- CONFIGURAÇÕES ---
PASTA_ARQUIVOS = "SISTEMA_CONSULTA/ARQUIVOS_IMPORTADOS"
os.makedirs(PASTA_ARQUIVOS, exist_ok=True)

# --- CONFIGURAÇÃO DOS CAMPOS DE MAPEAMENTO ---
CAMPOS_SISTEMA = {
    "CPF (Obrigatório)": "cpf",
    "Nome do Cliente": "nome",
    "RG": "identidade",
    "Data Nascimento": "data_nascimento",
    "Sexo": "sexo",
    "Nome da Mãe": "nome_mae",
    "Nome do Pai": "nome_pai",
    "Campanhas": "campanhas",
    "CNH": "cnh",
    "Título Eleitor": "titulo_eleitoral",
    "Convênio": "convenio",
    "CEP": "cep",
    "Rua": "rua",
    "Cidade": "cidade",
    "UF": "uf"
}

for i in range(1, 11): CAMPOS_SISTEMA[f"Telefone {i}"] = f"telefone_{i}"
for i in range(1, 4): CAMPOS_SISTEMA[f"E-mail {i}"] = f"email_{i}"

# --- FUNÇÕES AUXILIARES ---
def limpar_texto(valor):
    if pd.isna(valor) or valor is None: return ""
    return str(valor).strip()

def limpar_apenas_numeros(valor):
    if pd.isna(valor): return ""
    return ''.join(filter(str.isdigit, str(valor)))

def limpar_formatar_cpf(valor):
    if pd.isna(valor) or valor is None: return ""
    limpo = ''.join(filter(str.isdigit, str(valor)))
    if not limpo: return ""
    return limpo.zfill(11)

def limpar_formatar_telefone(valor):
    """
    Regras Telefone:
    - Retira pontuação, espaços e letras.
    - Garante que tenha 11 dígitos. 
    - Se vazio, nulo ou inválido, retorna string vazia "".
    """
    if pd.isna(valor) or valor is None: return ""
    
    limpo = ''.join(filter(str.isdigit, str(valor)))
    
    # Regra: Deve ter exatamente 11 dígitos (DDD + 9 + Número)
    if len(limpo) == 11:
        return limpo
    
    return "" # Retorna vazio se não atender a regra

def converter_data_iso(valor):
    if not valor or pd.isna(valor): return None
    try: return datetime.strptime(str(valor), "%d/%m/%Y").date()
    except:
        try: return datetime.strptime(str(valor), "%Y-%m-%d").date()
        except: return None

def get_db_connection():
    if not conexao: return None
    try:
        return psycopg2.connect(
            host=conexao.host, port=conexao.port, database=conexao.database,
            user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

def salvar_historico_importacao(nome_arq, novos, atualizados, erros, path_org, path_err):
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sistema_consulta.sistema_consulta_importacao 
                (nome_arquivo, qtd_novos, qtd_atualizados, qtd_erros, caminho_arquivo_original, caminho_arquivo_erro)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (nome_arq, str(novos), str(atualizados), str(erros), path_org, path_err))
            conn.commit()
    finally:
        conn.close()

# --- DIALOG DE DETALHES ---
@st.dialog("📋 Dados da Amostra")
def modal_detalhes_amostra(linha_dict, mapeamento):
    col_cpf = mapeamento.get(CAMPOS_SISTEMA["CPF (Obrigatório)"])
    cpf_visual = "---"
    if col_cpf:
        cpf_visual = limpar_formatar_cpf(linha_dict.get(col_cpf))

    st.markdown(f"### CPF: {cpf_visual}")
    tab1, tab2, tab3 = st.tabs(["Dados Pessoais", "Contatos", "Endereço"])
    
    with tab1:
        nome_col = mapeamento.get('nome')
        st.text_input("Nome", value=linha_dict.get(nome_col, '') if nome_col else '', disabled=True)
        
        pai_col = mapeamento.get('nome_pai')
        st.text_input("Nome do Pai", value=linha_dict.get(pai_col, '') if pai_col else '', disabled=True)
        
        camp_col = mapeamento.get('campanhas')
        st.text_input("Campanhas", value=linha_dict.get(camp_col, '') if camp_col else '', disabled=True)

        sexo_col = mapeamento.get('sexo')
        val_sexo = linha_dict.get(sexo_col, '') if sexo_col else ''
        if val_sexo.upper() in ['F', 'FEMININO']: val_sexo = 'Feminino'
        elif val_sexo.upper() in ['M', 'MASCULINO']: val_sexo = 'Masculino'
        st.text_input("Sexo", value=val_sexo, disabled=True)
        
        nasc_col = mapeamento.get('data_nascimento')
        st.text_input("Nascimento", value=linha_dict.get(nasc_col, '') if nasc_col else '', disabled=True)

    with tab2:
        st.markdown("##### 📞 Telefones")
        for i in range(1, 11):
            col_tel = mapeamento.get(f'telefone_{i}')
            if col_tel:
                val = linha_dict.get(col_tel, '')
                val_fmt = limpar_formatar_telefone(val)
                # Mostra input vazio se for inválido, em vez de ocultar ou mostrar None
                st.text_input(f"Telefone {i}", value=val_fmt, disabled=True, key=f"amostra_tel_{i}")

        st.markdown("##### 📧 E-mails")
        for i in range(1, 4):
            col_mail = mapeamento.get(f'email_{i}')
            if col_mail:
                val = linha_dict.get(col_mail, '')
                if val: st.text_input(f"E-mail {i}", value=val, disabled=True, key=f"amostra_mail_{i}")

    with tab3:
        rua_col = mapeamento.get('rua')
        cidade_col = mapeamento.get('cidade')
        uf_col = mapeamento.get('uf')
        cep_col = mapeamento.get('cep')
        st.text_input("Rua", value=linha_dict.get(rua_col, '') if rua_col else '', disabled=True)
        c1, c2 = st.columns(2)
        c1.text_input("Cidade", value=linha_dict.get(cidade_col, '') if cidade_col else '', disabled=True)
        c2.text_input("UF", value=linha_dict.get(uf_col, '') if uf_col else '', disabled=True)

# --- PROCESSAMENTO EM LOTE (STAGING) ---

def executar_importacao_em_massa(df, mapeamento_usuario):
    conn = get_db_connection()
    if not conn: return 0, 0, 0, []

    sessao_id = str(uuid.uuid4())
    lista_erros = []
    
    # 1. PREPARAÇÃO DO DATAFRAME
    cols_staging = ['sessao_id', 'cpf', 'nome', 'identidade', 'data_nascimento', 'sexo', 'nome_mae', 
                    'nome_pai', 'campanhas', 
                    'cnh', 'titulo_eleitoral', 'convenio', 'cep', 'rua', 'cidade', 'uf']
    cols_staging += [f"telefone_{i}" for i in range(1, 11)]
    cols_staging += [f"email_{i}" for i in range(1, 4)]

    df_staging = pd.DataFrame()
    df_staging['sessao_id'] = [sessao_id] * len(df)

    # --- APLICAÇÃO DA REGRA DE CPF ---
    df_staging['cpf'] = df[mapeamento_usuario['cpf']].apply(limpar_formatar_cpf)
    
    mask_cpf_valido = df_staging['cpf'].str.len() == 11
    df_erros_cpf = df[~mask_cpf_valido] 
    if not df_erros_cpf.empty:
        lista_erros = df_erros_cpf.to_dict('records')
    
    df_staging = df_staging[mask_cpf_valido].copy()
    
    for col_sys in cols_staging:
        if col_sys in ['sessao_id', 'cpf']: continue
        
        col_excel = None
        for k, v in mapeamento_usuario.items():
            if k == col_sys:
                col_excel = v
                break
        
        if col_excel:
            serie = df.loc[mask_cpf_valido, col_excel]
            
            if col_sys == 'data_nascimento':
                df_staging[col_sys] = serie.apply(converter_data_iso)
            elif col_sys == 'sexo':
                def trata_sexo(x):
                    s = str(x).strip().upper()
                    return 'Feminino' if s in ['F', 'FEMININO'] else ('Masculino' if s in ['M', 'MASCULINO'] else x)
                df_staging[col_sys] = serie.apply(trata_sexo)
            elif col_sys.startswith('telefone_'):
                # Aplica a nova regra de telefone (retorna "" se inválido)
                df_staging[col_sys] = serie.apply(limpar_formatar_telefone)
            else:
                df_staging[col_sys] = serie.apply(limpar_texto)
        else:
            df_staging[col_sys] = None

    if df_staging.empty:
        conn.close()
        return 0, 0, len(lista_erros), lista_erros

    try:
        cur = conn.cursor()
        
        csv_buffer = io.StringIO()
        df_staging[cols_staging].to_csv(csv_buffer, index=False, header=False, sep='\t', na_rep='\\N')
        csv_buffer.seek(0)
        
        # Garante tabela temporária compatível com as colunas
        cur.execute(f"""
            CREATE TEMP TABLE IF NOT EXISTS temp_staging_import (
                sessao_id UUID, cpf VARCHAR(20), nome TEXT, identidade TEXT, data_nascimento DATE, 
                sexo TEXT, nome_mae TEXT, nome_pai TEXT, campanhas TEXT,
                cnh TEXT, titulo_eleitoral TEXT, convenio TEXT, cep TEXT, rua TEXT, cidade TEXT, uf TEXT,
                telefone_1 TEXT, telefone_2 TEXT, telefone_3 TEXT, telefone_4 TEXT, telefone_5 TEXT,
                telefone_6 TEXT, telefone_7 TEXT, telefone_8 TEXT, telefone_9 TEXT, telefone_10 TEXT,
                email_1 TEXT, email_2 TEXT, email_3 TEXT
            ) ON COMMIT DROP;
        """)
        
        cur.copy_expert(f"COPY temp_staging_import ({','.join(cols_staging)}) FROM STDIN WITH NULL '\\N'", csv_buffer)
        
        # 3. DISTRIBUIÇÃO SQL
        
        # A) CPFs
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf)
            SELECT DISTINCT cpf FROM temp_staging_import WHERE sessao_id = %s
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        # B) Dados Cadastrais (UPSERT com ID IMPORTACAO)
        cur.execute("""
            WITH rows_to_insert AS (
                SELECT * FROM temp_staging_import WHERE sessao_id = %s
            ),
            existing AS (
                SELECT cpf FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf
                WHERE cpf IN (SELECT cpf FROM rows_to_insert)
            ),
            updates AS (
                UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_cpf t
                SET nome = COALESCE(s.nome, t.nome),
                    identidade = COALESCE(s.identidade, t.identidade),
                    data_nascimento = COALESCE(s.data_nascimento, t.data_nascimento),
                    sexo = COALESCE(s.sexo, t.sexo),
                    nome_mae = COALESCE(s.nome_mae, t.nome_mae),
                    nome_pai = COALESCE(s.nome_pai, t.nome_pai),
                    campanhas = COALESCE(s.campanhas, t.campanhas),
                    cnh = COALESCE(s.cnh, t.cnh),
                    titulo_eleitoral = COALESCE(s.titulo_eleitoral, t.titulo_eleitoral),
                    id_importacao = s.sessao_id::text
                FROM rows_to_insert s
                WHERE t.cpf = s.cpf
                RETURNING t.cpf
            ),
            inserts AS (
                INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_cpf 
                (cpf, nome, identidade, data_nascimento, sexo, nome_mae, nome_pai, campanhas, cnh, titulo_eleitoral, id_importacao)
                SELECT s.cpf, s.nome, s.identidade, s.data_nascimento, s.sexo, s.nome_mae, s.nome_pai, s.campanhas, s.cnh, s.titulo_eleitoral, s.sessao_id::text
                FROM rows_to_insert s
                WHERE s.cpf NOT IN (SELECT cpf FROM existing)
                RETURNING cpf
            )
            SELECT (SELECT count(*) FROM inserts) as novos, (SELECT count(*) FROM updates) as atualizados;
        """, (sessao_id,))
        
        resultado = cur.fetchone()
        qtd_novos = resultado[0]
        qtd_atualizados = resultado[1]

        # C) Telefones (Filtra vazios '')
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone)
            SELECT DISTINCT s.cpf, t.tel
            FROM temp_staging_import s,
            LATERAL (VALUES (telefone_1), (telefone_2), (telefone_3), (telefone_4), (telefone_5),
                            (telefone_6), (telefone_7), (telefone_8), (telefone_9), (telefone_10)) AS t(tel)
            WHERE s.sessao_id = %s AND t.tel IS NOT NULL AND t.tel <> ''
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        # D) Emails
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email)
            SELECT DISTINCT s.cpf, e.mail
            FROM temp_staging_import s,
            LATERAL (VALUES (email_1), (email_2), (email_3)) AS e(mail)
            WHERE s.sessao_id = %s AND e.mail IS NOT NULL AND e.mail <> ''
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        # E) Convênios
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio)
            SELECT DISTINCT cpf, convenio FROM temp_staging_import
            WHERE sessao_id = %s AND convenio IS NOT NULL AND convenio <> ''
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        # F) Endereços
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_endereco (cpf, cep, rua, cidade, uf)
            SELECT DISTINCT cpf, cep, rua, cidade, uf FROM temp_staging_import s
            WHERE sessao_id = %s AND (rua IS NOT NULL OR cep IS NOT NULL)
            AND NOT EXISTS (
                SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco e 
                WHERE e.cpf = s.cpf AND (e.rua = s.rua OR e.cep = s.cep)
            )
        """, (sessao_id,))

        cur.execute("DELETE FROM temp_staging_import WHERE sessao_id = %s", (sessao_id,))
        
        conn.commit()
        return qtd_novos, qtd_atualizados, len(lista_erros), lista_erros

    except Exception as e:
        conn.rollback()
        st.error(f"Erro Crítico na Importação: {e}")
        return 0, 0, 0, []
    finally:
        conn.close()

if __name__ == "__main__":
    tela_importacao()