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
    """Remove tudo que não for dígito"""
    if pd.isna(valor): return ""
    return ''.join(filter(str.isdigit, str(valor)))

def limpar_formatar_cpf(valor):
    """
    Regras CPF:
    - Retira pontuação, espaços e letras.
    - Inclui zeros à esquerda se necessário (Padroniza em 11 dígitos).
    """
    if pd.isna(valor) or valor is None: return ""
    # Remove tudo que não é número
    limpo = ''.join(filter(str.isdigit, str(valor)))
    
    if not limpo: return ""
    
    # Aplica zeros à esquerda até ter 11 dígitos
    # Ex: '6504802440' vira '06504802440'
    return limpo.zfill(11)

def limpar_formatar_telefone(valor):
    """
    Regras Telefone:
    - Retira pontuação, espaços e letras.
    - Garante que tenha 11 dígitos. Se não tiver, retorna None (inválido).
    """
    if pd.isna(valor) or valor is None: return None
    # Remove tudo que não é número
    limpo = ''.join(filter(str.isdigit, str(valor)))
    
    # Regra: Deve ter exatamente 11 dígitos (DDD + 9 + Número)
    if len(limpo) == 11:
        return limpo
    
    return None # Retorna vazio se não atender a regra

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

def processar_linha_banco(dados_linha, mapeamento_reverso):
    """
    (Função legado usada na Amostra - Atualizada com novas regras)
    """
    # ... A amostra usa apenas visualização, a lógica pesada está no executar_importacao_em_massa ...
    # Mas para garantir consistência visual na amostra:
    col_cpf = mapeamento_reverso.get("cpf")
    if col_cpf:
        dados_linha[col_cpf] = limpar_formatar_cpf(dados_linha.get(col_cpf))
    
    return "visualizacao"

# --- NOVA LÓGICA: PROCESSAMENTO EM LOTE (STAGING) ---

def executar_importacao_em_massa(df, mapeamento_usuario):
    """
    1. Prepara DataFrame
    2. Copia para Staging (Bulk)
    3. Distribui via SQL (Alta Performance)
    """
    conn = get_db_connection()
    if not conn: return 0, 0, 0, []

    sessao_id = str(uuid.uuid4())
    lista_erros = []
    
    # 1. PREPARAÇÃO DO DATAFRAME (Limpeza em Memória)
    cols_staging = ['sessao_id', 'cpf', 'nome', 'identidade', 'data_nascimento', 'sexo', 'nome_mae', 
                    'cnh', 'titulo_eleitoral', 'convenio', 'cep', 'rua', 'cidade', 'uf']
    cols_staging += [f"telefone_{i}" for i in range(1, 11)]
    cols_staging += [f"email_{i}" for i in range(1, 4)]

    df_staging = pd.DataFrame()
    df_staging['sessao_id'] = [sessao_id] * len(df)

    # --- APLICAÇÃO DA REGRA DE CPF ---
    # Aplica a formatação (zeros, remove letras)
    df_staging['cpf'] = df[mapeamento_usuario['cpf']].apply(limpar_formatar_cpf)
    
    # Filtra CPFs inválidos (Vazios ou com tamanho errado após formatação, embora zfill garanta 11 se tiver numero)
    # A regra básica é: tem que ter 11 dígitos numéricos
    mask_cpf_valido = df_staging['cpf'].str.len() == 11
    
    df_erros_cpf = df[~mask_cpf_valido] 
    if not df_erros_cpf.empty:
        lista_erros = df_erros_cpf.to_dict('records')
    
    # Mantém apenas linhas com CPF válido
    df_staging = df_staging[mask_cpf_valido].copy()
    
    # Processa demais colunas
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
            
            # --- APLICAÇÃO DA REGRA DE TELEFONE ---
            elif col_sys.startswith('telefone_'):
                df_staging[col_sys] = serie.apply(limpar_formatar_telefone)
            # --------------------------------------
            
            else:
                df_staging[col_sys] = serie.apply(limpar_texto)
        else:
            df_staging[col_sys] = None

    if df_staging.empty:
        conn.close()
        return 0, 0, len(lista_erros), lista_erros

    try:
        cur = conn.cursor()
        
        # 2. BULK INSERT PARA STAGING
        csv_buffer = io.StringIO()
        df_staging[cols_staging].to_csv(csv_buffer, index=False, header=False, sep='\t', na_rep='\\N')
        csv_buffer.seek(0)
        
        cur.copy_expert(f"COPY sistema_consulta.importacao_staging ({','.join(cols_staging)}) FROM STDIN WITH NULL '\\N'", csv_buffer)
        
        # 3. DISTRIBUIÇÃO SQL
        
        # A) Inserir CPFs na tabela de controle
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf)
            SELECT DISTINCT cpf FROM sistema_consulta.importacao_staging WHERE sessao_id = %s
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        # B) Dados Cadastrais (Upsert)
        cur.execute("""
            WITH rows_to_insert AS (
                SELECT * FROM sistema_consulta.importacao_staging WHERE sessao_id = %s
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
                    cnh = COALESCE(s.cnh, t.cnh),
                    titulo_eleitoral = COALESCE(s.titulo_eleitoral, t.titulo_eleitoral)
                FROM rows_to_insert s
                WHERE t.cpf = s.cpf
                RETURNING t.cpf
            ),
            inserts AS (
                INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_cpf 
                (cpf, nome, identidade, data_nascimento, sexo, nome_mae, cnh, titulo_eleitoral)
                SELECT s.cpf, s.nome, s.identidade, s.data_nascimento, s.sexo, s.nome_mae, s.cnh, s.titulo_eleitoral
                FROM rows_to_insert s
                WHERE s.cpf NOT IN (SELECT cpf FROM existing)
                RETURNING cpf
            )
            SELECT (SELECT count(*) FROM inserts) as novos, (SELECT count(*) FROM updates) as atualizados;
        """, (sessao_id,))
        
        resultado = cur.fetchone()
        qtd_novos = resultado[0]
        qtd_atualizados = resultado[1]

        # C) Inserir Telefones (Unpivot)
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone)
            SELECT DISTINCT s.cpf, t.tel
            FROM sistema_consulta.importacao_staging s,
            LATERAL (VALUES (telefone_1), (telefone_2), (telefone_3), (telefone_4), (telefone_5),
                            (telefone_6), (telefone_7), (telefone_8), (telefone_9), (telefone_10)) AS t(tel)
            WHERE s.sessao_id = %s AND t.tel IS NOT NULL AND t.tel <> ''
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        # D) Inserir Emails
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email)
            SELECT DISTINCT s.cpf, e.mail
            FROM sistema_consulta.importacao_staging s,
            LATERAL (VALUES (email_1), (email_2), (email_3)) AS e(mail)
            WHERE s.sessao_id = %s AND e.mail IS NOT NULL AND e.mail <> ''
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        # E) Inserir Convênios
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio)
            SELECT DISTINCT cpf, convenio FROM sistema_consulta.importacao_staging
            WHERE sessao_id = %s AND convenio IS NOT NULL AND convenio <> ''
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        # F) Inserir Endereços
        cur.execute("""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_endereco (cpf, cep, rua, cidade, uf)
            SELECT DISTINCT cpf, cep, rua, cidade, uf FROM sistema_consulta.importacao_staging s
            WHERE sessao_id = %s AND (rua IS NOT NULL OR cep IS NOT NULL)
            AND NOT EXISTS (
                SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco e 
                WHERE e.cpf = s.cpf AND (e.rua = s.rua OR e.cep = s.cep)
            )
        """, (sessao_id,))

        # 4. LIMPEZA DA STAGING
        cur.execute("DELETE FROM sistema_consulta.importacao_staging WHERE sessao_id = %s", (sessao_id,))
        
        conn.commit()
        return qtd_novos, qtd_atualizados, len(lista_erros), lista_erros

    except Exception as e:
        conn.rollback()
        st.error(f"Erro Crítico na Importação: {e}")
        return 0, 0, 0, []
    finally:
        conn.close()

# --- DIALOG DE DETALHES ---
@st.dialog("📋 Dados da Amostra")
def modal_detalhes_amostra(linha_dict, mapeamento):
    # Aplica formatação visual na amostra também
    col_cpf = mapeamento.get(CAMPOS_SISTEMA["CPF (Obrigatório)"])
    cpf_visual = "---"
    if col_cpf:
        cpf_visual = limpar_formatar_cpf(linha_dict.get(col_cpf))

    st.markdown(f"### CPF: {cpf_visual}")
    tab1, tab2, tab3 = st.tabs(["Dados Pessoais", "Contatos", "Endereço"])
    
    with tab1:
        nome_col = mapeamento.get('nome')
        st.text_input("Nome", value=linha_dict.get(nome_col, '') if nome_col else '', disabled=True)
        sexo_col = mapeamento.get('sexo')
        val_sexo = linha_dict.get(sexo_col, '') if sexo_col else ''
        if val_sexo.upper() in ['F', 'FEMININO']: val_sexo = 'Feminino'
        elif val_sexo.upper() in ['M', 'MASCULINO']: val_sexo = 'Masculino'
        st.text_input("Sexo", value=val_sexo, disabled=True)
        nasc_col = mapeamento.get('data_nascimento')
        st.text_input("Nascimento", value=linha_dict.get(nasc_col, '') if nasc_col else '', disabled=True)
        rg_col = mapeamento.get('identidade')
        st.text_input("RG", value=linha_dict.get(rg_col, '') if rg_col else '', disabled=True)

    with tab2:
        st.markdown("##### 📞 Telefones")
        for i in range(1, 11):
            col_tel = mapeamento.get(f'telefone_{i}')
            if col_tel:
                val = linha_dict.get(col_tel, '')
                # Mostra como ficaria formatado
                val_fmt = limpar_formatar_telefone(val)
                if val_fmt: st.text_input(f"Telefone {i}", value=val_fmt, disabled=True, key=f"amostra_tel_{i}")
                else: st.caption(f"Telefone {i}: Valor inválido ou vazio ({val})")

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
        c1, c2, c3 = st.columns([2, 1, 1])
        c1.text_input("Cidade", value=linha_dict.get(cidade_col, '') if cidade_col else '', disabled=True)
        c2.text_input("UF", value=linha_dict.get(uf_col, '') if uf_col else '', disabled=True)
        c3.text_input("CEP", value=linha_dict.get(cep_col, '') if cep_col else '', disabled=True)

# --- INTERFACE ---

def tela_importacao():
    st.markdown("## 📥 Importar Dados (Enterprise Mode)")
    
    if 'etapa_importacao' not in st.session_state:
        st.session_state['etapa_importacao'] = 'upload'
    
    # 1. UPLOAD
    if st.session_state['etapa_importacao'] == 'upload':
        arquivo = st.file_uploader("Selecione o arquivo (CSV ou Excel)", type=['csv', 'xlsx'])
        if arquivo:
            try:
                if arquivo.name.endswith('.csv'):
                    df = pd.read_csv(arquivo, sep=';', dtype=str)
                    if df.shape[1] < 2: 
                        arquivo.seek(0)
                        df = pd.read_csv(arquivo, sep=',', dtype=str)
                else:
                    df = pd.read_excel(arquivo, dtype=str)
                
                st.session_state['df_importacao'] = df
                st.session_state['nome_arquivo_importacao'] = arquivo.name
                st.session_state['etapa_importacao'] = 'mapeamento'
                st.session_state['amostra_gerada'] = False 
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao ler arquivo: {e}")

    # 2. MAPEAMENTO & AMOSTRA
    elif st.session_state['etapa_importacao'] == 'mapeamento':
        df = st.session_state['df_importacao']
        colunas_arquivo = list(df.columns)
        
        st.info(f"Arquivo: **{st.session_state['nome_arquivo_importacao']}** | Linhas: {len(df)}")
        
        with st.expander("⚙️ Mapeamento de Colunas", expanded=True):
            cols_map = st.columns(6)
            mapeamento_usuario = {}
            opcoes_sistema = ["(Selecione)"] + list(CAMPOS_SISTEMA.keys())
            
            for i, col_arquivo in enumerate(colunas_arquivo):
                index_sugestao = 0
                for idx, op in enumerate(opcoes_sistema):
                    if op == "(Selecione)": continue
                    sys_key = CAMPOS_SISTEMA[op]
                    if sys_key.split('_')[0] in col_arquivo.lower() or op.lower() in col_arquivo.lower():
                        index_sugestao = idx
                        break
                
                col_container = cols_map[i % 6]
                col_container.markdown(f"**{col_arquivo}**")
                escolha = col_container.selectbox("Corresponde a:", opcoes_sistema, index=index_sugestao, key=f"map_col_{i}", label_visibility="collapsed")
                
                if escolha != "(Selecione)":
                    mapeamento_usuario[CAMPOS_SISTEMA[escolha]] = col_arquivo

        if 'cpf' not in mapeamento_usuario:
            st.error("⚠️ Obrigatório mapear **CPF**.")
        else:
            st.divider()
            
            if not st.session_state.get('amostra_gerada'):
                if st.button("🎲 GERAR AMOSTRA (5 Linhas)", type="primary"):
                    st.session_state['amostra_gerada'] = True
                    st.rerun()
            
            if st.session_state.get('amostra_gerada'):
                st.subheader("🔍 Amostra Processada")
                amostra = df.head(5).copy()
                ch1, ch2, ch3 = st.columns([2, 4, 1])
                ch1.markdown("**CPF**")
                ch2.markdown("**Nome**")
                ch3.markdown("**Ação**")
                st.divider()

                for idx, row in amostra.iterrows():
                    c1, c2, c3 = st.columns([2, 4, 1])
                    # Aplica formatação de CPF na amostra visual
                    raw_cpf = row[mapeamento_usuario['cpf']] if 'cpf' in mapeamento_usuario else ""
                    val_cpf = limpar_formatar_cpf(raw_cpf)
                    
                    val_nome = row[mapeamento_usuario['nome']] if 'nome' in mapeamento_usuario else "---"
                    c1.write(val_cpf)
                    c2.write(val_nome)
                    if c3.button("👁️ Ver", key=f"btn_ver_{idx}"):
                        modal_detalhes_amostra(row.to_dict(), mapeamento_usuario)
                
                st.divider()
                col_act1, col_act2 = st.columns([1, 1])
                
                if col_act1.button("❌ Cancelar", type="secondary", use_container_width=True):
                    del st.session_state['df_importacao']
                    del st.session_state['etapa_importacao']
                    del st.session_state['amostra_gerada']
                    st.rerun()
                
                if col_act2.button("✅ FINALIZAR (Processamento Rápido)", type="primary", use_container_width=True):
                    with st.spinner("🚀 Processando em alta velocidade... Aguarde."):
                        novos, atualizados, erros, lista_erros = executar_importacao_em_massa(df, mapeamento_usuario)
                    
                    # Salva Arquivos
                    timestamp = datetime.now().strftime("%Y%m%d%H%M")
                    nome_arq_safe = st.session_state['nome_arquivo_importacao'].replace(" ", "_")
                    path_final = os.path.join(PASTA_ARQUIVOS, f"{timestamp}_{nome_arq_safe}")
                    df.to_csv(path_final, sep=';', index=False)
                    
                    path_erro_final = ""
                    if lista_erros:
                        path_erro_final = os.path.join(PASTA_ARQUIVOS, f"{timestamp}_ERROS_{nome_arq_safe}")
                        pd.DataFrame(lista_erros).to_csv(path_erro_final, sep=';', index=False)

                    salvar_historico_importacao(st.session_state['nome_arquivo_importacao'], novos, atualizados, erros, path_final, path_erro_final)

                    st.balloons()
                    st.success("Importação Finalizada com Sucesso! 🚀")
                    st.info(f"Novos: {novos} | Atualizados: {atualizados} | Erros (CPF inválido): {erros}")
                    
                    time.sleep(5)
                    del st.session_state['df_importacao']
                    del st.session_state['etapa_importacao']
                    del st.session_state['amostra_gerada']
                    st.rerun()

if __name__ == "__main__":
    tela_importacao()