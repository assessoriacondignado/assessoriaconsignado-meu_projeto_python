import streamlit as st
import pandas as pd
import psycopg2
import os
from datetime import datetime
import time
import uuid
import io
import json

# Tenta importar a conexão do sistema principal
try:
    import conexao
except ImportError:
    conexao = None

# --- CONFIGURAÇÕES ---
PASTA_ARQUIVOS = "SISTEMA_CONSULTA/ARQUIVOS_IMPORTADOS"
os.makedirs(PASTA_ARQUIVOS, exist_ok=True)

# --- CONFIGURAÇÃO DOS CAMPOS DE MAPEAMENTO (GLOBAL) ---
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
    "Bairro": "bairro",
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
    if pd.isna(valor) or valor is None: return None
    limpo = ''.join(filter(str.isdigit, str(valor)))
    if len(limpo) == 11:
        return limpo
    return None

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

# --- GERENCIAMENTO DE HISTÓRICO E CONFIGURAÇÃO ---

def registrar_inicio_importacao(nome_arq, path_org, id_usr, nome_usr):
    conn = get_db_connection()
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sistema_consulta.sistema_consulta_importacao 
                (nome_arquivo, caminho_arquivo_original, id_usuario, nome_usuario, data_importacao, qtd_novos, qtd_atualizados, qtd_erros)
                VALUES (%s, %s, %s, %s, NOW(), '0', '0', '0')
                RETURNING id
            """, (nome_arq, path_org, str(id_usr), str(nome_usr)))
            id_gerado = cur.fetchone()[0]
            conn.commit()
            return id_gerado
    except Exception as e:
        st.error(f"Erro ao registrar início: {e}")
        return None
    finally:
        conn.close()

def atualizar_fim_importacao(id_imp, novos, atualizados, erros, path_err):
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE sistema_consulta.sistema_consulta_importacao 
                SET qtd_novos = %s, qtd_atualizados = %s, qtd_erros = %s, caminho_arquivo_erro = %s
                WHERE id = %s
            """, (str(novos), str(atualizados), str(erros), path_err, id_imp))
            conn.commit()
    except Exception as e:
        st.error(f"Erro ao finalizar registro: {e}")
    finally:
        conn.close()

def get_tipos_importacao():
    conn = get_db_connection()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('sistema_consulta.sistema_importacao_tipo')")
            if cur.fetchone()[0] is None: return []
            
            cur.execute("SELECT id, convenio, nome_planilha, colunas_filtro FROM sistema_consulta.sistema_importacao_tipo ORDER BY convenio")
            return cur.fetchall()
    except Exception as e:
        st.error(f"Erro ao buscar tipos: {e}")
        return []
    finally:
        conn.close()

def salvar_tipo_importacao(convenio, planilha, colunas_json):
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sistema_consulta.sistema_importacao_tipo (convenio, nome_planilha, colunas_filtro)
                VALUES (%s, %s, %s)
            """, (convenio, planilha, colunas_json))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar configuração: {e}")
        return False
    finally:
        conn.close()

def atualizar_tipo_importacao(id_tipo, convenio, planilha, colunas_json):
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE sistema_consulta.sistema_importacao_tipo 
                SET convenio = %s, nome_planilha = %s, colunas_filtro = %s
                WHERE id = %s
            """, (convenio, planilha, colunas_json, id_tipo))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar configuração: {e}")
        return False
    finally:
        conn.close()

def excluir_tipo_importacao(id_tipo):
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sistema_consulta.sistema_importacao_tipo WHERE id = %s", (id_tipo,))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao excluir configuração: {e}")
        return False
    finally:
        conn.close()

# --- DIALOG DE DETALHES ---
@st.dialog("📋 Dados da Amostra")
def modal_detalhes_amostra(linha_dict, mapeamento):
    def get_val(chave_sistema):
        col_arq = mapeamento.get(chave_sistema)
        if col_arq: return linha_dict.get(col_arq, '')
        return ''

    cpf_visual = limpar_formatar_cpf(get_val('cpf'))
    nome_visual = get_val('nome')
    st.markdown(f"## 👤 {nome_visual}")
    st.caption(f"CPF: {cpf_visual}")
    st.divider()

    tab1, tab2, tab3 = st.tabs(["Dados Pessoais", "Contatos & Convênios", "Endereço"])
    
    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.text_input("Nome", value=nome_visual, disabled=True)
        c2.text_input("RG", value=get_val('identidade'), disabled=True)
        
        raw_nasc = get_val('data_nascimento')
        val_nasc = str(raw_nasc)
        dt_obj = converter_data_iso(raw_nasc)
        if dt_obj: val_nasc = dt_obj.strftime("%d/%m/%Y")
        c3.text_input("Data Nasc.", value=val_nasc, disabled=True)
        
        c4, c5 = st.columns(2)
        c4.text_input("CNH", value=get_val('cnh'), disabled=True)
        c5.text_input("Título Eleitor", value=get_val('titulo_eleitoral'), disabled=True)
        
        c6, c7 = st.columns(2)
        c6.text_input("Nome da Mãe", value=get_val('nome_mae'), disabled=True)
        c7.text_input("Nome do Pai", value=get_val('nome_pai'), disabled=True)
        
        st.text_input("Campanhas", value=get_val('campanhas'), disabled=True)
        val_sexo = get_val('sexo')
        if val_sexo.upper() in ['F', 'FEMININO']: val_sexo = 'Feminino'
        elif val_sexo.upper() in ['M', 'MASCULINO']: val_sexo = 'Masculino'
        st.text_input("Sexo", value=val_sexo, disabled=True)

    with tab2:
        c_contato, c_convenio = st.columns(2)
        with c_contato:
            st.markdown("##### 📞 Telefones")
            for i in range(1, 11):
                raw = get_val(f'telefone_{i}')
                fmt = limpar_formatar_telefone(raw)
                if fmt: st.text_input(f"Telefone {i}", value=fmt, disabled=True, key=f"amostra_tel_{i}")
            st.markdown("##### 📧 E-mails")
            for i in range(1, 4):
                val = get_val(f'email_{i}')
                if val: st.text_input(f"E-mail {i}", value=val, disabled=True, key=f"amostra_mail_{i}")
        with c_convenio:
            st.markdown("##### 💼 Convênio")
            val_conv = get_val('convenio')
            if val_conv: st.text_input("Convênio", value=val_conv, disabled=True)
            else: st.caption("Nenhum convênio mapeado.")

    with tab3:
        st.markdown("##### 🏠 Endereço")
        cep_val = get_val('cep')
        st.text_input("CEP", value=cep_val, disabled=True)
        rua_val = get_val('rua')
        st.text_input("Rua", value=rua_val, disabled=True)
        bairro_val = get_val('bairro')
        st.text_input("Bairro", value=bairro_val, disabled=True)
        c_cid, c_uf = st.columns([3, 1])
        c_cid.text_input("Cidade", value=get_val('cidade'), disabled=True)
        c_uf.text_input("UF", value=get_val('uf'), disabled=True)

# --- PROCESSAMENTO EM LOTE ---

def executar_importacao_em_massa(df, mapeamento_usuario, id_importacao_db, tabela_destino):
    conn = get_db_connection()
    if not conn: return 0, 0, 0, []

    sessao_id = str(uuid.uuid4())
    lista_erros = []
    
    cols_staging = ['sessao_id', 'cpf', 'nome', 'identidade', 'data_nascimento', 'sexo', 'nome_mae', 
                    'nome_pai', 'campanhas',
                    'cnh', 'titulo_eleitoral', 'convenio', 'cep', 'rua', 'bairro', 'cidade', 'uf']
    cols_staging += [f"telefone_{i}" for i in range(1, 11)]
    cols_staging += [f"email_{i}" for i in range(1, 4)]

    df_staging = pd.DataFrame()
    df_staging['sessao_id'] = [sessao_id] * len(df)

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
        
        cur.copy_expert(f"COPY {tabela_destino} ({','.join(cols_staging)}) FROM STDIN WITH NULL '\\N'", csv_buffer)
        
        cur.execute(f"""
            INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf)
            SELECT DISTINCT cpf FROM {tabela_destino} WHERE sessao_id = %s
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        id_imp_str = str(id_importacao_db)

        cur.execute(f"""
            WITH rows_to_insert AS (
                SELECT * FROM {tabela_destino} WHERE sessao_id = %s
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
                    id_importacao = CASE 
                        WHEN t.id_importacao IS NULL OR t.id_importacao = '' THEN %s 
                        ELSE t.id_importacao || ';' || %s 
                    END
                FROM rows_to_insert s
                WHERE t.cpf = s.cpf
                RETURNING t.cpf
            ),
            inserts AS (
                INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_cpf 
                (cpf, nome, identidade, data_nascimento, sexo, nome_mae, nome_pai, campanhas, cnh, titulo_eleitoral, id_importacao)
                SELECT s.cpf, s.nome, s.identidade, s.data_nascimento, s.sexo, s.nome_mae, s.nome_pai, s.campanhas, s.cnh, s.titulo_eleitoral, %s
                FROM rows_to_insert s
                WHERE s.cpf NOT IN (SELECT cpf FROM existing)
                RETURNING cpf
            )
            SELECT (SELECT count(*) FROM inserts) as novos, (SELECT count(*) FROM updates) as atualizados;
        """, (sessao_id, id_imp_str, id_imp_str, id_imp_str))
        
        resultado = cur.fetchone()
        qtd_novos = resultado[0]
        qtd_atualizados = resultado[1]

        cur.execute(f"""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone)
            SELECT DISTINCT s.cpf, t.tel
            FROM {tabela_destino} s,
            LATERAL (VALUES (telefone_1), (telefone_2), (telefone_3), (telefone_4), (telefone_5),
                            (telefone_6), (telefone_7), (telefone_8), (telefone_9), (telefone_10)) AS t(tel)
            WHERE s.sessao_id = %s AND t.tel IS NOT NULL AND t.tel <> ''
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        cur.execute(f"""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email)
            SELECT DISTINCT s.cpf, e.mail
            FROM {tabela_destino} s,
            LATERAL (VALUES (email_1), (email_2), (email_3)) AS e(mail)
            WHERE s.sessao_id = %s AND e.mail IS NOT NULL AND e.mail <> ''
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        cur.execute(f"""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio)
            SELECT DISTINCT cpf, convenio FROM {tabela_destino}
            WHERE sessao_id = %s AND convenio IS NOT NULL AND convenio <> ''
            ON CONFLICT DO NOTHING
        """, (sessao_id,))

        cur.execute(f"""
            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_endereco (cpf, cep, rua, bairro, cidade, uf)
            SELECT DISTINCT cpf, cep, rua, bairro, cidade, uf FROM {tabela_destino} s
            WHERE sessao_id = %s AND (rua IS NOT NULL OR cep IS NOT NULL)
            AND NOT EXISTS (
                SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco e 
                WHERE e.cpf = s.cpf AND (e.rua = s.rua OR e.cep = s.cep)
            )
        """, (sessao_id,))

        cur.execute(f"DELETE FROM {tabela_destino} WHERE sessao_id = %s", (sessao_id,))
        conn.commit()
        return qtd_novos, qtd_atualizados, len(lista_erros), lista_erros

    except Exception as e:
        conn.rollback()
        st.error(f"Erro Crítico na Importação: {e}")
        return 0, 0, 0, []
    finally:
        conn.close()

# --- INTERFACE ---

def tela_importacao():
    st.markdown("## 📥 Importar Dados (Enterprise Mode)")
    
    # --- ABAS PRINCIPAIS ---
    tab_import, tab_config = st.tabs(["Importação", "Config"])
    
    # === ABA IMPORTAÇÃO ===
    with tab_import:
        if 'etapa_importacao' not in st.session_state:
            st.session_state['etapa_importacao'] = 'selecao_tipo' # Passo 1
        
        # PASSO 1: SELEÇÃO DO TIPO
        if st.session_state['etapa_importacao'] == 'selecao_tipo':
            st.subheader("1. Selecione o Tipo de Importação")
            tipos = get_tipos_importacao()
            
            if not tipos:
                st.warning("Nenhum tipo de importação configurado. Vá para a aba 'Config' e crie um.")
            else:
                opcoes_tipos = {t[1]: t for t in tipos} # Nome -> Tupla
                escolha = st.selectbox("Tipo de Importação:", ["(Selecione)"] + list(opcoes_tipos.keys()))
                
                if escolha != "(Selecione)":
                    dados_tipo = opcoes_tipos[escolha]
                    st.info(f"**Convênio:** {dados_tipo[1]} | **Planilha Ref:** {dados_tipo[2]}")
                    
                    # Carrega colunas da config
                    colunas_ativas = []
                    try: colunas_ativas = json.loads(dados_tipo[3])
                    except: colunas_ativas = []
                    
                    if colunas_ativas:
                        st.markdown("**Colunas Configuradas:**")
                        st.caption(", ".join(colunas_ativas))
                    
                    if st.button("Próximo: Upload de Arquivo"):
                        st.session_state['import_tipo_selecionado'] = dados_tipo
                        st.session_state['import_colunas_ativas'] = colunas_ativas
                        st.session_state['etapa_importacao'] = 'upload'
                        st.rerun()

        # PASSO 2: UPLOAD
        elif st.session_state['etapa_importacao'] == 'upload':
            st.subheader(f"2. Upload de Arquivo ({st.session_state['import_tipo_selecionado'][1]})")
            if st.button("⬅️ Voltar"):
                st.session_state['etapa_importacao'] = 'selecao_tipo'
                st.rerun()
                
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

        # PASSO 3: MAPEAMENTO
        elif st.session_state['etapa_importacao'] == 'mapeamento':
            df = st.session_state['df_importacao']
            colunas_arquivo = list(df.columns)
            
            st.info(f"Arquivo: **{st.session_state['nome_arquivo_importacao']}** | Linhas: {len(df)}")

            # Botões de Ação
            c_act1, c_act2, c_act3 = st.columns([1.5, 1.5, 4])
            if c_act1.button("🧹 Limpar Filtro", use_container_width=True):
                for i in range(len(colunas_arquivo)):
                    if f"map_col_{i}" in st.session_state:
                        st.session_state[f"map_col_{i}"] = "(Selecione)"
                st.rerun()

            if c_act2.button("❌ Cancelar", type="secondary", use_container_width=True):
                del st.session_state['df_importacao']
                del st.session_state['etapa_importacao']
                if 'amostra_gerada' in st.session_state:
                    del st.session_state['amostra_gerada']
                st.rerun()
            
            with st.expander("⚙️ Mapeamento de Colunas", expanded=True):
                cols_map = st.columns(6)
                mapeamento_usuario = {}
                
                # FILTRAGEM DINÂMICA
                colunas_permitidas = st.session_state.get('import_colunas_ativas', [])
                if not colunas_permitidas:
                    opcoes_sistema = ["(Selecione)"] + list(CAMPOS_SISTEMA.keys())
                else:
                    opcoes_filtradas = [k for k in CAMPOS_SISTEMA.keys() if k in colunas_permitidas]
                    if "CPF (Obrigatório)" not in opcoes_filtradas:
                        opcoes_filtradas.insert(0, "CPF (Obrigatório)")
                    opcoes_sistema = ["(Selecione)"] + opcoes_filtradas
                
                for i, col_arquivo in enumerate(colunas_arquivo):
                    index_sugestao = 0
                    if f"map_col_{i}" not in st.session_state:
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
                        raw_cpf = row[mapeamento_usuario['cpf']] if 'cpf' in mapeamento_usuario else ""
                        val_cpf = limpar_formatar_cpf(raw_cpf)
                        
                        val_nome = row[mapeamento_usuario['nome']] if 'nome' in mapeamento_usuario else "---"
                        c1.write(val_cpf)
                        c2.write(val_nome)
                        if c3.button("👁️ Ver", key=f"btn_ver_{idx}"):
                            modal_detalhes_amostra(row.to_dict(), mapeamento_usuario)
                    
                    st.divider()
                    col_act1, col_act2 = st.columns([1, 1])
                    
                    if col_act1.button("❌ Cancelar (Inferior)", type="secondary", use_container_width=True):
                        del st.session_state['df_importacao']
                        del st.session_state['etapa_importacao']
                        del st.session_state['amostra_gerada']
                        st.rerun()
                    
                    if col_act2.button("✅ FINALIZAR (Processamento Rápido)", type="primary", use_container_width=True):
                        
                        timestamp = datetime.now().strftime("%Y%m%d%H%M")
                        nome_arq_safe = st.session_state['nome_arquivo_importacao'].replace(" ", "_")
                        
                        path_final = os.path.join(PASTA_ARQUIVOS, f"{timestamp}_{nome_arq_safe}")
                        df.to_csv(path_final, sep=';', index=False)

                        user_id = st.session_state.get('usuario_id', '0')
                        user_nome = st.session_state.get('usuario_nome', 'Sistema')
                        
                        # ID da importação
                        id_imp = registrar_inicio_importacao(st.session_state['nome_arquivo_importacao'], path_final, user_id, user_nome)
                        
                        # Tabela de destino (Do Config)
                        tabela_destino = st.session_state['import_tipo_selecionado'][2]
                        if not tabela_destino:
                            # Fallback caso vazio, para evitar crash
                            tabela_destino = "sistema_consulta.importacao_staging"

                        if id_imp:
                            with st.spinner(f"🚀 Processando Importação ID: {id_imp} na tabela {tabela_destino}... Aguarde."):
                                novos, atualizados, erros, lista_erros = executar_importacao_em_massa(df, mapeamento_usuario, id_imp, tabela_destino)
                            
                            path_erro_final = ""
                            if lista_erros:
                                path_erro_final = os.path.join(PASTA_ARQUIVOS, f"{timestamp}_ERROS_{nome_arq_safe}")
                                pd.DataFrame(lista_erros).to_csv(path_erro_final, sep=';', index=False)

                            atualizar_fim_importacao(id_imp, novos, atualizados, erros, path_erro_final)

                            st.balloons()
                            st.success(f"Importação #{id_imp} Finalizada com Sucesso! 🚀")
                            st.info(f"Novos: {novos} | Atualizados: {atualizados} | Erros (CPF inválido): {erros}")
                            
                            time.sleep(5)
                            del st.session_state['df_importacao']
                            del st.session_state['etapa_importacao']
                            del st.session_state['amostra_gerada']
                            st.rerun()
                        else:
                            st.error("Falha ao inicializar registro de importação. Tente novamente.")

    # === ABA CONFIG (Submenu) ===
    with tab_config:
        st.subheader("⚙️ Configurar Novo Tipo de Importação")
        
        # Variáveis de Controle de Edição
        if 'config_editando_id' not in st.session_state:
            st.session_state['config_editando_id'] = None
            st.session_state['config_convenio'] = ""
            st.session_state['config_planilha'] = ""
            st.session_state['config_colunas'] = ["CPF (Obrigatório)", "Nome do Cliente"]

        with st.form("form_config_importacao"):
            conf_convenio = st.text_input("Nome do Convênio (Tipo)", value=st.session_state['config_convenio'])
            conf_planilha = st.text_input("Nome da Planilha (Referência)", value=st.session_state['config_planilha'])
            
            st.markdown("Selecione as colunas que devem aparecer para mapeamento:")
            opcoes_campos = list(CAMPOS_SISTEMA.keys())
            
            # Garante que as colunas salvas no estado estejam nas opções disponíveis
            default_opts = [c for c in st.session_state['config_colunas'] if c in opcoes_campos]
            if not default_opts: default_opts = ["CPF (Obrigatório)", "Nome do Cliente"]
            
            conf_colunas = st.multiselect("Colunas para Filtro (Mapeamento)", options=opcoes_campos, default=default_opts)
            
            # Botões do Formulário
            c_submit, c_cancel = st.columns([1, 1])
            
            label_btn = "💾 Salvar Configuração" if not st.session_state['config_editando_id'] else "🔄 Atualizar Configuração"
            submitted = c_submit.form_submit_button(label_btn)
            
            # Botão Cancelar fora do form_submit_button padrão (gambiarra visual ou usar state change fora)
            # Como st.form tem limitações, o cancelamento é melhor gerido fora se precisar de lógica imediata,
            # mas aqui colocaremos um botão fora do form para cancelar a edição.

            if submitted:
                if not conf_convenio:
                    st.error("O nome do convênio é obrigatório.")
                else:
                    json_colunas = json.dumps(conf_colunas)
                    
                    if st.session_state['config_editando_id']:
                        # ATUALIZAR
                        if atualizar_tipo_importacao(st.session_state['config_editando_id'], conf_convenio, conf_planilha, json_colunas):
                            st.success(f"Configuração '{conf_convenio}' atualizada!")
                            # Limpar estado
                            st.session_state['config_editando_id'] = None
                            st.session_state['config_convenio'] = ""
                            st.session_state['config_planilha'] = ""
                            st.session_state['config_colunas'] = ["CPF (Obrigatório)", "Nome do Cliente"]
                            time.sleep(1)
                            st.rerun()
                    else:
                        # SALVAR NOVO
                        if salvar_tipo_importacao(conf_convenio, conf_planilha, json_colunas):
                            st.success(f"Configuração '{conf_convenio}' salva com sucesso!")
                            st.session_state['config_convenio'] = ""
                            st.session_state['config_planilha'] = ""
                            st.session_state['config_colunas'] = ["CPF (Obrigatório)", "Nome do Cliente"]
                            time.sleep(1)
                            st.rerun()

        # Botão Cancelar Edição (fora do form)
        if st.session_state['config_editando_id']:
            if st.button("❌ Cancelar Edição"):
                st.session_state['config_editando_id'] = None
                st.session_state['config_convenio'] = ""
                st.session_state['config_planilha'] = ""
                st.session_state['config_colunas'] = ["CPF (Obrigatório)", "Nome do Cliente"]
                st.rerun()
        
        st.divider()
        st.markdown("#### Configurações Existentes")
        lista_tipos = get_tipos_importacao()
        
        if lista_tipos:
            for item in lista_tipos:
                # item: (id, convenio, nome_planilha, colunas_filtro)
                with st.expander(f"📂 {item[1]}"):
                    st.write(f"**Planilha Ref:** {item[2]}")
                    colunas_list = []
                    try:
                        colunas_list = json.loads(item[3])
                        st.code(", ".join(colunas_list), language="text")
                    except:
                        st.write("Erro ao ler colunas.")
                    
                    # Botões de Ação
                    c_edit, c_del = st.columns([1, 1])
                    
                    if c_edit.button("✏️ Editar", key=f"btn_edit_{item[0]}"):
                        st.session_state['config_editando_id'] = item[0]
                        st.session_state['config_convenio'] = item[1]
                        st.session_state['config_planilha'] = item[2]
                        st.session_state['config_colunas'] = colunas_list
                        st.rerun()
                        
                    if c_del.button("🗑️ Excluir", key=f"btn_del_{item[0]}", type="primary"):
                        if excluir_tipo_importacao(item[0]):
                            st.success("Configuração excluída.")
                            time.sleep(1)
                            st.rerun()
        else:
            st.info("Nenhuma configuração cadastrada.")

if __name__ == "__main__":
    tela_importacao()