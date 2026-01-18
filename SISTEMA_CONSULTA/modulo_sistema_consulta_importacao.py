import streamlit as st
import pandas as pd
import psycopg2
import os
import shutil
from datetime import datetime
import time

# Tenta importar a conexão do sistema principal
try:
    import conexao
except ImportError:
    conexao = None

# --- CONFIGURAÇÕES ---
PASTA_ARQUIVOS = "SISTEMA_CONSULTA/ARQUIVOS_IMPORTADOS"
os.makedirs(PASTA_ARQUIVOS, exist_ok=True)

# Mapeamento dos campos do sistema para destino no banco
CAMPOS_SISTEMA = {
    "CPF (Obrigatório)": "cpf",
    "Nome do Cliente": "nome",
    "RG": "identidade",
    "Data Nascimento": "data_nascimento",
    "Sexo": "sexo",
    "Nome da Mãe": "nome_mae",
    "CNH": "cnh",
    "Título Eleitor": "titulo_eleitoral",
    "Telefone (Principal)": "telefone",
    "E-mail": "email",
    "CEP": "cep",
    "Rua": "rua",
    "Cidade": "cidade",
    "UF": "uf",
    "Convênio": "convenio"
}

# --- FUNÇÕES AUXILIARES (REPLICADAS DO CADASTRO) ---
def limpar_texto(valor):
    """Remove espaços e trata None"""
    if pd.isna(valor) or valor is None:
        return ""
    return str(valor).strip()

def limpar_apenas_numeros(valor):
    """Mantém apenas dígitos"""
    if pd.isna(valor): return ""
    return ''.join(filter(str.isdigit, str(valor)))

def converter_data_iso(valor):
    """Tenta converter formatos diversos para YYYY-MM-DD"""
    if not valor or pd.isna(valor): return None
    try:
        # Tenta converter string DD/MM/YYYY
        return datetime.strptime(str(valor), "%d/%m/%Y").date()
    except:
        try:
            # Tenta converter string YYYY-MM-DD
            return datetime.strptime(str(valor), "%Y-%m-%d").date()
        except:
            return None

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

# --- LÓGICA DE IMPORTAÇÃO ---

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
    except Exception as e:
        st.error(f"Erro ao salvar histórico: {e}")
    finally:
        conn.close()

def processar_linha_banco(dados_linha, mapeamento_reverso):
    """Insere ou Atualiza um cliente e seus dados vinculados"""
    conn = get_db_connection()
    if not conn: return "erro_conexao"
    
    # Extrai CPF
    col_cpf_planilha = mapeamento_reverso.get("cpf")
    cpf_valor = limpar_apenas_numeros(dados_linha.get(col_cpf_planilha))
    
    if not cpf_valor or len(cpf_valor) < 11:
        conn.close()
        return "erro_cpf"

    status = "atualizado" # Assume atualização, se não existir vira novo

    try:
        with conn.cursor() as cur:
            # 1. Verifica se existe
            cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf WHERE cpf = %s", (cpf_valor,))
            existe = cur.fetchone()
            
            # Mapeia valores básicos
            campos_basicos = {}
            for sis_key, sis_col in CAMPOS_SISTEMA.items():
                if sis_col in ['telefone', 'email', 'cep', 'rua', 'cidade', 'uf', 'convenio']: continue
                
                col_planilha = mapeamento_reverso.get(sis_col)
                if col_planilha:
                    val = dados_linha.get(col_planilha)
                    if sis_col == 'data_nascimento':
                        campos_basicos[sis_col] = converter_data_iso(val)
                    else:
                        campos_basicos[sis_col] = limpar_texto(val)
            
            # INSERT ou UPDATE Tabela Principal
            if not existe:
                status = "novo"
                # Garante inserção na tabela de controle de CPF
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf) VALUES (%s) ON CONFLICT DO NOTHING", (cpf_valor,))
                
                cols = list(campos_basicos.keys())
                vals = list(campos_basicos.values())
                # Adiciona CPF nos campos
                cols.append('cpf')
                vals.append(cpf_valor)
                
                placeholders = ", ".join(["%s"] * len(cols))
                columns = ", ".join(cols)
                sql = f"INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_cpf ({columns}) VALUES ({placeholders})"
                cur.execute(sql, vals)
            else:
                # Update apenas se houver campos mapeados
                if campos_basicos:
                    set_clause = ", ".join([f"{k} = %s" for k in campos_basicos.keys()])
                    vals = list(campos_basicos.values())
                    vals.append(cpf_valor)
                    sql = f"UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_cpf SET {set_clause} WHERE cpf = %s"
                    cur.execute(sql, vals)

            # 2. Dados Satélites (Inserir apenas se não existir igual)
            
            # Telefone
            col_tel = mapeamento_reverso.get("telefone")
            if col_tel:
                tel_val = limpar_texto(dados_linha.get(col_tel))
                if tel_val:
                    cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone) SELECT %s, %s WHERE NOT EXISTS (SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf=%s AND telefone=%s)", (cpf_valor, tel_val, cpf_valor, tel_val))

            # Email
            col_mail = mapeamento_reverso.get("email")
            if col_mail:
                mail_val = limpar_texto(dados_linha.get(col_mail))
                if mail_val:
                    cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email) SELECT %s, %s WHERE NOT EXISTS (SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf=%s AND email=%s)", (cpf_valor, mail_val, cpf_valor, mail_val))

            # Convênio
            col_conv = mapeamento_reverso.get("convenio")
            if col_conv:
                conv_val = limpar_texto(dados_linha.get(col_conv))
                if conv_val:
                    cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio) SELECT %s, %s WHERE NOT EXISTS (SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf=%s AND convenio=%s)", (cpf_valor, conv_val, cpf_valor, conv_val))

            # Endereço (Lógica simplificada: insere novo se vier dados)
            col_cep = mapeamento_reverso.get("cep")
            col_rua = mapeamento_reverso.get("rua")
            if col_cep or col_rua:
                cep_val = limpar_texto(dados_linha.get(col_cep)) if col_cep else ""
                rua_val = limpar_texto(dados_linha.get(col_rua)) if col_rua else ""
                cid_val = limpar_texto(dados_linha.get(mapeamento_reverso.get("cidade"))) if mapeamento_reverso.get("cidade") else ""
                uf_val = limpar_texto(dados_linha.get(mapeamento_reverso.get("uf"))) if mapeamento_reverso.get("uf") else ""
                
                if cep_val or rua_val:
                     cur.execute("""
                        INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_endereco 
                        (cpf, cep, rua, cidade, uf) 
                        SELECT %s, %s, %s, %s, %s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco 
                            WHERE cpf=%s AND (rua=%s OR cep=%s)
                        )
                     """, (cpf_valor, cep_val, rua_val, cid_val, uf_val, cpf_valor, rua_val, cep_val))

            conn.commit()
            return status

    except Exception as e:
        return f"erro_sql: {e}"
    finally:
        conn.close()

# --- DIALOG DE DETALHES ---
@st.dialog("📋 Dados da Amostra")
def modal_detalhes_amostra(linha_dict, mapeamento):
    st.markdown(f"### CPF: {linha_dict.get(mapeamento.get('cpf', ''), 'Não Identificado')}")
    
    col1, col2 = st.columns(2)
    with col1:
        st.caption("Dados Pessoais")
        nome_col = mapeamento.get('nome')
        st.text_input("Nome", value=linha_dict.get(nome_col, '') if nome_col else '', disabled=True)
        
        nasc_col = mapeamento.get('data_nascimento')
        st.text_input("Nascimento", value=linha_dict.get(nasc_col, '') if nasc_col else '', disabled=True)

    with col2:
        st.caption("Contatos")
        tel_col = mapeamento.get('telefone')
        st.text_input("Telefone", value=linha_dict.get(tel_col, '') if tel_col else '', disabled=True)
        
        mail_col = mapeamento.get('email')
        st.text_input("E-mail", value=linha_dict.get(mail_col, '') if mail_col else '', disabled=True)

# --- INTERFACE ---

def tela_importacao():
    st.markdown("## 📥 Importar Dados - Sistema Consulta")
    st.info("Importe planilhas (CSV/Excel) para alimentar a base de consultas unificada.")

    if 'etapa_importacao' not in st.session_state:
        st.session_state['etapa_importacao'] = 'upload'
    
    # 1. UPLOAD
    if st.session_state['etapa_importacao'] == 'upload':
        arquivo = st.file_uploader("Selecione o arquivo", type=['csv', 'xlsx'])
        if arquivo:
            try:
                if arquivo.name.endswith('.csv'):
                    df = pd.read_csv(arquivo, sep=';', dtype=str) # Tenta ponto e vírgula
                    if df.shape[1] < 2: 
                        arquivo.seek(0)
                        df = pd.read_csv(arquivo, sep=',', dtype=str)
                else:
                    df = pd.read_excel(arquivo, dtype=str)
                
                st.session_state['df_importacao'] = df
                st.session_state['nome_arquivo_importacao'] = arquivo.name
                st.session_state['etapa_importacao'] = 'mapeamento'
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao ler arquivo: {e}")

    # 2. MAPEAMENTO & AMOSTRA
    elif st.session_state['etapa_importacao'] == 'mapeamento':
        df = st.session_state['df_importacao']
        colunas_arquivo = ["(Ignorar)"] + list(df.columns)
        
        st.write(f"Arquivo carregado: **{st.session_state['nome_arquivo_importacao']}** ({len(df)} linhas)")
        
        with st.expander("⚙️ Mapeamento de Colunas", expanded=True):
            st.warning("Selecione qual coluna do seu arquivo corresponde a cada campo do sistema.")
            
            cols_map = st.columns(3)
            mapeamento_usuario = {}
            
            for i, (label_sistema, key_sistema) in enumerate(CAMPOS_SISTEMA.items()):
                # Tenta sugerir automaticamente
                index_sugestao = 0
                for idx, col_file in enumerate(colunas_arquivo):
                    if key_sistema.lower() in col_file.lower():
                        index_sugestao = idx
                        break
                
                escolha = cols_map[i % 3].selectbox(
                    f"{label_sistema}", 
                    colunas_arquivo, 
                    index=index_sugestao,
                    key=f"map_{key_sistema}"
                )
                if escolha != "(Ignorar)":
                    mapeamento_usuario[key_sistema] = escolha

        # Validação Mínima
        if 'cpf' not in mapeamento_usuario:
            st.error("⚠️ É obrigatório mapear a coluna de **CPF**.")
        else:
            st.divider()
            st.subheader("🔍 Amostra dos Dados (5 primeiros)")
            st.caption("Clique no botão 'Ver' para simular a ficha do cliente.")

            # Exibe Amostra como Tabela Interativa
            amostra = df.head(5).copy()
            # Mostra apenas colunas mapeadas para facilitar
            cols_visuais = [v for k, v in mapeamento_usuario.items()]
            
            # Loop visual da amostra
            for idx, row in amostra.iterrows():
                c1, c2, c3, c4 = st.columns([3, 3, 3, 1])
                
                # Pega valores mapeados
                val_cpf = row[mapeamento_usuario['cpf']] if 'cpf' in mapeamento_usuario else "---"
                val_nome = row[mapeamento_usuario['nome']] if 'nome' in mapeamento_usuario else "---"
                
                c1.write(f"**CPF:** {val_cpf}")
                c2.write(f"**Nome:** {val_nome}")
                c3.caption("Linha " + str(idx + 1))
                
                if c4.button("👁️ Ver", key=f"btn_ver_{idx}"):
                    modal_detalhes_amostra(row.to_dict(), mapeamento_usuario)
            
            st.divider()
            
            col_act1, col_act2 = st.columns([1, 1])
            
            if col_act1.button("❌ Cancelar Importação", type="secondary"):
                del st.session_state['df_importacao']
                del st.session_state['etapa_importacao']
                st.rerun()
            
            if col_act2.button("✅ FINALIZAR IMPORTAÇÃO", type="primary"):
                # Inicia Processamento Real
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total = len(df)
                novos = 0
                atualizados = 0
                erros = 0
                
                # Inverte o mapeamento para facilitar busca (Sistema -> Coluna Planilha)
                mapeamento_final = mapeamento_usuario
                
                lista_erros = []

                for i, (_, row) in enumerate(df.iterrows()):
                    # Atualiza barra a cada 10%
                    if i % (max(1, total // 10)) == 0:
                        progress_bar.progress(i / total)
                        status_text.text(f"Processando linha {i+1}/{total}...")
                    
                    res = processar_linha_banco(row.to_dict(), mapeamento_final)
                    
                    if res == "novo": novos += 1
                    elif res == "atualizado": atualizados += 1
                    else: 
                        erros += 1
                        lista_erros.append(row.to_dict())

                progress_bar.progress(100)
                status_text.text("Concluído!")
                
                # Salva Arquivo Original
                timestamp = datetime.now().strftime("%Y%m%d%H%M")
                nome_arq_safe = st.session_state['nome_arquivo_importacao'].replace(" ", "_")
                path_final = os.path.join(PASTA_ARQUIVOS, f"{timestamp}_{nome_arq_safe}")
                
                # Como df está na memória, salvamos o CSV correspondente
                df.to_csv(path_final, sep=';', index=False)
                
                path_erro_final = ""
                if lista_erros:
                    path_erro_final = os.path.join(PASTA_ARQUIVOS, f"{timestamp}_ERROS_{nome_arq_safe}")
                    pd.DataFrame(lista_erros).to_csv(path_erro_final, sep=';', index=False)

                # Grava Log no Banco
                salvar_historico_importacao(
                    st.session_state['nome_arquivo_importacao'],
                    novos, atualizados, erros,
                    path_final, path_erro_final
                )

                st.success(f"Importação Finalizada! ✅")
                st.markdown(f"""
                - **Novos Cadastros:** {novos}
                - **Atualizados:** {atualizados}
                - **Erros/Ignorados:** {erros}
                """)
                
                time.sleep(4)
                del st.session_state['df_importacao']
                del st.session_state['etapa_importacao']
                st.rerun()

if __name__ == "__main__":
    tela_importacao()