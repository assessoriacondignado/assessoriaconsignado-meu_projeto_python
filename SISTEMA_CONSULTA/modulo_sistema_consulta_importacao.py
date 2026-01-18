import streamlit as st
import pandas as pd
import psycopg2
import os
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

# --- CONFIGURAÇÃO DOS CAMPOS DE MAPEAMENTO ---
# Define os campos básicos
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
    # Endereço (1 Endereço)
    "CEP": "cep",
    "Rua": "rua",
    "Cidade": "cidade",
    "UF": "uf"
}

# Adiciona Opção de até 10 Telefones
for i in range(1, 11):
    CAMPOS_SISTEMA[f"Telefone {i}"] = f"telefone_{i}"

# Adiciona Opção de até 3 E-mails
for i in range(1, 4):
    CAMPOS_SISTEMA[f"E-mail {i}"] = f"email_{i}"

# --- FUNÇÕES AUXILIARES ---
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
        return datetime.strptime(str(valor), "%d/%m/%Y").date()
    except:
        try:
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

    status = "atualizado"

    try:
        with conn.cursor() as cur:
            # 1. Verifica se existe
            cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf WHERE cpf = %s", (cpf_valor,))
            existe = cur.fetchone()
            
            # Mapeia valores básicos (Ignora campos satélites no loop principal)
            campos_basicos = {}
            campos_satelites_prefixos = ['telefone_', 'email_', 'cep', 'rua', 'cidade', 'uf', 'convenio']
            
            for sis_key, sis_col in CAMPOS_SISTEMA.items():
                # Pula se for campo satélite (começa com prefixo ou está na lista)
                eh_satelite = False
                for p in campos_satelites_prefixos:
                    if sis_col.startswith(p) or sis_col == p:
                        eh_satelite = True
                        break
                
                if eh_satelite: continue
                
                col_planilha = mapeamento_reverso.get(sis_col)
                if col_planilha:
                    val = dados_linha.get(col_planilha)
                    val = limpar_texto(val)
                    
                    # --- NOVA REGRA: Tratamento de Sexo ---
                    if sis_col == 'sexo':
                        if val.upper() == 'F': val = 'Feminino'
                        elif val.upper() == 'M': val = 'Masculino'
                    # --------------------------------------

                    if sis_col == 'data_nascimento':
                        campos_basicos[sis_col] = converter_data_iso(val)
                    else:
                        campos_basicos[sis_col] = val
            
            # INSERT ou UPDATE Tabela Principal
            if not existe:
                status = "novo"
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf) VALUES (%s) ON CONFLICT DO NOTHING", (cpf_valor,))
                
                cols = list(campos_basicos.keys())
                vals = list(campos_basicos.values())
                cols.append('cpf')
                vals.append(cpf_valor)
                
                placeholders = ", ".join(["%s"] * len(cols))
                columns = ", ".join(cols)
                sql = f"INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_cpf ({columns}) VALUES ({placeholders})"
                cur.execute(sql, vals)
            else:
                if campos_basicos:
                    set_clause = ", ".join([f"{k} = %s" for k in campos_basicos.keys()])
                    vals = list(campos_basicos.values())
                    vals.append(cpf_valor)
                    sql = f"UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_cpf SET {set_clause} WHERE cpf = %s"
                    cur.execute(sql, vals)

            # 2. Telefones (Loop 1 a 10)
            for i in range(1, 11):
                col_tel = mapeamento_reverso.get(f"telefone_{i}")
                if col_tel:
                    tel_val = limpar_texto(dados_linha.get(col_tel))
                    if tel_val:
                        # Insere se não existir
                        cur.execute("""
                            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone) 
                            SELECT %s, %s 
                            WHERE NOT EXISTS (SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf=%s AND telefone=%s)
                        """, (cpf_valor, tel_val, cpf_valor, tel_val))

            # 3. Emails (Loop 1 a 3)
            for i in range(1, 4):
                col_mail = mapeamento_reverso.get(f"email_{i}")
                if col_mail:
                    mail_val = limpar_texto(dados_linha.get(col_mail))
                    if mail_val:
                        cur.execute("""
                            INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email) 
                            SELECT %s, %s 
                            WHERE NOT EXISTS (SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf=%s AND email=%s)
                        """, (cpf_valor, mail_val, cpf_valor, mail_val))

            # 4. Convênio
            col_conv = mapeamento_reverso.get("convenio")
            if col_conv:
                conv_val = limpar_texto(dados_linha.get(col_conv))
                if conv_val:
                    cur.execute("""
                        INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio) 
                        SELECT %s, %s 
                        WHERE NOT EXISTS (SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf=%s AND convenio=%s)
                    """, (cpf_valor, conv_val, cpf_valor, conv_val))

            # 5. Endereço
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
    
    tab1, tab2, tab3 = st.tabs(["Dados Pessoais", "Contatos (Tels/Emails)", "Endereço"])
    
    with tab1:
        nome_col = mapeamento.get('nome')
        st.text_input("Nome", value=linha_dict.get(nome_col, '') if nome_col else '', disabled=True)
        
        sexo_col = mapeamento.get('sexo')
        val_sexo = linha_dict.get(sexo_col, '') if sexo_col else ''
        if val_sexo.upper() == 'F': val_sexo = 'Feminino'
        elif val_sexo.upper() == 'M': val_sexo = 'Masculino'
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
                if val: st.text_input(f"Telefone {i}", value=val, disabled=True, key=f"amostra_tel_{i}")
        
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
    st.markdown("## 📥 Importar Dados - Sistema Consulta")
    
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
                st.session_state['amostra_gerada'] = False # Reset flag
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao ler arquivo: {e}")

    # 2. MAPEAMENTO & AMOSTRA
    elif st.session_state['etapa_importacao'] == 'mapeamento':
        df = st.session_state['df_importacao']
        colunas_arquivo = list(df.columns)
        
        st.info(f"Arquivo: **{st.session_state['nome_arquivo_importacao']}** | Linhas: {len(df)}")
        
        with st.expander("⚙️ Mapeamento de Colunas (Selecione a que corresponde)", expanded=True):
            # Layout compactado com 6 colunas
            cols_map = st.columns(6)
            mapeamento_usuario = {}
            
            # Opções do sistema (invertido)
            opcoes_sistema = ["(Selecione)"] + list(CAMPOS_SISTEMA.keys())
            
            # Itera sobre AS COLUNAS DO ARQUIVO
            for i, col_arquivo in enumerate(colunas_arquivo):
                # Tenta sugerir automaticamente
                index_sugestao = 0
                for idx, op in enumerate(opcoes_sistema):
                    if op == "(Selecione)": continue
                    # Pega a chave interna para comparar
                    sys_key = CAMPOS_SISTEMA[op]
                    # Compara nome da coluna com chave do sistema ou label
                    if sys_key.split('_')[0] in col_arquivo.lower() or op.lower() in col_arquivo.lower():
                        index_sugestao = idx
                        break
                
                col_container = cols_map[i % 6]
                
                # Exibe o NOME DA COLUNA DO ARQUIVO em negrito
                col_container.markdown(f"**{col_arquivo}**")
                
                escolha = col_container.selectbox(
                    "Corresponde a:", 
                    opcoes_sistema, 
                    index=index_sugestao,
                    key=f"map_col_{i}",
                    label_visibility="collapsed" # Esconde label redundante
                )
                
                if escolha != "(Selecione)":
                    # Mapeia: Chave do Sistema = Nome da Coluna do Arquivo
                    chave_sistema = CAMPOS_SISTEMA[escolha]
                    mapeamento_usuario[chave_sistema] = col_arquivo

        # Validação Mínima
        if 'cpf' not in mapeamento_usuario:
            st.error("⚠️ É obrigatório selecionar a coluna correspondente ao **CPF**.")
        else:
            st.divider()
            
            # Botão para Gerar Amostra
            if not st.session_state.get('amostra_gerada'):
                if st.button("🎲 GERAR AMOSTRA (5 Linhas)", type="primary"):
                    st.session_state['amostra_gerada'] = True
                    st.rerun()
            
            # Exibe Amostra se gerada
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
                    
                    val_cpf = row[mapeamento_usuario['cpf']] if 'cpf' in mapeamento_usuario else "---"
                    val_nome = row[mapeamento_usuario['nome']] if 'nome' in mapeamento_usuario else "---"
                    
                    c1.write(val_cpf)
                    c2.write(val_nome)
                    
                    if c3.button("👁️ Ver", key=f"btn_ver_{idx}"):
                        modal_detalhes_amostra(row.to_dict(), mapeamento_usuario)
                
                st.divider()
                
                # Botões Finais
                col_act1, col_act2 = st.columns([1, 1])
                
                if col_act1.button("❌ Cancelar", type="secondary", use_container_width=True):
                    del st.session_state['df_importacao']
                    del st.session_state['etapa_importacao']
                    del st.session_state['amostra_gerada']
                    st.rerun()
                
                if col_act2.button("✅ FINALIZAR IMPORTAÇÃO", type="primary", use_container_width=True):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    total = len(df)
                    novos = 0
                    atualizados = 0
                    erros = 0
                    
                    mapeamento_final = mapeamento_usuario
                    lista_erros = []

                    for i, (_, row) in enumerate(df.iterrows()):
                        if i % (max(1, total // 20)) == 0:
                            progress_bar.progress(i / total)
                            status_text.text(f"Processando {i+1}/{total}...")
                        
                        res = processar_linha_banco(row.to_dict(), mapeamento_final)
                        
                        if res == "novo": novos += 1
                        elif res == "atualizado": atualizados += 1
                        else: 
                            erros += 1
                            lista_erros.append(row.to_dict())

                    progress_bar.progress(100)
                    status_text.text("Concluído!")
                    
                    timestamp = datetime.now().strftime("%Y%m%d%H%M")
                    nome_arq_safe = st.session_state['nome_arquivo_importacao'].replace(" ", "_")
                    path_final = os.path.join(PASTA_ARQUIVOS, f"{timestamp}_{nome_arq_safe}")
                    df.to_csv(path_final, sep=';', index=False)
                    
                    path_erro_final = ""
                    if lista_erros:
                        path_erro_final = os.path.join(PASTA_ARQUIVOS, f"{timestamp}_ERROS_{nome_arq_safe}")
                        pd.DataFrame(lista_erros).to_csv(path_erro_final, sep=';', index=False)

                    salvar_historico_importacao(
                        st.session_state['nome_arquivo_importacao'],
                        novos, atualizados, erros,
                        path_final, path_erro_final
                    )

                    st.balloons()
                    st.success(f"Importação Finalizada!")
                    st.info(f"Novos: {novos} | Atualizados: {atualizados} | Erros: {erros}")
                    
                    time.sleep(5)
                    del st.session_state['df_importacao']
                    del st.session_state['etapa_importacao']
                    del st.session_state['amostra_gerada']
                    st.rerun()

if __name__ == "__main__":
    tela_importacao()