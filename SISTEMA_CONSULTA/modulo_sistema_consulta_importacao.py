import streamlit as st
import pandas as pd
import psycopg2
import os
import sys 
from datetime import datetime, date
import time
import uuid
import io
import json
import re

# Importa os validadores
try:
    from modulo_validadores import ValidadorDocumentos, ValidadorContato, ValidadorData
except ImportError:
    st.error("ERRO CRÍTICO: modulo_validadores.py não encontrado.")

# --- CONFIGURAÇÃO DE CAMINHOS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    import conexao
except ImportError:
    conexao = None

# --- CONFIGURAÇÕES ---
PASTA_ARQUIVOS = os.path.join(current_dir, "ARQUIVOS_IMPORTADOS")
PASTA_ERROS = os.path.join(PASTA_ARQUIVOS, "ERROS")
os.makedirs(PASTA_ARQUIVOS, exist_ok=True)
os.makedirs(PASTA_ERROS, exist_ok=True)

# --- ALIAS LEGADO ---
CAMPOS_SISTEMA_ALIAS = {
    "CPF (Obrigatório)": "cpf", "Nome do Cliente": "nome", "RG": "identidade",
    "Data Nascimento": "data_nascimento", "Sexo": "sexo", "Nome da Mãe": "nome_mae",
    "Nome do Pai": "nome_pai", "Campanhas": "campanhas", "CNH": "cnh",
    "Título Eleitor": "titulo_eleitoral", "Convênio": "convenio", "CEP": "cep",
    "Rua": "rua", "Bairro": "bairro", "Cidade": "cidade", "UF": "uf",
    "Matrícula": "matricula", "CNPJ Nome": "cnpj_nome", "CNPJ Número": "cnpj_numero",
    "Qtd Funcionários": "qtd_funcionarios", "Data Abertura Empresa": "data_abertura_empresa",
    "CNAE Nome": "cnae_nome", "CNAE Código": "cnae_codigo", "Data Admissão": "data_admissao",
    "CBO Código": "cbo_codigo", "CBO Nome": "cbo_nome", "Data Início Emprego": "data_inicio_emprego"
}
for i in range(1, 11): CAMPOS_SISTEMA_ALIAS[f"Telefone {i}"] = f"telefone_{i}"
for i in range(1, 4): CAMPOS_SISTEMA_ALIAS[f"E-mail {i}"] = f"email_{i}"
ALIAS_INVERSO = {v: k for k, v in CAMPOS_SISTEMA_ALIAS.items()}

# --- FUNÇÕES AUXILIARES DE DB ---
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

# --- FUNÇÕES DE IMPORTAÇÃO ( LÓGICA PYTHON ) ---
def buscar_cpfs_existentes(lista_cpfs_bigint):
    if not lista_cpfs_bigint: return {}
    conn = get_db_connection()
    if not conn: return {}
    dados_existentes = {}
    try:
        with conn.cursor() as cur:
            cpfs_str = ",".join(str(c) for c in lista_cpfs_bigint)
            query = f"""
                SELECT cpf, nome, identidade, data_nascimento, sexo, nome_mae, cnh, titulo_eleitoral
                FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf 
                WHERE cpf IN ({cpfs_str})
            """
            cur.execute(query)
            colunas = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                dados_existentes[row[0]] = dict(zip(colunas, row))
    except Exception as e:
        st.error(f"Erro ao buscar CPFs: {e}")
    finally:
        conn.close()
    return dados_existentes

def executar_importacao_em_massa(df, mapeamento_usuario, id_importacao_db, tabela_destino):
    conn = get_db_connection()
    if not conn: return 0, 0, 0, []
    sessao_id = str(uuid.uuid4())
    
    qtd_novos = 0
    qtd_atualizados = 0
    qtd_erros = 0
    linhas_erro = [] 

    map_excel_sys = {v: k for k, v in mapeamento_usuario.items()}
    cache_processamento = [] 
    cpfs_validos_lote = set()
    cpfs_vistos_arquivo = set()

    # 1. Pré-processamento e Validação
    for idx, row in df.iterrows():
        col_cpf_excel = mapeamento_usuario.get('cpf')
        raw_cpf = row.get(col_cpf_excel) if col_cpf_excel else None
        
        cpf_bigint = ValidadorDocumentos.cpf_para_bigint(raw_cpf)
        
        if not cpf_bigint:
            qtd_erros += 1
            linhas_erro.append({"linha": idx + 2, "erro": "CPF Inválido ou Ausente", "dados": str(row.to_dict())})
            continue

        if cpf_bigint in cpfs_vistos_arquivo:
            qtd_erros += 1
            linhas_erro.append({"linha": idx + 2, "erro": "CPF Duplicado no arquivo", "dados": str(raw_cpf)})
            continue
        
        cpfs_vistos_arquivo.add(cpf_bigint)
        cpfs_validos_lote.add(cpf_bigint)
        
        dados_limpos = {'cpf': cpf_bigint, 'idx_origem': idx}
        for campo_sys, col_excel in mapeamento_usuario.items():
            if campo_sys == 'cpf': continue
            valor = row.get(col_excel)
            if 'data' in campo_sys:
                dados_limpos[campo_sys] = ValidadorData.para_sql(valor)
            else:
                dados_limpos[campo_sys] = str(valor).strip() if pd.notnull(valor) and str(valor).strip() != '' else None
        cache_processamento.append(dados_limpos)

    # 2. Busca Dados
    db_cache_dados = buscar_cpfs_existentes(list(cpfs_validos_lote))

    try:
        cursor = conn.cursor()
        inserts_cadastro = []
        updates_cadastro = [] 
        inserts_telefones = []
        inserts_emails = []
        inserts_endereco = []

        # 3. Distribuição
        for item in cache_processamento:
            cpf = item['cpf']
            existe_no_db = cpf in db_cache_dados
            dados_db = db_cache_dados.get(cpf, {})
            
            # Cadastro
            if not existe_no_db:
                qtd_novos += 1
                inserts_cadastro.append((
                    cpf, item.get('nome'), item.get('data_nascimento'), item.get('identidade'), 
                    item.get('sexo'), item.get('nome_mae'), item.get('cnh'), item.get('titulo_eleitoral')
                ))
                cursor.execute("INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf) VALUES (%s) ON CONFLICT DO NOTHING", (cpf,))
            else:
                # Atualização
                campos_atualizar = {}
                mapa_campos = {'nome': 'nome', 'data_nascimento': 'data_nascimento', 'identidade': 'identidade',
                               'sexo': 'sexo', 'nome_mae': 'nome_mae', 'cnh': 'cnh', 'titulo_eleitoral': 'titulo_eleitoral'}
                flag_atualizou = False
                for campo_db, campo_item in mapa_campos.items():
                    val_db = dados_db.get(campo_db)
                    val_novo = item.get(campo_item)
                    if (val_db is None or str(val_db).strip() == '') and (val_novo is not None):
                        campos_atualizar[campo_db] = val_novo
                        flag_atualizou = True
                
                if flag_atualizou:
                    qtd_atualizados += 1
                    set_clause = ", ".join([f"{k} = %s" for k in campos_atualizar.keys()])
                    vals = list(campos_atualizar.values()) + [cpf]
                    updates_cadastro.append((f"UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_cpf SET {set_clause} WHERE cpf = %s", vals))

            # Telefones
            for key, val in item.items():
                if key.startswith('telefone_') and val:
                    tel_limpo = ValidadorContato.telefone_para_sql(val)
                    if tel_limpo: inserts_telefones.append((cpf, tel_limpo))

            # Emails
            for key, val in item.items():
                if key.startswith('email_') and val:
                    if ValidadorContato.email_valido(val): inserts_emails.append((cpf, val))

            # Endereço
            tem_endereco = any(item.get(k) for k in ['rua', 'cep', 'bairro', 'cidade', 'uf'])
            if tem_endereco:
                inserts_endereco.append((
                    cpf, item.get('cep'), item.get('rua'), item.get('bairro'), item.get('cidade'), item.get('uf'), item.get('complemento')
                ))

        # 4. Commits
        if inserts_cadastro:
            sql_insert = """
                INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_cpf 
                (cpf, nome, data_nascimento, identidade, sexo, nome_mae, cnh, titulo_eleitoral)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cpf) DO NOTHING
            """
            cursor.executemany(sql_insert, inserts_cadastro)
        
        for query, params in updates_cadastro:
            cursor.execute(query, params)

        if inserts_telefones:
            inserts_telefones = list(set(inserts_telefones))
            cursor.executemany("""
                INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, inserts_telefones)

        if inserts_emails:
            inserts_emails = list(set(inserts_emails))
            cursor.executemany("""
                INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, inserts_emails)

        if inserts_endereco:
            cursor.executemany("""
                INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_endereco 
                (cpf, cep, rua, bairro, cidade, uf, complemento)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cpf) DO UPDATE SET
                cep = EXCLUDED.cep, rua = EXCLUDED.rua, bairro = EXCLUDED.bairro,
                cidade = EXCLUDED.cidade, uf = EXCLUDED.uf, complemento = EXCLUDED.complemento
            """, inserts_endereco)

        conn.commit()
        
        # 5. Relatório Erros
        path_erro = None
        if linhas_erro:
            df_erro = pd.DataFrame(linhas_erro)
            nome_arq_erro = f"erros_{id_importacao_db}_{int(time.time())}.csv"
            path_erro = os.path.join(PASTA_ERROS, nome_arq_erro)
            df_erro.to_csv(path_erro, index=False, sep=';')
        
        atualizar_fim_importacao(id_importacao_db, qtd_novos, qtd_atualizados, qtd_erros, path_erro)
        return qtd_novos, qtd_atualizados, qtd_erros, linhas_erro

    except Exception as e:
        conn.rollback()
        st.error(f"Erro Crítico no processamento: {e}")
        return 0, 0, 0, [{'erro': str(e)}]
    finally:
        conn.close()

# --- FUNÇÕES DE SUPORTE UI/DB ---
def registrar_inicio_importacao(nome_arq, path_org, id_usr, nome_usr):
    conn = get_db_connection()
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sistema_consulta.sistema_consulta_importacao 
                (nome_arquivo, caminho_arquivo_original, id_usuario, nome_usuario, data_importacao, qtd_novos, qtd_atualizados, qtd_erros)
                VALUES (%s, %s, %s, %s, NOW(), '0', '0', '0') RETURNING id
            """, (nome_arq, path_org, str(id_usr), str(nome_usr)))
            id_gerado = cur.fetchone()[0]
            conn.commit()
            return id_gerado
    except: return None
    finally: conn.close()

def atualizar_fim_importacao(id_imp, novos, atualizados, erros, path_err):
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE sistema_consulta.sistema_consulta_importacao SET qtd_novos = %s, qtd_atualizados = %s, qtd_erros = %s, caminho_arquivo_erro = %s WHERE id = %s", (str(novos), str(atualizados), str(erros), path_err, id_imp))
            conn.commit()
    except: pass
    finally: conn.close()

def get_tipos_importacao():
    conn = get_db_connection()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, convenio, nome_planilha, colunas_filtro FROM sistema_consulta.sistema_importacao_tipo ORDER BY convenio")
            return cur.fetchall()
    except: return []
    finally: conn.close()

# --- UI DIALOGS ---
@st.dialog("📋 Dados da Amostra")
def modal_detalhes_amostra(linha_dict, mapeamento):
    # (Mantém implementação visual inalterada)
    def get_val_db(col_db):
        col_excel = mapeamento.get(col_db)
        if col_excel: return linha_dict.get(col_excel, '')
        return ''
    
    val_cpf = get_val_db('cpf')
    cpf_visual = ValidadorDocumentos.cpf_para_tela(val_cpf) if val_cpf else ""
    st.markdown(f"## 👤 {get_val_db('nome')}")
    st.caption(f"CPF: {cpf_visual}")
    st.divider()
    
    t1, t2 = st.tabs(["Dados", "Contatos/End"])
    with t1:
        c1, c2 = st.columns(2)
        c1.text_input("RG", value=get_val_db('identidade'), disabled=True)
        c2.text_input("Nasc.", value=ValidadorData.para_tela(ValidadorData.para_sql(get_val_db('data_nascimento'))), disabled=True)
    with t2:
        st.caption("Visualização Rápida")
        st.text_input("Telefone 1", value=get_val_db('telefone_1'), disabled=True)
        st.text_input("CEP", value=get_val_db('cep'), disabled=True)

# --- INTERFACE ---
def tela_importacao():
    
    # Se houver um resultado pendente de confirmação, mostra apenas o resultado
    if 'resultado_importacao' in st.session_state:
        res = st.session_state['resultado_importacao']
        
        st.title("📊 Resultado da Importação")
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Novos", res['novos'])
        c2.metric("Atualizados", res['atualizados'])
        c3.metric("Erros", res['erros'], delta_color="inverse")
        
        st.divider()
        
        if res['erros'] > 0:
            st.error(f"⚠️ ATENÇÃO: Ocorreram {res['erros']} erros durante a importação.")
            st.markdown("Verifique o arquivo de log gerado na pasta de erros para detalhes (CPFs inválidos, duplicados, etc).")
            
            st.write("---")
            # REGRA 2: Aviso de erro exige confirmação
            st.warning("⚠️ **Confirmação Obrigatória**: Para liberar o sistema para novas importações, confirme que visualizou os erros.")
            
            if st.button("✅ CONFIRMAR LEITURA E FECHAR", type="primary", use_container_width=True):
                del st.session_state['resultado_importacao']
                st.rerun()
        else:
            st.success("✅ Importação realizada com sucesso total!")
            if st.button("Voltar ao Início", use_container_width=True):
                del st.session_state['resultado_importacao']
                st.rerun()
        
        return # Interrompe a renderização do restante da tela

    # --- TELA NORMAL (Se não houver resultado pendente) ---
    
    # Aba única agora, pois Config foi removida da edição
    st.subheader("Módulo de Importação")
    
    if 'etapa_importacao' not in st.session_state: st.session_state['etapa_importacao'] = 'selecao_tipo' 
    
    if st.session_state['etapa_importacao'] == 'selecao_tipo':
        st.info("ℹ️ As regras de importação são gerenciadas internamente pelo sistema.")
        st.markdown("### 1. Selecione o Layout")
        tipos = get_tipos_importacao()
        
        if not tipos:
            st.warning("Nenhum layout base encontrado.")
        else:
            opcoes_tipos = {t[1]: t for t in tipos} 
            escolha = st.selectbox("Modelo de Importação:", ["(Selecione)"] + list(opcoes_tipos.keys()))
            
            if escolha != "(Selecione)":
                dados_tipo = opcoes_tipos[escolha]
                # Carrega colunas sugeridas
                try: colunas_ativas = json.loads(dados_tipo[3])
                except: colunas_ativas = []
                
                if st.button("Próximo: Upload de Arquivo"):
                    st.session_state['import_tipo_selecionado'] = dados_tipo
                    st.session_state['import_colunas_ativas'] = colunas_ativas
                    st.session_state['etapa_importacao'] = 'upload'
                    st.rerun()

    elif st.session_state['etapa_importacao'] == 'upload':
        st.markdown(f"### 2. Upload ({st.session_state['import_tipo_selecionado'][1]})")
        if st.button("⬅️ Voltar"):
            st.session_state['etapa_importacao'] = 'selecao_tipo'; st.rerun()
            
        arquivo = st.file_uploader("Selecione o arquivo (.csv ou .xlsx)", type=['csv', 'xlsx'])
        if arquivo:
            try:
                if arquivo.name.endswith('.csv'):
                    df = pd.read_csv(arquivo, sep=';', dtype=str)
                    if df.shape[1] < 2: 
                        arquivo.seek(0); df = pd.read_csv(arquivo, sep=',', dtype=str)
                else:
                    df = pd.read_excel(arquivo, dtype=str)
                
                st.session_state['df_importacao'] = df
                st.session_state['nome_arquivo_importacao'] = arquivo.name
                st.session_state['etapa_importacao'] = 'mapeamento'
                st.session_state['amostra_gerada'] = False 
                st.rerun()
            except Exception as e: st.error(f"Erro ao ler arquivo: {e}")

    elif st.session_state['etapa_importacao'] == 'mapeamento':
        df = st.session_state['df_importacao']
        st.info(f"Arquivo: **{st.session_state['nome_arquivo_importacao']}** | Linhas: {len(df)}")

        if st.button("❌ Cancelar Importação"):
            del st.session_state['df_importacao']; del st.session_state['etapa_importacao']; st.rerun()
        
        with st.expander("⚙️ Conferência de Colunas (De/Para)", expanded=True):
            colunas_arquivo = list(df.columns)
            cols_map = st.columns(4)
            mapeamento_usuario = {}
            colunas_permitidas = st.session_state.get('import_colunas_ativas', [])
            
            opcoes_display = ["(Selecione)"]
            mapa_display_to_tecnico = {}
            
            if colunas_permitidas:
                for col_tec in colunas_permitidas:
                    col_tecnica_real = CAMPOS_SISTEMA_ALIAS.get(col_tec, col_tec)
                    nome_visual = ALIAS_INVERSO.get(col_tecnica_real, col_tecnica_real)
                    opcoes_display.append(nome_visual)
                    mapa_display_to_tecnico[nome_visual] = col_tecnica_real
            else:
                opcoes_display = ["(Selecione)"] + list(CAMPOS_SISTEMA_ALIAS.keys())
                mapa_display_to_tecnico = CAMPOS_SISTEMA_ALIAS

            for i, col_arquivo in enumerate(colunas_arquivo):
                idx_sug = 0
                for idx, op in enumerate(opcoes_display):
                    if op == "(Selecione)": continue
                    if mapa_display_to_tecnico[op].lower() in col_arquivo.lower(): idx_sug = idx; break
                
                col_cont = cols_map[i % 4]
                escolha = col_cont.selectbox(f"{col_arquivo}", options=opcoes_display, index=idx_sug, key=f"map_{i}")
                if escolha != "(Selecione)":
                    mapeamento_usuario[mapa_display_to_tecnico[escolha]] = col_arquivo

        if not st.session_state.get('amostra_gerada'):
            if st.button("🎲 Gerar Amostra para Validação"): st.session_state['amostra_gerada'] = True; st.rerun()
        
        if st.session_state.get('amostra_gerada'):
            st.markdown("### 3. Validação da Amostra")
            st.caption("Verifique se os dados abaixo correspondem ao esperado antes de confirmar.")
            amostra = df.head(5).copy()
            
            cols = st.columns(5)
            for i, row in amostra.iterrows():
                with cols[i]:
                    lbl = row[mapeamento_usuario.get('nome')] if mapeamento_usuario.get('nome') in row else f"Linha {i}"
                    if st.button(f"🔍 {lbl}", key=f"btn_amostra_{i}"):
                        modal_detalhes_amostra(row.to_dict(), mapeamento_usuario)

            st.write("---")
            if st.button("✅ EXECUTAR IMPORTAÇÃO", type="primary"):
                nome_arq = st.session_state['nome_arquivo_importacao']
                tabela_destino = st.session_state['import_tipo_selecionado'][2]
                
                id_imp = registrar_inicio_importacao(nome_arq, "upload_direto", 0, "Usuario")
                
                with st.spinner("Processando... Aguarde a finalização."):
                    novos, atualizados, erros, lista_erros = executar_importacao_em_massa(df, mapeamento_usuario, id_imp, tabela_destino)
                    
                    # SALVA O ESTADO DO RESULTADO E REINICIA PARA MOSTRAR A TELA DE CONFIRMAÇÃO
                    st.session_state['resultado_importacao'] = {
                        'novos': novos,
                        'atualizados': atualizados,
                        'erros': erros,
                        'id_imp': id_imp
                    }
                    
                    # Limpa dados temporários
                    del st.session_state['df_importacao']
                    del st.session_state['etapa_importacao']
                    del st.session_state['amostra_gerada']
                    
                    st.rerun()

if __name__ == "__main__":
    tela_importacao()