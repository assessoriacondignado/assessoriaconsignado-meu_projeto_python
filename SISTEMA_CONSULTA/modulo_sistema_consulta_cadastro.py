import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import pool, sql
from datetime import datetime, date
import time
import contextlib
import sys
import os
import math

# ==============================================================================
# 0. CONFIGURAÇÃO DE CAMINHOS (PATH FIX)
# ==============================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# ==============================================================================
# IMPORTAÇÕES DOS MÓDULOS DA RAIZ
# ==============================================================================

try:
    import conexao
except ImportError:
    conexao = None

try:
    import modulo_validadores as v
except ImportError as e:
    st.error(f"Erro crítico: Não foi possível importar 'modulo_validadores'. Detalhe: {e}")
    st.stop()

# --- IMPORTAÇÃO DO MÓDULO FATOR CONFERI (PARA O BOTÃO) ---
try:
    import modulo_fator_conferi
except ImportError:
    modulo_fator_conferi = None

# ==============================================================================
# 1. CONFIGURAÇÃO DE PERFORMANCE (CONNECTION POOL BLINDADO)
# ==============================================================================

@st.cache_resource
def get_pool():
    if not conexao: return None
    try:
        return psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=20,
            host=conexao.host, port=conexao.port,
            database=conexao.database, user=conexao.user, password=conexao.password,
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5
        )
    except Exception as e:
        st.error(f"Erro fatal ao conectar no banco (Pool): {e}")
        return None

@contextlib.contextmanager
def get_db_connection():
    pool_obj = get_pool()
    if not pool_obj:
        yield None
        return
    
    conn = pool_obj.getconn()
    try:
        conn.rollback()
        yield conn
        pool_obj.putconn(conn)
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        try: pool_obj.putconn(conn, close=True)
        except: pass
        try:
            conn = pool_obj.getconn()
            yield conn
            pool_obj.putconn(conn)
        except Exception as e:
            st.error(f"Falha ao reconectar ao banco: {e}")
            yield None
    except Exception as e:
        pool_obj.putconn(conn)
        raise e

# ==============================================================================
# 2. CONSTANTES E CONFIGURAÇÕES
# ==============================================================================

MAPA_CAMPOS_PESQUISA = {
    "Dados Pessoais": {
        "Nome do Cliente": {"col": "t.nome", "tipo": "texto"},
        "CPF": {"col": "t.cpf", "tipo": "texto"},
        "RG (Identidade)": {"col": "t.identidade", "tipo": "texto"},
        "Data de Nascimento": {"col": "t.data_nascimento", "tipo": "data"},
        "Idade": {"col": "age(t.data_nascimento)", "tipo": "numero_calculado"}, 
        "Sexo": {"col": "t.sexo", "tipo": "texto"},
    },
    "Contatos": {
        "Telefone": {"col": "telefone", "table": "sistema_consulta_dados_cadastrais_telefone", "tipo": "texto_vinculado"},
        "E-mail": {"col": "email", "table": "sistema_consulta_dados_cadastrais_email", "tipo": "texto_vinculado"},
    },
    "Endereço": {
        "Rua": {"col": "rua", "table": "sistema_consulta_dados_cadastrais_endereco", "tipo": "texto_vinculado"},
        "Bairro": {"col": "bairro", "table": "sistema_consulta_dados_cadastrais_endereco", "tipo": "texto_vinculado"},
        "Cidade": {"col": "cidade", "table": "sistema_consulta_dados_cadastrais_endereco", "tipo": "texto_vinculado"},
        "UF": {"col": "uf", "table": "sistema_consulta_dados_cadastrais_endereco", "tipo": "texto_vinculado"},
        "CEP": {"col": "cep", "table": "sistema_consulta_dados_cadastrais_endereco", "tipo": "texto_vinculado"},
    },
    "Filiação e Sistema": {
        "Nome da Mãe": {"col": "t.nome_mae", "tipo": "texto"},
        "Nome do Pai": {"col": "t.nome_pai", "tipo": "texto"},
        "Campanhas": {"col": "t.campanhas", "tipo": "texto"},
        "Título de Eleitor": {"col": "t.titulo_eleitoral", "tipo": "texto"},
        "CNH": {"col": "t.cnh", "tipo": "texto"},
        "ID Importação": {"col": "t.id_importacao", "tipo": "texto"},
    }
}

OPERADORES_SQL = {
    "texto": {
        "Contém (..aa..)": {"sql": "ILIKE", "mask": "%{}%", "desc": "Contém o texto"},
        "Igual (=)": {"sql": "=", "mask": "{}", "desc": "Exatamente igual"},
        "Começa com (^aa)": {"sql": "ILIKE", "mask": "{}%", "desc": "Começa com..."},
        "Termina com (aa$)": {"sql": "ILIKE", "mask": "%{}", "desc": "Termina com..."},
        "Diferente (!=)": {"sql": "!=", "mask": "{}", "desc": "Não é igual a"},
        "Vazio (Ø)": {"sql": "IS NULL", "mask": "", "desc": "Campo está vazio"},
        "Não Vazio": {"sql": "IS NOT NULL", "mask": "", "desc": "Campo preenchido"}
    },
    "data": {
        "Entre Datas (><)": {"sql": "BETWEEN", "mask": "{}", "desc": "Intervalo de datas"},
        "Igual (=)": {"sql": "=", "mask": "{}", "desc": "Data exata"},
        "Vazio (Ø)": {"sql": "IS NULL", "mask": "", "desc": "Sem data"}
    },
    "numero": {
        "Igual (=)": {"sql": "=", "mask": "{}", "desc": "Igual a"},
        "Maior que (>)": {"sql": ">", "mask": "{}", "desc": "Maior que"},
        "Menor que (<)": {"sql": "<", "mask": "{}", "desc": "Menor que"},
        "Entre (><)": {"sql": "BETWEEN", "mask": "{}", "desc": "Faixa de valores"},
        "Vazio (Ø)": {"sql": "IS NULL", "mask": "", "desc": "Sem valor"}
    }
}

# ==============================================================================
# 3. FUNÇÕES DE BUSCA E INTEGRAÇÃO FATOR
# ==============================================================================

def processar_atualizacao_cadastral(cpf, nome):
    if not modulo_fator_conferi:
        return False, "<div style='color:red; font-size:10px;'>Módulo Fator Conferi não carregado.</div>"
    
    try:
        resultado = modulo_fator_conferi.realizar_consulta_cpf_segura(
            cpf=str(cpf), 
            ambiente="sistema_consulta_usuario", 
            forcar_nova=False
        )
    except Exception as e:
        return False, f"<div style='color:red; font-size:10px;'>Erro na consulta: {e}</div>"

    if resultado['sucesso']:
        sucessos, erros = modulo_fator_conferi.executar_distribuicao_dinamica(resultado['dados'])
        fin = resultado.get('financeiro', {})
        saldo_ant = float(fin.get('saldo_anterior', 0))
        valor_deb = float(fin.get('valor_debitado', 0))
        saldo_fim = float(fin.get('saldo_final', 0))
        agora = datetime.now().strftime("%d/%m %H:%M")
        
        lista_tabs = ""
        if sucessos:
            for s in sucessos:
                nome_tab = s.replace("sistema_consulta.sistema_consulta_dados_", "").replace("sistema_consulta.sistema_consulta_", "").replace("sistema_consulta.", "").replace("sistema_consulta_dados_", "").replace("sistema_consulta_", "")
                lista_tabs += f"<li style='margin:0; padding:0;'>{nome_tab}</li>"
        else:
            lista_tabs = "<li style='margin:0; padding:0;'>Nenhum dado novo.</li>"

        html_recibo = f"""<div style="background-color: #f1f8e9; border: 1px solid #4caf50; border-radius: 4px; padding: 5px; font-family: sans-serif; color: #1b5e20; font-size: 10px; line-height: 1.1;">
<div style="font-weight:bold; color: #2e7d32; border-bottom: 1px solid #a5d6a7; margin-bottom: 3px; padding-bottom: 2px;">✅ Atualizado</div>
<div style="margin-bottom: 3px;"><b>Data:</b> {agora}<br></div>
<div style="font-weight:bold; margin-top: 4px;">Tabelas:</div>
<ul style="margin: 0 0 5px 0; padding-left: 12px; font-size: 9px;">{lista_tabs}</ul>
<div style="background-color: #ffffff; padding: 4px; border-radius: 3px; border: 1px solid #c8e6c9; margin-top: 4px;">
<div style="margin-bottom: 2px;"><b>Saldo Ant:</b> {saldo_ant:,.2f}</div>
<div style="margin-bottom: 2px; color: #d32f2f;"><b>Débito:</b> -{valor_deb:,.2f}</div>
<div style="font-weight:bold; border-top: 1px dotted #ccc; padding-top:2px;">Saldo: {saldo_fim:,.2f}</div>
</div>
</div>"""
        
        if erros:
             html_recibo += f"<div style='color:orange; font-size:9px; margin-top:2px;'>⚠️ {len(erros)} avisos na importação.</div>"
             
        return True, html_recibo

    else:
        msg = resultado.get('msg', 'Erro desconhecido')
        return False, f"<div style='color:red; font-size:10px; border:1px solid red; padding:5px;'>❌ Falha: {msg}</div>"


@st.cache_data(ttl=300)
def buscar_relacao_auxiliar(tipo):
    with get_db_connection() as conn:
        if not conn: return [], []
        try:
            with conn.cursor() as cur:
                if tipo == 'Importação':
                    cur.execute("SELECT id, nome_arquivo, TO_CHAR(data_importacao, 'DD/MM/YYYY HH24:MI') as data, qtd_novos, qtd_atualizados FROM sistema_consulta.sistema_consulta_importacao ORDER BY id DESC LIMIT 100")
                    dados = cur.fetchall()
                    colunas = ['ID', 'Nome do Arquivo', 'Data', 'Novos', 'Atualizados']
                elif tipo == 'Agrupamento':
                    cur.execute("SELECT agrupamento, COUNT(*) FROM sistema_consulta.sistema_consulta_dados_cadastrais_agrupamento_cpf GROUP BY agrupamento ORDER BY 2 DESC")
                    dados = cur.fetchall()
                    colunas = ['Nome Agrupamento', 'Qtd CPFs']
                elif tipo == 'Campanha':
                    cur.execute("SELECT campanhas, COUNT(*) FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf WHERE campanhas IS NOT NULL AND campanhas <> '' GROUP BY campanhas ORDER BY 2 DESC")
                    dados = cur.fetchall()
                    colunas = ['Nome Campanha', 'Qtd CPFs']
            return dados, colunas
        except Exception as e:
            st.error(f"Erro ao buscar auxiliar: {e}")
            return [], []

def buscar_cliente_rapida(termo):
    with get_db_connection() as conn:
        if not conn: return []
        termo_texto = termo.strip()
        param_like_texto = f"%{termo_texto}%"
        cpf_pesquisa_int = v.ValidadorDocumentos.cpf_para_bigint(termo)
        query = """
            SELECT t.id, t.nome, t.cpf, t.identidade 
            FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf t
            WHERE t.nome ILIKE %s OR t.identidade ILIKE %s OR t.nome_pai ILIKE %s OR t.campanhas ILIKE %s OR t.id_importacao ILIKE %s OR
                EXISTS (SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = t.cpf AND CAST(telefone AS TEXT) ILIKE %s)
        """
        params = [param_like_texto] * 6
        if cpf_pesquisa_int:
            query += " OR t.cpf = %s "
            params.append(cpf_pesquisa_int)
        else:
            query += " OR CAST(t.cpf AS TEXT) ILIKE %s "
            params.append(param_like_texto)
        query += " LIMIT 30"
        try:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                return cur.fetchall()
        except Exception as e:
            st.error(f"Erro na busca: {e}")
            return []

def buscar_cliente_dinamica(filtros_aplicados):
    with get_db_connection() as conn:
        if not conn: return []
        base_query = "SELECT DISTINCT t.id, t.nome, t.cpf, t.identidade, t.id_importacao FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf t"
        where_clauses = ["1=1"]
        params = []
        for filtro in filtros_aplicados:
            coluna = filtro['col']
            operador_sql = filtro['op']
            tipo_dado = filtro['tipo']
            valor_original = filtro['val']
            if coluna in ['t.cpf', 'cpf', 'matricula', 'nb']:
                if operador_sql in ['=', 'IN']:
                    val_int = v.ValidadorDocumentos.cpf_para_bigint(valor_original)
                    if val_int is not None:
                        where_clauses.append(f"{coluna} {operador_sql} %s")
                        params.append(val_int)
                        continue
            def preparar_valor_filtro_texto(valor):
                if coluna in ['telefone']: return v.ValidadorDocumentos.limpar_numero(valor)
                return str(valor).strip()
            if tipo_dado == 'texto_vinculado':
                tabela_satelite = filtro['table']
                val_raw = preparar_valor_filtro_texto(valor_original)
                val_final = filtro['mask'].format(val_raw) if '{}' in filtro['mask'] else val_raw
                sub_where = f"CAST({coluna} AS TEXT) {operador_sql} %s" if coluna == 'telefone' and 'LIKE' in operador_sql else f"{coluna} {operador_sql} %s"
                where_clauses.append(f"EXISTS (SELECT 1 FROM sistema_consulta.{tabela_satelite} WHERE cpf = t.cpf AND {sub_where})")
                params.append(val_final)
            elif tipo_dado == 'numero_calculado':
                if "IS NULL" in operador_sql: where_clauses.append(f"t.data_nascimento IS NULL")
                elif operador_sql == 'BETWEEN':
                    where_clauses.append(f"EXTRACT(YEAR FROM age(t.data_nascimento)) BETWEEN %s AND %s")
                    params.append(valor_original[0]); params.append(valor_original[1])
                else:
                    where_clauses.append(f"EXTRACT(YEAR FROM age(t.data_nascimento)) {operador_sql} %s")
                    params.append(valor_original)
            else:
                if operador_sql == 'BETWEEN' and tipo_dado == 'data':
                    where_clauses.append(f"{coluna} BETWEEN %s AND %s")
                    params.append(valor_original[0]); params.append(valor_original[1])
                else:
                    if coluna == 't.cpf' and 'LIKE' in operador_sql:
                          where_clauses.append(f"CAST({coluna} AS TEXT) {operador_sql} %s")
                          val_raw = preparar_valor_filtro_texto(valor_original)
                          params.append(filtro['mask'].format(val_raw))
                    else:
                        val_raw = preparar_valor_filtro_texto(valor_original)
                        where_clauses.append(f"{coluna} {operador_sql} %s")
                        val_final = filtro['mask'].format(val_raw) if '{}' in filtro['mask'] else val_raw
                        params.append(val_final)
        full_query = f"{base_query} WHERE {' AND '.join(where_clauses)} LIMIT 50"
        try:
            with conn.cursor() as cur:
                cur.execute(full_query, tuple(params))
                return cur.fetchall()
        except Exception as e:
            st.error(f"Erro SQL: {e}")
            return []

def carregar_dados_cliente_completo(cpf):
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(str(cpf))
    with get_db_connection() as conn:
        if not conn: return {}
        dados = {}
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf WHERE cpf = %s", (cpf_val,))
                cols_pessoais = [desc[0] for desc in cur.description]
                row_pessoais = cur.fetchone()
                if row_pessoais:
                    d_pessoal = dict(zip(cols_pessoais, row_pessoais))
                    for k, val in d_pessoal.items():
                        if val is None and k != 'data_nascimento': d_pessoal[k] = ""
                    dados['pessoal'] = d_pessoal
                else: dados['pessoal'] = {}
                try:
                    cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_clt WHERE cpf = %s LIMIT 1", (cpf_val,))
                    cols_clt = [desc[0] for desc in cur.description]
                    row_clt = cur.fetchone()
                    if row_clt:
                        d_clt = dict(zip(cols_clt, row_clt))
                        for k, val in d_clt.items():
                            if val is None and 'data' not in k: d_clt[k] = ""
                        dados['clt'] = d_clt
                except: dados['clt'] = {}
                cur.execute("SELECT id, telefone FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s ORDER BY id", (cpf_val,))
                dados['telefones'] = [{'id': r[0], 'valor': r[1] or ""} for r in cur.fetchall()]
                cur.execute("SELECT id, email FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf = %s ORDER BY id", (cpf_val,))
                dados['emails'] = [{'id': r[0], 'valor': r[1] or ""} for r in cur.fetchall()]
                cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco WHERE cpf = %s ORDER BY id", (cpf_val,))
                cols_end = [desc[0] for desc in cur.description]
                dados['enderecos'] = []
                for r in cur.fetchall():
                    d_end = dict(zip(cols_end, r))
                    for k, val in d_end.items():
                        if val is None: d_end[k] = ""
                    dados['enderecos'].append(d_end)
                cur.execute("SELECT agrupamento FROM sistema_consulta.sistema_consulta_dados_cadastrais_agrupamento_cpf WHERE cpf = %s", (cpf_val,))
                dados['agrupamentos'] = [r[0] for r in cur.fetchall() if r[0]]
                try:
                    cur.execute("SELECT convenio FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s", (cpf_val,))
                    dados['convenios_lista'] = [r[0] for r in cur.fetchall() if r[0]]
                except: dados['convenios_lista'] = []
        except Exception as e:
            st.error(f"Erro ao carregar cliente: {e}")
        return dados

def listar_contratos_cliente(cpf):
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(str(cpf))
    with get_db_connection() as conn:
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT matricula, convenio FROM sistema_consulta.sistema_consulta_contrato WHERE cpf = %s AND matricula IS NOT NULL AND convenio IS NOT NULL", (cpf_val,))
                return cur.fetchall()
        except Exception: return []

def listar_convenios_cliente(cpf):
    """Retorna lista de convênios vinculados ao cliente (Tabela Dados Cadastrais Convenio)"""
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(str(cpf))
    with get_db_connection() as conn:
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT convenio FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s ORDER BY convenio", (cpf_val,))
                return [r[0] for r in cur.fetchall()]
        except Exception: return []

def buscar_tabela_por_convenio(nome_convenio):
    with get_db_connection() as conn:
        if not conn: return None
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT tabela_referencia FROM sistema_consulta.sistema_consulta_convenio_tipo WHERE nome_convenio = %s", (nome_convenio,))
                res = cur.fetchone()
                return res[0] if res else None
        except Exception: return None

@st.cache_data(ttl=3600)
def listar_colunas_tabela(nome_tabela):
    with get_db_connection() as conn:
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'sistema_consulta' AND table_name = %s", (nome_tabela.replace('sistema_consulta.', ''),))
                return [row[0] for row in cur.fetchall()]
        except Exception: return []

def listar_tipos_convenio_disponiveis():
    with get_db_connection() as conn:
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT nome_convenio FROM sistema_consulta.sistema_consulta_convenio_tipo ORDER BY nome_convenio")
                return [r[0] for r in cur.fetchall()]
        except Exception: return []

def buscar_dados_dinamicos_especificos(tabela, cpf, matricula):
    """Busca dados de uma tabela dinâmica específica usando CPF e Matrícula"""
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(str(cpf))
    mat_val = v.ValidadorDocumentos.nb_para_bigint(str(matricula))
    with get_db_connection() as conn:
        if not conn: return {}
        try:
            with conn.cursor() as cur:
                # Verifica se a tabela tem coluna matricula
                colunas = listar_colunas_tabela(tabela)
                if 'matricula' not in colunas:
                      query = sql.SQL("SELECT * FROM sistema_consulta.{} WHERE cpf = %s LIMIT 1").format(sql.Identifier(tabela.replace('sistema_consulta.', '')))
                      cur.execute(query, (cpf_val,))
                else:
                      query = sql.SQL("SELECT * FROM sistema_consulta.{} WHERE cpf = %s AND matricula = %s LIMIT 1").format(sql.Identifier(tabela.replace('sistema_consulta.', '')))
                      cur.execute(query, (cpf_val, mat_val))
                
                row = cur.fetchone()
                if row:
                    cols = [desc[0] for desc in cur.description]
                    return dict(zip(cols, row))
                return {}
        except Exception: return {}

def buscar_hierarquia_financeira(cpf):
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(str(cpf))
    with get_db_connection() as conn:
        if not conn: return {}
        estrutura = {}
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_contrato WHERE cpf = %s ORDER BY convenio, matricula, data_inicio DESC", (cpf_val,))
                cols_contrato = [desc[0] for desc in cur.description]
                rows_contratos = cur.fetchall()
                if not rows_contratos: return {}
                mapa_convenio_tabela = {} 
                for row in rows_contratos:
                    d_contrato = dict(zip(cols_contrato, row))
                    for k, val in d_contrato.items():
                        if val is None: d_contrato[k] = ""
                    nome_conv = d_contrato.get('convenio') or 'DESCONHECIDO'
                    num_matr = d_contrato.get('matricula')
                    if num_matr is None: num_matr = 0
                    chave = (nome_conv, num_matr)
                    if chave not in estrutura: estrutura[chave] = {'contratos': [], 'dados_convenio': {}, 'tabela_ref': None}
                    estrutura[chave]['contratos'].append(d_contrato)
                convenios_unicos = list(set([k[0] for k in estrutura.keys()]))
                if convenios_unicos:
                    cur.execute("SELECT nome_convenio, tabela_referencia FROM sistema_consulta.sistema_consulta_convenio_tipo WHERE nome_convenio = ANY(%s)", (convenios_unicos,))
                    for r in cur.fetchall(): mapa_convenio_tabela[r[0]] = r[1]
                for (nome_conv, num_matr), dados_grupo in estrutura.items():
                    tabela_ref = mapa_convenio_tabela.get(nome_conv)
                    if tabela_ref:
                        dados_grupo['tabela_ref'] = tabela_ref
                        try:
                            cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = 'sistema_consulta' AND table_name = %s"), (tabela_ref.replace('sistema_consulta.', ''),))
                            colunas_tabela_ref = [c[0] for c in cur.fetchall()]
                            tem_matricula = 'matricula' in colunas_tabela_ref
                            query_dinamica = sql.SQL("SELECT * FROM sistema_consulta.{} WHERE cpf = %s").format(sql.Identifier(tabela_ref.replace('sistema_consulta.', '')))
                            params_dinamica = [cpf_val]
                            if tem_matricula and num_matr != 0:
                                query_dinamica = sql.SQL("SELECT * FROM sistema_consulta.{} WHERE cpf = %s AND matricula = %s LIMIT 1").format(sql.Identifier(tabela_ref.replace('sistema_consulta.', '')))
                                params_dinamica.append(num_matr)
                            else:
                                query_dinamica = sql.SQL("SELECT * FROM sistema_consulta.{} WHERE cpf = %s LIMIT 1").format(sql.Identifier(tabela_ref.replace('sistema_consulta.', '')))
                            cur.execute(query_dinamica, params_dinamica)
                            row_dados = cur.fetchone()
                            if row_dados:
                                cols_desc = [d[0] for d in cur.description]
                                dict_dados = dict(zip(cols_desc, row_dados))
                                for k, val in dict_dados.items():
                                    if val is None: dict_dados[k] = ""
                                dados_grupo['dados_convenio'] = dict_dados
                        except Exception: pass
        except Exception as e: st.error(f"Erro na busca financeira: {e}")
        return estrutura

def salvar_novo_cliente(dados_form):
    cpf_bigint = v.ValidadorDocumentos.cpf_para_bigint(dados_form.get('cpf'))
    if not cpf_bigint:
        st.error("CPF Inválido!")
        return False
    nasc_sql = v.ValidadorData.para_sql(dados_form.get('data_nascimento'))
    if not nasc_sql:
        st.error("Data de Nascimento inválida.")
        return False
    dados_limpos = {
        "nome": str(dados_form.get('nome') or "").upper().strip(),
        "cpf": cpf_bigint,
        "identidade": str(dados_form.get('identidade') or "").strip(),
        "sexo": dados_form.get('sexo'),
        "nome_mae": str(dados_form.get('nome_mae') or "").upper().strip(),
        "nome_pai": str(dados_form.get('nome_pai') or "").upper().strip(),
        "campanhas": str(dados_form.get('campanhas') or "").strip(),
        "data_nascimento": nasc_sql
    }
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cols = list(dados_limpos.keys())
                vals = list(dados_limpos.values())
                placeholders = ", ".join(["%s"] * len(cols))
                columns = ", ".join(cols)
                sql_insert = f"INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_cpf ({columns}) VALUES ({placeholders})"
                cur.execute(sql_insert, vals)
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf) VALUES (%s) ON CONFLICT DO NOTHING", (cpf_bigint,))
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")
            return False

def inserir_dado_extra(tipo, cpf, dados):
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(str(cpf))
    with get_db_connection() as conn:
        if not conn: return "erro"
        try:
            with conn.cursor() as cur:
                if tipo == "DadosDinâmicos":
                    tabela_alvo = dados.get('_tabela')
                    campos_dados = {k: val for k, val in dados.items() if not k.startswith('_')}
                    if not tabela_alvo or not campos_dados: return "erro"
                    
                    if 'cpf' in campos_dados: campos_dados['cpf'] = cpf_val
                    mat_original = campos_dados.get('matricula')
                    if mat_original: campos_dados['matricula'] = v.ValidadorDocumentos.nb_para_bigint(mat_original)
                    
                    # Logica de Vinculo (Contratos/Convenio) - Só faz se for solicitado (novo)
                    if dados.get('_criar_vinculo'):
                        new_mat = campos_dados.get('matricula')
                        new_conv = dados.get('convenio')
                        if new_mat:
                            cur.execute("INSERT INTO sistema_consulta.sistema_consulta_contrato (cpf, matricula, convenio) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (cpf_val, new_mat, new_conv))
                            cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio) VALUES (%s, %s) ON CONFLICT DO NOTHING", (cpf_val, new_conv))
                    
                    # Logica UPSERT para Dados da Tabela Dinâmica
                    colunas = list(campos_dados.keys())
                    valores = [val if val != "" else None for val in campos_dados.values()]
                    
                    # Verifica se já existe o registro (Para Update)
                    reg_existe = False
                    if 'matricula' in campos_dados and campos_dados['matricula']:
                          query_check = sql.SQL("SELECT 1 FROM sistema_consulta.{} WHERE cpf = %s AND matricula = %s").format(sql.Identifier(tabela_alvo.replace('sistema_consulta.', '')))
                          cur.execute(query_check, (cpf_val, campos_dados['matricula']))
                          if cur.fetchone(): reg_existe = True
                    else:
                          query_check = sql.SQL("SELECT 1 FROM sistema_consulta.{} WHERE cpf = %s").format(sql.Identifier(tabela_alvo.replace('sistema_consulta.', '')))
                          cur.execute(query_check, (cpf_val,))
                          if cur.fetchone(): reg_existe = True

                    if reg_existe:
                        # UPDATE
                        set_clauses = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in colunas]
                        if 'matricula' in campos_dados and campos_dados['matricula']:
                            query = sql.SQL("UPDATE sistema_consulta.{} SET {} WHERE cpf = %s AND matricula = %s").format(
                                sql.Identifier(tabela_alvo.replace('sistema_consulta.', '')),
                                sql.SQL(', ').join(set_clauses)
                            )
                            # Params: values + cpf + matricula
                            cur.execute(query, valores + [cpf_val, campos_dados['matricula']])
                        else:
                            query = sql.SQL("UPDATE sistema_consulta.{} SET {} WHERE cpf = %s").format(
                                sql.Identifier(tabela_alvo.replace('sistema_consulta.', '')),
                                sql.SQL(', ').join(set_clauses)
                            )
                            cur.execute(query, valores + [cpf_val])
                    else:
                        # INSERT
                        query = sql.SQL("INSERT INTO sistema_consulta.{} ({}) VALUES ({})").format(
                            sql.Identifier(tabela_alvo.replace('sistema_consulta.', '')),
                            sql.SQL(', ').join(map(sql.Identifier, colunas)),
                            sql.SQL(', ').join(sql.Placeholder() * len(colunas))
                        )
                        cur.execute(query, valores)

                elif tipo == "Contrato":
                    dt_inicio = v.ValidadorData.para_sql(dados.get('data_inicio'))
                    dt_final = v.ValidadorData.para_sql(dados.get('data_final'))
                    mat_int = v.ValidadorDocumentos.nb_para_bigint(dados.get('matricula'))
                    campos = ["cpf", "matricula", "convenio", "numero_contrato", "valor_parcela", "prazo_total", "prazo_aberto", "prazo_pago", "saldo_devedor", "taxa_juros", "valor_contrato_inicial", "data_inicio", "data_final"]
                    valores = [cpf_val, mat_int, str(dados.get('convenio') or "").strip(), str(dados.get('numero_contrato') or "").strip(), v.ValidadorFinanceiro.para_sql(dados.get('valor_parcela')), str(dados.get('prazo_total') or "").strip(), str(dados.get('prazo_aberto') or "").strip(), str(dados.get('prazo_pago') or "").strip(), v.ValidadorFinanceiro.para_sql(dados.get('saldo_devedor')), v.ValidadorFinanceiro.para_sql(dados.get('taxa_juros')), v.ValidadorFinanceiro.para_sql(dados.get('valor_contrato_inicial')), dt_inicio, dt_final]
                    placeholders = ", ".join(["%s"] * len(valores))
                    query = f"INSERT INTO sistema_consulta.sistema_consulta_contrato ({', '.join(campos)}) VALUES ({placeholders})"
                    cur.execute(query, valores)
                elif tipo == "Telefone":
                    val = v.ValidadorContato.telefone_para_sql(dados.get('valor'))
                    if not val: st.error("Telefone inválido."); return "erro"
                    cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s AND telefone = %s", (cpf_val, val))
                    if cur.fetchone(): return "duplicado"
                    cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone) VALUES (%s, %s)", (cpf_val, val))
                elif tipo == "E-mail":
                    val = str(dados.get('valor')).strip()
                    if not v.ValidadorContato.email_valido(val): st.error("E-mail inválido."); return "erro"
                    cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf = %s AND email = %s", (cpf_val, val))
                    if cur.fetchone(): return "duplicado"
                    cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email) VALUES (%s, %s)", (cpf_val, val))
                elif tipo == "Endereço":
                    cur.execute("""INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_endereco (cpf, cep, rua, bairro, cidade, uf) VALUES (%s, %s, %s, %s, %s, %s)""", (cpf_val, dados.get('cep'), dados.get('rua'), dados.get('bairro'), dados.get('cidade'), dados.get('uf')))
                elif tipo == "Convênio (Cadastro)":
                    val = str(dados.get('valor')).strip()
                    cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s AND convenio = %s", (cpf_val, val))
                    if cur.fetchone(): return "duplicado"
                    cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio) VALUES (%s, %s)", (cpf_val, val))
                conn.commit()
                return "sucesso"
        except Exception as e: st.error(f"Erro insert extra: {e}"); return "erro"

def atualizar_dados_dinamicos(alteracoes_dinamicas):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                for alt in alteracoes_dinamicas:
                    tabela = alt['tabela']
                    id_reg = alt['id']
                    campos_novos = alt['dados']
                    if not campos_novos: continue
                    set_clauses = []
                    values = []
                    for col, val in campos_novos.items():
                        set_clauses.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
                        values.append(val)
                    values.append(id_reg)
                    query = sql.SQL("UPDATE sistema_consulta.{} SET {} WHERE id = %s").format(sql.Identifier(tabela.replace('sistema_consulta.', '')), sql.SQL(', ').join(set_clauses))
                    cur.execute(query, values)
                conn.commit()
                return True
        except Exception as e: st.error(f"Erro update dinâmico: {e}"); return False

def atualizar_dados_cliente_lote(cpf, dados_editados, dados_dinamicos=None):
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(str(cpf))
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                pessoal = dados_editados['pessoal']
                nasc_sql = v.ValidadorData.para_sql(pessoal['data_nascimento'])
                cur.execute("""UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_cpf SET nome = %s, data_nascimento = %s, identidade = %s, sexo = %s, cnh = %s, titulo_eleitoral = %s, nome_mae = %s, nome_pai = %s, campanhas = %s WHERE cpf = %s""", (str(pessoal['nome']).strip(), nasc_sql, str(pessoal['identidade']).strip(), str(pessoal['sexo']).strip(), str(pessoal['cnh']).strip(), str(pessoal['titulo_eleitoral']).strip(), str(pessoal['nome_mae']).strip(), str(pessoal.get('nome_pai')).strip(), str(pessoal.get('campanhas')).strip(), cpf_val))
                for item in dados_editados.get('telefones', []):
                    val = v.ValidadorContato.telefone_para_sql(item['valor'])
                    if not val: cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE id = %s", (item['id'],))
                    else: cur.execute("UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_telefone SET telefone = %s WHERE id = %s", (val, item['id']))
                for item in dados_editados.get('emails', []):
                    val = str(item['valor']).strip()
                    if not val: cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE id = %s", (item['id'],))
                    else: cur.execute("UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_email SET email = %s WHERE id = %s", (val, item['id']))
                for item in dados_editados.get('enderecos', []):
                       cur.execute("""UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_endereco SET rua = %s, bairro = %s, cidade = %s, uf = %s, cep = %s WHERE id = %s""", (item['rua'], item['bairro'], item['cidade'], item['uf'], item['cep'], item['id']))
                conn.commit()
        except Exception as e: st.error(f"Erro ao atualizar: {e}"); return False
    if dados_dinamicos: atualizar_dados_dinamicos(dados_dinamicos)
    return True

def excluir_cliente_total(cpf):
    cpf_val = v.ValidadorDocumentos.cpf_para_bigint(str(cpf))
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                tabelas = ["sistema_consulta_dados_cadastrais_telefone", "sistema_consulta_dados_cadastrais_email", "sistema_consulta_dados_cadastrais_endereco", "sistema_consulta_dados_cadastrais_convenio", "sistema_consulta_dados_cadastrais_agrupamento_cpf", "sistema_consulta_dados_ctt", "sistema_consulta_dados_clt", "sistema_consulta_contrato", "sistema_consulta_dados_cadastrais_cpf", "sistema_consulta_cpf"]
                for t in tabelas:
                    cur.execute(sql.SQL("DELETE FROM sistema_consulta.{} WHERE cpf = %s").format(sql.Identifier(t.replace('sistema_consulta.', ''))), (cpf_val,))
                conn.commit()
                return True
        except Exception as e: st.error(f"Erro ao excluir: {e}"); return False

# ==============================================================================
# 5. INTERFACE DO USUÁRIO
# ==============================================================================

@st.dialog("➕ Inserir Dados Extras")
def modal_inserir_dados(cpf, nome_cliente):
    st.write(f"Cliente: **{nome_cliente}**")
    tipo_insercao = st.selectbox("Selecione o Tipo", ["Telefone", "E-mail", "Endereço", "Contrato", "Dados de Convênio", "Convênio (Cadastro)"])
    with st.form("form_insercao_modal"):
        dados_submit = {}
        if tipo_insercao == "Dados de Convênio":
            st.info("Informe os dados para incluir um novo registro.")
            # 1. Listar apenas convênios que o CPF já possui
            lista_convenios_existentes = listar_convenios_cliente(cpf)
            
            if not lista_convenios_existentes:
                st.warning("Este cliente não possui convênios vinculados para inclusão de dados.")
            else:
                # 2. Selecionar Convênio
                convenio_sel = st.selectbox("Selecione o Convênio Vinculado", options=["(Selecione)"] + lista_convenios_existentes)
                
                if convenio_sel and convenio_sel != "(Selecione)":
                    # 3. Informar Matrícula (Campo Livre para Inserção)
                    matricula_sel = st.text_input("Informe a Matrícula (Novo Registro)")
                    
                    if matricula_sel:
                        tabela_alvo = buscar_tabela_por_convenio(convenio_sel)
                        if tabela_alvo:
                            colunas = listar_colunas_tabela(tabela_alvo)
                            if not colunas:
                                st.error("Tabela sem colunas configuradas.")
                            else:
                                dados_submit['_tabela'] = tabela_alvo
                                dados_submit['cpf'] = cpf
                                dados_submit['matricula'] = matricula_sel
                                dados_submit['convenio'] = convenio_sel
                                dados_submit['_criar_vinculo'] = True # Mantém para garantir consistência
                                
                                cols_form = st.columns(2)
                                idx = 0
                                for col in colunas:
                                    if col not in ['id', 'cpf', 'matricula', 'nome', 'agrupamento']:
                                        with cols_form[idx % 2]:
                                            # SEMPRE VAZIO - APENAS INCLUSÃO
                                            if 'data' in col.lower():
                                                dados_submit[col] = st.date_input(col.replace('_', ' ').capitalize(), value=None, format="DD/MM/YYYY")
                                            else:
                                                dados_submit[col] = st.text_input(col.replace('_', ' ').capitalize(), value="")
                                        idx += 1
                        else:
                            st.error(f"Tabela não encontrada para o convênio: {convenio_sel}")

        elif tipo_insercao == "Contrato":
            c1, c2 = st.columns(2)
            dados_submit['matricula'] = c1.text_input("Matrícula")
            dados_submit['convenio'] = c2.text_input("Convênio")
            dados_submit['numero_contrato'] = st.text_input("Número do Contrato")
            c3, c4, c5 = st.columns(3)
            dados_submit['valor_parcela'] = c3.text_input("Valor Parcela (R$)")
            dados_submit['prazo_total'] = c4.text_input("Prazo Total")
            dados_submit['prazo_aberto'] = c5.text_input("Prazo Aberto")
            c6, c7, c8 = st.columns(3)
            dados_submit['prazo_pago'] = c6.text_input("Prazo Pago")
            dados_submit['saldo_devedor'] = c7.text_input("Saldo Devedor")
            dados_submit['taxa_juros'] = c8.text_input("Taxa Juros")
            dados_submit['valor_contrato_inicial'] = st.text_input("Valor Inicial Contrato")
            c9, c10 = st.columns(2)
            dados_submit['data_inicio'] = c9.date_input("Data Início", value=None)
            dados_submit['data_final'] = c10.date_input("Data Final", value=None)
        elif tipo_insercao == "Telefone": dados_submit['valor'] = st.text_input("Novo Telefone", placeholder="(00) 00000-0000")
        elif tipo_insercao == "E-mail": dados_submit['valor'] = st.text_input("Novo E-mail")
        elif tipo_insercao == "Endereço":
            dados_submit['cep'] = st.text_input("CEP"); dados_submit['rua'] = st.text_input("Rua"); dados_submit['bairro'] = st.text_input("Bairro"); dados_submit['cidade'] = st.text_input("Cidade"); dados_submit['uf'] = st.text_input("UF", max_chars=2)
        elif tipo_insercao == "Convênio (Cadastro)": 
            lista_todos_convenios = listar_tipos_convenio_disponiveis()
            dados_submit['valor'] = st.selectbox("Nome do Convênio", options=["(Selecione)"] + lista_todos_convenios)
        
        if st.form_submit_button("✅ Salvar Inclusão"):
            if tipo_insercao == "Convênio (Cadastro)" and dados_submit['valor'] == "(Selecione)":
                st.error("Selecione um convênio válido.")
            else:
                tipo_envio = "DadosDinâmicos" if tipo_insercao == "Dados de Convênio" else tipo_insercao
                status = inserir_dado_extra(tipo_envio, cpf, dados_submit)
                if status == "sucesso": st.success(f"{tipo_insercao} inserido/atualizado com sucesso!"); time.sleep(1); st.rerun()
                elif status == "duplicado": st.warning("Dado já existente!")
                else: st.error("Erro ao inserir.")

@st.dialog("📂 Visualizador de Agrupamentos")
def modal_agrupamentos():
    st.markdown("### Selecione o tipo para visualizar")
    tipo = st.selectbox("Tipo:", ["Importação", "Agrupamento", "Campanha"])
    if tipo:
        with st.spinner("Carregando dados..."): dados, colunas = buscar_relacao_auxiliar(tipo)
        if dados: st.dataframe(pd.DataFrame(dados, columns=colunas), use_container_width=True, hide_index=True)
        else: st.info("Nenhum registro encontrado.")

@st.dialog("⚠️ Confirmar Exclusão")
def modal_confirmar_exclusao(cpf):
    st.warning("Tem certeza? Essa ação não pode ser desfeita.")
    if st.button("🚨 SIM, EXCLUIR DEFINITIVAMENTE", type="primary"):
        if excluir_cliente_total(cpf):
            st.success("Cliente excluído!"); st.session_state['cliente_ativo_cpf'] = None; st.session_state['modo_visualizacao'] = None; st.session_state['resultados_pesquisa'] = []; time.sleep(1.5); st.rerun()

def tela_ficha_cliente(cpf, modo='visualizar'):
    if 'modo_edicao' not in st.session_state: st.session_state['modo_edicao'] = False
    if modo == 'novo':
        st.markdown("## ✨ Novo Cadastro")
        with st.form("form_novo"):
            c1, c2, c3 = st.columns(3)
            nome = c1.text_input("Nome*")
            cpf_in = c2.text_input("CPF*")
            nasc = c3.date_input("Nascimento", value=None, min_value=date(1900,1,1), max_value=date(2050,1,1), format="DD/MM/YYYY")
            c4, c5, c6 = st.columns(3)
            rg = c4.text_input("RG")
            sexo = c5.selectbox("Sexo", ["Masculino", "Feminino", "Outros"])
            mae = c6.text_input("Nome da Mãe")
            c7, c8 = st.columns(2)
            pai = c7.text_input("Nome do Pai")
            campanhas = c8.text_input("Campanhas (Separar por vírgula)")
            if st.form_submit_button("💾 Salvar"):
                if salvar_novo_cliente({"nome": nome, "cpf": cpf_in, "data_nascimento": nasc, "identidade": rg, "sexo": sexo, "nome_mae": mae, "nome_pai": pai, "campanhas": campanhas}):
                    st.success("Cadastrado!"); st.session_state['cliente_ativo_cpf'] = v.ValidadorDocumentos.cpf_para_bigint(cpf_in); st.session_state['modo_visualizacao'] = 'visualizar'; time.sleep(1); st.rerun()
        return

    dados = carregar_dados_cliente_completo(cpf)
    pessoal = dados.get('pessoal', {})
    financeiro = buscar_hierarquia_financeira(cpf)

    st.markdown("""<style>.stTextInput label p, .stTextArea label p, .stDateInput label p, .stSelectbox label p {color: black !important; text-decoration: underline; font-weight: bold;} input:disabled, textarea:disabled {color: black !important; -webkit-text-fill-color: black !important; opacity: 1 !important; font-weight: 500;}</style>""", unsafe_allow_html=True)

    c_dados, c_lateral = st.columns([9, 1])
    with c_dados:
        st.divider()
        c_head, c_act = st.columns([4, 5])
        cpf_show = v.ValidadorDocumentos.cpf_para_tela(pessoal.get('cpf', ''))
        c_head.markdown(f"## 👤 {pessoal.get('nome', 'Sem Nome')}")
        c_head.caption(f"CPF: {cpf_show}")
        with c_act:
            c_back, c_ins, c_edit, c_del = st.columns(4)
            with c_back:
                if st.button("⬅️ Voltar", use_container_width=True):
                    st.session_state['cliente_ativo_cpf'] = None; st.session_state['modo_visualizacao'] = None; st.session_state['modo_edicao'] = False; st.rerun()
            with c_ins:
                if st.button("➕ Extra", help="Inserir telefone, email, contrato", use_container_width=True): modal_inserir_dados(cpf, pessoal.get('nome'))
            with c_edit:
                if st.session_state['modo_edicao']:
                     if st.button("👁️ Ver", help="Sair do modo edição", use_container_width=True): st.session_state['modo_edicao'] = False; st.rerun()
                else:
                    if st.button("✏️ Editar", type="primary", use_container_width=True): st.session_state['modo_edicao'] = True; st.rerun()
            with c_del:
                if st.button("🗑️ Excluir", type="primary", use_container_width=True): modal_confirmar_exclusao(cpf)
        st.divider()

        if st.session_state['modo_edicao']:
            with st.form("form_edicao_cliente"):
                st.info("✏️ Modo Edição Ativo. Edite os campos abaixo e clique em Salvar.")
                with st.expander("📄 Dados Cadastrais", expanded=True):
                    c1, c2, c3, c4, c5, c6 = st.columns([3, 1.5, 1.5, 1, 1.5, 1.5])
                    e_nome = c1.text_input("Nome Completo", value=pessoal.get('nome','')); c2.text_input("CPF", value=cpf_show, disabled=True); e_rg = c3.text_input("RG", value=pessoal.get('identidade','')); e_sexo = c4.selectbox("Sexo", ["Masculino", "Feminino", "Outros"], index=["Masculino", "Feminino", "Outros"].index(pessoal.get('sexo', 'Outros')) if pessoal.get('sexo') in ["Masculino", "Feminino", "Outros"] else 0); e_nasc = c5.date_input("Data Nasc.", value=v.ValidadorData.para_sql(pessoal.get('data_nascimento')), format="DD/MM/YYYY"); idade_str = v.ValidadorData.calcular_tempo(pessoal.get('data_nascimento'), 'completo'); c6.text_input("Idade", value=idade_str, disabled=True)
                    c7, c8 = st.columns(2); e_mae = c7.text_input("Nome da Mãe", value=pessoal.get('nome_mae','')); e_pai = c8.text_input("Nome do Pai", value=pessoal.get('nome_pai',''))
                    c9, c10, c11, c12 = st.columns(4); e_cnh = c9.text_input("CNH", value=pessoal.get('cnh','')); e_titulo = c10.text_input("Título Eleitor", value=pessoal.get('titulo_eleitoral','')); e_campanhas = c11.text_input("Campanha", value=pessoal.get('campanhas','')); agrupamento_val = dados.get('agrupamentos')[0] if dados.get('agrupamentos') else ""; c12.text_input("Agrupamento", value=agrupamento_val, disabled=True); lista_convs = dados.get('convenios_lista', []); conv_str = ", ".join(lista_convs); st.text_input("Convênios Vinculados", value=conv_str, disabled=True)
                with st.expander("📍 Contatos e Endereço", expanded=True):
                    edicoes_telefones = []; edicoes_emails = []; edicoes_enderecos = []
                    col_lista1, col_lista2 = st.columns(2)
                    with col_lista1:
                        st.markdown("###### 📞 Telefones")
                        if dados.get('telefones'):
                            for i, tel in enumerate(dados['telefones']): novo_val = st.text_input(f"Tel {i+1}", value=tel['valor'], key=f"tel_{tel['id']}"); edicoes_telefones.append({'id': tel['id'], 'valor': novo_val})
                        else: st.caption("Sem telefones.")
                    with col_lista2:
                        st.markdown("###### 📧 E-mails")
                        if dados.get('emails'):
                            for i, mail in enumerate(dados['emails']): novo_val = st.text_input(f"Email {i+1}", value=mail['valor'], key=f"mail_{mail['id']}"); edicoes_emails.append({'id': mail['id'], 'valor': novo_val})
                    st.divider(); st.markdown("###### 🏠 Endereço")
                    if dados.get('enderecos'):
                        for i, end in enumerate(dados.get('enderecos', [])):
                            ce1, ce2, ce3, ce4, ce5 = st.columns([3, 2, 2, 1, 1.5]); nr_rua = ce1.text_input("Rua", value=end.get('rua',''), key=f"er_{i}"); nr_bairro = ce2.text_input("Bairro", value=end.get('bairro',''), key=f"eb_{i}"); nr_cidade = ce3.text_input("Cidade", value=end.get('cidade',''), key=f"ec_{i}"); nr_uf = ce4.text_input("UF", value=end.get('uf',''), key=f"eu_{i}"); nr_cep = ce5.text_input("CEP", value=end.get('cep',''), key=f"ecp_{i}"); edicoes_enderecos.append({'id': end['id'], 'rua': nr_rua, 'bairro': nr_bairro, 'cidade': nr_cidade, 'uf': nr_uf, 'cep': nr_cep})
                    else: st.caption("Sem endereço cadastrado.")
                st.divider()
                alteracoes_dinamicas = [] 
                if financeiro:
                    st.markdown("### 📋 Editar Dados de Convênios")
                    for (nome_convenio, matricula), grupo in financeiro.items():
                        with st.expander(f"Editar: {nome_convenio} - Matrícula {matricula}", expanded=True):
                            dados_esp = grupo.get('dados_convenio'); tabela_ref = grupo.get('tabela_ref')
                            if dados_esp and tabela_ref and 'id' in dados_esp:
                                edicao_grupo = {}; cols_dyn = st.columns(3); idx_col = 0
                                for k, val in dados_esp.items():
                                    if k not in ['id', 'cpf', 'matricula', 'nome', 'agrupamento']:
                                        val_novo = cols_dyn[idx_col % 3].text_input(k.replace('_', ' ').capitalize(), value=str(val), key=f"edyn_{tabela_ref}_{dados_esp['id']}_{k}")
                                        if val_novo != str(val): edicao_grupo[k] = val_novo
                                        idx_col += 1
                                if edicao_grupo: alteracoes_dinamicas.append({'tabela': tabela_ref, 'id': dados_esp['id'], 'dados': edicao_grupo})
                st.divider()
                if st.form_submit_button("💾 CONFIRMAR ALTERAÇÕES", type="primary"):
                    pacote_dados = {"pessoal": {"nome": e_nome, "identidade": e_rg, "data_nascimento": e_nasc, "cnh": e_cnh, "titulo_eleitoral": e_titulo, "sexo": e_sexo, "nome_mae": e_mae, "nome_pai": e_pai, "campanhas": e_campanhas}, "telefones": edicoes_telefones, "emails": edicoes_emails, "enderecos": edicoes_enderecos}
                    if atualizar_dados_cliente_lote(cpf, pacote_dados, dados_dinamicos=alteracoes_dinamicas):
                        st.success("Dados atualizados com sucesso!"); st.session_state['modo_edicao'] = False; time.sleep(1); st.rerun()
            return

        with st.expander("📄 Dados Cadastrais", expanded=False):
            c1, c2, c3, c4, c5, c6 = st.columns([3, 1.5, 1.5, 1, 1.5, 1.5])
            c1.text_input("Nome Completo", value=pessoal.get('nome',''), disabled=True); c2.text_input("CPF", value=cpf_show, disabled=True); c3.text_input("RG", value=pessoal.get('identidade',''), disabled=True); c4.text_input("Sexo", value=pessoal.get('sexo',''), disabled=True); nasc_fmt = v.ValidadorData.para_tela(pessoal.get('data_nascimento')); idade_str = v.ValidadorData.calcular_tempo(pessoal.get('data_nascimento'), 'completo'); c5.text_input("Data Nasc.", value=nasc_fmt, disabled=True); c6.text_input("Idade", value=idade_str, disabled=True)
            c7, c8 = st.columns(2); c7.text_input("Nome da Mãe", value=pessoal.get('nome_mae',''), disabled=True); c8.text_input("Nome do Pai", value=pessoal.get('nome_pai',''), disabled=True)
            c9, c10, c11, c12 = st.columns(4); c9.text_input("CNH", value=pessoal.get('cnh',''), disabled=True); c10.text_input("Título Eleitor", value=pessoal.get('titulo_eleitoral',''), disabled=True); c11.text_input("Campanha", value=pessoal.get('campanhas',''), disabled=True); agrupamento_val = dados.get('agrupamentos')[0] if dados.get('agrupamentos') else ""; c12.text_input("Agrupamento", value=agrupamento_val, disabled=True); lista_convs = dados.get('convenios_lista', []); conv_str = ", ".join(lista_convs); st.text_input("Convênios Vinculados", value=conv_str, disabled=True)

        with st.expander("📍 Contatos e Endereço", expanded=False):
            if dados.get('telefones') or dados.get('emails'):
                cols_contato = st.columns(4); idx_c = 0
                for tel in dados.get('telefones', []): val_fmt = v.ValidadorContato.telefone_para_tela(tel['valor']); cols_contato[idx_c % 4].text_input(f"Telefone {idx_c+1}", value=val_fmt, disabled=True); idx_c += 1
                for mail in dados.get('emails', []): cols_contato[idx_c % 4].text_input(f"E-mail {idx_c+1}", value=mail['valor'], disabled=True); idx_c += 1
            else: st.caption("Sem contatos cadastrados.")
            st.divider(); st.markdown("**🏠 Endereço**")
            if dados.get('enderecos'):
                for i, end in enumerate(dados.get('enderecos', [])):
                    ce1, ce2, ce3, ce4, ce5 = st.columns([3, 2, 2, 1, 1.5]); ce1.text_input("Rua", value=end.get('rua',''), key=f"r_{i}", disabled=True); ce2.text_input("Bairro", value=end.get('bairro',''), key=f"b_{i}", disabled=True); ce3.text_input("Cidade", value=end.get('cidade',''), key=f"c_{i}", disabled=True); ce4.text_input("UF", value=end.get('uf',''), key=f"u_{i}", disabled=True); cep_fmt = v.ValidadorContato.cep_para_tela(end.get('cep')); ce5.text_input("CEP", value=cep_fmt, key=f"cp_{i}", disabled=True)
            else: st.caption("Sem endereço cadastrado.")

        if financeiro:
            for (nome_convenio, matricula), grupo in financeiro.items():
                with st.expander(f"📋 {nome_convenio} - Matrícula: {matricula}", expanded=False):
                    st.markdown(f"###### 🏥 Dados do Convênio")
                    dados_esp = grupo.get('dados_convenio')
                    if dados_esp:
                        chaves = [k for k in dados_esp.keys() if k not in ['id', 'cpf', 'matricula', 'nome', 'agrupamento']]
                        cols_fin = st.columns(4)
                        for i, k in enumerate(chaves): cols_fin[i % 4].text_input(k.replace('_', ' ').capitalize(), value=str(dados_esp[k]), disabled=True, key=f"v_dconv_{matricula}_{k}")
                    else: st.info("Sem dados adicionais.")
                    st.divider(); st.markdown(f"###### 📄 Contratos")
                    lista_contratos = grupo.get('contratos', [])
                    if lista_contratos:
                        df_contratos = pd.DataFrame(lista_contratos)
                        colunas_ordenadas = ['numero_contrato', 'data_inicio', 'data_averbacao', 'data_final', 'valor_parcela', 'prazo_aberto', 'prazo_pago', 'prazo_total', 'taxa_juros', 'tipo_taxa', 'valor_contrato_inicial', 'saldo_devedor']
                        cols_existentes = [c for c in colunas_ordenadas if c in df_contratos.columns]
                        df_show = df_contratos[cols_existentes].copy()
                        if 'valor_parcela' in df_show.columns: df_show['valor_parcela'] = df_show['valor_parcela'].apply(v.ValidadorFinanceiro.para_tela)
                        if 'valor_contrato_inicial' in df_show.columns: df_show['valor_contrato_inicial'] = df_show['valor_contrato_inicial'].apply(v.ValidadorFinanceiro.para_tela)
                        if 'saldo_devedor' in df_show.columns: df_show['saldo_devedor'] = df_show['saldo_devedor'].apply(v.ValidadorFinanceiro.para_tela)
                        for col_date in ['data_inicio', 'data_averbacao', 'data_final']:
                            if col_date in df_show.columns: df_show[col_date] = df_show[col_date].apply(v.ValidadorData.para_tela)
                        
                        renomear = {'numero_contrato': 'Nº Contrato', 'valor_parcela': 'Vlr. Parc.', 'prazo_aberto': 'Pz Aberto', 'prazo_pago': 'Pz Pago', 'prazo_total': 'Pz Total', 'taxa_juros': 'Taxa %', 'tipo_taxa': 'Tipo Taxa', 'valor_contrato_inicial': 'Vlr. Inicial', 'saldo_devedor': 'Saldo Dev.', 'data_inicio': 'Dt Início', 'data_averbacao': 'Dt Averb.', 'data_final': 'Dt Final'}
                        df_show.rename(columns=renomear, inplace=True)
                        st.dataframe(df_show, use_container_width=True, hide_index=True)
                    else: st.caption("Nenhum contrato ativo.")
        else:
            with st.expander("📋 Convênios e Contratos", expanded=False): st.info("Nenhum convênio ou contrato localizado.")

    with c_lateral:
        with st.container(border=True):
            st.markdown("###### CONEXÃO")
            key_recibo = f"recibo_atualizacao_{cpf}"
            if st.session_state.get(key_recibo):
                st.markdown(st.session_state[key_recibo], unsafe_allow_html=True)
                if st.button("❌ Fechar", key=f"btn_close_{cpf}", use_container_width=True): del st.session_state[key_recibo]; st.rerun()
            else:
                if st.button("🔄 Atualizar", key=f"btn_upd_{cpf}", use_container_width=True, help="Consultar e atualizar dados (Custo Aplicável)"):
                    with st.spinner("Atualizando..."): sucesso, html_result = processar_atualizacao_cadastral(cpf, pessoal.get('nome', 'Cliente')); st.session_state[key_recibo] = html_result; st.rerun()

def tela_pesquisa():
    st.markdown("#### 🔍 Buscar Cliente")
    tab1, tab2 = st.tabs(["Pesquisa Rápida", "Pesquisa Completa"])
    with tab1:
        c_input, c_btn_search, c_btn_clear = st.columns([0.8, 0.1, 0.1])
        termo = c_input.text_input("Digite CPF, Nome ou Telefone", label_visibility="collapsed", key="search_term_input")
        if c_btn_search.button("🔍", type="primary", use_container_width=True, help="Buscar"):
            if len(termo) < 3: st.warning("Mínimo 3 caracteres.")
            else:
                apenas_num = v.ValidadorDocumentos.limpar_numero(termo)
                res = buscar_cliente_rapida(termo)
                st.session_state['resultados_pesquisa'] = res
                st.session_state['pagina_atual'] = 1  # Reset página
                if not res: st.warning("Nada encontrado.")
        if c_btn_clear.button("🧹", type="secondary", use_container_width=True, help="Limpar"):
            st.session_state['resultados_pesquisa'] = []
            st.session_state['pagina_atual'] = 1
            st.rerun()

    with tab2:
        st.markdown("Configure os filtros abaixo para uma busca detalhada.")
        c_clear, c_group, c_vazio = st.columns([1.5, 1.5, 4])
        if c_clear.button("🧹 Limpar Filtros", type="secondary", use_container_width=True):
            st.session_state['resultados_pesquisa'] = []
            st.session_state['pagina_atual'] = 1
            for key in list(st.session_state.keys()):
                if "val_input_" in key or "op_input_" in key: del st.session_state[key]
            st.rerun()
        if c_group.button("📂 Agrupamentos", use_container_width=True): modal_agrupamentos()
        st.write("") 
        with st.form("form_pesquisa_completa"):
            filtros_para_query = []
            tab_pessoais, tab_contatos, tab_outros = st.tabs(["👤 Dados Pessoais", "📍 Contatos e Endereço", "📋 Filiação e Sistema"])
            distribuicao_abas = [(tab_pessoais, ["Dados Pessoais"]), (tab_contatos, ["Contatos", "Endereço"]), (tab_outros, ["Filiação e Sistema"])]
            for aba_atual, lista_grupos in distribuicao_abas:
                with aba_atual:
                    for grupo in lista_grupos:
                        campos = MAPA_CAMPOS_PESQUISA.get(grupo)
                        if not campos: continue
                        if len(lista_grupos) > 1: st.markdown(f"###### {grupo}")
                        cols_visual = st.columns(3, gap="small")
                        for i, (nome_campo, config) in enumerate(campos.items()):
                            with cols_visual[i % 3]:
                                safe_key = f"{config['col']}_{config.get('table','')}".replace(".", "_").replace(" ", "_")
                                c_tit, c_op = st.columns([2, 1.5])
                                c_tit.markdown(f"**{nome_campo}**")
                                tipo_ops = 'texto'
                                if config['tipo'] == 'data': tipo_ops = 'data'
                                elif config['tipo'] == 'numero_calculado': tipo_ops = 'numero'
                                opcoes_ops = list(OPERADORES_SQL[tipo_ops].keys())
                                op_key = f"op_input_{safe_key}"
                                operador_escolhido = c_op.selectbox("Op", opcoes_ops, key=op_key, label_visibility="collapsed")
                                sql_op_code = OPERADORES_SQL[tipo_ops][operador_escolhido]['sql']
                                val_key = f"val_input_{safe_key}"
                                valor_final = None
                                if "IS NULL" in sql_op_code or "IS NOT NULL" in sql_op_code: valor_final = "ignore"
                                elif config['tipo'] == 'data':
                                    cd1, cd2 = st.columns(2)
                                    d1 = cd1.date_input("De", value=None, key=f"d1_{val_key}", format="DD/MM/YYYY", label_visibility="collapsed")
                                    d2 = cd2.date_input("Até", value=None, key=f"d2_{val_key}", format="DD/MM/YYYY", label_visibility="collapsed")
                                    if d1: valor_final = [str(d1), str(d2) if d2 else str(d1)]
                                elif config['tipo'] == 'numero_calculado':
                                    if operador_escolhido == "Entre (><)":
                                        cn1, cn2 = st.columns(2)
                                        n1 = cn1.number_input("De", step=1, key=f"n1_{val_key}", label_visibility="collapsed")
                                        n2 = cn2.number_input("Até", step=1, key=f"n2_{val_key}", label_visibility="collapsed")
                                        if n1 != 0 or n2 != 0: valor_final = [n1, n2]
                                    else:
                                        n_val = st.number_input("Valor", step=1, key=f"num_{val_key}", label_visibility="collapsed")
                                        if n_val != 0: valor_final = n_val
                                else:
                                    txt_val = st.text_input("Valor", key=f"txt_{val_key}", placeholder="Digite...", label_visibility="collapsed")
                                    if txt_val: valor_final = txt_val
                                if valor_final is not None:
                                    filtros_para_query.append({'col': config['col'], 'op': sql_op_code, 'mask': OPERADORES_SQL[tipo_ops][operador_escolhido]['mask'], 'val': valor_final, 'tipo': config['tipo'], 'table': config.get('table')})
            st.write("")
            if st.form_submit_button("🚀 APLICAR FILTROS", type="primary", use_container_width=True):
                if filtros_para_query:
                    res_completa = buscar_cliente_dinamica(filtros_para_query)
                    st.session_state['resultados_pesquisa'] = res_completa
                    st.session_state['pagina_atual'] = 1
                    if not res_completa: st.toast("Nenhum registro encontrado.", icon="⚠️")
                else: st.warning("Preencha pelo menos um campo para filtrar.")

    resultados = st.session_state.get('resultados_pesquisa', [])
    if resultados:
        st.divider()
        items_por_pagina = 10
        total_items = len(resultados)
        total_paginas = math.ceil(total_items / items_por_pagina)
        
        if 'pagina_atual' not in st.session_state: st.session_state['pagina_atual'] = 1
        pg = st.session_state['pagina_atual']
        
        start_idx = (pg - 1) * items_por_pagina
        end_idx = start_idx + items_por_pagina
        batch = resultados[start_idx:end_idx]
        
        st.markdown(f"**Resultados: {total_items} (Página {pg} de {total_paginas})**")
        
        cols = st.columns([1, 4, 3, 2])
        cols[0].write("**ID**")
        cols[1].write("**Nome**")
        cols[2].write("**CPF**")
        cols[3].write("**Ação**")
        
        for row in batch:
            c = st.columns([1, 4, 3, 2])
            c[0].write(str(row[0]))
            c[1].write(row[1])
            c[2].write(v.ValidadorDocumentos.cpf_para_tela(row[2]))
            if c[3].button("📂 Abrir", key=f"abrir_{row[0]}"):
                st.session_state['cliente_ativo_cpf'] = row[2]
                st.session_state['modo_visualizacao'] = 'visualizar'
                st.rerun()
        
        if total_paginas > 1:
            st.write("---")
            c_nav = st.columns([1, 1, 3, 1, 1])
            if c_nav[0].button("⏪", disabled=(pg==1), help="Primeira"):
                st.session_state['pagina_atual'] = 1; st.rerun()
            if c_nav[1].button("◀️", disabled=(pg==1), help="Anterior"):
                st.session_state['pagina_atual'] -= 1; st.rerun()
            
            with c_nav[2]:
                st.markdown(f"<div style='text-align:center; padding-top:5px;'><b>{pg} / {total_paginas}</b></div>", unsafe_allow_html=True)
            
            if c_nav[3].button("▶️", disabled=(pg==total_paginas), help="Próxima"):
                st.session_state['pagina_atual'] += 1; st.rerun()
            if c_nav[4].button("⏩", disabled=(pg==total_paginas), help="Última"):
                st.session_state['pagina_atual'] = total_paginas; st.rerun()

def app_cadastro():
    st.markdown("""
        <style>
        .stButton > button {
            padding: 0px 5px !important; 
            font-size: 0.85em !important;
            line-height: 1.2 !important;
            min-height: 0px !important;
            height: auto !important;
            border-radius: 4px;
        }
        .stButton > button:hover {
            background-color: #ffe6e6 !important;
            color: black !important;
            border-color: #ffcccc !important;
        }
        thead tr th {
            background-color: #ffe6e6 !important;
            color: black !important;
        }
        </style>
    """, unsafe_allow_html=True)

    if 'modo_visualizacao' not in st.session_state: st.session_state['modo_visualizacao'] = None
    if st.session_state['modo_visualizacao'] == 'visualizar':
        if st.session_state.get('cliente_ativo_cpf'): tela_ficha_cliente(st.session_state['cliente_ativo_cpf'])
        else:
            st.session_state['modo_visualizacao'] = None
            st.rerun()
    elif st.session_state['modo_visualizacao'] == 'novo': tela_ficha_cliente(None, modo='novo')
    else: tela_pesquisa()

if __name__ == "__main__":
    if get_pool(): app_cadastro()
    else: st.error("🚫 Falha Crítica: Não foi possível conectar ao Banco de Dados. Verifique o arquivo 'conexao.py'.")