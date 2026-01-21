import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import pool, sql
from datetime import datetime, date
import time
import contextlib

# Tenta importar a conexão
try:
    import conexao
except ImportError:
    conexao = None

# IMPORTA SEU NOVO MÓDULO DE VALIDAÇÃO
import modulo_validadores as v

# ==============================================================================
# 1. CONFIGURAÇÃO DE PERFORMANCE (CONNECTION POOL)
# ==============================================================================

@st.cache_resource
def get_pool():
    if not conexao: return None
    try:
        # Cria um pool de 1 a 20 conexões simultâneas
        return psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=20,
            host=conexao.host, port=conexao.port,
            database=conexao.database, user=conexao.user, password=conexao.password
        )
    except Exception as e:
        st.error(f"Erro fatal ao conectar no banco (Pool): {e}")
        return None

@contextlib.contextmanager
def get_db_connection():
    """Gerenciador de contexto para pegar e devolver conexões ao pool automaticamente"""
    pool_obj = get_pool()
    if not pool_obj:
        yield None
        return
    
    conn = pool_obj.getconn()
    try:
        yield conn
    finally:
        pool_obj.putconn(conn)

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

# Funções auxiliares antigas foram removidas pois agora usamos v.Validador...

# ==============================================================================
# 3. FUNÇÕES DE BUSCA (LEITURA)
# ==============================================================================

@st.cache_data(ttl=300) # Cache curto de 5 min
def buscar_relacao_auxiliar(tipo):
    with get_db_connection() as conn:
        if not conn: return [], []
        
        dados = []
        colunas = []
        try:
            with conn.cursor() as cur:
                if tipo == 'Importação':
                    cur.execute("""
                        SELECT id, nome_arquivo, TO_CHAR(data_importacao, 'DD/MM/YYYY HH24:MI') as data, 
                               qtd_novos, qtd_atualizados 
                        FROM sistema_consulta.sistema_consulta_importacao 
                        ORDER BY id DESC LIMIT 100
                    """)
                    dados = cur.fetchall()
                    colunas = ['ID', 'Nome do Arquivo', 'Data', 'Novos', 'Atualizados']
                elif tipo == 'Agrupamento':
                    cur.execute("""
                        SELECT agrupamento, COUNT(*) 
                        FROM sistema_consulta.sistema_consulta_dados_cadastrais_agrupamento_cpf 
                        GROUP BY agrupamento ORDER BY 2 DESC
                    """)
                    dados = cur.fetchall()
                    colunas = ['Nome Agrupamento', 'Qtd CPFs']
                elif tipo == 'Campanha':
                    cur.execute("""
                        SELECT campanhas, COUNT(*) 
                        FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf 
                        WHERE campanhas IS NOT NULL AND campanhas <> '' 
                        GROUP BY campanhas ORDER BY 2 DESC
                    """)
                    dados = cur.fetchall()
                    colunas = ['Nome Campanha', 'Qtd CPFs']
            return dados, colunas
        except Exception as e:
            st.error(f"Erro ao buscar auxiliar: {e}")
            return [], []

def buscar_cliente_rapida(termo):
    """Busca otimizada com limpeza de dados via Validador"""
    with get_db_connection() as conn:
        if not conn: return []
        
        # Limpa entrada (remove pontos/traços) para busca mais eficiente
        termo_limpo = v.ValidadorDocumentos.limpar_numero(termo)
        param_like = f"%{termo.strip()}%"
        
        query = """
            SELECT t.id, t.nome, t.cpf, t.identidade 
            FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf t
            WHERE 
                t.nome ILIKE %s OR 
                t.cpf ILIKE %s OR
                t.identidade ILIKE %s OR
                t.nome_pai ILIKE %s OR
                t.campanhas ILIKE %s OR
                t.id_importacao ILIKE %s OR
                EXISTS (SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = t.cpf AND telefone ILIKE %s)
            LIMIT 30
        """
        
        try:
            with conn.cursor() as cur:
                # Se for número puro e tamanho de CPF (11), tenta match exato no CPF (muito mais rápido)
                if termo_limpo and len(termo_limpo) == 11:
                    cur.execute(query + " OR t.cpf = %s", (
                        param_like, param_like, param_like, param_like, 
                        param_like, param_like, param_like, termo_limpo
                    ))
                else:
                    cur.execute(query, (
                        param_like, param_like, param_like, param_like, 
                        param_like, param_like, param_like
                    ))
                return cur.fetchall()
        except Exception:
            return []

def buscar_cliente_dinamica(filtros_aplicados):
    with get_db_connection() as conn:
        if not conn: return []

        base_query = """
            SELECT DISTINCT t.id, t.nome, t.cpf, t.identidade, t.id_importacao
            FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf t
        """
        where_clauses = ["1=1"]
        params = []

        for filtro in filtros_aplicados:
            coluna = filtro['col']
            operador_sql = filtro['op']
            tipo_dado = filtro['tipo']
            
            # Lógica complexa original mantida
            if tipo_dado == 'texto_vinculado':
                tabela_satelite = filtro['table']
                sub_where = f"{coluna} {operador_sql} %s"
                val_final = filtro['mask'].format(filtro['val']) if '{}' in filtro['mask'] else filtro['val']
                
                if "IS NULL" in operador_sql or "IS NOT NULL" in operador_sql:
                     exists_clause = f"EXISTS (SELECT 1 FROM sistema_consulta.{tabela_satelite} WHERE cpf = t.cpf AND {coluna} {operador_sql})"
                     where_clauses.append(exists_clause)
                else:
                    exists_clause = f"EXISTS (SELECT 1 FROM sistema_consulta.{tabela_satelite} WHERE cpf = t.cpf AND {sub_where})"
                    where_clauses.append(exists_clause)
                    params.append(val_final)

            elif tipo_dado == 'numero_calculado':
                if "IS NULL" in operador_sql:
                     where_clauses.append(f"t.data_nascimento IS NULL")
                else:
                    if operador_sql == 'BETWEEN':
                        where_clauses.append(f"EXTRACT(YEAR FROM age(t.data_nascimento)) BETWEEN %s AND %s")
                        params.append(filtro['val'][0])
                        params.append(filtro['val'][1])
                    else:
                        where_clauses.append(f"EXTRACT(YEAR FROM age(t.data_nascimento)) {operador_sql} %s")
                        params.append(filtro['val'])

            else:
                if "IS NULL" in operador_sql or "IS NOT NULL" in operador_sql:
                    if "NOT" in operador_sql:
                        where_clauses.append(f"({coluna} IS NOT NULL AND {coluna} != '')")
                    else:
                        where_clauses.append(f"({coluna} IS NULL OR {coluna} = '')")
                elif operador_sql == 'BETWEEN' and tipo_dado == 'data':
                    where_clauses.append(f"{coluna} BETWEEN %s AND %s")
                    # Converter datas para SQL format se necessário, mas o validador já deve ter entregue pronto
                    params.append(filtro['val'][0])
                    params.append(filtro['val'][1])
                else:
                    if filtro['val']:
                        valores = str(filtro['val']).split(';')
                        ors = []
                        for v_str in valores:
                            v_str = v_str.strip()
                            if v_str:
                                ors.append(f"{coluna} {operador_sql} %s")
                                val_final = filtro['mask'].format(v_str) if '{}' in filtro['mask'] else v_str
                                params.append(val_final)
                        if ors:
                            where_clauses.append(f"({' OR '.join(ors)})")

        full_query = f"{base_query} WHERE {' AND '.join(where_clauses)} LIMIT 50"

        try:
            with conn.cursor() as cur:
                cur.execute(full_query, tuple(params))
                return cur.fetchall()
        except Exception as e:
            st.error(f"Erro SQL: {e}")
            return []

def carregar_dados_cliente_completo(cpf):
    with get_db_connection() as conn:
        if not conn: return {}
        dados = {}
        try:
            with conn.cursor() as cur:
                # 1. Dados Pessoais
                cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf WHERE cpf = %s", (cpf,))
                cols_pessoais = [desc[0] for desc in cur.description]
                row_pessoais = cur.fetchone()
                
                if row_pessoais:
                    d_pessoal = dict(zip(cols_pessoais, row_pessoais))
                    # Limpa nulls para string vazia para evitar erro no st.text_input
                    for k, v in d_pessoal.items():
                        if v is None and k != 'data_nascimento': d_pessoal[k] = ""
                    dados['pessoal'] = d_pessoal
                else:
                    dados['pessoal'] = {}

                # 2. Dados CLT (Mantido conforme original)
                try:
                    cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_clt WHERE cpf = %s LIMIT 1", (cpf,))
                    cols_clt = [desc[0] for desc in cur.description]
                    row_clt = cur.fetchone()
                    if row_clt:
                        d_clt = dict(zip(cols_clt, row_clt))
                        for k, v in d_clt.items():
                            if v is None and 'data' not in k: d_clt[k] = ""
                        dados['clt'] = d_clt
                except:
                    dados['clt'] = {}

                # 3. Listas
                cur.execute("SELECT id, telefone FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s ORDER BY id", (cpf,))
                dados['telefones'] = [{'id': r[0], 'valor': r[1] or ""} for r in cur.fetchall()]

                cur.execute("SELECT id, email FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf = %s ORDER BY id", (cpf,))
                dados['emails'] = [{'id': r[0], 'valor': r[1] or ""} for r in cur.fetchall()]

                cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco WHERE cpf = %s ORDER BY id", (cpf,))
                cols_end = [desc[0] for desc in cur.description]
                dados['enderecos'] = []
                for r in cur.fetchall():
                    d_end = dict(zip(cols_end, r))
                    for k, v in d_end.items():
                        if v is None: d_end[k] = ""
                    dados['enderecos'].append(d_end)

                # Agrupamentos
                cur.execute("SELECT agrupamento FROM sistema_consulta.sistema_consulta_dados_cadastrais_agrupamento_cpf WHERE cpf = %s", (cpf,))
                dados['agrupamentos'] = [r[0] for r in cur.fetchall() if r[0]]

                # Convênios
                try:
                    cur.execute("SELECT convenio FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s", (cpf,))
                    dados['convenios_lista'] = [r[0] for r in cur.fetchall() if r[0]]
                except:
                    dados['convenios_lista'] = []
                
        except Exception as e:
            st.error(f"Erro ao carregar cliente: {e}")
        return dados

# --- FUNÇÕES FINANCEIRAS (Lógica Complexa Mantida) ---

def listar_contratos_cliente(cpf):
    with get_db_connection() as conn:
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT matricula, convenio 
                    FROM sistema_consulta.sistema_consulta_contrato 
                    WHERE cpf = %s AND matricula IS NOT NULL AND convenio IS NOT NULL
                """, (cpf,))
                return cur.fetchall()
        except Exception:
            return []

def buscar_tabela_por_convenio(nome_convenio):
    with get_db_connection() as conn:
        if not conn: return None
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT tabela_referencia FROM sistema_consulta.sistema_consulta_convenio_tipo WHERE nome_convenio = %s", (nome_convenio,))
                res = cur.fetchone()
                return res[0] if res else None
        except Exception:
            return None

@st.cache_data(ttl=3600) # OTIMIZAÇÃO: Cache para não consultar schema toda hora
def listar_colunas_tabela(nome_tabela):
    with get_db_connection() as conn:
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_schema = 'sistema_consulta' AND table_name = %s
                """, (nome_tabela.replace('sistema_consulta.', ''),))
                return [row[0] for row in cur.fetchall()]
        except Exception:
            return []

def listar_tipos_convenio_disponiveis():
    with get_db_connection() as conn:
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT nome_convenio FROM sistema_consulta.sistema_consulta_convenio_tipo ORDER BY nome_convenio")
                return [r[0] for r in cur.fetchall()]
        except Exception:
            return []

def buscar_hierarquia_financeira(cpf):
    """
    Função complexa que busca contratos e varre tabelas dinâmicas.
    Otimizada com Connection Pool para reuso da conexão.
    """
    with get_db_connection() as conn:
        if not conn: return {}

        estrutura = {}
        try:
            with conn.cursor() as cur:
                # 1. Contratos Base
                cur.execute("""
                    SELECT * FROM sistema_consulta.sistema_consulta_contrato 
                    WHERE cpf = %s ORDER BY convenio, matricula, data_inicio DESC
                """, (cpf,))
                cols_contrato = [desc[0] for desc in cur.description]
                rows_contratos = cur.fetchall()

                if not rows_contratos: return {}

                # 2. Mapeamento de Convênios
                mapa_convenio_tabela = {} 
                for row in rows_contratos:
                    d_contrato = dict(zip(cols_contrato, row))
                    for k, val in d_contrato.items():
                        if val is None: d_contrato[k] = ""
                    
                    nome_conv = d_contrato.get('convenio') or 'DESCONHECIDO'
                    num_matr = d_contrato.get('matricula') or 'S/N'
                    chave = (nome_conv, num_matr)

                    if chave not in estrutura:
                        estrutura[chave] = {
                            'contratos': [], 'dados_convenio': {}, 'tabela_ref': None
                        }
                    estrutura[chave]['contratos'].append(d_contrato)

                # Busca tabelas satélites
                convenios_unicos = list(set([k[0] for k in estrutura.keys()]))
                if convenios_unicos:
                    cur.execute("""
                        SELECT nome_convenio, tabela_referencia 
                        FROM sistema_consulta.sistema_consulta_convenio_tipo 
                        WHERE nome_convenio = ANY(%s)
                    """, (convenios_unicos,))
                    for r in cur.fetchall():
                        mapa_convenio_tabela[r[0]] = r[1]

                # 3. Varredura Dinâmica
                for (nome_conv, num_matr), dados_grupo in estrutura.items():
                    tabela_ref = mapa_convenio_tabela.get(nome_conv)
                    if tabela_ref:
                        dados_grupo['tabela_ref'] = tabela_ref
                        try:
                            # Verifica colunas da tabela dinâmica
                            cur.execute(sql.SQL("""
                                SELECT column_name FROM information_schema.columns 
                                WHERE table_schema = 'sistema_consulta' AND table_name = %s
                            """), (tabela_ref.replace('sistema_consulta.', ''),))
                            colunas_tabela_ref = [c[0] for c in cur.fetchall()]
                            
                            tem_matricula = 'matricula' in colunas_tabela_ref
                            
                            query_dinamica = sql.SQL("SELECT * FROM sistema_consulta.{} WHERE cpf = %s").format(sql.Identifier(tabela_ref.replace('sistema_consulta.', '')))
                            params_dinamica = [cpf]
                            
                            if tem_matricula:
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
                        except Exception:
                            pass

        except Exception as e:
            st.error(f"Erro na busca financeira: {e}")
            
        return estrutura

# ==============================================================================
# 4. FUNÇÕES DE ESCRITA (CRUD)
# ==============================================================================

def salvar_novo_cliente(dados_form):
    """
    Refatorado: Usa modulo_validadores para proteger o banco.
    """
    # Validações prévias
    cpf_sql = v.ValidadorDocumentos.cpf_para_sql(dados_form.get('cpf'))
    if not cpf_sql:
        st.error("CPF Inválido! Verifique os dígitos.")
        return False
        
    nasc_sql = v.ValidadorData.para_sql(dados_form.get('data_nascimento'))
    if not nasc_sql:
        st.error("Data de Nascimento inválida (deve ser 1900-2050).")
        return False

    dados_limpos = {
        "nome": str(dados_form.get('nome') or "").upper().strip(),
        "cpf": cpf_sql,
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
                
                # Garante na tabela indice
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf) VALUES (%s) ON CONFLICT DO NOTHING", (cpf_sql,))
                
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")
            return False

def inserir_dado_extra(tipo, cpf, dados):
    """Refatorado com Validadores"""
    with get_db_connection() as conn:
        if not conn: return "erro"
        try:
            with conn.cursor() as cur:
                if tipo == "DadosDinâmicos":
                    # Lógica original para criar novo vínculo
                    if dados.get('_criar_vinculo'):
                        new_mat = dados.get('matricula')
                        new_conv = dados.get('convenio')
                        cur.execute("INSERT INTO sistema_consulta.sistema_consulta_contrato (cpf, matricula, convenio) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (cpf, new_mat, new_conv))
                        cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio) VALUES (%s, %s) ON CONFLICT DO NOTHING", (cpf, new_conv))

                    tabela_alvo = dados.get('_tabela')
                    campos_dados = {k: val for k, val in dados.items() if not k.startswith('_')}
                    
                    if not tabela_alvo or not campos_dados: return "erro"

                    colunas = list(campos_dados.keys())
                    # Tratamento simples para valores vazios
                    valores = [val if val != "" else None for val in campos_dados.values()]
                    
                    query = sql.SQL("INSERT INTO sistema_consulta.{} ({}) VALUES ({})").format(
                        sql.Identifier(tabela_alvo.replace('sistema_consulta.', '')),
                        sql.SQL(', ').join(map(sql.Identifier, colunas)),
                        sql.SQL(', ').join(sql.Placeholder() * len(colunas))
                    )
                    cur.execute(query, valores)

                elif tipo == "Contrato":
                    # Valida Moedas e Datas
                    dt_inicio = v.ValidadorData.para_sql(dados.get('data_inicio'))
                    dt_final = v.ValidadorData.para_sql(dados.get('data_final'))
                    
                    campos = [
                        "cpf", "matricula", "convenio", "numero_contrato", 
                        "valor_parcela", "prazo_total", "prazo_aberto", 
                        "prazo_pago", "saldo_devedor", "taxa_juros", 
                        "valor_contrato_inicial", "data_inicio", "data_final"
                    ]
                    
                    valores = [
                        cpf,
                        str(dados.get('matricula') or "").strip(),
                        str(dados.get('convenio') or "").strip(),
                        str(dados.get('numero_contrato') or "").strip(),
                        v.ValidadorFinanceiro.para_sql(dados.get('valor_parcela')),
                        str(dados.get('prazo_total') or "").strip(),
                        str(dados.get('prazo_aberto') or "").strip(),
                        str(dados.get('prazo_pago') or "").strip(),
                        v.ValidadorFinanceiro.para_sql(dados.get('saldo_devedor')),
                        v.ValidadorFinanceiro.para_sql(dados.get('taxa_juros')),
                        v.ValidadorFinanceiro.para_sql(dados.get('valor_contrato_inicial')),
                        dt_inicio,
                        dt_final
                    ]
                    
                    placeholders = ", ".join(["%s"] * len(valores))
                    query = f"INSERT INTO sistema_consulta.sistema_consulta_contrato ({', '.join(campos)}) VALUES ({placeholders})"
                    cur.execute(query, valores)

                elif tipo == "Telefone":
                    val = v.ValidadorContato.telefone_para_sql(dados.get('valor'))
                    if not val:
                        st.error("Telefone inválido (formato incorreto ou DDD inexistente).")
                        return "erro"
                        
                    cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s AND telefone = %s", (cpf, val))
                    if cur.fetchone(): return "duplicado"
                    cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone) VALUES (%s, %s)", (cpf, val))
                
                elif tipo == "E-mail":
                    val = str(dados.get('valor')).strip()
                    if not v.ValidadorContato.email_valido(val):
                        st.error("E-mail inválido.")
                        return "erro"
                        
                    cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf = %s AND email = %s", (cpf, val))
                    if cur.fetchone(): return "duplicado"
                    cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email) VALUES (%s, %s)", (cpf, val))
                
                elif tipo == "Endereço":
                    cur.execute("""
                        INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_endereco 
                        (cpf, cep, rua, bairro, cidade, uf) VALUES (%s, %s, %s, %s, %s, %s)
                    """, (cpf, dados.get('cep'), dados.get('rua'), dados.get('bairro'), dados.get('cidade'), dados.get('uf')))
                
                elif tipo == "Convênio (Cadastro)":
                    val = str(dados.get('valor')).strip()
                    cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s AND convenio = %s", (cpf, val))
                    if cur.fetchone(): return "duplicado"
                    cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio) VALUES (%s, %s)", (cpf, val))
                
                conn.commit()
                return "sucesso"
        except Exception as e:
            st.error(f"Erro insert extra: {e}")
            return "erro"

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
                    query = sql.SQL("UPDATE sistema_consulta.{} SET {} WHERE id = %s").format(
                        sql.Identifier(tabela.replace('sistema_consulta.', '')),
                        sql.SQL(', ').join(set_clauses)
                    )
                    cur.execute(query, values)
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Erro update dinâmico: {e}")
            return False

def atualizar_dados_cliente_lote(cpf, dados_editados, dados_dinamicos=None):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                # 1. Dados Pessoais
                pessoal = dados_editados['pessoal']
                # Validar datas no update também
                nasc_sql = v.ValidadorData.para_sql(pessoal['data_nascimento'])
                
                cur.execute("""
                    UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_cpf
                    SET nome = %s, data_nascimento = %s, identidade = %s, 
                        sexo = %s, cnh = %s, titulo_eleitoral = %s, nome_mae = %s,
                        nome_pai = %s, campanhas = %s
                    WHERE cpf = %s
                """, (
                    str(pessoal['nome']).strip(), nasc_sql, str(pessoal['identidade']).strip(),
                    str(pessoal['sexo']).strip(), str(pessoal['cnh']).strip(), 
                    str(pessoal['titulo_eleitoral']).strip(), str(pessoal['nome_mae']).strip(),
                    str(pessoal.get('nome_pai')).strip(), str(pessoal.get('campanhas')).strip(),
                    cpf
                ))
                
                # 2. Telefones (Update/Delete)
                for item in dados_editados.get('telefones', []):
                    val = v.ValidadorContato.telefone_para_sql(item['valor'])
                    if not val:
                        cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE id = %s", (item['id'],))
                    else:
                        cur.execute("UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_telefone SET telefone = %s WHERE id = %s", (val, item['id']))
                
                # 3. Emails (Update/Delete)
                for item in dados_editados.get('emails', []):
                    val = str(item['valor']).strip()
                    if not val:
                        cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE id = %s", (item['id'],))
                    else:
                        cur.execute("UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_email SET email = %s WHERE id = %s", (val, item['id']))
                
                conn.commit()
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}")
            return False

    if dados_dinamicos:
        atualizar_dados_dinamicos(dados_dinamicos)
        
    return True

def excluir_cliente_total(cpf):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                tabelas = [
                    "sistema_consulta_dados_cadastrais_telefone", "sistema_consulta_dados_cadastrais_email",
                    "sistema_consulta_dados_cadastrais_endereco", "sistema_consulta_dados_cadastrais_convenio",
                    "sistema_consulta_dados_cadastrais_agrupamento_cpf", "sistema_consulta_dados_ctt",
                    "sistema_consulta_dados_clt", "sistema_consulta_contrato", 
                    "sistema_consulta_dados_cadastrais_cpf", "sistema_consulta_cpf"
                ]
                for t in tabelas:
                    cur.execute(sql.SQL("DELETE FROM sistema_consulta.{} WHERE cpf = %s").format(sql.Identifier(t.replace('sistema_consulta.', ''))), (cpf,))
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Erro ao excluir: {e}")
            return False

# ==============================================================================
# 5. INTERFACE DO USUÁRIO (DIALOGS E TELAS)
# ==============================================================================

@st.dialog("➕ Inserir Dados Extras")
def modal_inserir_dados(cpf, nome_cliente):
    st.write(f"Cliente: **{nome_cliente}**")
    tipo_insercao = st.selectbox("Selecione o Tipo", ["Telefone", "E-mail", "Endereço", "Contrato", "Dados de Convênio", "Convênio (Cadastro)"])
    
    with st.form("form_insercao_modal"):
        dados_submit = {}
        
        if tipo_insercao == "Dados de Convênio":
            lista_contratos = listar_contratos_cliente(cpf)
            opcoes_matr = [f"{m} - {c}" for m, c in lista_contratos]
            opcoes_matr.append("➕ Cadastrar Novo Convênio/Matrícula")
            
            selecao = st.selectbox("Selecione a Matrícula", options=opcoes_matr)
            criar_novo = False
            matricula_sel = None
            convenio_sel = None

            if selecao == "➕ Cadastrar Novo Convênio/Matrícula":
                st.info("Informe os dados para criar um novo vínculo.")
                c_new1, c_new2 = st.columns(2)
                lista_tipos = listar_tipos_convenio_disponiveis()
                sel_tipo = c_new1.selectbox("Selecione o Convênio", options=["(Selecione)"] + lista_tipos)
                matricula_sel = c_new2.text_input("Nova Matrícula")
                if sel_tipo and sel_tipo != "(Selecione)" and matricula_sel:
                    convenio_sel = sel_tipo
                    criar_novo = True
            elif selecao:
                matricula_sel, convenio_sel = selecao.split(" - ", 1)
            
            if matricula_sel and convenio_sel:
                tabela_alvo = buscar_tabela_por_convenio(convenio_sel)
                if tabela_alvo:
                    colunas = listar_colunas_tabela(tabela_alvo)
                    if not colunas:
                        st.error("Tabela sem colunas.")
                    else:
                        dados_submit['_tabela'] = tabela_alvo
                        dados_submit['cpf'] = cpf
                        dados_submit['matricula'] = matricula_sel
                        if criar_novo:
                            dados_submit['_criar_vinculo'] = True
                            dados_submit['convenio'] = convenio_sel
                        
                        cols_form = st.columns(2)
                        idx = 0
                        for col in colunas:
                            if col not in ['id', 'cpf', 'matricula', 'nome', 'agrupamento']:
                                with cols_form[idx % 2]:
                                    if 'data' in col.lower():
                                        dados_submit[col] = st.date_input(col.replace('_', ' ').capitalize(), value=None, format="DD/MM/YYYY")
                                    else:
                                        dados_submit[col] = st.text_input(col.replace('_', ' ').capitalize())
                                idx += 1

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

        elif tipo_insercao == "Telefone":
            dados_submit['valor'] = st.text_input("Novo Telefone", placeholder="(00) 00000-0000")
        elif tipo_insercao == "E-mail":
            dados_submit['valor'] = st.text_input("Novo E-mail")
        elif tipo_insercao == "Endereço":
            dados_submit['cep'] = st.text_input("CEP")
            dados_submit['rua'] = st.text_input("Rua")
            dados_submit['bairro'] = st.text_input("Bairro")
            dados_submit['cidade'] = st.text_input("Cidade")
            dados_submit['uf'] = st.text_input("UF", max_chars=2)
        elif tipo_insercao == "Convênio (Cadastro)":
            dados_submit['valor'] = st.text_input("Nome do Convênio")
        
        if st.form_submit_button("✅ Salvar Inclusão"):
            tipo_envio = "DadosDinâmicos" if tipo_insercao == "Dados de Convênio" else tipo_insercao
            status = inserir_dado_extra(tipo_envio, cpf, dados_submit)
            if status == "sucesso":
                st.success(f"{tipo_insercao} inserido com sucesso!")
                time.sleep(1)
                st.rerun()
            elif status == "duplicado":
                st.warning("Dado já existente!")
            else:
                st.error("Erro ao inserir.")

@st.dialog("📂 Visualizador de Agrupamentos")
def modal_agrupamentos():
    st.markdown("### Selecione o tipo para visualizar")
    tipo = st.selectbox("Tipo:", ["Importação", "Agrupamento", "Campanha"])
    if tipo:
        with st.spinner("Carregando dados..."):
            dados, colunas = buscar_relacao_auxiliar(tipo)
        if dados:
            st.dataframe(pd.DataFrame(dados, columns=colunas), use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum registro encontrado.")

@st.dialog("⚠️ Confirmar Exclusão")
def modal_confirmar_exclusao(cpf):
    st.warning("Tem certeza? Essa ação não pode ser desfeita.")
    if st.button("🚨 SIM, EXCLUIR DEFINITIVAMENTE", type="primary"):
        if excluir_cliente_total(cpf):
            st.success("Cliente excluído!")
            st.session_state['cliente_ativo_cpf'] = None
            st.session_state['modo_visualizacao'] = None
            st.session_state['resultados_pesquisa'] = []
            time.sleep(1.5)
            st.rerun()

def tela_ficha_cliente(cpf, modo='visualizar'):
    if 'modo_edicao' not in st.session_state:
        st.session_state['modo_edicao'] = False

    if st.button("⬅️ Voltar", use_container_width=True):
        st.session_state['cliente_ativo_cpf'] = None
        st.session_state['modo_visualizacao'] = None
        st.session_state['modo_edicao'] = False
        st.rerun()

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
                if salvar_novo_cliente({
                    "nome": nome, "cpf": cpf_in, "data_nascimento": nasc, 
                    "identidade": rg, "sexo": sexo, "nome_mae": mae,
                    "nome_pai": pai, "campanhas": campanhas
                }):
                    st.success("Cadastrado!")
                    st.session_state['cliente_ativo_cpf'] = v.ValidadorDocumentos.cpf_para_sql(cpf_in)
                    st.session_state['modo_visualizacao'] = 'visualizar'
                    time.sleep(1)
                    st.rerun()
        return

    dados = carregar_dados_cliente_completo(cpf)
    pessoal = dados.get('pessoal', {})
    financeiro = buscar_hierarquia_financeira(cpf)

    # Injeção CSS (Labels Pretos e Sublinhados)
    st.markdown("""
        <style>
        .stTextInput label p, .stTextArea label p, .stDateInput label p, .stSelectbox label p {
            color: black !important;
            text-decoration: underline;
            font-weight: bold;
        }
        input:disabled, textarea:disabled {
            color: black !important;
            -webkit-text-fill-color: black !important;
            opacity: 1 !important;
            font-weight: 500;
        }
        </style>
    """, unsafe_allow_html=True)

    st.divider()
    c_head, c_act = st.columns([3, 2])
    
    # Formatação Visual via Validador
    cpf_show = v.ValidadorDocumentos.cpf_para_tela(pessoal.get('cpf', ''))
    
    c_head.markdown(f"## 👤 {pessoal.get('nome', 'Sem Nome')}")
    c_head.caption(f"CPF: {cpf_show}")
    
    with c_act:
        c_ins, c_edit, c_del = st.columns(3)
        with c_ins:
            if st.button("➕ Extra", help="Inserir telefone, email, contrato", use_container_width=True):
                modal_inserir_dados(cpf, pessoal.get('nome'))
        with c_edit:
            if st.session_state['modo_edicao']:
                 if st.button("👁️ Ver", help="Sair do modo edição", use_container_width=True):
                      st.session_state['modo_edicao'] = False
                      st.rerun()
            else:
                if st.button("✏️ Editar", type="primary", use_container_width=True):
                    st.session_state['modo_edicao'] = True
                    st.rerun()
        with c_del:
            if st.button("🗑️ Excluir", type="primary", use_container_width=True):
                modal_confirmar_exclusao(cpf)

    st.divider()

    # --- MODO EDIÇÃO ---
    if st.session_state['modo_edicao']:
        with st.form("form_edicao_cliente"):
            st.info("✏️ Modo Edição Ativo. Edite os campos abaixo e clique em Salvar.")
            st.markdown("### 📄 Dados Pessoais")
            ec1, ec2, ec3 = st.columns(3)
            e_nome = ec1.text_input("Nome", value=pessoal.get('nome',''))
            e_rg = ec2.text_input("RG", value=pessoal.get('identidade',''))
            e_nasc = ec3.date_input("Data Nasc.", value=v.ValidadorData.para_sql(pessoal.get('data_nascimento')), format="DD/MM/YYYY")
            
            ec4, ec5, ec6 = st.columns(3)
            e_cnh = ec4.text_input("CNH", value=pessoal.get('cnh',''))
            e_titulo = ec5.text_input("Título Eleitor", value=pessoal.get('titulo_eleitoral',''))
            e_sexo = ec6.selectbox("Sexo", ["Masculino", "Feminino", "Outros"], index=["Masculino", "Feminino", "Outros"].index(pessoal.get('sexo', 'Outros')) if pessoal.get('sexo') in ["Masculino", "Feminino", "Outros"] else 0)
            e_mae = st.text_input("Nome da Mãe", value=pessoal.get('nome_mae', ''))
            
            ec7, ec8 = st.columns(2)
            e_pai = ec7.text_input("Nome do Pai", value=pessoal.get('nome_pai', ''))
            e_campanhas = ec8.text_input("Campanhas", value=pessoal.get('campanhas', ''))
            
            st.markdown("---")
            st.caption("Campos abaixo são gerenciados via '➕ Extra'")
            ec9, ec10 = st.columns(2)
            agrupamento_val = dados.get('agrupamentos')[0] if dados.get('agrupamentos') else ""
            ec9.text_input("Agrupamento (Leitura)", value=agrupamento_val, disabled=True)
            lista_convs = dados.get('convenios_lista', [])
            conv_str = ", ".join(lista_convs)
            ec10.text_input("Convênios (Leitura)", value=conv_str, disabled=True)

            st.divider()
            col_lista1, col_lista2 = st.columns(2)
            edicoes_telefones = []
            edicoes_emails = []

            with col_lista1:
                st.markdown("### 📞 Telefones")
                if dados.get('telefones'):
                    for i, tel in enumerate(dados['telefones']):
                        novo_val = st.text_input(f"Tel {i+1}", value=tel['valor'], key=f"tel_{tel['id']}")
                        edicoes_telefones.append({'id': tel['id'], 'valor': novo_val})
                else:
                    st.caption("Sem telefones.")

            with col_lista2:
                st.markdown("### 📧 E-mails")
                if dados.get('emails'):
                    for i, mail in enumerate(dados['emails']):
                        novo_val = st.text_input(f"Email {i+1}", value=mail['valor'], key=f"mail_{mail['id']}")
                        edicoes_emails.append({'id': mail['id'], 'valor': novo_val})

            st.divider()
            alteracoes_dinamicas = [] 
            if financeiro:
                st.markdown("### 📋 Editar Dados de Convênios")
                for (nome_convenio, matricula), grupo in financeiro.items():
                    with st.expander(f"Editar: {nome_convenio} - Matrícula {matricula}", expanded=True):
                        dados_esp = grupo.get('dados_convenio')
                        tabela_ref = grupo.get('tabela_ref')
                        if dados_esp and tabela_ref and 'id' in dados_esp:
                            edicao_grupo = {}
                            cols_dyn = st.columns(3)
                            idx_col = 0
                            for k, val in dados_esp.items():
                                if k not in ['id', 'cpf', 'matricula', 'nome', 'agrupamento']:
                                    val_novo = cols_dyn[idx_col % 3].text_input(k.replace('_', ' ').capitalize(), value=str(val), key=f"edyn_{tabela_ref}_{dados_esp['id']}_{k}")
                                    if val_novo != str(val):
                                        edicao_grupo[k] = val_novo
                                    idx_col += 1
                            if edicao_grupo:
                                alteracoes_dinamicas.append({'tabela': tabela_ref, 'id': dados_esp['id'], 'dados': edicao_grupo})
            
            st.divider()
            if st.form_submit_button("💾 CONFIRMAR ALTERAÇÕES", type="primary"):
                pacote_dados = {
                    "pessoal": {
                        "nome": e_nome, "identidade": e_rg, "data_nascimento": e_nasc,
                        "cnh": e_cnh, "titulo_eleitoral": e_titulo, "sexo": e_sexo, "nome_mae": e_mae,
                        "nome_pai": e_pai, "campanhas": e_campanhas
                    },
                    "telefones": edicoes_telefones,
                    "emails": edicoes_emails
                }
                if atualizar_dados_cliente_lote(cpf, pacote_dados, dados_dinamicos=alteracoes_dinamicas):
                    st.success("Dados atualizados com sucesso!")
                    st.session_state['modo_edicao'] = False
                    time.sleep(1)
                    st.rerun()
        return

    # --- VISUALIZAÇÃO ---
    with st.expander("📄 Dados Cadastrais", expanded=False):
        c1, c2, c3, c4, c5, c6 = st.columns([3, 1.5, 1.5, 1, 1.5, 1.5])
        c1.text_input("Nome Completo", value=pessoal.get('nome',''), disabled=True)
        c2.text_input("CPF", value=cpf_show, disabled=True)
        c3.text_input("RG", value=pessoal.get('identidade',''), disabled=True)
        c4.text_input("Sexo", value=pessoal.get('sexo',''), disabled=True)
        
        nasc_fmt = v.ValidadorData.para_tela(pessoal.get('data_nascimento'))
        idade_str = v.ValidadorData.calcular_tempo(pessoal.get('data_nascimento'), 'completo')
        
        c5.text_input("Data Nasc.", value=nasc_fmt, disabled=True)
        c6.text_input("Idade", value=idade_str, disabled=True)
        
        c7, c8 = st.columns(2)
        c7.text_input("Nome da Mãe", value=pessoal.get('nome_mae',''), disabled=True)
        c8.text_input("Nome do Pai", value=pessoal.get('nome_pai',''), disabled=True)
        
        c9, c10, c11, c12 = st.columns(4)
        c9.text_input("CNH", value=pessoal.get('cnh',''), disabled=True)
        c10.text_input("Título Eleitor", value=pessoal.get('titulo_eleitoral',''), disabled=True)
        c11.text_input("Campanha", value=pessoal.get('campanhas',''), disabled=True)
        agrupamento_val = dados.get('agrupamentos')[0] if dados.get('agrupamentos') else ""
        c12.text_input("Agrupamento", value=agrupamento_val, disabled=True)
        
        lista_convs = dados.get('convenios_lista', [])
        conv_str = ", ".join(lista_convs)
        st.text_input("Convênios Vinculados", value=conv_str, disabled=True)

    with st.expander("📍 Contatos e Endereço", expanded=False):
        if dados.get('telefones') or dados.get('emails'):
            cols_contato = st.columns(4)
            idx_c = 0
            for tel in dados.get('telefones', []):
                # Formata visualmente
                val_fmt = v.ValidadorContato.telefone_para_tela(tel['valor'])
                cols_contato[idx_c % 4].text_input(f"Telefone {idx_c+1}", value=val_fmt, disabled=True)
                idx_c += 1
            for mail in dados.get('emails', []):
                cols_contato[idx_c % 4].text_input(f"E-mail {idx_c+1}", value=mail['valor'], disabled=True)
                idx_c += 1
        else:
            st.caption("Sem contatos cadastrados.")
        
        st.divider()
        st.markdown("**🏠 Endereço**")
        if dados.get('enderecos'):
            for i, end in enumerate(dados.get('enderecos', [])):
                ce1, ce2, ce3, ce4, ce5 = st.columns([3, 2, 2, 1, 1.5])
                ce1.text_input("Rua", value=end.get('rua',''), key=f"r_{i}", disabled=True)
                ce2.text_input("Bairro", value=end.get('bairro',''), key=f"b_{i}", disabled=True)
                ce3.text_input("Cidade", value=end.get('cidade',''), key=f"c_{i}", disabled=True)
                ce4.text_input("UF", value=end.get('uf',''), key=f"u_{i}", disabled=True)
                # Formata CEP
                cep_fmt = v.ValidadorContato.cep_para_tela(end.get('cep'))
                ce5.text_input("CEP", value=cep_fmt, key=f"cp_{i}", disabled=True)
        else:
            st.caption("Sem endereço cadastrado.")

    if financeiro:
        for (nome_convenio, matricula), grupo in financeiro.items():
            with st.expander(f"📋 {nome_convenio} - Matrícula: {matricula}", expanded=False):
                st.markdown(f"###### 🏥 Dados do Convênio")
                dados_esp = grupo.get('dados_convenio')
                if dados_esp:
                    chaves = [k for k in dados_esp.keys() if k not in ['id', 'cpf', 'matricula', 'nome', 'agrupamento']]
                    cols_fin = st.columns(4)
                    for i, k in enumerate(chaves):
                        cols_fin[i % 4].text_input(k.replace('_', ' ').capitalize(), value=str(dados_esp[k]), disabled=True, key=f"v_dconv_{matricula}_{k}")
                else:
                    st.info("Sem dados adicionais.")

                st.divider()
                st.markdown(f"###### 📄 Contratos")
                lista_contratos = grupo.get('contratos', [])
                if lista_contratos:
                    df_contratos = pd.DataFrame(lista_contratos)
                    colunas_ordenadas = [
                        'numero_contrato', 'valor_parcela', 'prazo_aberto', 'prazo_pago',
                        'prazo_total', 'taxa_juros', 'tipo_taxa', 'valor_contrato_inicial',
                        'saldo_devedor', 'data_inicio', 'data_averbacao', 'data_final'
                    ]
                    cols_existentes = [c for c in colunas_ordenadas if c in df_contratos.columns]
                    df_show = df_contratos[cols_existentes].copy()
                    
                    # Formatação Visual via Validadores
                    if 'valor_parcela' in df_show.columns: df_show['valor_parcela'] = df_show['valor_parcela'].apply(v.ValidadorFinanceiro.para_tela)
                    if 'valor_contrato_inicial' in df_show.columns: df_show['valor_contrato_inicial'] = df_show['valor_contrato_inicial'].apply(v.ValidadorFinanceiro.para_tela)
                    if 'saldo_devedor' in df_show.columns: df_show['saldo_devedor'] = df_show['saldo_devedor'].apply(v.ValidadorFinanceiro.para_tela)
                    
                    for col_date in ['data_inicio', 'data_averbacao', 'data_final']:
                        if col_date in df_show.columns:
                            df_show[col_date] = df_show[col_date].apply(v.ValidadorData.para_tela)

                    renomear = {
                        'numero_contrato': 'Nº Contrato', 'valor_parcela': 'Vlr. Parc.',
                        'prazo_aberto': 'Pz Aberto', 'prazo_pago': 'Pz Pago', 'prazo_total': 'Pz Total',
                        'taxa_juros': 'Taxa %', 'valor_contrato_inicial': 'Vlr. Inicial',
                        'saldo_devedor': 'Saldo Dev.', 'data_inicio': 'Dt Início',
                        'data_averbacao': 'Dt Averb.', 'data_final': 'Dt Final'
                    }
                    df_show.rename(columns=renomear, inplace=True)
                    st.table(df_show)
                else:
                    st.caption("Nenhum contrato ativo.")
    else:
        with st.expander("📋 Convênios e Contratos", expanded=False):
            st.info("Nenhum convênio ou contrato localizado.")

def tela_pesquisa():
    st.markdown("#### 🔍 Buscar Cliente")
    tab1, tab2 = st.tabs(["Pesquisa Rápida", "Pesquisa Completa"])
    
    with tab1:
        c_input, c_btn = st.columns([4, 1])
        termo = c_input.text_input("Digite CPF, Nome ou Telefone", label_visibility="collapsed")
        if c_btn.button("🔍 Buscar", type="primary", use_container_width=True):
            if len(termo) < 3:
                st.warning("Mínimo 3 caracteres.")
            else:
                # Validar tipo de dado para feedback visual
                apenas_num = v.ValidadorDocumentos.limpar_numero(termo)
                if len(apenas_num) == 11:
                    cpf_fmt = v.ValidadorDocumentos.cpf_para_tela(apenas_num)
                    st.toast(f"Buscando por CPF: {cpf_fmt}", icon="🔢")
                
                res = buscar_cliente_rapida(termo)
                st.session_state['resultados_pesquisa'] = res
                if not res: st.warning("Nada encontrado.")

    with tab2:
        st.markdown("Configure os filtros abaixo para uma busca detalhada.")
        c_clear, c_group, c_vazio = st.columns([1.5, 1.5, 4])
        if c_clear.button("🧹 Limpar Filtros", type="secondary", use_container_width=True):
            st.session_state['resultados_pesquisa'] = []
            for key in list(st.session_state.keys()):
                if "val_input_" in key or "op_input_" in key:
                    del st.session_state[key]
            st.rerun()
        if c_group.button("📂 Agrupamentos", use_container_width=True):
            modal_agrupamentos()
        
        st.write("") 
        with st.form("form_pesquisa_completa"):
            filtros_para_query = []
            tab_pessoais, tab_contatos, tab_outros = st.tabs(["👤 Dados Pessoais", "📍 Contatos e Endereço", "📋 Filiação e Sistema"])
            distribuicao_abas = [
                (tab_pessoais, ["Dados Pessoais"]),
                (tab_contatos, ["Contatos", "Endereço"]),
                (tab_outros, ["Filiação e Sistema"])
            ]
            
            # (Lógica original de montagem dinâmica dos filtros mantida)
            for aba_atual, lista_grupos in distribuicao_abas:
                with aba_atual:
                    for grupo in lista_grupos:
                        campos = MAPA_CAMPOS_PESQUISA.get(grupo)
                        if not campos: continue
                        if len(lista_grupos) > 1: st.markdown(f"###### {grupo}")
                        col_esq, col_dir = st.columns(2, gap="large")
                        for i, (nome_campo, config) in enumerate(campos.items()):
                            col_atual = col_esq if i % 2 == 0 else col_dir
                            with col_atual:
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

                                if "IS NULL" in sql_op_code or "IS NOT NULL" in sql_op_code:
                                    st.info("--- (Sem valor)")
                                    valor_final = "ignore"
                                elif config['tipo'] == 'data':
                                    cd1, cd2 = st.columns(2)
                                    d1 = cd1.date_input("De", value=None, key=f"d1_{val_key}", format="DD/MM/YYYY", label_visibility="collapsed")
                                    d2 = cd2.date_input("Até", value=None, key=f"d2_{val_key}", format="DD/MM/YYYY", label_visibility="collapsed")
                                    # Usa validador para garantir string ISO
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
                                    filtros_para_query.append({
                                        'col': config['col'], 'op': sql_op_code,
                                        'mask': OPERADORES_SQL[tipo_ops][operador_escolhido]['mask'],
                                        'val': valor_final, 'tipo': config['tipo'],
                                        'table': config.get('table')
                                    })
                                st.write("")

            st.write("")
            if st.form_submit_button("🚀 APLICAR FILTROS", type="primary", use_container_width=True):
                if filtros_para_query:
                    res_completa = buscar_cliente_dinamica(filtros_para_query)
                    st.session_state['resultados_pesquisa'] = res_completa
                    if not res_completa: st.toast("Nenhum registro encontrado.", icon="⚠️")
                else:
                    st.warning("Preencha pelo menos um campo para filtrar.")

    if st.session_state.get('resultados_pesquisa'):
        st.divider()
        st.markdown(f"**Resultados: {len(st.session_state['resultados_pesquisa'])}**")
        cols = st.columns([1, 4, 3, 2])
        cols[0].write("**ID**")
        cols[1].write("**Nome**")
        cols[2].write("**CPF**")
        cols[3].write("**Ação**")
        for row in st.session_state['resultados_pesquisa']:
            c = st.columns([1, 4, 3, 2])
            c[0].write(str(row[0]))
            c[1].write(row[1])
            # Formata CPF na lista
            c[2].write(v.ValidadorDocumentos.cpf_para_tela(row[2]))
            if c[3].button("📂 Abrir", key=f"abrir_{row[0]}"):
                st.session_state['cliente_ativo_cpf'] = row[2]
                st.session_state['modo_visualizacao'] = 'visualizar'
                st.rerun()

def app_cadastro():
    if 'modo_visualizacao' not in st.session_state:
        st.session_state['modo_visualizacao'] = None
    
    if st.session_state['modo_visualizacao'] == 'visualizar':
        if st.session_state.get('cliente_ativo_cpf'):
            tela_ficha_cliente(st.session_state['cliente_ativo_cpf'])
        else:
            st.session_state['modo_visualizacao'] = None
            st.rerun()
    elif st.session_state['modo_visualizacao'] == 'novo':
        tela_ficha_cliente(None, modo='novo')
    else:
        tela_pesquisa()

if __name__ == "__main__":
    if get_pool():
        app_cadastro()
    else:
        st.error("Falha ao conectar no banco de dados. Verifique conexao.py")