import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, date
import time

# Tenta importar a conexão do sistema principal
try:
    import conexao
except ImportError:
    conexao = None

# --- CONSTANTES E CONFIGURAÇÕES DE PESQUISA ---

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

# --- FUNÇÕES AUXILIARES ---
def limpar_texto(valor):
    if valor is None: return ""
    s_valor = str(valor).strip()
    if s_valor.lower() in ['none', 'null']: return ""
    return s_valor

def converter_data_iso(data_obj):
    if isinstance(data_obj, (date, datetime)):
        return data_obj.strftime("%Y-%m-%d")
    return str(data_obj)

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

# --- FUNÇÕES DE BUSCA ---

def buscar_relacao_auxiliar(tipo):
    conn = get_db_connection()
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
    finally:
        conn.close()

def buscar_cliente_rapida(termo):
    conn = get_db_connection()
    if not conn: return []
    
    termo = termo.strip()
    termo_limpo = ''.join(filter(str.isdigit, termo))
    
    query = """
        SELECT t.id, t.nome, t.cpf, t.identidade 
        FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf t
        WHERE 
            t.nome ILIKE %s OR 
            t.cpf ILIKE %s OR
            t.cpf = %s OR
            t.nome_pai ILIKE %s OR
            t.campanhas ILIKE %s OR
            t.id_importacao ILIKE %s OR
            EXISTS (SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = t.cpf AND telefone ILIKE %s)
        LIMIT 30
    """
    param_termo = f"%{termo}%"
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (
                param_termo, param_termo, 
                termo_limpo if termo_limpo else '00000000000', 
                param_termo, param_termo, param_termo, param_termo
            ))
            return cur.fetchall()
    finally:
        conn.close()

def buscar_cliente_dinamica(filtros_aplicados):
    conn = get_db_connection()
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
                params.append(filtro['val'][0])
                params.append(filtro['val'][1])
            else:
                if filtro['val']:
                    valores = str(filtro['val']).split(';')
                    ors = []
                    for v in valores:
                        v = v.strip()
                        if v:
                            ors.append(f"{coluna} {operador_sql} %s")
                            val_final = filtro['mask'].format(v) if '{}' in filtro['mask'] else v
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
    finally:
        conn.close()

def carregar_dados_cliente_completo(cpf):
    conn = get_db_connection()
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
                for k, v in d_pessoal.items():
                    if v is None and k != 'data_nascimento': d_pessoal[k] = ""
                dados['pessoal'] = d_pessoal
            else:
                dados['pessoal'] = {}

            # 2. Dados CLT (Se houver)
            try:
                cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_ctt WHERE cpf = %s LIMIT 1", (cpf,))
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

            cur.execute("SELECT id, convenio FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s ORDER BY id", (cpf,))
            dados['convenios'] = [{'id': r[0], 'valor': r[1] or ""} for r in cur.fetchall()]

            cur.execute("SELECT agrupamento FROM sistema_consulta.sistema_consulta_dados_cadastrais_agrupamento_cpf WHERE cpf = %s", (cpf,))
            dados['agrupamentos'] = [r[0] for r in cur.fetchall() if r[0]]
            
    except Exception as e:
        st.error(f"Erro ao carregar cliente: {e}")
    finally:
        conn.close()
    
    return dados

# --- FUNÇÕES DE ESCRITA (CRUD) ---

def salvar_novo_cliente(dados_form):
    conn = get_db_connection()
    if not conn: return False
    
    dados_limpos = {
        "nome": limpar_texto(dados_form.get('nome')),
        "cpf": limpar_texto(dados_form.get('cpf')),
        "identidade": limpar_texto(dados_form.get('identidade')),
        "sexo": limpar_texto(dados_form.get('sexo')),
        "nome_mae": limpar_texto(dados_form.get('nome_mae')),
        "nome_pai": limpar_texto(dados_form.get('nome_pai')),
        "campanhas": limpar_texto(dados_form.get('campanhas')),
        "data_nascimento": dados_form.get('data_nascimento')
    }

    try:
        with conn.cursor() as cur:
            cols = list(dados_limpos.keys())
            vals = list(dados_limpos.values())
            placeholders = ", ".join(["%s"] * len(cols))
            columns = ", ".join(cols)
            
            sql = f"INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_cpf ({columns}) VALUES ({placeholders})"
            cur.execute(sql, vals)
            
            cur.execute("INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf) VALUES (%s) ON CONFLICT DO NOTHING", (dados_limpos['cpf'],))
            
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False
    finally:
        conn.close()

def inserir_dado_extra(tipo, cpf, dados):
    conn = get_db_connection()
    if not conn: return "erro"
    
    valor = limpar_texto(dados.get('valor'))
    
    try:
        with conn.cursor() as cur:
            if tipo == "Telefone":
                cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s AND telefone = %s", (cpf, valor))
                if cur.fetchone(): return "duplicado"
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone) VALUES (%s, %s)", (cpf, valor))
            elif tipo == "E-mail":
                cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf = %s AND email = %s", (cpf, valor))
                if cur.fetchone(): return "duplicado"
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email) VALUES (%s, %s)", (cpf, valor))
            elif tipo == "Endereço":
                cur.execute("""
                    INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_endereco 
                    (cpf, cep, rua, bairro, cidade, uf) VALUES (%s, %s, %s, %s, %s, %s)
                """, (cpf, limpar_texto(dados.get('cep')), limpar_texto(dados.get('rua')), 
                      limpar_texto(dados.get('bairro')), limpar_texto(dados.get('cidade')), limpar_texto(dados.get('uf'))))
            elif tipo == "Convênio":
                cur.execute("SELECT 1 FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s AND convenio = %s", (cpf, valor))
                if cur.fetchone(): return "duplicado"
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio) VALUES (%s, %s)", (cpf, valor))
            
            conn.commit()
            return "sucesso"
    except Exception as e:
        st.error(f"Erro ao inserir dado extra: {e}")
        return "erro"
    finally:
        conn.close()

def atualizar_dados_cliente_lote(cpf, dados_editados):
    conn = get_db_connection()
    if not conn: return False
    
    try:
        with conn.cursor() as cur:
            pessoal = dados_editados['pessoal']
            cur.execute("""
                UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_cpf
                SET nome = %s, data_nascimento = %s, identidade = %s, 
                    sexo = %s, cnh = %s, titulo_eleitoral = %s, nome_mae = %s,
                    nome_pai = %s, campanhas = %s
                WHERE cpf = %s
            """, (
                limpar_texto(pessoal['nome']), pessoal['data_nascimento'], limpar_texto(pessoal['identidade']),
                limpar_texto(pessoal['sexo']), limpar_texto(pessoal['cnh']), 
                limpar_texto(pessoal['titulo_eleitoral']), limpar_texto(pessoal['nome_mae']),
                limpar_texto(pessoal.get('nome_pai')), limpar_texto(pessoal.get('campanhas')),
                cpf
            ))
            
            for item in dados_editados.get('telefones', []):
                val = limpar_texto(item['valor'])
                if not val:
                    cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE id = %s", (item['id'],))
                else:
                    cur.execute("UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_telefone SET telefone = %s WHERE id = %s", (val, item['id']))
            
            for item in dados_editados.get('emails', []):
                val = limpar_texto(item['valor'])
                if not val:
                    cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE id = %s", (item['id'],))
                else:
                    cur.execute("UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_email SET email = %s WHERE id = %s", (val, item['id']))
            
            for item in dados_editados.get('convenios', []):
                val = limpar_texto(item['valor'])
                if not val:
                    cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE id = %s", (item['id'],))
                else:
                    cur.execute("UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_convenio SET convenio = %s WHERE id = %s", (val, item['id']))

            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar: {e}")
        return False
    finally:
        conn.close()

def excluir_cliente_total(cpf):
    conn = get_db_connection()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_agrupamento_cpf WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_ctt WHERE cpf = %s", (cpf,)) # Exclui CLT também
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_cpf WHERE cpf = %s", (cpf,))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao excluir cliente: {e}")
        return False
    finally:
        conn.close()

# --- DIALOGS ---
@st.dialog("➕ Inserir Dados Extras")
def modal_inserir_dados(cpf, nome_cliente):
    st.write(f"Cliente: **{nome_cliente}**")
    tipo_insercao = st.selectbox("Selecione o Tipo", ["Telefone", "E-mail", "Endereço", "Convênio"])
    
    with st.form("form_insercao_modal"):
        dados_submit = {}
        if tipo_insercao == "Telefone":
            dados_submit['valor'] = st.text_input("Novo Telefone", placeholder="(00) 00000-0000")
        elif tipo_insercao == "E-mail":
            dados_submit['valor'] = st.text_input("Novo E-mail")
        elif tipo_insercao == "Endereço":
            dados_submit['cep'] = st.text_input("CEP")
            dados_submit['rua'] = st.text_input("Rua")
            dados_submit['bairro'] = st.text_input("Bairro")
            dados_submit['cidade'] = st.text_input("Cidade")
            dados_submit['uf'] = st.text_input("UF", max_chars=2)
        elif tipo_insercao == "Convênio":
                dados_submit['valor'] = st.text_input("Nome do Convênio")
        
        if st.form_submit_button("✅ Salvar Inclusão"):
            status = inserir_dado_extra(tipo_insercao, cpf, dados_submit)
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

# --- TELA DE FICHA DO CLIENTE (COM O LAYOUT SOLICITADO) ---

def tela_ficha_cliente(cpf, modo='visualizar'):
    if 'modo_edicao' not in st.session_state:
        st.session_state['modo_edicao'] = False

    # Botão Voltar
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
                    st.session_state['cliente_ativo_cpf'] = cpf_in
                    st.session_state['modo_visualizacao'] = 'visualizar'
                    time.sleep(1)
                    st.rerun()
        return

    # CARREGA DADOS
    dados = carregar_dados_cliente_completo(cpf)
    pessoal = dados.get('pessoal', {})
    clt = dados.get('clt', {})

    # Cabeçalho
    st.divider()
    c_head, c_act = st.columns([3, 2])
    c_head.markdown(f"## 👤 {pessoal.get('nome', 'Sem Nome')}")
    c_head.caption(f"CPF: {pessoal.get('cpf', '')}")
    
    with c_act:
        c_ins, c_edit, c_del = st.columns(3)
        with c_ins:
            if st.button("➕ Extra", help="Inserir telefone, email, etc", use_container_width=True):
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
            st.info("✏️ Modo Edição Ativo. Limpe um campo de lista para excluí-lo.")
            
            st.markdown("### 📄 Dados Pessoais")
            ec1, ec2, ec3 = st.columns(3)
            e_nome = ec1.text_input("Nome", value=pessoal.get('nome',''))
            e_rg = ec2.text_input("RG", value=pessoal.get('identidade',''))
            e_nasc = ec3.date_input("Data Nasc.", value=pessoal.get('data_nascimento'), format="DD/MM/YYYY")
            
            ec4, ec5, ec6 = st.columns(3)
            e_cnh = ec4.text_input("CNH", value=pessoal.get('cnh',''))
            e_titulo = ec5.text_input("Título Eleitor", value=pessoal.get('titulo_eleitoral',''))
            e_sexo = ec6.selectbox("Sexo", ["Masculino", "Feminino", "Outros"], index=["Masculino", "Feminino", "Outros"].index(pessoal.get('sexo', 'Outros')) if pessoal.get('sexo') in ["Masculino", "Feminino", "Outros"] else 0)
            e_mae = st.text_input("Nome da Mãe", value=pessoal.get('nome_mae', ''))
            
            ec7, ec8 = st.columns(2)
            e_pai = ec7.text_input("Nome do Pai", value=pessoal.get('nome_pai', ''))
            e_campanhas = ec8.text_input("Campanhas", value=pessoal.get('campanhas', ''))

            st.divider()
            
            col_lista1, col_lista2 = st.columns(2)
            edicoes_telefones = []
            edicoes_emails = []
            edicoes_convenios = []

            with col_lista1:
                st.markdown("### 📞 Telefones")
                if dados.get('telefones'):
                    for i, tel in enumerate(dados['telefones']):
                        novo_val = st.text_input(f"Tel {i+1}", value=tel['valor'], key=f"tel_{tel['id']}")
                        edicoes_telefones.append({'id': tel['id'], 'valor': novo_val})
                else:
                    st.caption("Sem telefones.")

                st.markdown("### 💼 Convênios")
                if dados.get('convenios'):
                    for i, conv in enumerate(dados['convenios']):
                        novo_val = st.text_input(f"Convênio {i+1}", value=conv['valor'], key=f"conv_{conv['id']}")
                        edicoes_convenios.append({'id': conv['id'], 'valor': novo_val})

            with col_lista2:
                st.markdown("### 📧 E-mails")
                if dados.get('emails'):
                    for i, mail in enumerate(dados['emails']):
                        novo_val = st.text_input(f"Email {i+1}", value=mail['valor'], key=f"mail_{mail['id']}")
                        edicoes_emails.append({'id': mail['id'], 'valor': novo_val})
            
            st.divider()
            
            fb1, fb2 = st.columns([1, 1])
            if fb1.form_submit_button("💾 CONFIRMAR ALTERAÇÕES", type="primary"):
                pacote_dados = {
                    "pessoal": {
                        "nome": e_nome, "identidade": e_rg, "data_nascimento": e_nasc,
                        "cnh": e_cnh, "titulo_eleitoral": e_titulo, "sexo": e_sexo, "nome_mae": e_mae,
                        "nome_pai": e_pai, "campanhas": e_campanhas
                    },
                    "telefones": edicoes_telefones,
                    "emails": edicoes_emails,
                    "convenios": edicoes_convenios
                }
                if atualizar_dados_cliente_lote(cpf, pacote_dados):
                    st.success("Dados atualizados com sucesso!")
                    st.session_state['modo_edicao'] = False
                    time.sleep(1)
                    st.rerun()
        return

    # =========================================================================
    # LAYOUT PERSONALIZADO: 60% DADOS PESSOAIS | 40% CONTATOS E ENDEREÇO
    # =========================================================================
    
    col_esquerda, col_direita = st.columns([6, 4], gap="medium")

    # --- 1ª COLUNA (ESQUERDA - 60%): DADOS CADASTRAIS ---
    with col_esquerda:
        st.subheader("📄 Dados Cadastrais")
        
        # Linha 1
        c1, c2 = st.columns([3, 2])
        c1.text_input("Nome Completo", value=pessoal.get('nome',''), disabled=True)
        c2.text_input("CPF", value=pessoal.get('cpf',''), disabled=True)
        
        # Linha 2
        c3, c4, c5 = st.columns(3)
        c3.text_input("RG", value=pessoal.get('identidade',''), disabled=True)
        
        dt_nasc = pessoal.get('data_nascimento')
        if dt_nasc: dt_nasc = dt_nasc.strftime('%d/%m/%Y')
        c4.text_input("Data Nasc.", value=str(dt_nasc), disabled=True)
        c5.text_input("Sexo", value=pessoal.get('sexo',''), disabled=True)
        
        # Linha 3
        c6, c7 = st.columns(2)
        c6.text_input("Nome da Mãe", value=pessoal.get('nome_mae',''), disabled=True)
        c7.text_input("Nome do Pai", value=pessoal.get('nome_pai',''), disabled=True)
        
        # Linha 4
        c8, c9 = st.columns(2)
        c8.text_input("CNH", value=pessoal.get('cnh',''), disabled=True)
        c9.text_input("Título Eleitor", value=pessoal.get('titulo_eleitoral',''), disabled=True)
        
        # Extras
        st.text_input("Campanhas", value=pessoal.get('campanhas',''), disabled=True)
        
        # Se tiver dados CLT, mostra aqui também
        if clt:
            st.markdown("---")
            st.markdown("##### 💼 Dados CLT")
            cl1, cl2 = st.columns(2)
            cl1.text_input("Matrícula", value=clt.get('matricula',''), disabled=True)
            cl2.text_input("CNPJ", value=f"{clt.get('cnpj_nome','')} ({clt.get('cnpj_numero','')})", disabled=True)
            
            cl3, cl4 = st.columns(2)
            cl3.text_input("CBO", value=f"{clt.get('cbo_codigo','')} - {clt.get('cbo_nome','')}", disabled=True)
            dt_adm = clt.get('data_admissao')
            if dt_adm: dt_adm = dt_adm.strftime('%d/%m/%Y')
            cl4.text_input("Admissão", value=str(dt_adm), disabled=True)

    # --- 2ª COLUNA (DIREITA - 40%): CONTATOS E ENDEREÇO ---
    with col_direita:
        
        # PARTE 1: TELEFONES (2 Colunas internas)
        st.subheader("📞 Telefones")
        if dados.get('telefones'):
            t_col1, t_col2 = st.columns(2)
            for i, tel in enumerate(dados.get('telefones', [])):
                # Alterna entre coluna 1 e 2
                col_alvo = t_col1 if i % 2 == 0 else t_col2
                col_alvo.text_input(f"Tel {i+1}", value=tel['valor'], key=f"t_{i}", disabled=True, label_visibility="collapsed")
        else:
            st.info("Nenhum telefone.")

        st.divider()

        # PARTE 2: E-MAILS
        st.subheader("📧 E-mails")
        if dados.get('emails'):
            for i, mail in enumerate(dados.get('emails', [])):
                st.text_input(f"Email {i+1}", value=mail['valor'], key=f"e_{i}", disabled=True, label_visibility="collapsed")
        else:
            st.info("Nenhum e-mail.")

        st.divider()

        # PARTE 3: ENDEREÇOS
        st.subheader("🏠 Endereço")
        if dados.get('enderecos'):
            for i, end in enumerate(dados.get('enderecos', [])):
                with st.container(border=True):
                    st.caption(f"Endereço {i+1}")
                    st.text(f"{end.get('rua','')}, {end.get('bairro','')}")
                    st.text(f"{end.get('cidade','')} - {end.get('uf','')}")
                    st.text(f"CEP: {end.get('cep','')}")
        else:
            st.info("Nenhum endereço.")
            
        st.divider()
        st.subheader("📋 Convênios")
        if dados.get('convenios'):
             st.write(", ".join([c['valor'] for c in dados.get('convenios', [])]))
        else:
             st.caption("Sem convênios.")

# --- TELA DE PESQUISA (Principal) ---

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
            
            for grupo, campos in MAPA_CAMPOS_PESQUISA.items():
                with st.expander(f"📂 {grupo}", expanded=True):
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
                                if d1: valor_final = [converter_data_iso(d1), converter_data_iso(d2) if d2 else converter_data_iso(d1)]
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
                                    'col': config['col'],
                                    'op': sql_op_code,
                                    'mask': OPERADORES_SQL[tipo_ops][operador_escolhido]['mask'],
                                    'val': valor_final,
                                    'tipo': config['tipo'],
                                    'table': config.get('table')
                                })
                            st.write("") 

            st.write("")
            submitted = st.form_submit_button("🚀 APLICAR FILTROS", type="primary", use_container_width=True)
            
            if submitted:
                if filtros_para_query:
                    res_completa = buscar_cliente_dinamica(filtros_para_query)
                    st.session_state['resultados_pesquisa'] = res_completa
                    if not res_completa:
                        st.toast("Nenhum registro encontrado.", icon="⚠️")
                else:
                    st.warning("Preencha pelo menos um campo para filtrar.")

    # RESULTADOS
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
            c[2].write(row[2])
            if c[3].button("📂 Abrir", key=f"abrir_{row[0]}"):
                st.session_state['cliente_ativo_cpf'] = row[2]
                st.session_state['modo_visualizacao'] = 'visualizar'
                st.rerun()

# --- APP PRINCIPAL ---

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
    app_cadastro()