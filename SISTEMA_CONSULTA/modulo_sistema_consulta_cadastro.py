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

# Mapeamento de Nome Visual -> Coluna/Tabela e Tipo de Dado
MAPA_CAMPOS_PESQUISA = {
    "Dados Pessoais": {
        "Nome do Cliente": {"col": "t.nome", "tipo": "texto"},
        "CPF": {"col": "t.cpf", "tipo": "texto"},
        "RG (Identidade)": {"col": "t.identidade", "tipo": "texto"},
        "Data de Nascimento": {"col": "t.data_nascimento", "tipo": "data"},
        "Idade": {"col": "age(t.data_nascimento)", "tipo": "numero_calculado"}, 
        "Sexo": {"col": "t.sexo", "tipo": "texto"},
    },
    "Contatos e Endereço": {
        "Telefone": {"col": "tel.telefone", "table": "sistema_consulta_dados_cadastrais_telefone", "alias": "tel", "tipo": "texto_vinculado"},
        "E-mail": {"col": "mail.email", "table": "sistema_consulta_dados_cadastrais_email", "alias": "mail", "tipo": "texto_vinculado"},
        "Endereço (Rua/Cidade)": {"col": "endr", "table": "sistema_consulta_dados_cadastrais_endereco", "alias": "endr", "tipo": "endereco_vinculado"},
    },
    "Filiação e Outros": {
        "Nome da Mãe": {"col": "t.nome_mae", "tipo": "texto"},
        "Título de Eleitor": {"col": "t.titulo_eleitoral", "tipo": "texto"},
        "CNH": {"col": "t.cnh", "tipo": "texto"},
    }
}

# Operadores com Símbolos e SQL correspondente
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

# --- FUNÇÕES AUXILIARES DE LIMPEZA ---
def limpar_texto(valor):
    """Converte None ou 'None' para string vazia."""
    if valor is None:
        return ""
    s_valor = str(valor).strip()
    if s_valor.lower() in ['none', 'null']:
        return ""
    return s_valor

def converter_data_iso(data_obj):
    """Garante formato YYYY-MM-DD para SQL"""
    if isinstance(data_obj, (date, datetime)):
        return data_obj.strftime("%Y-%m-%d")
    return str(data_obj)

# --- FUNÇÕES DE BANCO DE DADOS ---

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

def buscar_cliente_rapida(termo):
    """Busca por Nome, CPF ou Telefone (limite 30)"""
    conn = get_db_connection()
    if not conn: return []
    
    termo = termo.strip()
    termo_limpo = ''.join(filter(str.isdigit, termo))
    
    query = """
        SELECT id, nome, cpf, identidade 
        FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf
        WHERE 
            nome ILIKE %s OR 
            cpf ILIKE %s OR
            cpf = %s
        LIMIT 30
    """
    param_nome = f"%{termo}%"
    param_cpf = f"%{termo}%"
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (param_nome, param_cpf, termo_limpo if termo_limpo else '00000000000'))
            return cur.fetchall()
    finally:
        conn.close()

def buscar_cliente_dinamica(filtros_aplicados):
    """
    Constrói query dinâmica com JOINs e Subqueries conforme necessário.
    """
    conn = get_db_connection()
    if not conn: return []

    # Query Base (Alias t para a tabela principal)
    base_query = """
        SELECT DISTINCT t.id, t.nome, t.cpf, t.identidade 
        FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf t
    """
    
    # Cláusulas WHERE
    where_clauses = ["1=1"]
    params = []

    for filtro in filtros_aplicados:
        coluna = filtro['col']
        operador_sql = filtro['op']
        tipo_dado = filtro['tipo']
        
        # Tratamento especial para tabelas vinculadas (EXISTS)
        if tipo_dado in ['texto_vinculado', 'endereco_vinculado']:
            tabela_satelite = filtro['table']
            
            # Subquery baseada no tipo
            if tipo_dado == 'texto_vinculado': # Telefone, Email
                sub_where = f"{coluna} {operador_sql} %s"
                val_final = filtro['mask'].format(filtro['val']) if '{}' in filtro['mask'] else filtro['val']
                
                # Vazio
                if "IS NULL" in operador_sql or "IS NOT NULL" in operador_sql:
                     exists_clause = f"EXISTS (SELECT 1 FROM sistema_consulta.{tabela_satelite} WHERE cpf = t.cpf AND {coluna} {operador_sql})"
                     where_clauses.append(exists_clause)
                else:
                    # Busca normal
                    exists_clause = f"EXISTS (SELECT 1 FROM sistema_consulta.{tabela_satelite} WHERE cpf = t.cpf AND {sub_where})"
                    where_clauses.append(exists_clause)
                    params.append(val_final)

            elif tipo_dado == 'endereco_vinculado':
                # Busca em Rua OU Cidade OU UF
                val_final = f"%{filtro['val']}%"
                if "IS NULL" in operador_sql:
                     where_clauses.append(f"NOT EXISTS (SELECT 1 FROM sistema_consulta.{tabela_satelite} WHERE cpf = t.cpf)")
                elif "IS NOT NULL" in operador_sql:
                     where_clauses.append(f"EXISTS (SELECT 1 FROM sistema_consulta.{tabela_satelite} WHERE cpf = t.cpf)")
                else:
                    exists_clause = f"""
                        EXISTS (SELECT 1 FROM sistema_consulta.{tabela_satelite} 
                        WHERE cpf = t.cpf AND (rua ILIKE %s OR cidade ILIKE %s))
                    """
                    where_clauses.append(exists_clause)
                    params.append(val_final)
                    params.append(val_final)

        elif tipo_dado == 'numero_calculado': # Idade
            # Extract Year from Age for calculation
            if "IS NULL" in operador_sql:
                 where_clauses.append(f"t.data_nascimento IS NULL")
            else:
                if operador_sql == 'BETWEEN':
                    where_clauses.append(f"EXTRACT(YEAR FROM age(t.data_nascimento)) BETWEEN %s AND %s")
                    params.append(filtro['val'][0]) # Min
                    params.append(filtro['val'][1]) # Max
                else:
                    where_clauses.append(f"EXTRACT(YEAR FROM age(t.data_nascimento)) {operador_sql} %s")
                    params.append(filtro['val'])

        else: # Campos normais da tabela principal (Texto ou Data)
            if "IS NULL" in operador_sql or "IS NOT NULL" in operador_sql:
                # Tratamento para Vazio (Null ou String Vazia)
                if "NOT" in operador_sql:
                    where_clauses.append(f"({coluna} IS NOT NULL AND {coluna} != '')")
                else:
                    where_clauses.append(f"({coluna} IS NULL OR {coluna} = '')")
            
            elif operador_sql == 'BETWEEN' and tipo_dado == 'data':
                where_clauses.append(f"{coluna} BETWEEN %s AND %s")
                params.append(filtro['val'][0]) # Data Ini
                params.append(filtro['val'][1]) # Data Fim
            
            else:
                # Texto com ; (OR)
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
    """Carrega todos os dados vinculados a um CPF (Formato Dict para Edição)"""
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
                    if v is None and k != 'data_nascimento':
                        d_pessoal[k] = ""
                dados['pessoal'] = d_pessoal
            else:
                dados['pessoal'] = {}

            # 2. Telefones
            cur.execute("SELECT id, telefone FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s ORDER BY id", (cpf,))
            dados['telefones'] = [{'id': r[0], 'valor': r[1] or ""} for r in cur.fetchall()]

            # 3. Emails
            cur.execute("SELECT id, email FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf = %s ORDER BY id", (cpf,))
            dados['emails'] = [{'id': r[0], 'valor': r[1] or ""} for r in cur.fetchall()]

            # 4. Endereços
            cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco WHERE cpf = %s ORDER BY id", (cpf,))
            cols_end = [desc[0] for desc in cur.description]
            dados['enderecos'] = []
            for r in cur.fetchall():
                d_end = dict(zip(cols_end, r))
                for k, v in d_end.items():
                    if v is None: d_end[k] = ""
                dados['enderecos'].append(d_end)

            # 5. Convênios
            cur.execute("SELECT id, convenio FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s ORDER BY id", (cpf,))
            dados['convenios'] = [{'id': r[0], 'valor': r[1] or ""} for r in cur.fetchall()]
            
    except Exception as e:
        st.error(f"Erro ao carregar cliente: {e}")
    finally:
        conn.close()
    
    return dados

def salvar_novo_cliente(dados_form):
    """Insere o registro básico na tabela principal"""
    conn = get_db_connection()
    if not conn: return False
    
    dados_limpos = {
        "nome": limpar_texto(dados_form.get('nome')),
        "cpf": limpar_texto(dados_form.get('cpf')),
        "identidade": limpar_texto(dados_form.get('identidade')),
        "sexo": limpar_texto(dados_form.get('sexo')),
        "nome_mae": limpar_texto(dados_form.get('nome_mae')),
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
    """Insere dados novos nas tabelas satélites"""
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
                    (cpf, cep, rua, cidade, uf) VALUES (%s, %s, %s, %s, %s)
                """, (cpf, limpar_texto(dados.get('cep')), limpar_texto(dados.get('rua')), 
                      limpar_texto(dados.get('cidade')), limpar_texto(dados.get('uf'))))
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
    """Atualiza dados pessoais e listas"""
    conn = get_db_connection()
    if not conn: return False
    
    try:
        with conn.cursor() as cur:
            pessoal = dados_editados['pessoal']
            cur.execute("""
                UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_cpf
                SET nome = %s, data_nascimento = %s, identidade = %s, 
                    sexo = %s, cnh = %s, titulo_eleitoral = %s, nome_mae = %s
                WHERE cpf = %s
            """, (
                limpar_texto(pessoal['nome']), pessoal['data_nascimento'], limpar_texto(pessoal['identidade']),
                limpar_texto(pessoal['sexo']), limpar_texto(pessoal['cnh']), 
                limpar_texto(pessoal['titulo_eleitoral']), limpar_texto(pessoal['nome_mae']),
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
    """Exclusão em cascata"""
    conn = get_db_connection()
    if not conn: return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf WHERE cpf = %s", (cpf,))
            cur.execute("DELETE FROM sistema_consulta.sistema_consulta_cpf WHERE cpf = %s", (cpf,))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao excluir cliente: {e}")
        return False
    finally:
        conn.close()

# --- COMPONENTE MODAL ---
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

# --- DIALOG EXCLUSÃO ---
@st.dialog("⚠️ Confirmar Exclusão")
def modal_confirmar_exclusao(cpf):
    st.warning("Tem certeza que deseja excluir TODO o cadastro deste cliente? Essa ação não pode ser desfeita.")
    if st.button("🚨 SIM, EXCLUIR DEFINITIVAMENTE", type="primary"):
        if excluir_cliente_total(cpf):
            st.success("Cliente excluído com sucesso!")
            st.session_state['cliente_ativo_cpf'] = None
            st.session_state['modo_visualizacao'] = None
            st.session_state['resultados_pesquisa'] = []
            time.sleep(1.5)
            st.rerun()

# --- INTERFACE GRÁFICA ---

def tela_pesquisa():
    st.markdown("#### 🔍 Buscar Cliente")
    
    if 'campos_selecionados_pesquisa' not in st.session_state:
        st.session_state['campos_selecionados_pesquisa'] = []

    tab1, tab2 = st.tabs(["Pesquisa Rápida", "Pesquisa Completa"])
    
    with tab1:
        c1, c2 = st.columns([4, 1])
        termo = c1.text_input("Digite CPF, Nome ou Telefone", placeholder="Ex: 000.000.000-00 ou João")
        if c2.button("Pesquisar", use_container_width=True):
            if len(termo) < 3:
                st.warning("Digite min. 3 caracteres.")
            else:
                resultados = buscar_cliente_rapida(termo)
                st.session_state['resultados_pesquisa'] = resultados
                if not resultados: st.warning("Nenhum cliente localizado.")

    # --- TAB 2: PESQUISA COMPLETA ---
    with tab2:
        # 1. SELEÇÃO DE COLUNAS
        for grupo, campos in MAPA_CAMPOS_PESQUISA.items():
            with st.expander(f"📂 {grupo}", expanded=False):
                cols_layout = st.columns(3)
                for i, (nome_campo, config) in enumerate(campos.items()):
                    # CORREÇÃO: Checkbox com 'value' vinculado ao estado
                    is_selected = nome_campo in st.session_state['campos_selecionados_pesquisa']
                    if cols_layout[i % 3].checkbox(nome_campo, value=is_selected, key=f"chk_{nome_campo}"):
                        if not is_selected:
                            st.session_state['campos_selecionados_pesquisa'].append(nome_campo)
                    else:
                        if is_selected:
                            st.session_state['campos_selecionados_pesquisa'].remove(nome_campo)

        st.divider()

        col_filtros, col_resultados = st.columns([1.2, 2.5])
        filtros_para_query = []

        with col_filtros:
            # --- ÁREA DE FILTROS (LARANJA) ---
            with st.warning("🌪️ Filtros Ativos"):
                if not st.session_state['campos_selecionados_pesquisa']:
                    st.info("Nenhuma coluna selecionada.")
                
                # Validação para remover campos órfãos (ex: se o código mudou chaves)
                campos_validos = []
                for nc in st.session_state['campos_selecionados_pesquisa']:
                    encontrado = False
                    for grp in MAPA_CAMPOS_PESQUISA.values():
                        if nc in grp:
                            encontrado = True
                            break
                    if encontrado: campos_validos.append(nc)
                st.session_state['campos_selecionados_pesquisa'] = campos_validos

                for nome_campo in st.session_state['campos_selecionados_pesquisa']:
                    config_campo = None
                    for grp in MAPA_CAMPOS_PESQUISA.values():
                        if nome_campo in grp:
                            config_campo = grp[nome_campo]
                            break
                    
                    if config_campo:
                        st.markdown(f"**{nome_campo}**")
                        
                        # Definição do Tipo de Operador
                        tipo_ops = 'texto'
                        if config_campo['tipo'] == 'data': tipo_ops = 'data'
                        elif config_campo['tipo'] == 'numero_calculado': tipo_ops = 'numero'
                        elif config_campo['tipo'] in ['texto_vinculado', 'endereco_vinculado']: tipo_ops = 'texto'

                        opcoes_ops = list(OPERADORES_SQL[tipo_ops].keys())
                        
                        operador_escolhido = st.selectbox("Op", opcoes_ops, key=f"op_{nome_campo}", label_visibility="collapsed")
                        desc_op = OPERADORES_SQL[tipo_ops][operador_escolhido]['desc']
                        st.caption(f"ℹ️ {desc_op}")

                        sql_op_code = OPERADORES_SQL[tipo_ops][operador_escolhido]['sql']
                        
                        # Inputs
                        valor_final = None
                        
                        if "IS NULL" in sql_op_code or "IS NOT NULL" in sql_op_code:
                            valor_final = "ignore" # Marcador
                        
                        elif config_campo['tipo'] == 'data':
                            c_d1, c_d2 = st.columns(2)
                            d1 = c_d1.date_input("De", value=None, key=f"d1_{nome_campo}", format="DD/MM/YYYY")
                            d2 = c_d2.date_input("Até", value=None, key=f"d2_{nome_campo}", format="DD/MM/YYYY")
                            if d1 and d2: valor_final = [converter_data_iso(d1), converter_data_iso(d2)]
                            elif d1: valor_final = [converter_data_iso(d1), converter_data_iso(d1)] 
                            
                        elif config_campo['tipo'] == 'numero_calculado':
                            if operador_escolhido == "Entre (><)":
                                c_n1, c_n2 = st.columns(2)
                                n1 = c_n1.number_input("De", step=1, key=f"n1_{nome_campo}")
                                n2 = c_n2.number_input("Até", step=1, key=f"n2_{nome_campo}")
                                valor_final = [n1, n2]
                            else:
                                valor_final = st.number_input("Valor", step=1, key=f"n_val_{nome_campo}")
                        
                        else:
                            valor_input = st.text_input("Valor", key=f"val_{nome_campo}", placeholder="Ex: joao;maria")
                            if valor_input: valor_final = valor_input

                        if valor_final is not None:
                            filtros_para_query.append({
                                'col': config_campo['col'],
                                'op': sql_op_code,
                                'mask': OPERADORES_SQL[tipo_ops][operador_escolhido]['mask'],
                                'val': valor_final,
                                'tipo': config_campo['tipo'],
                                'table': config_campo.get('table')
                            })
                        st.divider()

                if filtros_para_query or any("IS NULL" in f['op'] for f in filtros_para_query):
                    if st.button("🚀 EXECUTAR PESQUISA", type="primary", use_container_width=True):
                        res_completa = buscar_cliente_dinamica(filtros_para_query)
                        st.session_state['resultados_pesquisa'] = res_completa
                        if not res_completa:
                            st.warning("Nenhum registro encontrado.")

        # 3. RESULTADOS
        with col_resultados:
            if 'resultados_pesquisa' in st.session_state and st.session_state['resultados_pesquisa']:
                st.markdown(f"### 📋 Resultados: {len(st.session_state['resultados_pesquisa'])}")
                cols_head = st.columns([1, 4, 2, 2, 1])
                cols_head[0].write("**ID**"); cols_head[1].write("**Nome**"); cols_head[2].write("**CPF**"); cols_head[3].write("**RG**"); cols_head[4].write("**Ação**")
                st.divider()

                for row in st.session_state['resultados_pesquisa']:
                    c = st.columns([1, 4, 2, 2, 1])
                    c[0].write(str(row[0]))
                    c[1].write(row[1])
                    c[2].write(row[2])
                    c[3].write(row[3])
                    if c[4].button("🔎", key=f"btn_res_{row[0]}"):
                        st.session_state['cliente_ativo_cpf'] = row[2]
                        st.session_state['modo_visualizacao'] = 'visualizar'
                        st.session_state['modo_edicao'] = False
                        st.rerun()

    st.divider()
    if st.button("➕ NOVO CADASTRO", type="primary"):
        st.session_state['cliente_ativo_cpf'] = None
        st.session_state['modo_visualizacao'] = 'novo'
        st.rerun()

def tela_ficha_cliente(cpf, modo='visualizar'):
    if 'modo_edicao' not in st.session_state:
        st.session_state['modo_edicao'] = False

    # --- BOTÃO VOLTAR (TOPO) ---
    if st.button("⬅️ Voltar"):
        st.session_state['cliente_ativo_cpf'] = None
        st.session_state['modo_visualizacao'] = None
        st.session_state['modo_edicao'] = False
        st.rerun()

    # --- MODO NOVO CADASTRO ---
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
            if st.form_submit_button("💾 Salvar"):
                if salvar_novo_cliente({"nome": nome, "cpf": cpf_in, "data_nascimento": nasc, "identidade": rg, "sexo": sexo, "nome_mae": mae}):
                    st.success("Cadastrado!")
                    st.session_state['cliente_ativo_cpf'] = cpf_in
                    st.session_state['modo_visualizacao'] = 'visualizar'
                    time.sleep(1)
                    st.rerun()
        return

    # --- CARREGA DADOS ---
    dados = carregar_dados_cliente_completo(cpf)
    pessoal = dados.get('pessoal', {})
    
    # --- LAYOUT CABEÇALHO (NOME + BOTÕES LADO A LADO) ---
    c_head_nome, c_head_btns = st.columns([0.6, 0.4])
    
    with c_head_nome:
        st.markdown(f"## 👤 {pessoal.get('nome', 'Sem Nome')}")
        st.markdown(f"**CPF:** {pessoal.get('cpf', '')} {'🔒 (Não editável)' if st.session_state['modo_edicao'] else ''}")

    with c_head_btns:
        st.write("") # Espaçamento
        st.write("") 
        c_btn_edit, c_btn_del = st.columns(2)
        
        with c_btn_edit:
            if st.session_state['modo_edicao']:
                 if st.button("👁️ Exibir", help="Sair do modo edição", use_container_width=True):
                     st.session_state['modo_edicao'] = False
                     st.rerun()
            else:
                if st.button("✏️ Editar", type="primary", use_container_width=True):
                    st.session_state['modo_edicao'] = True
                    st.rerun()
        
        with c_btn_del:
            if st.button("🗑️ Excluir", type="primary", use_container_width=True):
                modal_confirmar_exclusao(cpf)

    st.divider()

    # --- CONTAINER ISOLADO (FIX RENDERIZAÇÃO) ---
    chave_container = f"container_ficha_{'edicao' if st.session_state['modo_edicao'] else 'visualizacao'}"
    
    with st.container(border=False):
        with st.container(key=chave_container):
            
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
                                "cnh": e_cnh, "titulo_eleitoral": e_titulo, "sexo": e_sexo, "nome_mae": e_mae
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

            # --- MODO VISUALIZAÇÃO ---
            else:
                st.markdown("### 📄 Dados Pessoais")
                col1, col2, col3 = st.columns(3)
                col1.text_input("Nome", value=pessoal.get('nome',''), disabled=True)
                col2.text_input("RG", value=pessoal.get('identidade',''), disabled=True)
                
                data_vis = pessoal.get('data_nascimento')
                if data_vis: data_vis = data_vis.strftime('%d/%m/%Y')
                col3.text_input("Data Nasc.", value=str(data_vis), disabled=True)
                
                col4, col5 = st.columns(2)
                col4.text_input("CNH", value=pessoal.get('cnh',''), disabled=True)
                col5.text_input("Título Eleitor", value=pessoal.get('titulo_eleitoral',''), disabled=True)
                st.text_input("Nome da Mãe", value=pessoal.get('nome_mae',''), disabled=True)

                st.divider()
                c_contato, c_endereco = st.columns(2)
                with c_contato:
                    st.markdown("### 📞 Contatos")
                    for tel in dados.get('telefones', []):
                        st.code(f"📱 {tel['valor']}")
                    for email in dados.get('emails', []):
                        st.text(f"✉️ {email['valor']}")

                with c_endereco:
                    st.markdown("### 🏠 Endereços")
                    for end in dados.get('enderecos', []):
                        st.info(f"{end.get('rua')}, {end.get('cidade')}/{end.get('uf')} - CEP: {end.get('cep')}")

                st.divider()
                st.markdown("### 💼 Convênios")
                st.write(", ".join([c['valor'] for c in dados.get('convenios', [])]))
                st.divider()

                col_ins_lat, _ = st.columns([1, 4])
                if col_ins_lat.button("➕ Inserir Dados Extras"):
                    modal_inserir_dados(cpf, pessoal.get('nome'))

def app_cadastro():
    if 'modo_visualizacao' not in st.session_state:
        st.session_state['modo_visualizacao'] = None
    
    if st.session_state['modo_visualizacao'] == 'visualizar':
        cpf_ativo = st.session_state.get('cliente_ativo_cpf')
        if cpf_ativo:
            tela_ficha_cliente(cpf_ativo, modo='visualizar')
        else:
            st.session_state['modo_visualizacao'] = None
            st.rerun()
    elif st.session_state['modo_visualizacao'] == 'novo':
        tela_ficha_cliente(None, modo='novo')
    else:
        tela_pesquisa()

if __name__ == "__main__":
    app_cadastro()