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

def carregar_dados_cliente_completo(cpf):
    """Carrega todos os dados vinculados a um CPF"""
    conn = get_db_connection()
    if not conn: return {}
    
    dados = {}
    try:
        with conn.cursor() as cur:
            # 1. Dados Pessoais
            cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_cadastrais_cpf WHERE cpf = %s", (cpf,))
            cols_pessoais = [desc[0] for desc in cur.description]
            row_pessoais = cur.fetchone()
            dados['pessoal'] = dict(zip(cols_pessoais, row_pessoais)) if row_pessoais else {}

            # 2. Telefones
            cur.execute("SELECT telefone FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s", (cpf,))
            dados['telefones'] = [r[0] for r in cur.fetchall()]

            # 3. Emails
            cur.execute("SELECT email FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE cpf = %s", (cpf,))
            dados['emails'] = [r[0] for r in cur.fetchall()]

            # 4. Endereços
            cur.execute("SELECT * FROM sistema_consulta.sistema_consulta_dados_cadastrais_endereco WHERE cpf = %s", (cpf,))
            cols_end = [desc[0] for desc in cur.description]
            dados['enderecos'] = [dict(zip(cols_end, r)) for r in cur.fetchall()]

            # 5. Convênios
            cur.execute("SELECT convenio FROM sistema_consulta.sistema_consulta_dados_cadastrais_convenio WHERE cpf = %s", (cpf,))
            dados['convenios'] = [r[0] for r in cur.fetchall()]
            
    except Exception as e:
        st.error(f"Erro ao carregar cliente: {e}")
    finally:
        conn.close()
    
    return dados

def salvar_novo_cliente(dados_form):
    """Insere o registro básico na tabela principal"""
    conn = get_db_connection()
    if not conn: return False
    
    try:
        with conn.cursor() as cur:
            cols = list(dados_form.keys())
            vals = list(dados_form.values())
            placeholders = ", ".join(["%s"] * len(cols))
            columns = ", ".join(cols)
            
            sql = f"INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_cpf ({columns}) VALUES ({placeholders})"
            cur.execute(sql, vals)
            
            # Insere também na tabela de chaves CPF
            cur.execute("INSERT INTO sistema_consulta.sistema_consulta_cpf (cpf) VALUES (%s) ON CONFLICT DO NOTHING", (dados_form['cpf'],))
            
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False
    finally:
        conn.close()

def inserir_dado_extra(tipo, cpf, dados):
    """Função genérica para inserir dados nas tabelas satélites"""
    conn = get_db_connection()
    if not conn: return False
    
    try:
        with conn.cursor() as cur:
            if tipo == "Telefone":
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_telefone (cpf, telefone) VALUES (%s, %s)", (cpf, dados['valor']))
            elif tipo == "E-mail":
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_email (cpf, email) VALUES (%s, %s)", (cpf, dados['valor']))
            elif tipo == "Endereço":
                cur.execute("""
                    INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_endereco 
                    (cpf, cep, rua, cidade, uf) VALUES (%s, %s, %s, %s, %s)
                """, (cpf, dados['cep'], dados['rua'], dados['cidade'], dados['uf']))
            elif tipo == "Convênio":
                cur.execute("INSERT INTO sistema_consulta.sistema_consulta_dados_cadastrais_convenio (cpf, convenio) VALUES (%s, %s)", (cpf, dados['valor']))
            
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao inserir dado extra: {e}")
        return False
    finally:
        conn.close()

# --- INTERFACE GRÁFICA ---

def tela_pesquisa():
    st.markdown("#### 🔍 Buscar Cliente")
    
    tab1, tab2 = st.tabs(["Pesquisa Rápida", "Pesquisa Completa"])
    
    with tab1:
        c1, c2 = st.columns([4, 1])
        termo = c1.text_input("Digite CPF, Nome ou Telefone", placeholder="Ex: 000.000.000-00 ou João da Silva")
        if c2.button("Pesquisar", use_container_width=True):
            if len(termo) < 3:
                st.warning("Digite pelo menos 3 caracteres.")
            else:
                resultados = buscar_cliente_rapida(termo)
                st.session_state['resultados_pesquisa'] = resultados
                if not resultados:
                    st.warning("Nenhum cliente localizado.")
                    st.session_state['exibir_novo_cadastro'] = True 
    
    with tab2:
        st.info("Funcionalidade de Pesquisa Completa em desenvolvimento.")

    # Resultados
    if 'resultados_pesquisa' in st.session_state and st.session_state['resultados_pesquisa']:
        st.divider()
        st.markdown(f"**Resultados Encontrados:** {len(st.session_state['resultados_pesquisa'])}")
        
        cols = st.columns([1, 4, 2, 2, 1])
        cols[0].write("**ID**")
        cols[1].write("**Nome**")
        cols[2].write("**CPF**")
        cols[3].write("**RG**")
        cols[4].write("**Ação**")
        
        for row in st.session_state['resultados_pesquisa']:
            c = st.columns([1, 4, 2, 2, 1])
            c[0].write(str(row[0]))
            c[1].write(row[1])
            c[2].write(row[2])
            c[3].write(row[3])
            if c[4].button("🔎", key=f"btn_ver_{row[0]}"):
                st.session_state['cliente_ativo_cpf'] = row[2]
                st.session_state['modo_visualizacao'] = 'visualizar'
                st.rerun()

    st.divider()
    if st.button("➕ NOVO CADASTRO", type="primary"):
        st.session_state['cliente_ativo_cpf'] = None
        st.session_state['modo_visualizacao'] = 'novo'
        st.rerun()

def tela_ficha_cliente(cpf, modo='visualizar'):
    
    # Botão Voltar
    if st.button("⬅️ Voltar"):
        st.session_state['cliente_ativo_cpf'] = None
        st.session_state['modo_visualizacao'] = None
        st.session_state['sidebar_inserir_ativo'] = False
        st.rerun()
    
    # --- MODO NOVO CADASTRO ---
    if modo == 'novo':
        st.markdown("## ✨ Novo Cadastro de Cliente")
        with st.form("form_novo_cliente"):
            st.markdown("### Dados Pessoais")
            c1, c2, c3 = st.columns(3)
            nome = c1.text_input("Nome Completo*")
            cpf_input = c2.text_input("CPF*")
            
            # AJUSTE: DATA COM LIMITES E FORMATO BR
            nasc = c3.date_input(
                "Data Nascimento", 
                value=None,
                min_value=date(1900, 1, 1),
                max_value=date(2050, 1, 1),
                format="DD/MM/YYYY"
            )
            
            c4, c5, c6 = st.columns(3)
            rg = c4.text_input("Identidade (RG)")
            sexo = c5.selectbox("Sexo", ["Masculino", "Feminino", "Outros"])
            mae = c6.text_input("Nome da Mãe")
            
            if st.form_submit_button("💾 Salvar Cadastro"):
                if not nome or not cpf_input:
                    st.error("Nome e CPF são obrigatórios.")
                else:
                    dados = {
                        "nome": nome, "cpf": cpf_input, "data_nascimento": nasc,
                        "identidade": rg, "sexo": sexo
                    }
                    if salvar_novo_cliente(dados):
                        st.success("Cliente cadastrado com sucesso!")
                        st.session_state['cliente_ativo_cpf'] = cpf_input
                        st.session_state['modo_visualizacao'] = 'visualizar'
                        time.sleep(1)
                        st.rerun()
        return

    # --- MODO VISUALIZAR ---
    dados_completos = carregar_dados_cliente_completo(cpf)
    pessoal = dados_completos.get('pessoal', {})

    st.markdown(f"## 👤 {pessoal.get('nome', 'Cliente Sem Nome')}")
    st.markdown(f"**CPF:** {pessoal.get('cpf', '')}")
    
    st.divider()
    
    # 1. DADOS PESSOAIS (Visualização)
    st.markdown("### 📄 Dados Pessoais")
    col1, col2, col3 = st.columns(3)
    col1.text_input("Nome", value=pessoal.get('nome',''), disabled=True)
    col2.text_input("RG", value=pessoal.get('identidade',''), disabled=True)
    
    # Formata data para exibir visualmente
    data_nasc_visual = pessoal.get('data_nascimento')
    if data_nasc_visual:
        data_nasc_visual = data_nasc_visual.strftime('%d/%m/%Y')
    col3.text_input("Data Nasc.", value=str(data_nasc_visual), disabled=True)
    
    col4, col5 = st.columns(2)
    col4.text_input("CNH", value=pessoal.get('cnh',''), disabled=True)
    col5.text_input("Título Eleitor", value=pessoal.get('titulo_eleitoral',''), disabled=True)

    st.divider()
    
    # 2. CONTATOS E ENDEREÇOS
    c_contato, c_endereco = st.columns(2)
    
    with c_contato:
        st.markdown("### 📞 Contatos")
        if dados_completos.get('telefones'):
            for tel in dados_completos['telefones']:
                st.code(f"📱 {tel}")
        else:
            st.info("Sem telefones.")
            
        st.markdown("#### 📧 E-mails")
        if dados_completos.get('emails'):
            for email in dados_completos['emails']:
                st.text(f"✉️ {email}")

    with c_endereco:
        st.markdown("### 🏠 Endereços")
        if dados_completos.get('enderecos'):
            for end in dados_completos['enderecos']:
                texto_end = f"{end.get('rua')}, {end.get('cidade')}/{end.get('uf')} - CEP: {end.get('cep')}"
                st.info(texto_end)
        else:
            st.info("Sem endereços.")

    st.divider()
    
    # 3. CONVÊNIOS
    st.markdown("### 💼 Convênios")
    if dados_completos.get('convenios'):
        st.write(", ".join(dados_completos['convenios']))
    else:
        st.caption("Nenhum convênio vinculado.")

    st.divider()

    # --- BARRA DE AÇÕES ---
    col_btns = st.columns([1, 1, 1, 3])
    with col_btns[1]:
        # Botão que ativa a Sidebar
        if st.button("➕ Inserir Dados"):
            st.session_state['sidebar_inserir_ativo'] = True

    # --- SIDEBAR DE INSERÇÃO (AJUSTE SOLICITADO) ---
    if st.session_state.get('sidebar_inserir_ativo'):
        with st.sidebar:
            st.markdown("### ➕ Inserir Dados")
            st.info(f"Cliente: {pessoal.get('nome')}")
            
            # Opções para digitar
            tipo_insercao = st.selectbox("Selecione o Tipo", ["Telefone", "E-mail", "Endereço", "Convênio"])
            
            with st.form("form_insercao_lateral"):
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
                
                # Botão de confirmação
                if st.form_submit_button("✅ Salvar Inclusão"):
                    # Simula a inserção (ou chama a função real se ajustada)
                    sucesso = inserir_dado_extra(tipo_insercao, cpf, dados_submit)
                    
                    if sucesso:
                        st.success(f"{tipo_insercao} inserido com sucesso!")
                        time.sleep(1)
                        st.session_state['sidebar_inserir_ativo'] = False
                        st.rerun()
                    else:
                        st.error("Erro ao inserir.")
            
            if st.button("Fechar Aba"):
                st.session_state['sidebar_inserir_ativo'] = False
                st.rerun()

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