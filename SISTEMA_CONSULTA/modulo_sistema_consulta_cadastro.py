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

# --- FUNÇÕES AUXILIARES DE LIMPEZA ---
def limpar_texto(valor):
    """
    Converte None, 'None', 'null' ou espaços vazios para string vazia ''.
    """
    if valor is None:
        return ""
    s_valor = str(valor).strip()
    if s_valor.lower() in ['none', 'null']:
        return ""
    return s_valor

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
                # Limpeza de None para visualização
                for k, v in d_pessoal.items():
                    if v is None and k != 'data_nascimento':
                        d_pessoal[k] = ""
                dados['pessoal'] = d_pessoal
            else:
                dados['pessoal'] = {}

            # 2. Telefones (COM ID E VALOR)
            cur.execute("SELECT id, telefone FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE cpf = %s ORDER BY id", (cpf,))
            dados['telefones'] = [{'id': r[0], 'valor': r[1] or ""} for r in cur.fetchall()]

            # 3. Emails (COM ID E VALOR)
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

            # 5. Convênios (COM ID E VALOR)
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
    
    # Aplica limpeza
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
    """Insere dados novos nas tabelas satélites com verificação e limpeza"""
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
    """Atualiza dados pessoais e listas (telefones, emails) com limpeza"""
    conn = get_db_connection()
    if not conn: return False
    
    try:
        with conn.cursor() as cur:
            # 1. Atualizar Dados Pessoais
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
            
            # 2. Atualizar Telefones
            for item in dados_editados.get('telefones', []):
                val = limpar_texto(item['valor'])
                if not val: # Se vazio, exclui
                    cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_telefone WHERE id = %s", (item['id'],))
                else:
                    cur.execute("UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_telefone SET telefone = %s WHERE id = %s", (val, item['id']))
            
            # 3. Atualizar Emails
            for item in dados_editados.get('emails', []):
                val = limpar_texto(item['valor'])
                if not val:
                    cur.execute("DELETE FROM sistema_consulta.sistema_consulta_dados_cadastrais_email WHERE id = %s", (item['id'],))
                else:
                    cur.execute("UPDATE sistema_consulta.sistema_consulta_dados_cadastrais_email SET email = %s WHERE id = %s", (val, item['id']))
            
            # 4. Atualizar Convênios
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
    """Exclusão em cascata de todos os dados do CPF"""
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

    if 'resultados_pesquisa' in st.session_state and st.session_state['resultados_pesquisa']:
        st.divider()
        st.markdown(f"**Resultados:** {len(st.session_state['resultados_pesquisa'])}")
        
        cols = st.columns([1, 4, 2, 2, 1])
        cols[0].write("**ID**"); cols[1].write("**Nome**"); cols[2].write("**CPF**"); cols[3].write("**RG**"); cols[4].write("**Ver**")
        
        for row in st.session_state['resultados_pesquisa']:
            c = st.columns([1, 4, 2, 2, 1])
            c[0].write(str(row[0]))
            c[1].write(row[1])
            c[2].write(row[2])
            c[3].write(row[3])
            if c[4].button("🔎", key=f"btn_{row[0]}"):
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

    col_back, col_space, col_view, col_edit, col_del = st.columns([1.5, 3, 1.5, 1.5, 1.5])
    
    if col_back.button("⬅️ Voltar"):
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
    
    with col_view:
        if st.session_state['modo_edicao']:
             if st.button("👁️ Exibir", help="Sair do modo edição"):
                 st.session_state['modo_edicao'] = False
                 st.rerun()
    
    with col_edit:
        if not st.session_state['modo_edicao']:
            if st.button("✏️ Editar", type="secondary"):
                st.session_state['modo_edicao'] = True
                st.rerun()
    
    with col_del:
        if st.button("🗑️ Excluir", type="primary"):
            modal_confirmar_exclusao(cpf)

    st.markdown(f"## 👤 {pessoal.get('nome', 'Sem Nome')}")
    st.markdown(f"**CPF:** {pessoal.get('cpf', '')} {'🔒 (Não editável)' if st.session_state['modo_edicao'] else ''}")
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