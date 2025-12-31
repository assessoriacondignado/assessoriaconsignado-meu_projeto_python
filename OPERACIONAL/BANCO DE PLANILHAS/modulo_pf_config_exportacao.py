import streamlit as st
import pandas as pd
import time
import psycopg2
from datetime import date
import modulo_pf_cadastro as pf_core

# =============================================================================
# MAPEAMENTO DE TABELAS BRUTAS (Chave -> Tabela SQL)
# =============================================================================
MAPA_TABELAS_BRUTAS = {
    "pf_telefones": "banco_pf.pf_telefones",
    "pf_e-mails": "banco_pf.pf_emails",
    "pf_endereços": "banco_pf.pf_enderecos",
    "pf_convenio": "banco_pf.cpf_convenio", # Baseado no padrão de importação
    "pf_campanhas": "banco_pf.pf_campanhas",
    "pf_campanhas_exportação": "banco_pf.pf_campanhas",
    "pf_dados": "banco_pf.pf_dados",
    "pf_contratos": "banco_pf.pf_contratos",
    "pf_emprego_renda": "banco_pf.pf_emprego_renda",
    "pf_historico_importações": "banco_pf.pf_historico_importacoes",
    "pf_maricula_dados_clt": "banco_pf.pf_matricula_dados_clt",
    "pf_modelos_exportacao": "banco_pf.pf_modelos_exportacao",
    "pf_modelos_filtro_fixo": "banco_pf.pf_modelos_filtro_fixo",
    "pf_péradpres_de_filtro": "banco_pf.pf_operadores_de_filtro",
    "pf_referecias": "banco_pf.pf_referencias",
    "pf_tipo_exportacao": "banco_pf.pf_modelos_exportacao" 
}

# =============================================================================
# PARTE 1: FUNÇÕES DE BANCO (CRUD) E MOTOR DE EXPORTAÇÃO
# =============================================================================

def listar_modelos_ativos():
    conn = pf_core.get_conn()
    if conn:
        try:
            # Seleciona a coluna correta do banco
            query = "SELECT id, nome_modelo, descricao, data_criacao, status, codigo_de_consulta FROM banco_pf.pf_modelos_exportacao WHERE status='ATIVO' ORDER BY id"
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Erro ao listar: {e}")
            conn.close()
    return pd.DataFrame()

def salvar_modelo(nome, chave, desc):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            sql = """
                INSERT INTO banco_pf.pf_modelos_exportacao 
                (nome_modelo, codigo_de_consulta, descricao, status, data_criacao) 
                VALUES (%s, %s, %s, 'ATIVO', CURRENT_DATE)
            """
            cur.execute(sql, (nome, chave, desc))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao salvar SQL: {e}")
            conn.close()
            return False
    return False

def atualizar_modelo(id_mod, nome, chave, desc):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            sql = """
                UPDATE banco_pf.pf_modelos_exportacao 
                SET nome_modelo=%s, codigo_de_consulta=%s, descricao=%s 
                WHERE id=%s
            """
            cur.execute(sql, (nome, chave, desc, id_mod))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}")
            conn.close()
            return False
    return False

def excluir_modelo(id_mod):
    conn = pf_core.get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM banco_pf.pf_modelos_exportacao WHERE id=%s", (id_mod,))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    return False

def gerar_dataframe_por_modelo(id_modelo, lista_cpfs):
    conn = pf_core.get_conn()
    if not conn: return pd.DataFrame()
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo_de_consulta FROM banco_pf.pf_modelos_exportacao WHERE id=%s", (int(id_modelo),))
        res = cur.fetchone()
        codigo_consulta = res[0] if res else ""
        
        # 1. Verifica se é um modelo de Tabela Bruta (Mapeado)
        if codigo_consulta in MAPA_TABELAS_BRUTAS:
            tabela_sql = MAPA_TABELAS_BRUTAS[codigo_consulta]
            return _motor_tabela_bruta(conn, tabela_sql, lista_cpfs)
        
        # 2. Caso contrário, usa o motor de layout fixo completo (Padrão)
        else:
            if not lista_cpfs: return pd.DataFrame() # Layout fixo precisa de CPFs
            return _motor_layout_fixo_completo(conn, lista_cpfs)
            
    except Exception as e:
        st.error(f"Erro no roteamento: {e}")
        return pd.DataFrame()

def _motor_tabela_bruta(conn, tabela_sql, lista_cpfs):
    """
    Exporta TODAS as colunas de uma tabela específica.
    Se a tabela tiver coluna CPF/Matrícula, filtra pelos CPFs da pesquisa.
    Caso contrário, exporta tudo (Cuidado com tabelas grandes).
    """
    try:
        # Descobre as colunas da tabela
        cur = conn.cursor()
        # Tratamento para schema.tabela
        if '.' in tabela_sql:
            schema, table = tabela_sql.split('.')
        else:
            schema, table = 'public', tabela_sql
            
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        colunas = [r[0] for r in cur.fetchall()]
        
        if not colunas:
            st.warning(f"Tabela {tabela_sql} não encontrada ou sem colunas.")
            return pd.DataFrame()

        cols_str = ", ".join(colunas)
        query = f"SELECT {cols_str} FROM {tabela_sql}"
        params = []
        
        # Lógica de Filtro Inteligente
        # Se tivermos lista de CPFs, tentamos filtrar para não trazer o banco todo
        if lista_cpfs:
            if 'cpf' in colunas:
                placeholders = ",".join(["%s"] * len(lista_cpfs))
                query += f" WHERE cpf IN ({placeholders})"
                params = tuple(lista_cpfs)
            
            elif 'cpf_ref' in colunas:
                placeholders = ",".join(["%s"] * len(lista_cpfs))
                query += f" WHERE cpf_ref IN ({placeholders})"
                params = tuple(lista_cpfs)
            
            elif 'matricula' in colunas or 'matricula_ref' in colunas:
                # Tenta buscar as matrículas desses CPFs primeiro
                ph_cpf = ",".join(["%s"] * len(lista_cpfs))
                sql_mat = f"SELECT matricula FROM banco_pf.pf_emprego_renda WHERE cpf IN ({ph_cpf})"
                # Precisamos de um cursor novo ou executar direto no pandas
                df_mats = pd.read_sql(sql_mat, conn, params=tuple(lista_cpfs))
                
                if not df_mats.empty:
                    mats = df_mats['matricula'].dropna().unique().tolist()
                    if mats:
                        ph_mat = ",".join(["%s"] * len(mats))
                        col_mat = 'matricula' if 'matricula' in colunas else 'matricula_ref'
                        query += f" WHERE {col_mat} IN ({ph_mat})"
                        params = tuple(mats)
                    else:
                        # CPFs não tem matrícula, então resultado da tabela de contratos deve ser vazio
                        return pd.DataFrame(columns=colunas)
                else:
                    return pd.DataFrame(columns=colunas)

        # Se não tiver filtro (tabela de config) ou não tiver lista_cpfs, faz SELECT ALL
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df
        
    except Exception as e:
        st.error(f"Erro ao exportar tabela bruta {tabela_sql}: {e}")
        conn.close()
        return pd.DataFrame()

def _motor_layout_fixo_completo(conn, lista_cpfs):
    try:
        placeholders = ",".join(["%s"] * len(lista_cpfs))
        params = tuple(lista_cpfs)

        # 1. Dados Pessoais
        df_dados = pd.read_sql(f"SELECT * FROM banco_pf.pf_dados WHERE cpf IN ({placeholders})", conn, params=params)
        
        # REMOVE COLUNAS INDESEJADAS
        cols_rem = ['data_criacao', 'importacao_id', 'id_campanha']
        df_dados.drop(columns=cols_rem, inplace=True, errors='ignore')
        
        df_dados['cpf'] = df_dados['cpf'].apply(pf_core.formatar_cpf_visual)

        # 2. Dados Satélites
        df_tel = pd.read_sql(f"SELECT cpf, numero, tag_whats, tag_qualificacao FROM banco_pf.pf_telefones WHERE cpf IN ({placeholders})", conn, params=params)
        df_tel['numero'] = df_tel['numero'].apply(lambda x: pf_core.limpar_apenas_numeros(x))
        
        df_mail = pd.read_sql(f"SELECT cpf, email FROM banco_pf.pf_emails WHERE cpf IN ({placeholders})", conn, params=params)
        df_end = pd.read_sql(f"SELECT cpf, rua, bairro, cidade, uf, cep FROM banco_pf.pf_enderecos WHERE cpf IN ({placeholders})", conn, params=params)

        # Função de Pivotagem Fixa
        def pivotar_para_layout_fixo(df, col_id, qtd_max):
            if df.empty:
                return pd.DataFrame(columns=[col_id])
            
            df['seq'] = df.groupby(col_id).cumcount() + 1
            df = df[df['seq'] <= qtd_max] 
            
            df_pivot = df.pivot(index=col_id, columns='seq')
            df_pivot.columns = [f"{c[0]}_{c[1]}" for c in df_pivot.columns]
            df_pivot = df_pivot.reset_index()

            colunas_originais = [c for c in df.columns if c not in [col_id, 'seq']]
            for i in range(1, qtd_max + 1):
                for col in colunas_originais:
                    nome_col = f"{col}_{i}"
                    if nome_col not in df_pivot.columns:
                        df_pivot[nome_col] = ""
            return df_pivot

        # Aplica limites: 10 Telefones, 3 Emails, 3 Endereços
        df_tel_p = pivotar_para_layout_fixo(df_tel, 'cpf', 10)
        df_mail_p = pivotar_para_layout_fixo(df_mail, 'cpf', 3)
        df_end_p = pivotar_para_layout_fixo(df_end, 'cpf', 3)

        # 3. Consolidação (Merge)
        df_final = df_dados.merge(df_tel_p, on='cpf', how='left')\
                           .merge(df_mail_p, on='cpf', how='left')\
                           .merge(df_end_p, on='cpf', how='left')

        # 4. Padronização (Maiúsculo e Limpeza de Nulos)
        df_final = df_final.astype(str).apply(lambda x: x.str.upper())
        df_final = df_final.replace(['NONE', 'NAN', 'NAT', '#N/D', 'NULL', 'None', '<NA>'], '')

        conn.close()
        return df_final

    except Exception as e:
        if conn: conn.close()
        st.error(f"Erro no motor fixo: {e}")
        return pd.DataFrame()

# =============================================================================
# PARTE 2: INTERFACE DO USUÁRIO (TELA) E AUTO-CONFIGURAÇÃO
# =============================================================================

def verificar_criar_modelos_padrao():
    """Cria automaticamente os modelos de planilha se não existirem."""
    conn = pf_core.get_conn()
    if not conn: return
    try:
        cur = conn.cursor()
        
        # Lista dos modelos exigidos
        modelos_padrao = [
            ("Planilha: pf_telefones", "pf_telefones", "Exportação bruta da tabela de telefones"),
            ("Planilha: pf_e-mails", "pf_e-mails", "Exportação bruta da tabela de e-mails"),
            ("Planilha: pf_endereços", "pf_endereços", "Exportação bruta da tabela de endereços"),
            ("Planilha: pf_convenio", "pf_convenio", "Exportação bruta da tabela de convênios"),
            ("Planilha: pf_campanhas", "pf_campanhas", "Exportação bruta da tabela de campanhas"),
            ("Planilha: pf_campanhas_exportação", "pf_campanhas_exportação", "Exportação bruta de campanhas (Backup)"),
            ("Planilha: pf_dados", "pf_dados", "Exportação bruta da tabela de dados pessoais"),
            ("Planilha: pf_contratos", "pf_contratos", "Exportação bruta da tabela de contratos"),
            ("Planilha: pf_emprego_renda", "pf_emprego_renda", "Exportação bruta da tabela de emprego e renda"),
            ("Planilha: pf_historico_importações", "pf_historico_importações", "Histórico de Importações"),
            ("Planilha: pf_maricula_dados_clt", "pf_maricula_dados_clt", "Dados detalhados CLT"),
            ("Planilha: pf_modelos_exportacao", "pf_modelos_exportacao", "Configuração de Modelos de Exportação"),
            ("Planilha: pf_modelos_filtro_fixo", "pf_modelos_filtro_fixo", "Configuração de Filtros Fixos"),
            ("Planilha: pf_péradpres_de_filtro", "pf_péradpres_de_filtro", "Configuração de Operadores"),
            ("Planilha: pf_referecias", "pf_referecias", "Tabela de Referências"),
            ("Planilha: pf_tipo_exportacao", "pf_tipo_exportacao", "Tipos de Exportação")
        ]

        for nome, chave, desc in modelos_padrao:
            # Verifica se já existe pela chave
            cur.execute("SELECT id FROM banco_pf.pf_modelos_exportacao WHERE codigo_de_consulta = %s", (chave,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO banco_pf.pf_modelos_exportacao 
                    (nome_modelo, codigo_de_consulta, descricao, status, data_criacao) 
                    VALUES (%s, %s, %s, 'ATIVO', CURRENT_DATE)
                """, (nome, chave, desc))
        
        conn.commit()
        conn.close()
    except:
        conn.close()

def app_config_exportacao():
    # Garante que os modelos existam ao abrir a tela
    verificar_criar_modelos_padrao()
    
    st.markdown("## ⚙️ Configuração de Modelos de Exportação")
    st.caption("Gerencie as chaves que conectam os modelos de tela às regras de código (motor fixo e tabelas brutas).")

    # --- Bloco para Criar Novo ---
    with st.expander("➕ Criar Novo Modelo de Exportação", expanded=False):
        with st.form("form_novo_modelo"):
            nome = st.text_input("Nome Comercial do Modelo", placeholder="Ex: Dados Cadastrais Simples")
            chave_motor = st.text_input("Chave do Motor (Código de Consulta)", 
                                        help="Digite: Dados_Cadastrais_Simples ou nome da tabela (ex: pf_dados)")
            desc = st.text_area("Descrição / Observações")
            
            if st.form_submit_button("💾 Salvar Modelo"):
                if nome and chave_motor:
                    if salvar_modelo(nome, chave_motor, desc):
                        st.success(f"Modelo '{nome}' salvo com sucesso!")
                        time.sleep(1)
                        st.rerun()
                else:
                    st.warning("Preencha o Nome e a Chave do Motor.")

    st.divider()
    st.subheader("📋 Modelos Cadastrados")

    # Listagem
    df_modelos = listar_modelos_ativos()
    if not df_modelos.empty:
        for _, row in df_modelos.iterrows():
            chave_exibicao = row.get('codigo_de_consulta') or "SEM_CHAVE"
            # Destaque visual se for tabela bruta
            icon = "🗃️" if chave_exibicao in MAPA_TABELAS_BRUTAS else "📦"
            
            with st.expander(f"{icon} {row['nome_modelo']} (Chave: {chave_exibicao})"):
                st.write(f"**Descrição:** {row['descricao']}")
                st.caption(f"Criado em: {row['data_criacao']} | Status: {row['status']}")
                
                c1, c2 = st.columns([1, 1])
                with c1:
                    if st.button(f"✏️ Editar", key=f"edit_{row['id']}", use_container_width=True):
                        dialog_editar_modelo(row)
                with c2:
                    if st.button(f"🗑️ Excluir", key=f"del_{row['id']}", use_container_width=True):
                        dialog_excluir_modelo(row['id'], row['nome_modelo'])
    else:
        st.info("Nenhum modelo configurado no momento.")

# --- DIÁLOGOS ---

@st.dialog("✏️ Editar Modelo")
def dialog_editar_modelo(modelo):
    with st.form("form_edit_modelo"):
        novo_nome = st.text_input("Nome do Modelo", value=modelo['nome_modelo'])
        val_chave = modelo.get('codigo_de_consulta') or ""
        nova_chave = st.text_input("Chave do Motor", value=val_chave)
        nova_desc = st.text_area("Descrição", value=modelo['descricao'])
        
        c1, c2 = st.columns(2)
        if c1.form_submit_button("💾 Salvar Alterações"):
            if atualizar_modelo(modelo['id'], novo_nome, nova_chave, nova_desc):
                st.success("Atualizado!")
                time.sleep(1)
                st.rerun()
        if c2.form_submit_button("Cancelar"):
            st.rerun()

@st.dialog("⚠️ Confirmar Exclusão")
def dialog_excluir_modelo(id_modelo, nome_modelo):
    st.warning(f"Excluir: **{nome_modelo}**?")
    if st.button("🚨 CONFIRMAR EXCLUSÃO", use_container_width=True):
        if excluir_modelo(id_modelo):
            st.success("Removido!")
            time.sleep(1)
            st.rerun()
    if st.button("Cancelar", use_container_width=True):
        st.rerun()