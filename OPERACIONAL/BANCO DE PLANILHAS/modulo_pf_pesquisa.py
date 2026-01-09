import streamlit as st
import pandas as pd
from datetime import date, datetime
import re
import time
import json
import modulo_pf_cadastro as pf_core
import modulo_pf_config_exportacao as pf_export

# --- NOVA FUNÇÃO PRINCIPAL: GERENCIADOR DE TELAS ---
def app_gestao_pesquisa():
    """
    Função principal que gerencia a navegação interna do módulo de Gestão.
    Alterna entre Lista, Edição e Visualização sem depender do menu principal.
    """
    # Garante que existe um estado inicial
    if 'pf_view' not in st.session_state:
        st.session_state['pf_view'] = 'lista'

    # ROTEAMENTO INTERNO
    if st.session_state['pf_view'] == 'lista':
        interface_pesquisa_ampla()
    
    elif st.session_state['pf_view'] == 'visualizar':
        # Chama a tela de visualização LOCAL
        interface_visualizar_cliente()

    elif st.session_state['pf_view'] == 'editar':
        # Chama a tela de edição LOCAL
        interface_cadastro_pf()

# --- FUNÇÕES DE NAVEGAÇÃO ---
def ir_para_visualizar(cpf):
    st.session_state['pf_view'] = 'visualizar'
    st.session_state['pf_cpf_selecionado'] = str(cpf)

def ir_para_editar(cpf):
    st.session_state['pf_view'] = 'editar'
    st.session_state['pf_cpf_selecionado'] = str(cpf)
    st.session_state['form_loaded'] = False

# --- CONFIGURAÇÃO DE CADASTRO (MIGRADO) ---
CONFIG_CADASTRO = {
    "Dados Pessoais": [
        {"label": "Nome Completo", "key": "nome", "tabela": "geral", "tipo": "texto", "obrigatorio": True},
        {"label": "CPF", "key": "cpf", "tabela": "geral", "tipo": "cpf", "obrigatorio": True},
        # Campos abaixo só aparecem no modo EDITAR
        {"label": "RG", "key": "rg", "tabela": "geral", "tipo": "texto"},
        {"label": "Data Nascimento", "key": "data_nascimento", "tabela": "geral", "tipo": "data"},
        {"label": "Nome da Mãe", "key": "nome_mae", "tabela": "geral", "tipo": "texto"},
        {"label": "Nome do Pai", "key": "nome_pai", "tabela": "geral", "tipo": "texto"},
        {"label": "UF do RG", "key": "uf_rg", "tabela": "geral", "tipo": "texto"},
        {"label": "Dados Exp. RG", "key": "dados_exp_rg", "tabela": "geral", "tipo": "texto"},
        {"label": "PIS", "key": "pis", "tabela": "geral", "tipo": "texto"},
        {"label": "CNH", "key": "cnh", "tabela": "geral", "tipo": "texto"},
        {"label": "Série CTPS", "key": "serie_ctps", "tabela": "geral", "tipo": "texto"},
        # Procurador
        {"label": "Nome Procurador", "key": "nome_procurador", "tabela": "geral", "tipo": "texto"},
        {"label": "CPF Procurador", "key": "cpf_procurador", "tabela": "geral", "tipo": "cpf"}, 
    ],
    "Contatos": [
        {"label": "Telefone", "key": "numero", "tabela": "telefones", "tipo": "telefone", "multiplo": True},
        {"label": "E-mail", "key": "email", "tabela": "emails", "tipo": "email", "multiplo": True},
    ],
    "Endereços": [
        {"label": "CEP", "key": "cep", "tabela": "enderecos", "tipo": "cep", "multiplo": True, "agrupado": True}, 
        {"label": "Logradouro", "key": "rua", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "Bairro", "key": "bairro", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "Cidade", "key": "cidade", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
        {"label": "UF", "key": "uf", "tabela": "enderecos", "tipo": "texto", "multiplo": True, "vinculo": "cep"},
    ]
}

# --- FUNÇÕES DE INTERFACE MIGRADAS (VISUALIZAR E EDITAR) ---

def inserir_dado_staging(campo_config, valor, extras=None):
    tabela = campo_config['tabela']
    chave = campo_config['key']
    
    if tabela not in st.session_state['dados_staging']:
        if campo_config.get('multiplo'): st.session_state['dados_staging'][tabela] = []
        else: st.session_state['dados_staging'][tabela] = {}

    erro = None
    valor_final = valor
    
    if campo_config['tipo'] == 'cpf':
        val, erro = pf_core.validar_formatar_cpf(valor)
        if not erro: valor_final = pf_core.limpar_normalizar_cpf(val)
    elif campo_config['tipo'] == 'telefone':
        val, erro = pf_core.validar_formatar_telefone(valor)
        if not erro: valor_final = val
    elif campo_config['tipo'] == 'email':
        if not pf_core.validar_email(valor): erro = "E-mail inválido."
    
    if erro: st.error(erro); return
    if not valor_final and campo_config.get('obrigatorio'): st.toast(f"❌ O campo {campo_config['label']} é obrigatório."); return

    if campo_config.get('multiplo'):
        novo_item = {chave: valor_final}
        if extras: novo_item.update(extras)
        if isinstance(valor, dict): st.session_state['dados_staging'][tabela].append(valor)
        else: st.session_state['dados_staging'][tabela].append(novo_item)
        st.toast(f"✅ {campo_config['label']} adicionado!")
    else:
        st.session_state['dados_staging'][tabela][chave] = valor_final
        st.toast(f"✅ {campo_config['label']} atualizado!")

def interface_cadastro_pf():
    pf_core.init_db_structures()
    is_edit = st.session_state['pf_view'] == 'editar'
    
    cpf_formatado_titulo = ""
    if is_edit:
        raw_cpf = st.session_state.get('pf_cpf_selecionado', '')
        cpf_formatado_titulo = pf_core.formatar_cpf_visual(raw_cpf)
    
    titulo = f"✏️ Editar: {cpf_formatado_titulo}" if is_edit else "➕ Novo Cadastro"
    st.button("⬅️ Voltar", on_click=lambda: st.session_state.update({'pf_view': 'lista', 'form_loaded': False}))
    st.markdown(f"### {titulo}")

    if 'dados_staging' not in st.session_state:
        st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': [], 'dados_clt': []}

    if is_edit and not st.session_state.get('form_loaded'):
        dados_db = pf_core.carregar_dados_completos(st.session_state['pf_cpf_selecionado'])
        st.session_state['dados_staging'] = dados_db
        st.session_state['form_loaded'] = True
    elif not is_edit and not st.session_state.get('form_loaded'):
        st.session_state['dados_staging'] = {'geral': {}, 'telefones': [], 'emails': [], 'enderecos': [], 'empregos': [], 'contratos': [], 'dados_clt': []}
        st.session_state['form_loaded'] = True

    c_builder, c_preview = st.columns([3, 2])

    with c_builder:
        st.markdown("#### 🏗️ Inserir Dados")
        with st.expander("Dados Pessoais", expanded=True):
            if not is_edit:
                st.info("ℹ️ Para cadastrar dados complementares (RG, Filiação, Procurador, etc.), salve o Nome e CPF primeiro e depois edite o registro.")

            for campo in CONFIG_CADASTRO["Dados Pessoais"]:
                if not is_edit and campo['key'] not in ['nome', 'cpf']: continue

                if is_edit and campo['key'] == 'cpf':
                    c_lab, c_inp = st.columns([1.2, 3.5])
                    c_lab.markdown(f"**{campo['label']}:**")
                    val_atual = st.session_state['dados_staging']['geral'].get('cpf', '')
                    c_inp.text_input("CPF Display", value=pf_core.formatar_cpf_visual(val_atual), disabled=True, label_visibility="collapsed")
                    continue
                
                c_lbl, c_inp, c_btn = st.columns([1.2, 2.5, 1.0])
                c_lbl.markdown(f"**{campo['label']}:**")
                with c_inp:
                    if campo['tipo'] == 'data':
                        val_pre = st.session_state['dados_staging']['geral'].get(campo['key'])
                        if isinstance(val_pre, str):
                            try: val_pre = datetime.strptime(val_pre, '%Y-%m-%d').date()
                            except: val_pre = None
                        val = st.date_input("Data", value=val_pre, min_value=date(1900, 1, 1), max_value=date(2050, 12, 31), format="DD/MM/YYYY", key=f"in_{campo['key']}", label_visibility="collapsed")
                    else:
                        val_pre = st.session_state['dados_staging']['geral'].get(campo['key'], '')
                        val = st.text_input("Texto", value=val_pre, label_visibility="collapsed", key=f"in_{campo['key']}")
                
                with c_btn:
                    if st.button("Inserir", key=f"btn_{campo['key']}", type="primary", use_container_width=True): 
                        inserir_dado_staging(campo, val)
        
        with st.expander("Contatos"):
            if not is_edit:
                st.info("🚫 A inclusão de telefones e e-mails é permitida apenas no modo 'Editar', após salvar o cadastro inicial.")
            else:
                c_tel_in, c_tel_btn = st.columns([4, 2])
                with c_tel_in: tel = st.text_input("Número", key="in_tel_num", placeholder="Ex: (82)999025155")
                with c_tel_btn:
                    st.write(""); st.write("") 
                    if st.button("Inserir Telefone", type="primary", use_container_width=True):
                        cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'numero'][0]
                        inserir_dado_staging(cfg, tel, None)
                
                st.divider()
                st.markdown("##### 📧 Cadastro de E-mail")
                c_mail_in, c_mail_btn = st.columns([5, 2])
                with c_mail_in: mail = st.text_input("E-mail", key="in_mail", placeholder="exemplo@email.com")
                with c_mail_btn:
                    st.write(""); st.write("")
                    if st.button("Inserir E-mail", type="primary", use_container_width=True):
                        if pf_core.validar_email(mail):
                            emails_atuais = [e['email'] for e in st.session_state['dados_staging'].get('emails', [])]
                            if mail in emails_atuais: st.warning("⚠️ Este e-mail já está na lista deste cliente.")
                            else:
                                cfg = [c for c in CONFIG_CADASTRO["Contatos"] if c['key'] == 'email'][0]
                                inserir_dado_staging(cfg, mail)
                                st.success("E-mail validado e adicionado!")
                        else: st.error("⚠️ Formato de e-mail inválido.")

        with st.expander("Endereço"):
            if not is_edit:
                st.info("🚫 A inclusão de endereços é permitida apenas no modo 'Editar', após salvar o cadastro inicial.")
            else:
                st.markdown("##### 📍 Cadastro de Endereço")
                c_cep, c_rua = st.columns([1.5, 3.5])
                with c_cep: cep = st.text_input("CEP", key="in_end_cep", placeholder="00000-000")
                with c_rua: rua = st.text_input("Logradouro", key="in_end_rua", placeholder="Rua, Av, etc.")
                
                c_bai, c_cid, c_uf = st.columns([2, 2, 1])
                with c_bai: bairro = st.text_input("Bairro", key="in_end_bairro")
                with c_cid: cidade = st.text_input("Cidade", key="in_end_cid")
                with c_uf: uf_digitada = st.text_input("UF", key="in_end_uf", placeholder="UF", max_chars=2).upper()
                
                if st.button("Inserir Endereço", type="primary", use_container_width=True):
                    cep_num, cep_vis, erro_cep = pf_core.validar_formatar_cep(cep)
                    erro_uf = None
                    if not pf_core.validar_uf(uf_digitada): erro_uf = f"UF inválida: '{uf_digitada}'. Use siglas (ex: SP, MG, BA)."
                    
                    if erro_cep: st.error(erro_cep)
                    elif erro_uf: st.error(erro_uf)
                    elif not rua: st.warning("O campo Logradouro é obrigatório.")
                    else:
                        ends_atuais = st.session_state['dados_staging'].get('enderecos', [])
                        duplicado = False
                        for e in ends_atuais:
                            if e.get('cep') == cep_num and e.get('rua') == rua: duplicado = True; break
                        
                        if duplicado: st.warning("⚠️ Este endereço já está na lista deste cliente.")
                        else:
                            obj_end = {'cep': cep_num, 'rua': rua, 'bairro': bairro, 'cidade': cidade, 'uf': uf_digitada}
                            if 'enderecos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['enderecos'] = []
                            st.session_state['dados_staging']['enderecos'].append(obj_end)
                            st.toast(f"✅ Endereço adicionado! (CEP: {cep_vis})")
                            st.success("Endereço validado e incluído na lista temporária.")

        with st.expander("Emprego e Renda (Vínculo)"):
            c_conv, c_matr, c_btn_emp = st.columns([3, 3, 2])
            with c_conv: conv = st.text_input("Convênio", key="in_emp_conv", placeholder="Ex: INSS")
            with c_matr: matr = st.text_input("Matrícula", key="in_emp_matr")
            with c_btn_emp:
                st.write(""); st.write("")
                if st.button("Inserir Vínculo", type="primary", use_container_width=True):
                    if conv and matr:
                        obj_emp = {'convenio': conv.upper(), 'matricula': matr, 'dados_extras': ''}
                        if 'empregos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['empregos'] = []
                        st.session_state['dados_staging']['empregos'].append(obj_emp)
                        st.toast("✅ Vínculo adicionado!")
                        st.rerun()
                    else: st.warning("Campos obrigatórios.")

        with st.expander("Contratos / Planilhas"):
            lista_empregos = st.session_state['dados_staging'].get('empregos', [])
            if not lista_empregos: st.info("Insira um vínculo em 'Emprego e Renda' primeiro.")
            else:
                opcoes_matr = [f"{e['matricula']} - {e['convenio']}" for e in lista_empregos]
                sel_vinculo = st.selectbox("Vincular à Matrícula:", opcoes_matr, key="sel_vinc_contr")
                idx_vinc = opcoes_matr.index(sel_vinculo)
                dados_vinc = lista_empregos[idx_vinc]
                tabelas_destino = pf_core.listar_tabelas_por_convenio(dados_vinc['convenio'])
                
                if not tabelas_destino: st.warning(f"Sem planilhas configuradas para {dados_vinc['convenio']}.")
                for nome_tabela, tipo_tabela in tabelas_destino:
                    st.markdown("---")
                    st.markdown(f"###### 📝 {tipo_tabela or 'Dados'} ({nome_tabela})")
                    sufixo = f"{nome_tabela}_{idx_vinc}"
                    colunas_banco = pf_core.get_colunas_tabela(nome_tabela)
                    campos_ignorados = ['id', 'matricula_ref', 'matricula', 'convenio', 'tipo_planilha', 'importacao_id', 'data_criacao', 'data_atualizacao', 'cpf_ref']
                    inputs_gerados = {}
                    mapa_calculo_datas = {'tempo_abertura_anos': 'data_abertura_empresa', 'tempo_admissao_anos': 'data_admissao', 'tempo_inicio_emprego_anos': 'data_inicio_emprego'}
                    datas_preenchidas = {} 

                    cols_ui = st.columns(2)
                    for idx_col, (col_nome, col_tipo) in enumerate(colunas_banco):
                        if col_nome in campos_ignorados: continue
                        label_fmt = col_nome.replace('_', ' ').title()
                        with cols_ui[idx_col % 2]:
                            key_input = f"inp_{col_nome}_{sufixo}"
                            if col_nome in mapa_calculo_datas:
                                col_data_ref = mapa_calculo_datas[col_nome]
                                valor_data = datas_preenchidas.get(col_data_ref)
                                anos_calc = pf_core.calcular_idade_hoje(valor_data) if valor_data else 0
                                val = st.number_input(label_fmt, value=anos_calc, disabled=True, key=key_input)
                            elif 'date' in col_tipo.lower() or 'data' in col_nome.lower():
                                val = st.date_input(label_fmt, value=None, format="DD/MM/YYYY", key=key_input)
                                datas_preenchidas[col_nome] = val
                            else:
                                val = st.text_input(label_fmt, key=key_input)
                            inputs_gerados[col_nome] = val
                    
                    if st.button(f"Inserir em {tipo_tabela or nome_tabela}", key=f"btn_save_{sufixo}", type="primary"):
                        nomes_cols_tabela = [c[0] for c in colunas_banco]
                        if 'matricula' in nomes_cols_tabela: inputs_gerados['matricula'] = dados_vinc['matricula']
                        elif 'matricula_ref' in nomes_cols_tabela: inputs_gerados['matricula_ref'] = dados_vinc['matricula']
                        
                        if 'convenio' in nomes_cols_tabela: inputs_gerados['convenio'] = dados_vinc['convenio']
                        if 'tipo_planilha' in nomes_cols_tabela and tipo_tabela: inputs_gerados['tipo_planilha'] = tipo_tabela
                        
                        inputs_gerados['origem_tabela'] = nome_tabela
                        inputs_gerados['tipo_origem'] = tipo_tabela
                        
                        if 'contratos' not in st.session_state['dados_staging']: st.session_state['dados_staging']['contratos'] = []
                        st.session_state['dados_staging']['contratos'].append(inputs_gerados)
                        st.toast(f"✅ {tipo_tabela} adicionado!")

    with c_preview:
        st.markdown("### 📋 Resumo")
        st.info("👤 Dados Pessoais")
        geral = st.session_state['dados_staging'].get('geral', {})
        if geral:
            cols = st.columns(2)
            idx = 0
            for k, v in geral.items():
                if v:
                    val_str = v.strftime('%d/%m/%Y') if isinstance(v, (date, datetime)) else str(v)
                    if k == 'cpf' or k == 'cpf_procurador': val_str = pf_core.formatar_cpf_visual(val_str)
                    cols[idx%2].text_input(k.replace('_', ' ').upper(), value=val_str, disabled=True, key=f"view_geral_{k}")
                    idx += 1
        
        st.warning("📞 Contatos")
        tels = st.session_state['dados_staging'].get('telefones', [])
        if tels:
            for i, t in enumerate(tels):
                c1, c2 = st.columns([5, 1])
                val_view = pf_core.formatar_telefone_visual(t.get('numero'))
                c1.write(f"📱 **{val_view}**")
                if c2.button("🗑️", key=f"rm_tel_{i}"):
                    st.session_state['dados_staging']['telefones'].pop(i); st.rerun()
        
        mails = st.session_state['dados_staging'].get('emails', [])
        if mails:
            for i, m in enumerate(mails):
                c1, c2 = st.columns([5, 1])
                c1.write(f"📧 **{m.get('email')}**")
                if c2.button("🗑️", key=f"rm_mail_{i}"):
                    st.session_state['dados_staging']['emails'].pop(i); st.rerun()
        
        if not tels and not mails: st.caption("Nenhum contato.")

        st.warning("📍 Endereços")
        ends = st.session_state['dados_staging'].get('enderecos', [])
        if ends:
            for i, e in enumerate(ends):
                c1, c2 = st.columns([5, 1])
                _, cep_fmt, _ = pf_core.validar_formatar_cep(e.get('cep'))
                c1.write(f"🏠 **{e.get('rua')}** - {e.get('bairro')} | {e.get('cidade')}/{e.get('uf')} (CEP: {cep_fmt})")
                if c2.button("🗑️", key=f"rm_end_{i}"):
                    st.session_state['dados_staging']['enderecos'].pop(i); st.rerun()
        else: st.caption("Nenhum endereço.")
        
        st.warning("💼 Vínculos (Emprego)")
        emps = st.session_state['dados_staging'].get('empregos', [])
        if emps:
            for i, emp in enumerate(emps):
                c1, c2 = st.columns([5, 1])
                c1.write(f"🏢 **{emp.get('convenio')}** | Mat: {emp.get('matricula')}")
                if c2.button("🗑️", key=f"rm_emp_{i}"):
                    st.session_state['dados_staging']['empregos'].pop(i); st.rerun()
        else: st.caption("Nenhum vínculo inserido.")

        st.success("📝 Dados Financeiros / Planilhas")
        ctrs = st.session_state['dados_staging'].get('contratos', [])
        if ctrs:
            for i, c in enumerate(ctrs):
                c1, c2 = st.columns([5, 1])
                origem_nome = c.get('tipo_origem') or c.get('origem_tabela', 'Dado')
                chaves = [k for k in c.keys() if k not in ['origem_tabela', 'tipo_origem', 'matricula_ref', 'matricula', 'convenio', 'tipo_planilha']]
                display_txt = f"[{origem_nome}] "
                if len(chaves) > 0: display_txt += f"{c[chaves[0]]} "
                ref_matr = c.get('matricula') or c.get('matricula_ref')
                c1.write(f"📌 {display_txt} (Ref: {ref_matr})")
                if c2.button("🗑️", key=f"rm_ctr_{i}"):
                    st.session_state['dados_staging']['contratos'].pop(i); st.rerun()
        else: st.caption("Nenhum vínculo inserido.")

        st.divider()
        
        if st.button("💾 CONFIRMAR E SALVAR", type="primary", use_container_width=True):
            staging = st.session_state['dados_staging']
            if not staging['geral'].get('nome') or not staging['geral'].get('cpf'):
                st.error("Nome e CPF são obrigatórios.")
            else:
                df_tel = pd.DataFrame(staging['telefones'])
                df_email = pd.DataFrame(staging['emails'])
                df_end = pd.DataFrame(staging['enderecos'])
                df_emp = pd.DataFrame(staging['empregos'])
                df_contr = pd.DataFrame(staging['contratos'])
                
                modo_salvar = "editar" if is_edit else "novo"
                cpf_orig = pf_core.limpar_normalizar_cpf(st.session_state.get('pf_cpf_selecionado')) if is_edit else None
                
                sucesso, msg = pf_core.salvar_pf(staging['geral'], df_tel, df_email, df_end, df_emp, df_contr, modo_salvar, cpf_orig)
                if sucesso:
                    st.success(msg)
                    time.sleep(1.5)
                    st.session_state['pf_view'] = 'lista'
                    st.session_state['form_loaded'] = False
                    st.rerun()
                else: st.error(msg)
    
    st.markdown(f"<div style='text-align: right; color: gray; font-size: 0.8em; margin-top: 10px;'>código atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>", unsafe_allow_html=True)

def interface_visualizar_cliente():
    cpf_cliente = st.session_state.get('pf_cpf_selecionado')
    
    if st.button("⬅️ Voltar"):
        st.session_state['pf_view'] = 'lista'
        st.rerun()
    
    if not cpf_cliente:
        st.error("Nenhum cliente selecionado.")
        return

    cpf_vis = pf_core.formatar_cpf_visual(cpf_cliente)
    dados = pf_core.carregar_dados_completos(cpf_cliente)
    g = dados.get('geral', {})
    if not g: 
        st.error("Cliente não encontrado.")
        return
    
    st.markdown("""<style>.compact-header { margin-bottom: -15px; } .stMarkdown hr { margin-top: 5px; margin-bottom: 5px; }</style>""", unsafe_allow_html=True)
    st.markdown(f"<h3 class='compact-header'>👤 {g.get('nome', 'Nome não informado')}</h3>", unsafe_allow_html=True)
    st.markdown(f"**CPF:** {cpf_vis}")
    st.write("") 
    
    t1, t2, t3 = st.tabs(["📋 Cadastro & Vínculos", "💼 Detalhes Financeiros", "📞 Contatos & Endereços"])
    with t1:
        c1, c2 = st.columns(2)
        nasc = g.get('data_nascimento')
        txt_nasc = nasc.strftime('%d/%m/%Y') if nasc and isinstance(nasc, (date, datetime)) else pf_core.safe_view(nasc)
        c1.write(f"**Nascimento:** {txt_nasc}"); c1.write(f"**RG:** {pf_core.safe_view(g.get('rg'))}"); c2.write(f"**Mãe:** {pf_core.safe_view(g.get('nome_mae'))}")
        
        demais_campos = {k: v for k, v in g.items() if k not in ['data_nascimento', 'rg', 'nome_mae', 'id', 'cpf', 'nome', 'importacao_id', 'id_campanha', 'data_criacao']}
        if demais_campos:
            st.markdown("---"); st.markdown("##### 📌 Outras Informações")
            col_iter = st.columns(3); idx = 0
            for k, v in demais_campos.items(): 
                val_display = pf_core.safe_view(v)
                if 'cpf' in k: val_display = pf_core.formatar_cpf_visual(val_display)
                col_iter[idx % 3].write(f"**{k.replace('_', ' ').title()}:** {val_display}"); idx += 1
        
        st.divider(); st.markdown("##### 🔗 Vínculos")
        for v in dados.get('empregos', []): st.info(f"🆔 **{v['matricula']}** - {v['convenio'].upper()}")
        if not dados.get('empregos'): st.warning("Nenhum vínculo localizado.")
            
    with t2:
        st.markdown("##### 💰 Detalhes Financeiros & Contratos")
        for v in dados.get('empregos', []):
            ctrs = v.get('contratos', [])
            if ctrs:
                tipo_display = v.get('contratos')[0].get('tipo_origem') or 'Detalhes'
                with st.expander(f"📂 {v['convenio'].upper()} | {tipo_display} | Matr: {v['matricula']}", expanded=True):
                    df_ex = pd.DataFrame(ctrs)
                    cols_drop = ['id', 'matricula_ref', 'importacao_id', 'data_criacao', 'data_atualizacao', 'origem_tabela', 'tipo_origem']
                    st.dataframe(df_ex.drop(columns=cols_drop, errors='ignore'), hide_index=True, use_container_width=True)
            else: st.caption(f"Sem contratos detalhados para {v['convenio']}.")
    with t3:
        for t in dados.get('telefones', []): 
            st.write(f"📱 {pf_core.formatar_telefone_visual(t.get('numero'))}")
        for m in dados.get('emails', []): 
            st.write(f"📧 {pf_core.safe_view(m.get('email'))}")
        
        st.divider()
        st.markdown("##### 📍 Endereços")
        for end in dados.get('enderecos', []): 
            _, cep_view, _ = pf_core.validar_formatar_cep(end.get('cep'))
            cep_view = cep_view if cep_view else end.get('cep')
            st.success(f"🏠 {pf_core.safe_view(end.get('rua'))}, {pf_core.safe_view(end.get('bairro'))} - {pf_core.safe_view(end.get('cidade'))}/{pf_core.safe_view(end.get('uf'))} (CEP: {cep_view})")
    
    st.markdown(f"<div style='text-align: right; color: gray; font-size: 0.8em; margin-top: 10px;'>código atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>", unsafe_allow_html=True)

# --- (MANTIDO) CONFIGURAÇÕES E SQL (ORIGINAL) ---
CAMPOS_CONFIG = {
    "Dados Pessoais": [
        {"label": "Nome", "coluna": "d.nome", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CPF", "coluna": "d.cpf", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Nascimento", "coluna": "d.data_nascimento", "tipo": "data", "tabela": "banco_pf.pf_dados"},
        {"label": "Idade (Cálculo)", "coluna": "virtual_idade", "tipo": "numero", "tabela": "banco_pf.pf_dados"},
        {"label": "RG", "coluna": "d.rg", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "UF RG", "coluna": "d.uf_rg", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Exp. RG", "coluna": "d.data_exp_rg", "tipo": "data", "tabela": "banco_pf.pf_dados"},
        {"label": "CNH", "coluna": "d.cnh", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "PIS", "coluna": "d.pis", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CTPS/Série", "coluna": "d.ctps_serie", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome da Mãe", "coluna": "d.nome_mae", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome do Pai", "coluna": "d.nome_pai", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Nome Procurador", "coluna": "d.nome_procurador", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "CPF Procurador", "coluna": "d.cpf_procurador", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "ID Importação", "coluna": "d.importacao_id", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "ID Campanha", "coluna": "d.id_campanha", "tipo": "texto", "tabela": "banco_pf.pf_dados"},
        {"label": "Data Criação (Reg)", "coluna": "d.data_criacao", "tipo": "data", "tabela": "banco_pf.pf_dados"}
    ],
    "Endereços": [
        {"label": "Logradouro", "coluna": "ende.rua", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "Bairro", "coluna": "ende.bairro", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "Cidade", "coluna": "ende.cidade", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "UF", "coluna": "ende.uf", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"},
        {"label": "CEP", "coluna": "ende.cep", "tipo": "texto", "tabela": "banco_pf.pf_enderecos"}
    ],
    "Contatos": [
        {"label": "DDD (Telefone)", "coluna": "virtual_ddd", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Telefone (Número)", "coluna": "tel.numero", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Tag WhatsApp", "coluna": "tel.tag_whats", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Tag Qualificação", "coluna": "tel.tag_qualificacao", "tipo": "texto", "tabela": "banco_pf.pf_telefones"},
        {"label": "Data Atualização (Tel)", "coluna": "tel.data_atualizacao", "tipo": "data", "tabela": "banco_pf.pf_telefones"},
        {"label": "E-mail", "coluna": "em.email", "tipo": "texto", "tabela": "banco_pf.pf_emails"}
    ],
    "Profissional (Geral)": [
        {"label": "Matrícula (Geral)", "coluna": "emp.matricula", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Convênio (Geral)", "coluna": "emp.convenio", "tipo": "texto", "tabela": "banco_pf.pf_emprego_renda"},
        {"label": "Contrato Empréstimo", "coluna": "ctr.contrato", "tipo": "texto", "tabela": "banco_pf.pf_contratos"}
    ],
    "Contratos CLT / CAGED": [
        {"label": "Matrícula", "coluna": "clt.matricula", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Convênio", "coluna": "clt.convenio", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Nome Empresa", "coluna": "clt.cnpj_nome", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CNPJ", "coluna": "clt.cnpj_numero", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "TAG (Destaque)", "coluna": "clt.tag", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Qtd Funcionários", "coluna": "clt.qtd_funcionarios", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Abertura Empresa", "coluna": "clt.data_abertura_empresa", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Tempo Abertura (Anos)", "coluna": "clt.tempo_abertura_anos", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Admissão", "coluna": "clt.data_admissao", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Tempo Admissão (Anos)", "coluna": "clt.tempo_admissao_anos", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Início Emprego", "coluna": "clt.data_inicio_emprego", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Tempo Início (Anos)", "coluna": "clt.tempo_inicio_emprego_anos", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CNAE Nome", "coluna": "clt.cnae_nome", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CNAE Código", "coluna": "clt.cnae_codigo", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CBO Nome", "coluna": "clt.cbo_nome", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "CBO Código", "coluna": "clt.cbo_codigo", "tipo": "texto", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "ID Importação (CLT)", "coluna": "clt.importacao_id", "tipo": "numero", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Criação (CLT)", "coluna": "clt.data_criacao", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"},
        {"label": "Data Atualização (CLT)", "coluna": "clt.data_atualizacao", "tipo": "data", "tabela": "banco_pf.pf_matricula_dados_clt"}
    ]
}

def buscar_pf_simples(termo, filtro_importacao_id=None, pagina=1, itens_por_pagina=50):
    conn = pf_core.get_conn()
    if conn:
        try:
            termo_limpo = pf_core.limpar_normalizar_cpf(termo)
            param_nome = f"%{termo}%"
            sql_base = "SELECT d.id, d.nome, d.cpf, d.data_nascimento FROM banco_pf.pf_dados d "
            conds = ["d.nome ILIKE %s"]
            params = [param_nome]
            if termo_limpo: 
                sql_base += " LEFT JOIN banco_pf.pf_telefones t ON d.cpf = t.cpf" 
                conds.append("d.cpf ILIKE %s")
                conds.append("t.numero ILIKE %s")
                params.append(f"%{termo_limpo}%")
                params.append(f"%{termo_limpo}%")
            
            where = " WHERE " + " OR ".join(conds)
            cur = conn.cursor()
            part_from = sql_base.split('FROM', 1)[1]
            cur.execute(f"SELECT COUNT(DISTINCT d.id) FROM {part_from} {where}", tuple(params))
            total = cur.fetchone()[0]
            offset = (pagina-1)*itens_por_pagina
            df = pd.read_sql(f"{sql_base} {where} GROUP BY d.id ORDER BY d.nome LIMIT {itens_por_pagina} OFFSET {offset}", conn, params=tuple(params))
            conn.close()
            return df, total
        except Exception as e:
            st.error(f"Erro na busca simples: {e}") 
            if conn: conn.close()
    return pd.DataFrame(), 0

def executar_pesquisa_ampla(regras_ativas, pagina=1, itens_por_pagina=50):
    conn = pf_core.get_conn()
    if conn:
        try:
            sql_select = "SELECT DISTINCT d.id, d.nome, d.cpf, d.data_nascimento "
            sql_from = "FROM banco_pf.pf_dados d "
            joins_map = {
                'banco_pf.pf_telefones': "JOIN banco_pf.pf_telefones tel ON d.cpf = tel.cpf",
                'banco_pf.pf_emails': "JOIN banco_pf.pf_emails em ON d.cpf = em.cpf",
                'banco_pf.pf_enderecos': "JOIN banco_pf.pf_enderecos ende ON d.cpf = ende.cpf",
                'banco_pf.pf_emprego_renda': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf",
                'banco_pf.pf_contratos': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf JOIN banco_pf.pf_contratos ctr ON emp.matricula = ctr.matricula_ref",
                'banco_pf.pf_matricula_dados_clt': "JOIN banco_pf.pf_emprego_renda emp ON d.cpf = emp.cpf LEFT JOIN banco_pf.pf_matricula_dados_clt clt ON emp.matricula = clt.matricula"
            }
            active_joins = []; conditions = []; params = []

            for regra in regras_ativas:
                tabela = regra['tabela']; coluna = regra['coluna']; op = regra['operador']
                val_raw = regra['valor']; tipo = regra['tipo']
                if tabela == 'banco_pf.pf_contratos_clt': tabela = 'banco_pf.pf_matricula_dados_clt'
                if tabela in joins_map and joins_map[tabela] not in active_joins: active_joins.append(joins_map[tabela])
                col_sql = f"{coluna}"
                if coluna == 'virtual_idade': col_sql = "EXTRACT(YEAR FROM AGE(d.data_nascimento))"
                if coluna == 'virtual_ddd': col_sql = "SUBSTRING(tel.numero, 1, 2)"

                if op == "∅" or op == "Vazio": 
                    conditions.append(f"({col_sql} IS NULL OR {col_sql}::TEXT = '')"); continue
                if val_raw is None or str(val_raw).strip() == "": continue
                
                valores = [v.strip() for v in str(val_raw).split(',') if v.strip()]
                conds_or = []
                for val in valores:
                    if 'cpf' in coluna or 'cnpj' in coluna: val = pf_core.limpar_normalizar_cpf(val)
                    if tipo == 'numero': val = re.sub(r'\D', '', val)
                    if tipo == 'data':
                        if op == "=": conds_or.append(f"{col_sql} = %s"); params.append(val)
                        elif op == "≥" or op == "A Partir": conds_or.append(f"{col_sql} >= %s"); params.append(val)
                        elif op == "≤" or op == "Até": conds_or.append(f"{col_sql} <= %s"); params.append(val)
                        elif op == "≠": conds_or.append(f"{col_sql} <> %s"); params.append(val)
                        continue 
                    if op == "=>" or op == "Começa com": conds_or.append(f"{col_sql} ILIKE %s"); params.append(f"{val}%")
                    elif op == "<=>" or op == "Contém": conds_or.append(f"{col_sql} ILIKE %s"); params.append(f"%{val}%")
                    elif op == "=" or op == "Igual": 
                        if tipo == 'numero': conds_or.append(f"{col_sql} = %s"); params.append(val)
                        else: conds_or.append(f"{col_sql} ILIKE %s"); params.append(val)
                    elif op == "≠" or op == "Diferente": conds_or.append(f"{col_sql} <> %s"); params.append(val)
                    elif op == "<≠>" or op == "Não Contém": conds_or.append(f"{col_sql} NOT ILIKE %s"); params.append(f"%{val}%")
                    elif op in [">", "<", "≥", "≤"]:
                        sym = {">":">", "<":"<", "≥":">=", "≤":"<="}[op]
                        conds_or.append(f"{col_sql} {sym} %s"); params.append(val)
                
                if op == "o":
                    placeholders = ','.join(['%s'] * len(valores))
                    conditions.append(f"{col_sql} IN ({placeholders})"); params.extend(valores)
                elif conds_or: conditions.append(f"({' OR '.join(conds_or)})")

            full_joins = " ".join(active_joins)
            sql_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            count_sql = f"SELECT COUNT(DISTINCT d.id) {sql_from} {full_joins} {sql_where}"
            cur = conn.cursor()
            cur.execute(count_sql, tuple(params))
            total = cur.fetchone()[0]
            offset = (pagina - 1) * itens_por_pagina
            limit_clause = f"LIMIT {itens_por_pagina} OFFSET {offset}" if itens_por_pagina < 9999999 else ""
            query = f"{sql_select} {sql_from} {full_joins} {sql_where} ORDER BY d.nome {limit_clause}"
            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close()
            return df.fillna(""), total
        except Exception as e: 
            st.error(f"Erro SQL: {e}")
            return pd.DataFrame(), 0
    return pd.DataFrame(), 0

def executar_exclusao_lote(tipo, cpfs_alvo, convenio=None, sub_opcao=None):
    conn = pf_core.get_conn()
    if not conn: return False, "Erro de conexão."
    try:
        cur = conn.cursor()
        cpfs_tuple = tuple(str(c) for c in cpfs_alvo)
        if not cpfs_tuple: return False, "Nenhum CPF na lista."
        
        if tipo == "Cadastro Completo":
            query = "DELETE FROM banco_pf.pf_dados WHERE cpf IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "Telefones":
            query = "DELETE FROM banco_pf.pf_telefones WHERE cpf IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "E-mails":
            query = "DELETE FROM banco_pf.pf_emails WHERE cpf IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "Endereços":
            query = "DELETE FROM banco_pf.pf_enderecos WHERE cpf IN %s"
            cur.execute(query, (cpfs_tuple,))
        elif tipo == "Emprego e Renda":
            if not convenio: return False, "Convênio não selecionado."
            if sub_opcao == "Excluir Vínculo Completo (Matrícula + Contratos)":
                query = "DELETE FROM banco_pf.pf_emprego_renda WHERE cpf IN %s AND convenio = %s"
                cur.execute(query, (cpfs_tuple, convenio))
            elif sub_opcao == "Excluir Apenas Contratos":
                query = """
                    DELETE FROM banco_pf.pf_contratos 
                    WHERE matricula_ref IN (
                        SELECT matricula FROM banco_pf.pf_emprego_renda 
                        WHERE cpf IN %s AND convenio = %s
                    )
                """
                cur.execute(query, (cpfs_tuple, convenio))

        registros = cur.rowcount
        conn.commit(); conn.close()
        return True, f"Operação realizada com sucesso! {registros} registros afetados."
    except Exception as e:
        if conn: conn.close()
        return False, f"Erro na execução: {e}"

# --- FUNÇÕES PARA "TIPOS DE FILTRO" (MODELO FIXO) ---

def listar_tabelas_pf(conn):
    try:
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'banco_pf' AND table_name LIKE 'pf_%' ORDER BY table_name"
        cur = conn.cursor(); cur.execute(query); res = [r[0] for r in cur.fetchall()]; return res
    except: return []

def listar_colunas_tabela(conn, tabela):
    try:
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'banco_pf' AND table_name = %s ORDER BY ordinal_position"
        cur = conn.cursor(); cur.execute(query, (tabela,)); res = [r[0] for r in cur.fetchall()]; return res
    except: return []

def salvar_modelo_fixo(nome, tabela, lista_colunas, resumo):
    conn = pf_core.get_conn()
    if conn:
        try:
            colunas_json = json.dumps(lista_colunas); cur = conn.cursor()
            cur.execute("INSERT INTO banco_pf.pf_modelos_filtro_fixo (nome_modelo, tabela_alvo, coluna_alvo, resumo) VALUES (%s, %s, %s, %s)", (nome, tabela, colunas_json, resumo))
            conn.commit(); conn.close(); return True
        except Exception as e: st.error(f"Erro ao salvar: {e}"); conn.close()
    return False

def listar_modelos_fixos():
    conn = pf_core.get_conn()
    if conn:
        try:
            df = pd.read_sql("SELECT * FROM banco_pf.pf_modelos_filtro_fixo ORDER BY id DESC", conn); conn.close(); return df
        except: conn.close()
    return pd.DataFrame()

def executar_filtro_fixo(tabela, colunas_alvo):
    conn = pf_core.get_conn()
    if conn:
        try:
            try:
                lista_cols = json.loads(colunas_alvo)
                if not isinstance(lista_cols, list): lista_cols = [str(lista_cols)]
            except: lista_cols = [str(colunas_alvo)]
            cols_str = ", ".join(lista_cols); query = f"SELECT DISTINCT {cols_str} FROM banco_pf.{tabela} ORDER BY {lista_cols[0]}"
            df = pd.read_sql(query, conn); conn.close(); return df
        except Exception as e: st.error(f"Erro ao executar filtro: {e}"); conn.close()
    return pd.DataFrame()

@st.dialog("📂 Tipos de Filtro (Modelos Fixos)", width="large")
def dialog_tipos_filtro():
    t1, t2 = st.tabs(["CRIAR MODELO", "CONSULTAR FILTRO"])
    with t1:
        st.markdown("#### 🆕 Novo Modelo de Filtro")
        conn = pf_core.get_conn()
        if conn:
            tabelas = listar_tabelas_pf(conn)
            sel_tabela = st.selectbox("1. Selecione a Lista (Tabela SQL)", options=tabelas)
            cols = []
            if sel_tabela: cols = listar_colunas_tabela(conn, sel_tabela)
            sel_colunas = st.multiselect("2. Selecione os Itens (Cabeçalhos)", options=cols); conn.close(); st.divider()
            nome_modelo = st.text_input("3. Nome do Modelo de Filtro"); resumo_modelo = st.text_area("4. Resumo / Descrição")
            if st.button("💾 Salvar Modelo"):
                if nome_modelo and sel_tabela and sel_colunas:
                    if salvar_modelo_fixo(nome_modelo, sel_tabela, sel_colunas, resumo_modelo): st.success("Modelo salvo com sucesso!"); time.sleep(1); st.rerun()
                else: st.warning("Preencha todos os campos e selecione ao menos uma coluna.")
        else: st.error("Erro de conexão.")
    with t2:
        st.markdown("#### 🔎 Consultar Dados por Modelo")
        df_modelos = listar_modelos_fixos()
        if not df_modelos.empty:
            opcoes = df_modelos.apply(lambda x: f"{x['id']} - {x['nome_modelo']}", axis=1); sel_modelo = st.selectbox("Selecione o Modelo", options=opcoes)
            if sel_modelo:
                id_sel = int(sel_modelo.split(' - ')[0]); modelo = df_modelos[df_modelos['id'] == id_sel].iloc[0]; st.info(f"**Resumo:** {modelo['resumo']}")
                try: cols_list = json.loads(modelo['coluna_alvo']); cols_str = ", ".join(cols_list)
                except: cols_str = str(modelo['coluna_alvo'])
                st.caption(f"Fonte: Tabela `{modelo['tabela_alvo']}` | Colunas: `{cols_str}`")
                if st.button("👁️ Ver Resultado (Dados Únicos)"):
                    with st.spinner("Buscando dados únicos..."): df_res = executar_filtro_fixo(modelo['tabela_alvo'], modelo['coluna_alvo'])
                    if not df_res.empty: st.dataframe(df_res, use_container_width=True, hide_index=True); st.write(f"Total de itens únicos: {len(df_res)}")
                    else: st.warning("Nenhum dado encontrado.")
        else: st.info("Nenhum modelo cadastrado ainda.")

# --- INTERFACES VISUAIS ---

def interface_pesquisa_ampla():
    c_voltar, c_tipos, c_limpar, c_spacer = st.columns([1, 1.5, 1.5, 5])
    
    if c_voltar.button("⬅️ Voltar"): 
        st.session_state['pf_view'] = 'lista'
        st.rerun()
        
    if c_tipos.button("📂 Tipos de Filtro", help="Ver modelos de dados únicos"): dialog_tipos_filtro()
    if c_limpar.button("🗑️ Limpar Filtros"): 
        st.session_state['regras_pesquisa'] = []
        st.session_state['executar_busca'] = False
        st.session_state['pf_pagina_atual'] = 1
        if 'cache_export_ampla' in st.session_state: del st.session_state['cache_export_ampla']
        st.rerun()
    
    st.divider()
    conn = pf_core.get_conn(); ops_cache = {'texto': [], 'numero': [], 'data': []}; lista_convenios = []
    if conn:
        try:
            df_ops = pd.read_sql("SELECT tipo, simbolo, descricao FROM banco_pf.pf_operadores_de_filtro", conn)
            for _, r in df_ops.iterrows(): ops_cache[r['tipo']].append(f"{r['simbolo']} : {r['descricao']}")
            cur = conn.cursor(); cur.execute("SELECT DISTINCT convenio FROM banco_pf.pf_emprego_renda WHERE convenio IS NOT NULL ORDER BY convenio"); lista_convenios = [r[0] for r in cur.fetchall()]; cur.close()
        except: pass
        conn.close()
    
    c_menu, c_regras = st.columns([4, 2]) 
    with c_menu:
        st.markdown("### 🗂️ Campos Disponíveis")
        termo_filtro = st.text_input("🔍 Filtrar campos (Digite para buscar...)", key="filtro_campos_ampla")
        for grupo, campos in CAMPOS_CONFIG.items():
            campos_filtrados = [c for c in campos if termo_filtro.lower() in c['label'].lower()]
            if campos_filtrados:
                expandir_grupo = bool(termo_filtro)
                with st.expander(grupo, expanded=expandir_grupo):
                    colunas_botoes = st.columns(4)
                    for idx, campo in enumerate(campos_filtrados):
                        with colunas_botoes[idx % 4]: 
                            if st.button(f"➕ {campo['label']}", key=f"add_{campo['coluna']}", use_container_width=True):
                                st.session_state['regras_pesquisa'].append({'label': campo['label'], 'coluna': campo['coluna'], 'tabela': campo['tabela'], 'tipo': campo['tipo'], 'operador': None, 'valor': ''}); st.rerun()

    with c_regras:
        st.markdown("### 🎯 Regras Ativas")
        if not st.session_state['regras_pesquisa']: st.info("Nenhuma regra. Selecione ao lado.")
        regras_rem = []
        for i, regra in enumerate(st.session_state['regras_pesquisa']):
            with st.container(border=True):
                st.caption(f"**{regra['label']}**"); c_op, c_val, c_del = st.columns([2, 3, 1])
                opcoes = ops_cache.get(regra['tipo'], []); idx_sel = opcoes.index(regra['operador']) if regra['operador'] in opcoes else 0
                novo_op_full = c_op.selectbox("Op.", opcoes, index=idx_sel, key=f"op_{i}", label_visibility="collapsed")
                novo_op_simbolo = novo_op_full.split(' : ')[0] if novo_op_full else "="
                if novo_op_simbolo == '∅': c_val.text_input("Valor", value="[Vazio]", disabled=True, key=f"val_{i}", label_visibility="collapsed"); novo_valor = None
                elif regra['tipo'] == 'data': 
                    novo_valor = c_val.date_input("Data", value=None, min_value=date(1900,1,1), max_value=date(2050,12,31), key=f"val_{i}", format="DD/MM/YYYY", label_visibility="collapsed")
                else: novo_valor = c_val.text_input("Valor", value=regra['valor'], key=f"val_{i}", label_visibility="collapsed")
                st.session_state['regras_pesquisa'][i]['operador'] = novo_op_full; st.session_state['regras_pesquisa'][i]['valor'] = novo_valor
                if c_del.button("🗑️", key=f"del_{i}"): regras_rem.append(i)
        if regras_rem:
            for idx in sorted(regras_rem, reverse=True): st.session_state['regras_pesquisa'].pop(idx)
            st.rerun()
        st.divider()
        if st.button("🔎 FILTRAR AGORA", type="primary", use_container_width=True): st.session_state['executar_busca'] = True
    if st.session_state.get('executar_busca'):
        regras_limpas = []
        for r in st.session_state['regras_pesquisa']:
            r_copy = r.copy()
            if r_copy['operador']: r_copy['operador'] = r_copy['operador'].split(' : ')[0]
            regras_limpas.append(r_copy)
        df_res, total = executar_pesquisa_ampla(regras_limpas, st.session_state.get('pf_pagina_atual', 1))
        st.write(f"**Resultados:** {total}")
        
        if not df_res.empty:
            st.divider()

            # --- ÁREA DE EXPORTAÇÃO MASSIVA ---
            with st.expander("📂 Exportar Dados (Lotes)", expanded=bool(st.session_state.get('cache_export_ampla'))):
                if st.session_state.get('cache_export_ampla'):
                    st.success("✅ Arquivos gerados e prontos para download:")
                    arquivos = st.session_state['cache_export_ampla']
                    for i, item in enumerate(arquivos):
                        st.download_button(
                            label=f"💾 Baixar {item['nome']}",
                            data=item['data'],
                            file_name=item['nome'],
                            mime="text/csv",
                            key=f"dl_cached_ampla_{i}"
                        )
                    st.markdown("---")
                    if st.button("❌ Limpar / Fechar Exportação", key="cls_ampla"):
                        del st.session_state['cache_export_ampla']
                        st.rerun()
                else:
                    df_modelos = pf_export.listar_modelos_ativos()
                    if not df_modelos.empty:
                        c_sel, c_btn = st.columns([3, 1])
                        opcoes_mods = df_modelos.apply(lambda x: f"{x['id']} - {x['nome_modelo']}", axis=1)
                        idx_mod = c_sel.selectbox("Selecione o Modelo de Exportação:", range(len(df_modelos)), format_func=lambda x: opcoes_mods[x], key="mod_ampla")
                        
                        if c_btn.button("⬇️ Gerar Arquivos", key="btn_ampla_exp"):
                            with st.spinner("Processando e gerando arquivos em memória..."):
                                df_total, _ = executar_pesquisa_ampla(regras_limpas, 1, 9999999)
                                lista_cpfs_total = df_total['cpf'].unique().tolist()
                                limite = 200000
                                partes = (len(lista_cpfs_total) // limite) + (1 if len(lista_cpfs_total) % limite > 0 else 0)
                                st.info(f"Gerando {partes} lote(s) de 200 mil.")
                                
                                cache_data = []
                                for p in range(partes):
                                    cpfs_lote = lista_cpfs_total[p*limite : (p+1)*limite]
                                    df_final = pf_export.gerar_dataframe_por_modelo(df_modelos.iloc[idx_mod]['id'], cpfs_lote)
                                    if not df_final.empty:
                                        csv = df_final.to_csv(sep=';', index=False, encoding='utf-8-sig')
                                        cache_data.append({'nome': f"export_p{p+1}.csv", 'data': csv})
                                
                                st.session_state['cache_export_ampla'] = cache_data
                                st.rerun()

            with st.expander("🗑️ Zona de Perigo: Exclusão em Lote", expanded=False):
                st.error(f"Atenção: A exclusão será aplicada aos {total} clientes filtrados."); tipo_exc = st.selectbox("O que excluir?", ["Selecione...", "Cadastro Completo", "Telefones", "E-mails", "Endereços", "Emprego e Renda"])
                convenio_sel = None; sub_opcao_sel = None
                if tipo_exc == "Emprego e Renda":
                    c_emp1, c_emp2 = st.columns(2); convenio_sel = c_emp1.selectbox("Qual Convênio?", lista_convenios); sub_opcao_sel = c_emp2.radio("Nível", ["Excluir Vínculo Completo (Matrícula + Contratos)", "Excluir Apenas Contratos"])
                if tipo_exc != "Selecione...":
                    if st.button("Preparar Exclusão", key="btn_prep_exc"): st.session_state['confirm_delete_lote'] = True; st.rerun()
                    if st.session_state.get('confirm_delete_lote'):
                        st.warning(f"Excluir definitivamente {tipo_exc} de {total} clientes?"); c_sim, c_nao = st.columns(2)
                        if c_sim.button("🚨 SIM, EXCLUIR DEFINITIVAMENTE", type="primary", key="btn_conf_exc"):
                            df_total, _ = executar_pesquisa_ampla(regras_limpas, 1, 9999999); lista_cpfs = df_total['cpf'].tolist(); ok, msg = executar_exclusao_lote(tipo_exc, lista_cpfs, convenio_sel, sub_opcao_sel)
                            if ok: st.success(msg); st.session_state['confirm_delete_lote'] = False; time.sleep(2); st.rerun()
                            else: st.error(f"Erro: {msg}")
                        if c_nao.button("Cancelar", key="btn_canc_exc"): st.session_state['confirm_delete_lote'] = False; st.rerun()
            st.divider()
            st.markdown("""<div style="background-color: #f0f0f0; padding: 8px; font-weight: bold; display: flex;"><div style="flex: 2;">Ações</div><div style="flex: 1;">ID</div><div style="flex: 2;">CPF</div><div style="flex: 4;">Nome</div></div>""", unsafe_allow_html=True)
            for _, row in df_res.iterrows():
                c1, c2, c3, c4 = st.columns([2, 1, 2, 4])
                with c1:
                    b1, b2, b3 = st.columns(3)
                    # CORREÇÃO: CALLBACKS CORRETOS (on_click=ir_para_visualizar)
                    b1.button("👁️", key=f"v_{row['id']}", on_click=ir_para_visualizar, args=(row['cpf'],))
                    b2.button("✏️", key=f"e_{row['id']}", on_click=ir_para_editar, args=(row['cpf'],))
                    
                    with b3:
                        if st.button("🗑️", key=f"d_{row['id']}"): pf_core.dialog_excluir_pf(str(row['cpf']), row['nome'])
                c2.write(str(row['id'])); c3.write(pf_core.formatar_cpf_visual(row['cpf'])); c4.write(row['nome']); st.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)
            
            cp1, cp2, cp3 = st.columns([1, 3, 1])
            if cp1.button("⬅️ Ant.") and st.session_state.get('pf_pagina_atual', 1) > 1: st.session_state['pf_pagina_atual'] -= 1; st.rerun()
            if cp3.button("Próx. ➡️"): st.session_state['pf_pagina_atual'] = st.session_state.get('pf_pagina_atual', 1) + 1; st.rerun()
        else: st.warning("Nenhum registro encontrado.")