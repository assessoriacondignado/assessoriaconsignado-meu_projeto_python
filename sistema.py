# ... (Mantenha as importações e configurações iniciais até a função main) ...

# --- 7. INTERFACE PRINCIPAL ---
def main():
    # Timeout de sessão
    if 'last_action' not in st.session_state: st.session_state['last_action'] = datetime.now()
    if st.session_state.get('logado') and datetime.now() - st.session_state['last_action'] > timedelta(minutes=30):
        st.session_state.clear(); st.warning("Sessão encerrada por inatividade."); st.rerun()
    st.session_state['last_action'] = datetime.now()

    # --- INJEÇÃO DE CSS PERSONALIZADO (NOVO) ---
    st.markdown("""
        <style>
            /* 3 - Contêiner Lateral: Cor laranja claro com 30% de opacidade (aprox.) */
            [data-testid="stSidebar"] {
                background-color: rgba(255, 224, 178, 0.3) !important;
            }
            
            /* 2 - Centralizar botão Home/Chat */
            div.stButton > button {
                width: 100%;
                display: flex;
                justify-content: center;
                align-items: center;
            }

            /* 1.5 - Sublinhar ao passar o mouse no Menu */
            .nav-link:hover {
                text-decoration: underline !important;
                background-color: rgba(0,0,0,0.05) !important;
            }
        </style>
    """, unsafe_allow_html=True)

    # TELA DE LOGIN
    if not st.session_state.get('logado'):
        # ... (Mantenha o código da tela de login igual) ...
        st.markdown('<div style="text-align:center; padding:40px;"><h2>Assessoria Consignado</h2><p>Portal Integrado</p></div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            u = st.text_input("E-mail ou CPF")
            s = st.text_input("Senha", type="password")
            if st.button("ENTRAR", use_container_width=True, type="primary"):
                res = validar_login_db(u, s)
                if res:
                    if res.get('status') == "sucesso":
                        st.session_state.update({'logado': True, 'usuario_id': res['id'], 'usuario_nome': res['nome'], 'usuario_cargo': res['cargo']})
                        st.rerun()
                    elif res.get('status') == "bloqueado": st.error("🚨 USUÁRIO BLOQUEADO por múltiplas falhas.")
                    else: st.error(f"Senha incorreta. Tentativas restantes: {res.get('restantes')}")
                else: st.error("Acesso negado.")
            if st.button("Esqueci minha senha", use_container_width=True): dialog_reset_senha()
    
    # ÁREA LOGADA
    else:
        # Botão de ação global no topo
        col_m1, col_m2 = st.columns([10, 2])
        with col_m2:
            if st.button("🟢 Mensagem Rápida", use_container_width=True): dialog_mensagem_rapida()

        # MENU LATERAL
        with st.sidebar:
            st.markdown('<div style="font-size:16px; font-weight:800; color:#333;">ASSESSORIA CONSIGNADO</div>', unsafe_allow_html=True)
            st.caption(f"👤 {st.session_state['usuario_nome']} ({st.session_state['usuario_cargo']})")
            
            # 2 - Botão Centralizado (controlado pelo CSS acima)
            if st.button("🏠 Home / Chat"): st.rerun()
            st.divider()
            
            cargo = st.session_state.get('usuario_cargo', 'Cliente')
            
            opcoes = ["Início"]
            if cargo in ["Admin", "Gerente"]:
                opcoes += ["COMERCIAL", "FINANCEIRO", "OPERACIONAL", "CONEXÕES"]
            else:
                opcoes += ["OPERACIONAL"]
                
            # 1 - CONFIGURAÇÃO DO MENU (Redução de tamanho, nome e cores)
            mod = option_menu(
                "MENU",  # 1.2 - Nome alterado para MENU
                opcoes, 
                icons=["chat-dots", "cart", "cash", "gear", "plug"], 
                default_index=0,
                styles={
                    "container": {"padding": "0!important", "background-color": "transparent"},
                    # 1.1 - Reduzir tamanho do título (MENU)
                    "menu-title": {"font-size": "14px", "font-weight": "bold", "margin-bottom": "5px"},
                    # 1.3 - Texto dentro do menu reduzido
                    "nav-link": {
                        "font-size": "12px",  # 1.3.1 - Reduzido
                        "text-align": "left", 
                        "margin": "0px", 
                        "--hover-color": "#eee"
                    },
                    "nav-link-selected": {"background-color": "#ff6f00"}, # Laranja para seleção
                    # 1.4 - Ícones coloridos (Definindo uma cor vibrante para os ícones)
                    "icon": {"color": "#e65100", "font-size": "14px"} 
                }
            )
            
            sub = None
            # Configuração dos submenus (mantendo o estilo padrão ou aplicando o mesmo se desejar)
            if mod == "COMERCIAL":
                sub = option_menu(None, ["Produtos", "Pedidos", "Tarefas", "Renovação"], 
                                  icons=["box", "cart-check", "check2-all", "arrow-repeat"],
                                  styles={"nav-link": {"font-size": "12px"}, "icon": {"color": "#e65100", "font-size": "12px"}})
            elif mod == "OPERACIONAL":
                sub = option_menu(None, ["Clientes", "Usuários", "Banco PF", "WhatsApp"], 
                                  icons=["people", "lock", "person-vcard", "whatsapp"],
                                  styles={"nav-link": {"font-size": "12px"}, "icon": {"color": "#e65100", "font-size": "12px"}})
            
            if st.sidebar.button("Sair"): st.session_state.clear(); st.rerun()

        # ROTEAMENTO DOS MÓDULOS (Lógica mantida igual)
        
        # 1. TELA INICIAL (CHAT)
        if mod == "Início":
            if modulo_chat:
                modulo_chat.app_chat_screen()
            else:
                st.info("Bem-vindo! Selecione um módulo no menu lateral.")
                st.warning("Módulo de Chat não encontrado na pasta OPERACIONAL/MODULO_CHAT.")

        # 2. MÓDULOS COMERCIAIS
        elif mod == "COMERCIAL":
            if sub == "Produtos" and modulo_produtos: modulo_produtos.app_produtos()
            elif sub == "Pedidos" and modulo_pedidos: modulo_pedidos.app_pedidos()
            elif sub == "Tarefas" and modulo_tarefas: modulo_tarefas.app_tarefas()
            elif sub == "Renovação" and modulo_rf: modulo_rf.app_renovacao_feedback()
            
        # 3. MÓDULOS OPERACIONAIS
        elif mod == "OPERACIONAL":
            if sub == "Clientes": modulo_cliente.app_clientes()
            elif sub == "Usuários": modulo_usuario.app_usuarios()
            elif sub == "Banco PF" and modulo_pf: modulo_pf.app_pessoa_fisica()
            elif sub == "Campanhas" and modulo_pf_campanhas: modulo_pf_campanhas.app_campanhas()
            elif sub == "WhatsApp": modulo_whats_controlador.app_wapi()

        # 4. MÓDULO CONEXÕES (NOVO)
        elif mod == "CONEXÕES":
            if modulo_conexoes:
                modulo_conexoes.app_conexoes()
            else:
                st.warning("Módulo 'modulo_conexoes.py' não encontrado na pasta CONEXÕES.")

if __name__ == "__main__": main()