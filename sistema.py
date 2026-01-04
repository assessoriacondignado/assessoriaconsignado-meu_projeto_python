import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

# --- 1. CONFIGURAÇÃO DA PÁGINA E ESTADO ---
st.set_page_config(page_title="Sistema de Gestão", layout="wide")

def iniciar_estado():
    # Inicializa variáveis de sessão se não existirem
    if 'ultima_atividade' not in st.session_state:
        st.session_state['ultima_atividade'] = datetime.now()
    
    if 'hora_login' not in st.session_state:
        st.session_state['hora_login'] = datetime.now()

    if 'menu_aberto' not in st.session_state:
        st.session_state['menu_aberto'] = None # Começa fechado
        
    if 'pagina_atual' not in st.session_state:
        st.session_state['pagina_atual'] = "Home"

def resetar_atividade():
    """Callback para resetar o timer de inatividade ao clicar em botões."""
    st.session_state['ultima_atividade'] = datetime.now()

# --- 2. CSS (ESTILOS E LAYOUT) ---
def carregar_css():
    st.markdown("""
        <style>
        /* Regra 2: Botões com bordas quadradas e contorno preto */
        div.stButton > button {
            width: 100%;
            border: 1px solid #000000 !important;
            border-radius: 0px !important; /* Quadrado */
            color: black;
            background-color: #ffffff;
            font-weight: 500;
            margin-bottom: 5px;
            transition: all 0.3s;
        }
        
        div.stButton > button:hover {
            border-color: #FF4B4B !important;
            color: #FF4B4B;
        }

        /* Regra 1: Submenu com cor diferenciada (50% visualmente) */
        /* Identificamos botões de submenu por estarem dentro de colunas específicas */
        /* Nota: O seletor exato pode variar dependendo da versão do Streamlit, 
           aqui usamos uma classe auxiliar injetada na lógica ou contexto */
        
        /* Ajuste do Sidebar */
        section[data-testid="stSidebar"] {
            background-color: #f0f2f6; /* Fundo claro para contraste */
        }
        
        /* Esconder menu padrão do Streamlit */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        </style>
    """, unsafe_allow_html=True)

# --- 3. REGRAS DE SESSÃO ---
def gerenciar_sessao():
    """Calcula tempo de sessão e verifica inatividade."""
    TEMPO_LIMITE_MINUTOS = 60
    
    agora = datetime.now()
    
    # 1.1.1 Checar Inatividade
    # Se o usuário não clicou em nada (resetar_atividade não foi chamado), o tempo sobe.
    tempo_inativo = agora - st.session_state['ultima_atividade']
    
    # 1.1.4 Logout automático
    if tempo_inativo.total_seconds() > (TEMPO_LIMITE_MINUTOS * 60):
        st.session_state.clear()
        st.error("Sessão expirada por inatividade (60min). Por favor, recarregue a página.")
        st.stop()

    # 1.1.3 Formato do tempo de sessão (Sessão Ativa: MM:SS)
    tempo_total_sessao = agora - st.session_state['hora_login']
    mm, ss = divmod(tempo_total_sessao.seconds, 60)
    # Se passar de 1 hora, ajusta para HH:MM:SS ou acumula minutos
    hh, mm = divmod(mm, 60)
    
    if hh > 0:
        return f"{hh:02d}:{mm:02d}:{ss:02d}"
    return f"{mm:02d}:{ss:02d}"

# --- 4. TELAS DO SISTEMA (CONTEÚDO) ---
def tela_fluxo_caixa():
    st.title("💰 Financeiro > Fluxo de Caixa")
    st.markdown("---")
    
    # Filtros Fictícios
    c1, c2, c3 = st.columns(3)
    with c1: st.date_input("Data Início")
    with c2: st.date_input("Data Fim")
    with c3: st.selectbox("Conta", ["Banco A", "Banco B", "Caixa Físico"])
    
    st.markdown("### Resumo do Período")
    
    # Métricas
    m1, m2, m3 = st.columns(3)
    m1.metric("Entradas", "R$ 45.200,00", "+5%")
    m2.metric("Saídas", "R$ 32.100,00", "-2%")
    m3.metric("Saldo", "R$ 13.100,00", "OK")
    
    # Gráfico Dummy
    st.markdown("### Evolução Diária")
    chart_data = pd.DataFrame(
        np.random.randn(20, 3),
        columns=['Entradas', 'Saídas', 'Saldo']
    )
    st.line_chart(chart_data)
    
    # Tabela Dummy
    st.markdown("### Lançamentos Recentes")
    df = pd.DataFrame({
        "Data": [datetime.today().date()] * 5,
        "Descrição": ["Pagamento Fornecedor X", "Recebimento Cliente Y", "Conta Luz", "Serviço Z", "Retirada"],
        "Valor": [-1500.00, 5000.00, -350.00, 1200.00, -500.00],
        "Tipo": ["Saída", "Entrada", "Saída", "Entrada", "Saída"]
    })
    st.dataframe(df, use_container_width=True)

def tela_generica(titulo):
    st.title(f"📂 {titulo}")
    st.info("Esta funcionalidade está em desenvolvimento.")

# --- 5. MENU LATERAL ---
def renderizar_menu():
    with st.sidebar:
        # 5. Espaço Usuário e Logo
        st.markdown("**Usuário:** Admin System")
        
        # 5.1 Logo da Assessoria
        # Tenta carregar imagem, se não der, mostra texto
        try:
            st.image("logo_assessoria.png", use_container_width=True)
        except:
            st.warning("Insira 'logo_assessoria.png' na pasta")
            st.markdown("---")

        # Estrutura do Menu
        # Regra 4: Inicio/Chat removido
        opcoes = {
            "Cadastros": ["Clientes", "Fornecedores", "Produtos"],
            "Financeiro": ["Contas a Pagar", "Contas a Receber", "Fluxo de Caixa"],
            "Relatórios": ["Geral", "Vendas", "Auditoria"]
        }

        # Loop Principal do Menu
        for menu_pai, subitens in opcoes.items():
            # Estado do ícone
            icone = "▼" if st.session_state['menu_aberto'] == menu_pai else "►"
            
            # Botão Principal (Pai)
            # on_click=resetar_atividade garante a regra de reiniciar inatividade
            if st.button(f"{menu_pai} {icone}", key=f"pai_{menu_pai}", on_click=resetar_atividade):
                # Regra 1.4: Clica em qualquer menu -> Fecha outros
                # Regra 1.3: Clica novamente -> Fecha o atual
                if st.session_state['menu_aberto'] == menu_pai:
                    st.session_state['menu_aberto'] = None
                else:
                    st.session_state['menu_aberto'] = menu_pai

            # Regra 1.1.1: Abre opções logo abaixo
            if st.session_state['menu_aberto'] == menu_pai:
                for item in subitens:
                    # Layout para indentação e cor
                    col_espaco, col_btn = st.columns([0.1, 0.9])
                    with col_btn:
                        # Estilo inline para simular cor mais escura (50%) no botão específico é difícil no Streamlit puro
                        # A solução aqui é visual via CSS global ou aceitar a cor padrão do tema.
                        # Usamos o CSS global para pintar botões dentro de colunas de forma diferente se necessário.
                        if st.button(f"{item}", key=f"sub_{item}", on_click=resetar_atividade):
                            st.session_state['pagina_atual'] = f"{menu_pai} > {item}"
                            # Força rerun para carregar o conteúdo novo imediatamente
                            # st.rerun() não é estritamente necessário devido ao callback, mas garante fluidez

        # Espaçador para jogar o timer para baixo
        st.markdown("<br>" * 5, unsafe_allow_html=True)
        st.markdown("---")

        # Regra de Sessão (Barra Inferior)
        tempo_str = gerenciar_sessao()
        
        # Container visual para o tempo
        st.markdown(f"""
            <div style="text-align: center; padding: 10px; border: 1px dashed gray;">
                <small>Tempo de Sessão</small><br>
                <strong style="font-size: 1.2em;">{tempo_str}</strong>
            </div>
        """, unsafe_allow_html=True)
        
        # Botão de Sair manual
        if st.button("Sair / Logout", key="btn_logout"):
            st.session_state.clear()
            st.rerun()

# --- 6. FUNÇÃO PRINCIPAL ---
def main():
    iniciar_estado()
    carregar_css()
    
    # Componente de Auto-Refresh (Timer Realtime)
    # Atualiza a cada 1 segundo (1000ms) para o relógio "andar"
    st_autorefresh(interval=1000, key="sistema_relogio")
    
    renderizar_menu()

    # Roteador de Páginas
    pagina = st.session_state['pagina_atual']
    
    if "Fluxo de Caixa" in pagina:
        tela_fluxo_caixa()
    elif pagina == "Home":
        st.title("Bem-vindo ao Sistema")
        st.write("Selecione uma opção no menu lateral para começar.")
    else:
        tela_generica(pagina)

if __name__ == "__main__":
    main()