import streamlit as st
import modulo_whats_disparador as disp
import modulo_whats_instancias as inst
import modulo_whats_modelos_mensagem as modelos
import modulo_whats_registros as regs
import modulo_whats_numeros as nums # Novo import

def app_wapi():
    st.markdown("## 📱 Módulo W-API")
    # Adicionada a aba "📒 Números"
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📤 Disparador", "🤖 Instâncias", "📒 Números", "📝 Modelos", "📋 Logs"])

    with tab1:
        disp.app_disparador()

    with tab2:
        inst.app_instancias()
        
    with tab3:
        nums.app_numeros() # Nova interface

    with tab4:
        modelos.app_modelos()

    with tab5:
        regs.app_registros()

if __name__ == "__main__":
    app_wapi()