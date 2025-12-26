import streamlit as st
import modulo_whats_disparador as disp
import modulo_whats_instancias as inst
import modulo_whats_modelos_mensagem as modelos
import modulo_whats_registros as regs

def app_wapi():
    st.markdown("## 📱 Módulo W-API")
    tab1, tab2, tab3, tab4 = st.tabs(["📤 Disparador", "🤖 Instâncias", "📝 Modelos", "📋 Registros"])

    with tab1:
        disp.app_disparador()

    with tab2:
        inst.app_instancias()

    with tab3:
        modelos.app_modelos()

    with tab4:
        regs.app_registros()

if __name__ == "__main__":
    app_wapi()