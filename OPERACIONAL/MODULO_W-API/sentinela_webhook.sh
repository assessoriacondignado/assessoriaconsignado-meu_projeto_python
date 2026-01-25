#!/bin/bash

# --- CONFIGURACOES ---
PORTA=5001
DIR_PROJETO="/root/meu_sistema/OPERACIONAL/MODULO_W-API"
PYTHON_VENV="/root/meu_sistema/venv/bin/python"
ARQUIVO_LOG="sentinela.log"

cd $DIR_PROJETO

# Verifica se a porta esta ouvindo
if ss -lptn "sport = :$PORTA" | grep -q $PORTA; then
    # Opcional: Descomente a linha abaixo se quiser logar tambem quando estiver tudo bem (pode encher o disco)
    # echo "$(date) - [OK] Webhook operando normal." >> $ARQUIVO_LOG
    exit 0
else
    echo "---------------------------------------------------" >> $ARQUIVO_LOG
    echo "$(date) - [ALERTA] Webhook CAIU ou esta PARADO!" >> $ARQUIVO_LOG
    
    # 1. Tenta limpar qualquer residuo na porta
    fuser -k $PORTA/tcp >> $ARQUIVO_LOG 2>&1
    
    # 2. Reinicia o servico
    nohup $PYTHON_VENV webhook_wapi.py > webhook.log 2>&1 &
    
    echo "$(date) - [SUCESSO] Comando de reinicio enviado." >> $ARQUIVO_LOG
    echo "---------------------------------------------------" >> $ARQUIVO_LOG
fi
