@echo off
setlocal

REM Navega para a pasta do script (onde o .bat está)
cd /d "%~dp0"

REM Executa autenticação SPX (abre o Chrome para login)
python main.py --auth

REM Mantém a janela aberta para leitura de mensagens
pause
