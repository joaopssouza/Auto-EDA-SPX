@echo off
setlocal

REM Navega para a pasta do script (onde o .bat está)
cd /d "%~dp0"

REM Executa autenticação SPX usando a venv local quando disponível
if exist ".venv\Scripts\python.exe" (
	.venv\Scripts\python.exe main.py --auth
) else (
	python main.py --auth
)

REM Mantém a janela aberta para leitura de mensagens
pause
