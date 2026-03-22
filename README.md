# Automated Handling

Automação modular para extração e monitoramento operacional com integração a Google Sheets e execução agendada via GitHub Actions.

English version: [README.en.md](README.en.md)

## Visão geral

- Execução por módulos independentes (CLI).
- Suporte a execução local e CI no GitHub Actions.
- Sessão/autenticação sincronizada via `CONFIG_CLOUD` para reduzir manutenção manual.

## Módulos disponíveis

| Módulo | Flag |
|--------|------|
| Exception Orders | `--exception` |
| Inbound | `--inbound` |
| Outbound | `--outbound` |
| Recebimento SOC | `--recebimento` |
| Escalation Ticket | `--escalation` |
| Liquidation | `--liquidation` |
| SPX Duplicados | `--spx-duplicados` |
| Status Duplicados | `--status-dup` |
| Workstation | `--workstation` |
| Online SOC Tracking | `--online-soc` |

## Execução local

1. Instale dependências:
   - `python -m pip install -r requirements.txt`
2. Crie o arquivo de ambiente:
   - copie `.env.example` para `.env`
3. Configure as variáveis necessárias.
4. Rode um módulo de teste:
   - `python main.py --exception`

## Execução no GitHub Actions

O workflow está em [.github/workflows/scrape_tratativa.yml](.github/workflows/scrape_tratativa.yml).

Cadastre em `Settings > Secrets and variables > Actions`:

- **Repository Secrets (obrigatórios):**
  - `GOOGLE_SHEETS_ID`
  - `GOOGLE_SERVICE_ACCOUNT`
  - `SPX_BASE_URL`
  - `SPX_API_TRACKING_LIST_SEARCH`
  - `SPX_API_TRACKING_INFO`
  - `SPX_API_ESCALATION_TICKET`
  - `SPX_API_EXCEPTION_ORDER_HISTORY`
  - `SPX_API_EXCEPTION_HANDLING_EO_LIST`
  - `SPX_API_WFM_DASHBOARD`
- **Repository Secrets (recomendado):**
  - `GOOGLE_OAUTH_CLIENT_SECRET`
- **Repository Variables (opcionais):**
  - `ONLINE_SOC_SPREADSHEET_ID`
  - `ONLINE_SOC_BASE_STATUS_SPREADSHEET_ID`

## Segurança

Política de sensíveis:

- Endpoints internos SPX ficam em `.env` local e em `GitHub Secrets` (nunca hardcoded em código/docstring).
- Credenciais Google (Service Account e OAuth client secret) ficam em `.env` local e/ou `GitHub Secrets`.
- Itens de sessão/operação ficam somente na aba `CONFIG_CLOUD` da planilha:
  - `cookies_json`, `x-sap-*`, `pg-i`, `device-id`, `app`, `Authorization`
  - `GOOGLE_OAUTH_TOKEN`, `GOOGLE_SESSION_COOKIES`
  - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `DISCORD_WEBHOOK_URL`

Arquivos sensíveis e operacionais são mantidos fora do Git via `.gitignore`.

---

Guia operacional local (não versionado): `README_LOCAL.md`.

# Auto-EDA-SPX
