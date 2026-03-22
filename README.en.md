# Automated Handling

Modular automation project for operational data extraction and monitoring, integrated with Google Sheets and scheduled execution through GitHub Actions.

## Overview

- Modular CLI-based execution.
- Supports both local runs and CI in GitHub Actions.
- Session/authentication can be synchronized through `CONFIG_CLOUD` to reduce manual maintenance.

## Available modules

| Module | Flag |
|--------|------|
| Exception Orders | `--exception` |
| Inbound | `--inbound` |
| Outbound | `--outbound` |
| Recebimento SOC | `--recebimento` |
| Escalation Ticket | `--escalation` |
| Liquidation | `--liquidation` |
| SPX Duplicates | `--spx-duplicados` |
| Duplicates Status | `--status-dup` |
| Workstation | `--workstation` |
| Online SOC Tracking | `--online-soc` |

## Local run

1. Install dependencies:
   - `python -m pip install -r requirements.txt`
2. Create your environment file:
   - copy `.env.example` to `.env`
3. Fill required environment variables.
4. Run a test module:
   - `python main.py --exception`

## GitHub Actions

Workflow file: [.github/workflows/scrape_tratativa.yml](.github/workflows/scrape_tratativa.yml).

Set the following in `Settings > Secrets and variables > Actions`:

- **Repository Secrets (required):**
  - `GOOGLE_SHEETS_ID`
  - `GOOGLE_SERVICE_ACCOUNT`
  - `SPX_BASE_URL`
  - `SPX_API_TRACKING_LIST_SEARCH`
  - `SPX_API_TRACKING_INFO`
  - `SPX_API_ESCALATION_TICKET`
  - `SPX_API_EXCEPTION_ORDER_HISTORY`
  - `SPX_API_EXCEPTION_HANDLING_EO_LIST`
  - `SPX_API_WFM_DASHBOARD`
- **Repository Secrets (recommended):**
  - `GOOGLE_OAUTH_CLIENT_SECRET`
- **Repository Variables (optional):**
  - `ONLINE_SOC_SPREADSHEET_ID`
  - `ONLINE_SOC_BASE_STATUS_SPREADSHEET_ID`

## Security

Sensitive-data policy:

- Internal SPX endpoints must live in local `.env` and GitHub Secrets (never hardcoded in code/docstrings).
- Google credentials (Service Account and OAuth client secret) must live in local `.env` and/or GitHub Secrets.
- Session/operational items must stay only in the spreadsheet `CONFIG_CLOUD` tab:
  - `cookies_json`, `x-sap-*`, `pg-i`, `device-id`, `app`, `Authorization`
  - `GOOGLE_OAUTH_TOKEN`, `GOOGLE_SESSION_COOKIES`
  - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `DISCORD_WEBHOOK_URL`

Sensitive and local operational files are kept out of Git through `.gitignore`.