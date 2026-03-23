# Auto-EDA-SPX

A modular automation engine for data extraction, monitoring, and operational integration, specifically tailored for **SPX (Shopee Xpress) operations**. This project streamlines data collection and synchronization with Google Sheets using scheduled GitHub Actions and independent operational modules.

## 🎯 Key Features

  - **Automated Data Extraction:** High-performance scraping and API integration for Inbound, Outbound, and Exception Orders.
  - **Seamless Google Sheets Integration:** Dynamic updates using Service Accounts and OAuth2.
  - **CI/CD Ready:** Native support for GitHub Actions for scheduled runs.
  - **Security-First Approach:** Strict isolation of sensitive endpoints and credentials via environment variables and encrypted secrets.

-----

## 🏗️ Architecture & Modules

The system is built as a **Command Line Interface (CLI)** with a modular architecture, allowing each operational flow to be executed independently.

| Module | Flag | Description |
| :--- | :--- | :--- |
| **Exception Orders** | `--exception` | Monitors and logs order exceptions. |
| **Inbound** | `--inbound` | Tracks incoming operational flow. |
| **Outbound** | `--outbound` | Monitors outgoing shipments and dispatch. |
| **SOC Receiving** | `--recebimento` | Real-time tracking of SOC reception status. |
| **Escalation Ticket** | `--escalation` | Manages critical operational tickets. |
| **Liquidation** | `--liquidation` | Processes liquidation data batches. |
| **Data Integrity** | `--spx-duplicados` | Identifies and handles duplicate SPX entries. |
| **Workstation** | `--workstation` | Assignment and history tracking for stations. |
| **Online SOC Tracking** | `--online-soc` | Real-time tracking for SOC operations. |

-----

## ⚙️ Local Setup

### 1\. Requirements

  - Python 3.9+
  - A Google Cloud Project with Sheets/Drive API enabled.

### 2\. Installation

```bash
# Install dependencies
python -m pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
```

### 3\. Execution

Run a specific module for testing:

```bash
python main.py --exception
```

-----

## 🔒 Security & Environment Configuration

This project follows a **Zero Hardcoded Secrets** policy. All sensitive data must be configured in your `.env` file (local) or **GitHub Secrets** (CI/CD).

### Configuration Mapping (`.env.example`)

  - **Google Credentials:** `GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_OAUTH_CLIENT_SECRET`.
  - **SPX Endpoints:** Internal API URLs (e.g., `SPX_API_WFM_DASHBOARD`) are kept private.
  - **Cloud Config:** Dynamic session data (cookies, tokens, `x-sap-*`) are retrieved from the `CONFIG_CLOUD` tab within the Google Sheet to avoid manual code updates.

> **Note:** Files like `.env`, session JSONs, and local logs are strictly excluded from version control via `.gitignore`.

-----

## 🤖 GitHub Actions Integration

The workflow defined in `.github/workflows/scrape_tratativa.yml` automates the execution. Ensure the following **Repository Secrets** are configured:

  * **Required:** `GOOGLE_SHEETS_ID`, `GOOGLE_SERVICE_ACCOUNT`, and all `SPX_API_*` endpoints.
  * **Optional Variables:** `ONLINE_SOC_SPREADSHEET_ID`, `ONLINE_SOC_BASE_STATUS_SPREADSHEET_ID`.

-----

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.
