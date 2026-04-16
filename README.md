# Auto-EDA-SPX

Solução modular de **Automação e Engenharia de Dados** voltada para operações SPX. O projeto foca na extração automatizada, tratamento e sincronização de dados operacionais diretamente com o Google Sheets, garantindo integridade e disponibilidade das informações.

[English version: README.en.md](README.en.md)

## 🚀 Visão Geral e Arquitetura

Este projeto foi construído sob princípios de **modularidade e separação de responsabilidades**. Cada fluxo operacional é tratado como um módulo independente, permitindo escalabilidade e facilidade de manutenção.

  * **Core Engine:** Execução via CLI (Command Line Interface).
  * **Pipeline CI/CD:** Automação completa via GitHub Actions para execuções agendadas.
  * **Data Sync:** Sincronização centralizada através do `CONFIG_CLOUD`, reduzindo a necessidade de intervenções manuais em tokens de sessão.

-----

## 🛠️ Módulos Operacionais

A execução é baseada em flags, permitindo rodar processos específicos ou em cadeia:

| Módulo | Comando | Descrição |
| :--- | :--- | :--- |
| **Exception Orders** | `--exception` | Monitoramento de ordens com exceção. |
| **Inbound/Outbound** | `--inbound` / `--outbound` | Fluxo de entrada e saída de mercadorias. |
| **Recebimento SOC** | `--recebimento` | Status de recebimento no SOC. |
| **Escalation Ticket** | `--escalation` | Gestão de tickets críticos. |
| **Liquidation** | `--liquidation` | Processamento de liquidações. |
| **Data Integrity** | `--spx-duplicados` / `--status-dup` | Validação e limpeza de duplicidade. |
| **Tracking** | `--online-soc` / `--workstation` | Rastreamento em tempo real e status de estação. |

-----

## 💻 Configuração Local

### Pré-requisitos

  * Python 3.9 ou superior.
  * Ambiente virtual (recomendado).

### Instalação

1.  Clone o repositório:

    ```bash
    git clone https://github.com/seu-usuario/Auto-EDA-SPX.git
    cd Auto-EDA-SPX
    ```

2.  Instale as dependências:

    ```bash
    PS D:\PROJETOS\Auto-EDA-SPX> python -m venv venv
    PS D:\PROJETOS\Auto-EDA-SPX> .\venv\Scripts\Activate.ps1
    (venv) PS D:\PROJETOS\Auto-EDA-SPX> python -m pip install -r requirements.txt
    ```

3.  Configure as variáveis de ambiente:

    ```bash
    cp .env.example .env
    ```

    *Edite o arquivo `.env` com suas credenciais e endpoints.*

4.  Execute um módulo:

    ```bash
    python main.py --exception
    ```

-----

## ⚙️ CI/CD e Secrets (GitHub Actions)

Para a execução agendada, configure os **Secrets** em `Settings > Secrets and variables > Actions`:

### Repository Secrets (Críticos)

| Secret | Descrição |
| :--- | :--- |
| `GOOGLE_SERVICE_ACCOUNT` | JSON da Service Account formatado. |
| `GOOGLE_SHEETS_ID` | ID da planilha mestre. |
| `SPX_BASE_URL` | Endpoint base para as requisições. |
| `SPX_API_*` | Tokens e endpoints específicos (Ex: `ESCALATION_TICKET`, `WFM_DASHBOARD`). |

### Repository Variables (Opcionais)

  * `ONLINE_SOC_SPREADSHEET_ID`: Planilha específica para tracking SOC.
  * `ONLINE_SOC_BASE_STATUS_SPREADSHEET_ID`: Base de status histórica.

-----

## 🛡️ Segurança e Compliance de Dados

Este projeto adota uma política rigorosa de **Zero Hardcoded Secrets**:

1.  **Isolamento de Credenciais:** Endpoints e tokens internos nunca são expostos no código fonte.
2.  **Gestão de Sessão:** Itens voláteis (cookies, `x-sap-*`, `Authorization`) são consumidos dinamicamente da aba `CONFIG_CLOUD` ou via Secrets.
3.  **Git Hygiene:** O arquivo `.gitignore` está configurado para impedir o versionamento acidental de `.env`, arquivos JSON de credenciais e logs operacionais.

-----

## 📄 Licença

Distribuído sob a licença MIT. Veja `LICENSE` para mais informações.
