#!/usr/bin/env python3
"""Setup inicial do espelho público."""

import shutil
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent
ENV_EXAMPLE = BASE_DIR / ".env.example"
ENV_FILE = BASE_DIR / ".env"


def print_header() -> None:
    print()
    print("=" * 60)
    print("  Setup - Automated Handling (espelho público)")
    print("=" * 60)
    print()


def create_env_file() -> bool:
    if ENV_FILE.exists():
        print("  .env já existe. Pulando cópia do template.")
        return False

    shutil.copyfile(ENV_EXAMPLE, ENV_FILE)
    print("  .env criado a partir de .env.example")
    return True


def create_directories() -> None:
    directories = ["output", "logs", "chrome_profile"]
    for directory in directories:
        (BASE_DIR / directory).mkdir(parents=True, exist_ok=True)
    print(f"  Diretórios garantidos: {', '.join(directories)}")


def install_dependencies() -> None:
    print("\nInstalando dependências...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], cwd=BASE_DIR, check=False)
    print("  Dependências instaladas.")


def main() -> None:
    print_header()
    create_directories()
    env_created = create_env_file()

    response = input("\nInstalar dependências agora? (s/n): ").strip().lower()
    if response in ("", "s", "sim", "y", "yes"):
        install_dependencies()

    print("\n" + "=" * 60)
    print("  Próximos passos")
    print("=" * 60)
    print(
        """
  1. Preencha o .env apenas para uso local.
  2. Configure GOOGLE_SERVICE_ACCOUNT e GOOGLE_SHEETS_ID no ambiente ou no GitHub.
  3. Gere oauth_client_secret.json localmente ou use GOOGLE_OAUTH_CLIENT_SECRET.
  4. Rode python main.py --setup-oauth e depois python main.py --auth para sincronizar a sessão.
  5. Teste um módulo com python main.py --exception.
"""
    )

    if env_created:
        print("  O .env foi criado com placeholders seguros.")


if __name__ == "__main__":
    main()
