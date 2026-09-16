from pathlib import Path

from dotenv import load_dotenv


def project_root_dir(module_file: str) -> Path:
    return Path(module_file).resolve().parent.parent


def load_project_dotenv(module_file: str) -> None:
    load_dotenv(dotenv_path=project_root_dir(module_file) / ".env")

