"""
Stub for Databricks dbutils so existing notebook code runs locally with minimal edits.

Secrets: resolved from environment variables.
  dbutils.secrets.get("any-scope", "my-secret-key")
  -> reads env var MY_SECRET_KEY (hyphens converted to underscores, uppercased)

Set secrets in your shell before starting JupyterLab:
  export MY_API_KEY=...
  export MY_API_SECRET=...
"""

import os


class _SecretsUtil:
    def get(self, scope: str, key: str) -> str:
        env_key = key.upper().replace("-", "_")
        value = os.getenv(env_key)
        if value is None:
            raise KeyError(
                f"Secret '{key}' not found locally. "
                f"Set environment variable {env_key} before starting JupyterLab."
            )
        return value


class _WidgetsUtil:
    def get(self, name: str, default: str = "") -> str:
        return os.getenv(name.upper(), default)

    def text(self, name: str, default: str = "", label: str = "") -> None:
        pass

    def dropdown(self, name: str, default: str, choices: list, label: str = "") -> None:
        pass


class _NotebookUtil:
    def exit(self, value: str) -> None:
        print(f"[notebook.exit] {value}")


class _DbutilsStub:
    secrets = _SecretsUtil()
    widgets = _WidgetsUtil()
    notebook = _NotebookUtil()


dbutils = _DbutilsStub()
