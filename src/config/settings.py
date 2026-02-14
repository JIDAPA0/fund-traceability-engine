"""Application settings loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    name: str


@dataclass(frozen=True)
class Settings:
    app_env: str
    log_level: str
    db_3306: DatabaseConfig
    db_3307: DatabaseConfig
    staging_db_name: str
    mart_db_name: str


def _db(prefix: str, default_name: str) -> DatabaseConfig:
    return DatabaseConfig(
        host=os.getenv(f"{prefix}_HOST", "127.0.0.1"),
        port=int(os.getenv(f"{prefix}_PORT", "3306")),
        user=os.getenv(f"{prefix}_USER", "root"),
        password=os.getenv(f"{prefix}_PASSWORD", ""),
        name=os.getenv(f"{prefix}_NAME", default_name),
    )


def get_settings() -> Settings:
    return Settings(
        app_env=os.getenv("APP_ENV", "local"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        db_3306=_db("DB_3306", "global_funds"),
        db_3307=_db("DB_3307", "thai_funds"),
        staging_db_name=os.getenv("STAGING_DB_NAME", "fund_traceability_staging"),
        mart_db_name=os.getenv("MART_DB_NAME", "fund_traceability_mart"),
    )


settings = get_settings()
