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
    global_raw_db: DatabaseConfig
    global_staging_db: DatabaseConfig
    global_mart_db: DatabaseConfig
    traceability_staging_db: DatabaseConfig
    traceability_mart_db: DatabaseConfig


def _db(server_prefix: str, default_port: int, name_env: str, default_name: str) -> DatabaseConfig:
    return DatabaseConfig(
        host=os.getenv(f"{server_prefix}_HOST", "127.0.0.1"),
        port=int(os.getenv(f"{server_prefix}_PORT", str(default_port))),
        user=os.getenv(f"{server_prefix}_USER", "root"),
        password=os.getenv(f"{server_prefix}_PASSWORD", ""),
        name=os.getenv(name_env, default_name),
    )


def get_settings() -> Settings:
    return Settings(
        app_env=os.getenv("APP_ENV", "local"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        global_raw_db=_db("GLOBAL_DB", 3306, "GLOBAL_RAW_DB_NAME", "global_funds_raw"),
        global_staging_db=_db("GLOBAL_DB", 3306, "GLOBAL_STAGING_DB_NAME", "global_funds_staging"),
        global_mart_db=_db("GLOBAL_DB", 3306, "GLOBAL_MART_DB_NAME", "global_funds_mart"),
        traceability_staging_db=_db(
            "TRACEABILITY_DB",
            3307,
            "TRACEABILITY_STAGING_DB_NAME",
            "fund_traceability_staging",
        ),
        traceability_mart_db=_db(
            "TRACEABILITY_DB",
            3307,
            "TRACEABILITY_MART_DB_NAME",
            "fund_traceability_mart",
        ),
    )


settings = get_settings()
