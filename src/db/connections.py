"""Create SQLAlchemy engines for source and target MySQL databases."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from config.settings import DatabaseConfig, settings


def _mysql_url(host: str, port: int, user: str, password: str, name: str) -> str:
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"


def _engine(db_config: DatabaseConfig) -> Engine:
    return create_engine(
        _mysql_url(
            db_config.host,
            db_config.port,
            db_config.user,
            db_config.password,
            db_config.name,
        ),
        pool_pre_ping=True,
    )


def create_global_raw_engine() -> Engine:
    return _engine(settings.global_raw_db)


def create_global_staging_engine() -> Engine:
    return _engine(settings.global_staging_db)


def create_global_mart_engine() -> Engine:
    return _engine(settings.global_mart_db)


def create_traceability_staging_engine() -> Engine:
    return _engine(settings.traceability_staging_db)


def create_traceability_mart_engine() -> Engine:
    return _engine(settings.traceability_mart_db)


# Backward-compatible aliases for the initial scaffold naming.
def create_engine_for_3306() -> Engine:
    return create_global_raw_engine()


def create_engine_for_3307() -> Engine:
    return create_traceability_staging_engine()


def create_engine_for_staging() -> Engine:
    return create_traceability_staging_engine()


def create_engine_for_mart() -> Engine:
    return create_traceability_mart_engine()
