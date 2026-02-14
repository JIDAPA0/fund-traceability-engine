"""Create SQLAlchemy engines for source and target MySQL databases."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from config.settings import settings


def _mysql_url(host: str, port: int, user: str, password: str, name: str) -> str:
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"


def create_engine_for_3306() -> Engine:
    db = settings.db_3306
    return create_engine(_mysql_url(db.host, db.port, db.user, db.password, db.name), pool_pre_ping=True)


def create_engine_for_3307() -> Engine:
    db = settings.db_3307
    return create_engine(_mysql_url(db.host, db.port, db.user, db.password, db.name), pool_pre_ping=True)


def create_engine_for_staging() -> Engine:
    db = settings.db_3306
    return create_engine(
        _mysql_url(db.host, db.port, db.user, db.password, settings.staging_db_name),
        pool_pre_ping=True,
    )


def create_engine_for_mart() -> Engine:
    db = settings.db_3306
    return create_engine(
        _mysql_url(db.host, db.port, db.user, db.password, settings.mart_db_name),
        pool_pre_ping=True,
    )
