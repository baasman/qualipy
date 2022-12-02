import logging
import os

from qualipy.store.initial_models import Base

import sqlalchemy as sa
from sqlalchemy.pool import (
    NullPool,
    QueuePool,
    SingletonThreadPool,
    StaticPool,
)


logger = logging.getLogger(__name__)


def _get_package_dir():
    """Returns directory containing MLflow python package."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(current_dir, os.pardir))


def _get_alembic_config(engine, alembic_dir: str = None):
    from alembic.config import Config

    final_alembic_dir = (
        os.path.join(_get_package_dir(), "store", "db_migrations")
        if alembic_dir is None
        else alembic_dir
    )
    db_url = str(engine.url)
    db_url = db_url.replace("%", "%%")
    config = Config(os.path.join(final_alembic_dir, "alembic.ini"))
    config.set_main_option("script_location", final_alembic_dir)
    config.set_main_option("sqlalchemy.url", db_url)
    config.attributes["configure_logger"] = False
    return config


def _upgrade_db(engine):
    from alembic import command

    logger.info("Updating database tables...")
    config = _get_alembic_config(engine=engine)
    with engine.begin() as connection:
        config.attributes["connection"] = connection  # pylint: disable=E1137
        # command.init(config, directory=os.path.dirname(config.config_file_name))
        command.upgrade(config, "heads")


def _stamp_db(engine):
    from alembic import command

    logger.info("Updating database tables...")
    config = _get_alembic_config(engine=engine)
    with engine.begin() as connection:
        config.attributes["connection"] = connection  # pylint: disable=E1137
        # command.init(config, directory=os.path.dirname(config.config_file_name))
        command.stamp(config, "heads")


def _initialize_tables(qconf):
    logger.info("Creating Qualipy tables...")
    engine = create_sqlalchemy_engine(qconf)
    # only create all if it doesnt exist yet, then stamp if created
    if not sa.inspect(engine).has_table("project"):
        Base.metadata.create_all(engine)
        _stamp_db(engine)
    _upgrade_db(engine)


def create_sqlalchemy_engine(qconfig):
    db_conf = qconfig.get("DB_CONFIG", {})
    pool_size = db_conf.get("SQLALCHEMYSTORE_POOL_SIZE")
    pool_max_overflow = db_conf.get("SQLALCHEMYSTORE_MAX_OVERFLOW")
    pool_recycle = db_conf.get("SQLALCHEMYSTORE_POOL_RECYCLE")
    echo = db_conf.get("SQLALCHEMYSTORE_ECHO")
    poolclass = db_conf.get("SQLALCHEMYSTORE_POOLCLASS")
    pool_kwargs = {}
    if pool_size:
        pool_kwargs["pool_size"] = pool_size
    if pool_max_overflow:
        pool_kwargs["max_overflow"] = pool_max_overflow
    if pool_recycle:
        pool_kwargs["pool_recycle"] = pool_recycle
    if echo:
        pool_kwargs["echo"] = echo
    if poolclass:
        pool_class_map = {
            "NullPool": NullPool,
            "QueuePool": QueuePool,
            "SingletonThreadPool": SingletonThreadPool,
            "StaticPool": StaticPool,
        }
        if poolclass not in pool_class_map:
            list_str = " ".join(pool_class_map.keys())
            err_str = (
                f"Invalid poolclass parameter: {poolclass}. Set environment variable "
                f"poolclass to empty or one of the following values: {list_str}"
            )
            logger.warning(err_str)
            raise ValueError(err_str)
        pool_kwargs["poolclass"] = pool_class_map[poolclass]
    if pool_kwargs:
        logger.info("Create SQLAlchemy engine with pool options %s", pool_kwargs)
    return sa.create_engine(qconfig["QUALIPY_DB"], pool_pre_ping=True, **pool_kwargs)


if __name__ == "__main__":
    from qualipy.config import QualipyConfig

    conf = QualipyConfig(config_dir="/home/baasman/projects/qualipy/examples/qpy_home")
    _initialize_tables(conf)
