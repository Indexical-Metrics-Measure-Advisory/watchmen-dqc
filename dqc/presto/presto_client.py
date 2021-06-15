import trino

from dqc.config.config import settings

conn = trino.dbapi.connect(
    host=settings.PRESTO_HOST,
    port=settings.PRESTO_PORT,
    user=settings.PRESTO_HOST,
    catalog=settings.PRESTO_CATALOG,
    schema=settings.PRESTO_SCHEMA,
)


def get_connection():
    return conn
