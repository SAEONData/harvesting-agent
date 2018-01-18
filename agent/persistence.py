from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from .history_meta import versioned_session
from . import config

engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(
    config.DB_USER, config.DB_PASS, config.DB_HOST, config.DB_NAME), echo=False)  # echo=True to emit SQLAlchemy log messages

Persistent = declarative_base()

_session_factory = sessionmaker(bind=engine)
versioned_session(_session_factory)
session = scoped_session(_session_factory)


@contextmanager
def transaction():
    """
    Provide a transactional scope around a series of operations using 'with' syntax.
    """
    try:
        yield
        session.commit()
    except:
        session.rollback()
        raise
