import contextlib
import copy
import logging
import MySQLdb
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import ujson as json

LOGGER = logging.getLogger(__name__)


# TODO: migrate to class methods
def get_creds_file():
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", 'creds.json'))


with open(get_creds_file(), 'r') as f:
    conn_param_admin = json.load(f)['conn_param_admin']


def infer_perm_route(perm):
    assert perm in ['r', 'rw']
    if perm == 'rw':
        return copy.deepcopy(conn_param_admin)
    else:
        raise NotImplementedError


try:
    mysql_operational_error = MySQLdb.OperationalError
except Exception:
    mysql_operational_error = Exception

try:
    mysql_prog_error = MySQLdb._exceptions.ProgrammingError
except Exception:
    mysql_prog_error = Exception


def sqlalchemy_engine(perm='rw', convert_unicode=True, db=None):
    conn_param = infer_perm_route(perm)
    # mysql+mysqldb
    engine = create_engine(
        "mysql://{}:{}@{}:{}/{}".format(
            conn_param['user'],
            conn_param['passwd'],
            conn_param['host'],
            conn_param['port'],
            db if db else conn_param['db'],  # Allow for custom db
        ),
        convert_unicode=convert_unicode)
    return engine


@contextlib.contextmanager
def sqlalchemy_session(engine):
    factory = sessionmaker(bind=engine)
    ss = scoped_session(factory)
    yield ss
    ss.close()


@contextlib.contextmanager
def sqlalchemy_engine_session(db=None):
    """this function is purely to save some typing. engine should be a singleton ideally, which can be done later"""
    engine = sqlalchemy_engine(db=db)
    with sqlalchemy_session(engine) as session:
        yield session
    engine.dispose()


if __name__ == '__main__':
    pass