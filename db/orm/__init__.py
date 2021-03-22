import contextlib
import logging
import os
import pandas as pd
import sqlalchemy
from typing import Union
import ujson

from sqlalchemy.orm import sessionmaker, scoped_session

LOGGER = logging.getLogger(__name__)

_engine = None
_factory = None

# TODO: _session should be thread local.
_ss = None


def get_creds_file():
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..", 'creds.json'))


def init(db: str = None):
    global _engine, _factory
    if _engine is None:
        with open(get_creds_file(), 'r') as f:
            param = ujson.load(f)['conn_param_admin']
        _engine = sqlalchemy.create_engine(
            f'mysql://{param["user"]}:{param["passwd"]}@{param["host"]}:{param["port"]}/{db if db else param["db"]}'
        )
    if _factory is None:
        _factory = sessionmaker(bind=_engine)


def destroy():
    global _engine
    if _engine is not None:
        _engine.dispose()


@contextlib.contextmanager
def _session(auto_commit=True):
    global _ss
    if _ss is not None:
        yield _ss
        # don't commit here in case there is a transaction going on.
        return

    init()
    ss = scoped_session(_factory)
    yield ss
    if auto_commit:
        try:
            ss.commit()
        except Exception:
            LOGGER.exception('failed to commit')
    try:
        ss.close()
    except Exception:
        LOGGER.exception('failed to close sqlalchemy session')


@contextlib.contextmanager
def transaction(func, *args, auto_commit=True):
    global _ss
    if _ss is not None:
        func(*args)
        return

    with _session(auto_commit=auto_commit) as ss:
        _ss = ss
        try:
            func(*args)
        except Exception:
            raise
        finally:
            _ss = None


def entity_to_df(rows):
    """
    convert a list of entity object to DataFrame
    TODO(x) to be backward compatible, DataFrame should always have columns even if it's empty.
    """
    d = {}
    n = 0
    if rows is not None:
        for row in rows:
            d[n] = entity_to_dict(row)
            n += 1
    return pd.DataFrame.from_dict(d, orient='index')


def _resultproxy_to_df(rp):
    """
    1. session.execute() returns a ResultProxy
    2. ResultProxy if it doesn't query ORM class.
    For ease of use, let's convert ResultProxy to DataFrame before returning.
    """
    d = {}
    n = 0
    if rp is not None:
        for row in rp:
            d[n] = dict(zip(row.keys(), row))
            n += 1
    return pd.DataFrame.from_dict(d, orient='index')


def entity_to_dict(r):
    d = {}
    for column in r.__table__.columns:
        d[column.name] = getattr(r, column.name)
    return d


class BaseDAOModel():
    """DAO classes can inherit this class to gain functional sql method. 
    Ensure that children sets the relevant ORM Entity as a class attribute.
    """
    Entity = None

    @classmethod
    def isValidIter(cls, item):
        return isinstance(item, list) or isinstance(item, tuple)

    # TODO(rl): implement retry on get method
    @classmethod
    def get(cls,
            by: Union[list, tuple, dict] = None,
            where: list = None,
            cols: Union[list, tuple, dict] = None,
            df_flag: bool = True) -> Union[pd.DataFrame, list]:
        """ Functional SQL - SQL as a function
        DAO.get(): Extract all data from a table. Use with care
        DAO.get(cols=["asset", "trading_day", "pnl"]): Select asset, trading_day and pnl table 
        DAO.get(where=[("trading_day", [datetime.date(2021, 3, 5)], "in")]): Filter for 5 mar in trading_day
        DAO.get(where=["trading_day", datetime.date(2021, 3, 5)]): Filter for trading_day equals 5 mar 
        DAO.get(by=["asset","trading_day"], cols={"pnl":"sum"}): Group by trading_day and asset, get sum of pnl
        DAO.get(cols={"trading_day": "max"}, by={"trading_day":"month"}): Group by month(trading_day), and get max date of each month
        DAO.get(cols={"pnl":"sum", "pnl_mean":("mean","pnl")}): Select all pnl records and rename it pnl_mean
        DAO.get(cols={"asset":"distinct"}): Select all distinct asset
        DAO.get(df_flag=False): Return in ORM classes instead of pandas dataframe
        All list or tuple parameters are interchangable in this method.
        User can mix the group by, where and select clauses in various combinations, e.g.
        ***
        DAO.get(by=["trading_day", "asset"],
        where=[("trading_day", [datetime.date(2021, 3, 5)], "in")],
        cols={
            "asset": None,
            "trading_day": None,
            "pnl": "sum",
            "theta": "sum",
            "spotPrice": "avg",
            "sum_spotPrice": ("sum", "spotPrice")
        })
        ***
        """
        func = sqlalchemy.func
        isValidIter = cls.isValidIter
        valid_where_ops = ["in", "between"]
        valid_ops_mysql = [
            "sum", "distinct", "min", "max", "day", "month", "year", "avg"
        ]
        valid_ops_brahman = valid_ops_mysql + ["mean"]

        with _session() as ss:
            query_obj = None
            entity_flag = False

            # Selections
            if isValidIter(cols):
                query_obj = ss.query(*[getattr(cls.Entity, x) for x in cols])
            elif isinstance(cols, dict):
                select_cols = []
                for field, args in cols.items():
                    rename_col = field
                    op = cols[field]
                    if isValidIter(args):
                        # Either aggregation required or rename of columns
                        rename_col = field
                        field = op[1]
                        op = op[0]
                    column = getattr(cls.Entity, field)

                    if op and op.lower() in valid_ops_mysql:
                        select_cols.append(
                            getattr(func, op)(column).label(rename_col))
                    elif op == "mean":
                        select_cols.append(func.avg(column).label(rename_col))
                    elif not op:
                        select_cols.append(column)
                    else:
                        raise Exception(
                            f"{cols[field]} operation not supported yet")
                query_obj = ss.query(*select_cols)
            else:
                query_obj = ss.query(cls.Entity)
                entity_flag = True

            # Filters - where clause
            if where and query_obj:
                if len(where) <= 3 and not (isValidIter(where[0])):
                    where = [where]
                for wc in where:
                    # Defaults to equals
                    field = wc[0]
                    params = wc[1]
                    column = getattr(cls.Entity, field)
                    if len(wc) == 2:
                        query_obj = query_obj.filter(column == params)
                    elif len(wc) == 3:
                        op = wc[2]
                        assert op in valid_where_ops
                        if op == "in":
                            query_obj = query_obj.filter(column.in_(params))
                        elif op == "between":
                            # Support open ended between conditions via None being passed as param
                            if params[0]:
                                query_obj = query_obj.filter(
                                    column >= params[0])
                            if params[1]:
                                query_obj = query_obj.filter(
                                    column <= params[1])
                        else:
                            raise Exception(
                                f"{op} operation is not supported yet")
                    else:
                        raise Exception(f"{wc} is an invalid where clause.")

            # Group by
            if by and query_obj:
                if isValidIter(by):
                    query_obj = query_obj.group_by(
                        *[getattr(cls.Entity, x) for x in by])
                else:
                    if isinstance(by, dict):
                        bys = []
                        for col in by:
                            op = by[col]
                            assert op in valid_ops_brahman
                            bys.append(
                                getattr(func, op)(getattr(cls.Entity, col)))
                        query_obj = query_obj.group_by(*bys)

        res = query_obj.all()
        if df_flag:
            if entity_flag:
                return entity_to_df(res)
            else:
                return _resultproxy_to_df(res)
        return res