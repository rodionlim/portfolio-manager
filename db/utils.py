import pandas as pd
from pandas.core.frame import DataFrame


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


def dao_entity_generators(tableName: str,
                          path: str = None,
                          df: DataFrame = None):
    """ This method serves as a string generator for ORM entities and dao functions. 
    Always generate those objects via this method, before performing any custom edits.
    It uses pandas to infer data types for each column, before having custom overrides.
    Either supply a filepath with csv or xls/xlsx extension, or an in memory pandas dataframe
    """
    assert isinstance(tableName, str)

    if isinstance(df, pd.DataFrame):
        dtypes = df.dtypes
    else:
        if ".xls" in path:
            dtypes = pd.read_excel(path, "raw_instruments").dtypes
        else:
            dtypes = pd.read_csv(path).dtypes

    dtype_mapper = {
        "bool": "Boolean",
        "int64": "BIGINT",
        "object": "String(80)",
        "float64": "DECIMAL(40, 8)",
        "datetime64[ns]": "DateTime"
    }

    entityName = "".join([x.capitalize() for x in tableName.split("_")])

    print(f'--- Paste this into entity.py ---')
    print()
    print(f"class {entityName}(Base):")
    print(f"    __tablename__ = '{tableName}'")
    print(f'    id = Column(Integer, primary_key=True)')

    setIndexStr = ", index=True"

    for col, dtype in dtypes.items():
        coltype = dtype_mapper.get(str(dtype))
        idx = ""
        # Sanitization
        if "date" in col.lower():
            coltype = "Date"
            if col.lower() in ["trade_date", "date", "ex_date"]:
                idx = setIndexStr
        if not coltype:
            coltype = "UNMAPPED"
        if col.lower() in [
                "instrument", "portfolio", "name", "short_name", "book",
                "strategy"
        ]:
            # Columns with index
            idx = setIndexStr
        if col.lower() in ["yahoo_ticker", "google_ticker"]:
            coltype = "String(30)"

        print(
            f'    {col.replace(" / ","_Or_").replace("&","").replace(" ","_").replace("/", "_").replace(".","_")} = Column({coltype}{idx})'
        )
    print()
    print(
        "**Remember to set indexes, __tablename__ and primary keys, and remove filedate if it is not necessary.."
    )

    print()
    dao_creator_str = f'''
--- Paste this into dao.py ---

class {entityName}(BaseDao):
    pass

def get_{entityName}_dao():
    """Don't create DAO object but use `get_xxx_dao()`, since DAO object should be singleton"""
    return {entityName}(entity.{entityName})

--- END ---
You might want to change the inheritence from BaseDao to something else
'''
    print(dao_creator_str)
