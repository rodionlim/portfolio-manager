from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DECIMAL

from pf_manager.db.orm import _session, BaseDAOModel


class Entity(declarative_base()):
    __tablename__ = 'reference_data'

    id = Column(Integer, primary_key=True)
    name = Column(String(80), index=True)
    short_name = Column(String(80), index=True)
    short_name_grouped = Column(String(80), index=True)
    description = Column(String(600))
    yahoo_ticker = Column(String(30), index=True)
    google_ticker = Column(String(30), index=True)
    tradingview_ticker = Column(String(50), index=True)
    asset_class = Column(String(80))
    product = Column(String(80))
    sub_product = Column(String(80))
    main_country = Column(String(80))
    exchange = Column(String(80))
    trade_ccy = Column(String(80))
    ccy1 = Column(String(80))
    ccy2 = Column(String(80))
    active = Column(Boolean)
    price_overwrite = Column(DECIMAL(40, 8))


class DAO(BaseDAOModel):
    Entity = Entity

    @classmethod
    def insert(cls, entity):
        with _session() as ss:
            ss.add(entity)
            ss.flush()
            ss.refresh(entity)

    @classmethod
    def del_by_ticker(cls, type: str = "yahoo", ticker: str = None):
        with _session() as ss:
            ss.query(Entity).filter(
                cls.get_ticker_type(type) == ticker).delete(
                    synchronize_session=False)

    @classmethod
    def get_distinct_tickers(cls, type: str = "yahoo", active: bool = True):
        with _session() as ss:
            return ss.query(cls.get_ticker_type(type)).filter(
                Entity.active == active).distinct().all()

    @classmethod
    def get_ticker_type(cls, type: str = "yahoo"):
        if type == "yahoo":
            return Entity.yahoo_ticker
        elif type == "google":
            return Entity.google_ticker
        elif type == "tv":
            return Entity.tradingview_ticker
        else:
            raise ValueError("Invalid market data source")


if __name__ == "__main__":
    print(DAO.get_distinct_tickers())
