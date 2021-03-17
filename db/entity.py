from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.types import Boolean, DateTime, Date, BIGINT, DECIMAL

Base = declarative_base()


class Blotter(Base):
    __tablename__ = 'blotter'
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True)
    account = Column(String(80))
    name = Column(String(80), index=True)
    strategy = Column(String(80), index=True)
    price = Column(DECIMAL(40, 8))
    qty = Column(BIGINT)
    price_qty = Column(DECIMAL(40, 8))
    action = Column(String(80))
    execution_status = Column(String(80))
    fees = Column(DECIMAL(40, 8))
    amount = Column(DECIMAL(40, 8))
    portfolio = Column(String(80), index=True)
    book = Column(String(80), index=True)


class MarketDividends(Base):
    __tablename__ = 'market_dividends'
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True)
    ex_date = Column(Date, index=True)
    name = Column(String(80), index=True)
    dividend_amount = Column(DECIMAL(40, 8))
    witholding_tax = Column(BIGINT)


class ReferenceData(Base):
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


class Metadata(Base):
    __tablename__ = 'metadata'
    id = Column(Integer, primary_key=True)
    table = Column(String(80))
    field = Column(String(80))
    description = Column(String(200))


class Dividends(Base):
    __tablename__ = 'dividends'
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True)
    name = Column(String(80), index=True)
    strategy = Column(String(80), index=True)
    portfolio = Column(String(80), index=True)
    book = Column(String(80), index=True)
    qty = Column(DECIMAL(40, 8))
    dps = Column(DECIMAL(40, 8))
    amount = Column(DECIMAL(40, 8))
