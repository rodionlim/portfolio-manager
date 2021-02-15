import logging
import pandas as pd

from pf_manager.db import sqlalchemy_engine_session, dao
from pf_manager.db.utils import entity_to_df


class PortfolioManager():
    def __init__(self, portfolio):
        self.portfolio = portfolio
        self.blotter_dao = dao.get_Blotter_dao()
        self.market_dividends_dao = dao.get_MarketDividends_dao()

    def positions(self, group_keys: list = []):
        """ Return live positions for a particular user """
        data = self._extract_blotter()
        if data.empty(): return data

        group_keys = ["name"] if not group_keys else group_keys
        return data.groupby(group_keys)["qty"].apply(lambda x: x.sum())

    def _compute_capital_gains_pl(self):
        # TODO: Augment NPV with market price * quantity
        pass

    def _compute_dividends_pl(self):
        # TODO: Augment realized PL (dividends)

        pass

    def _extract_blotter(self, all_flag=False):
        """ all_flag to extract all blotter """
        with sqlalchemy_engine_session() as session:
            logging.info(f"Getting {self.portfolio} positions from db")
            query_obj, Entity = self.blotter_dao.mget_all_custom(session)

            query_obj = query_obj if all_flag else query_obj.filter(
                Entity.portfolio == self.portfolio)

            return entity_to_df(
                query_obj.filter(Entity.execution_status == "y").all())

    def _extract_market_dividends(self):
        with sqlalchemy_engine_session() as session:
            logging.info(f"Getting market dividends from db")
            return entity_to_df(self.market_dividends_dao.mget_all(session))

    def _compute_single_dividend(self, market_dividend: dict,
                                 position: pd.DataFrame):
        name = market_dividend.get("name")
        ex_date = market_dividend.get("ex_date")
        wkdf = position[(position["name"] == name)
                        and (position["date"] < ex_date)]
        wkdf.groupby(["strategy", "portfolio", "book"])


if __name__ == "__main__":
    pm = PortfolioManager("rodion")
    print(pm._extract_market_dividends())
