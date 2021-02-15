import logging
import pandas as pd

from pf_manager.db import sqlalchemy_engine_session, dao
from pf_manager.db.utils import entity_to_df


class PortfolioManager():
    """ 
    All portfolio related actions are encapsulated in this class, such as 
    position derivation, dividends computation etc.
    """
    def __init__(self, portfolio):
        self.portfolio = portfolio
        self.blotter_dao = dao.get_Blotter_dao()
        self.market_dividends_dao = dao.get_MarketDividends_dao()
        self.dividends_dao = dao.get_Dividends_dao()

    def positions(self,
                  group_keys: list = [],
                  blotter_df: pd.DataFrame = None) -> pd.DataFrame:
        """ Return live positions for a particular user, grouped by ticker name """
        if not isinstance(blotter_df, pd.DataFrame):
            data = self._extract_blotter()
        else:
            data = blotter_df

        if data.empty: return data
        group_keys = ["name"] if not group_keys else group_keys
        return data.groupby(group_keys)["qty"].apply(lambda x: x.sum())

    def calc_dividends(self, force_calc: bool = False) -> pd.DataFrame:
        """ 
        Calculate dividend amount for portfolio based on positions, market dividend amount and ex-date.
        force_calc:  recalculate dividends whether it has been calculated previously or not
        """
        results = pd.DataFrame()
        blotter = self._extract_blotter()
        for ticker in blotter["name"].unique():
            logging.info(f"Computing dividends for {ticker}")
            entries = blotter[blotter["name"] == ticker]
            sdt = entries["date"].min()
            edt = entries["date"].max()

            # Market Dividends
            with sqlalchemy_engine_session() as session:
                query_obj, Entity = self.market_dividends_dao.mget_by_dates_custom(
                    session, sdt, edt)
                market_dividends = entity_to_df(
                    query_obj.filter(Entity.name == ticker).all())
                if market_dividends.empty: continue

                # Pre-computed dividends
                query_obj, Entity = self.dividends_dao.mget_by_dates_custom(
                    session, sdt, edt)
                dividends = entity_to_df(
                    query_obj.filter(Entity.name == ticker).all())

            if not dividends.empty and not force_calc:
                dates_to_be_computed = set(market_dividends["ex_date"]) - set(
                    dividends["date"])
            else:
                dates_to_be_computed = set(market_dividends["ex_date"])

            market_dividends = market_dividends.set_index(
                "ex_date").loc[dates_to_be_computed].reset_index()

            for _, dividend in market_dividends.iterrows():
                logging.info(
                    f"Computing dividends for {ticker}-{dividend.get('ex_date')}"
                )
                d = self._compute_single_dividend(dividend, entries)
                results = pd.concat([results, d])
        return results

    def _compute_capital_gains_pl(self):
        # TODO: Augment NPV with market price * quantity
        pass

    def _compute_dividends_pl(self):
        # TODO: Augment realized PL (dividends)

        pass

    def _extract_blotter(self, all_flag=False) -> pd.DataFrame:
        """ all_flag to extract all blotter instead of single portfolio """
        with sqlalchemy_engine_session() as session:
            logging.info(f"Getting {self.portfolio} positions from db")
            query_obj, Entity = self.blotter_dao.mget_all_custom(session)

            query_obj = query_obj if all_flag else query_obj.filter(
                Entity.portfolio == self.portfolio)

            return entity_to_df(
                query_obj.filter(Entity.execution_status == "y").all())

    def _extract_market_dividends(self) -> pd.DataFrame:
        with sqlalchemy_engine_session() as session:
            logging.info(f"Getting market dividends from db")
            return entity_to_df(self.market_dividends_dao.mget_all(session))

    def _compute_single_dividend(self, market_dividend: dict,
                                 blotter: pd.DataFrame) -> pd.DataFrame:
        """ Calculate actual dividends received based on dividend per share and blotter positions """
        name = market_dividend.get("name")
        ex_date = market_dividend.get("ex_date")
        dps = market_dividend.get("dividend_amount") * (
            1 - market_dividend.get("witholding_tax"))
        wkdf = blotter[(blotter["name"] == name) & (blotter["date"] < ex_date)]
        return (self.positions(["strategy", "portfolio", "book", "name"],
                               wkdf)).reset_index().assign(
                                   date=ex_date,
                                   amount=lambda x: x.qty * dps,
                                   dps=dps)


if __name__ == "__main__":
    pm = PortfolioManager("rodion")
    x = pm.calc_dividends()