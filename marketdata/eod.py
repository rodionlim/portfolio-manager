import json
import logging
import requests

from pf_manager.db import sqlalchemy_engine_session, dao
from pf_manager.db.utils import entity_to_df


class EodPriceManager:
    """ This class controls the extraction of market data via http protocol """
    def __init__(self):
        self._yahoo_address = "https://query1.finance.yahoo.com/v8/finance/chart"
        self._yahoo_relevant_fields = [
            "regularMarketPrice", "symbol", "previousClose"
        ]
        self._reference_dao = dao.get_ReferenceData_dao()

    def get_all(self):
        pass

    def get_market_data(self,
                        ticker: str = "ES3.SI") -> requests.models.Response:
        api = f"{self._yahoo_address}/{ticker}"
        resp = requests.get(api)
        logging.info(f'get ticker data from api')

        res = json.loads(resp.text)
        status = res.get("error")

        if not status:
            meta = res.get("chart").get("result")[0].get("meta")
            return {x: meta.get(x) for x in self._yahoo_relevant_fields}
        else:
            return {"error": status}

    def _get_relevant_tickers(self):
        with sqlalchemy_engine_session() as session:
            ref = self._reference_dao.mget_all(session)


if __name__ == "__main__":
    epm = EodPriceManager()
    print(epm.get_market_data("ES3.SI"))
