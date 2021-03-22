from aiohttp import ClientSession
import asyncio
from bs4 import BeautifulSoup
from collections import deque
import pandas as pd
import re

from pf_manager.db.orm.reference_data import DAO


class DividendManager:
    def __init__(self):
        self.URL = "https://www.dividends.sg/view/"

    async def get_all():
        relevant_sg_tickers = DAO.get(where=[("main_country", "singapore"),
                                             ("active", True),
                                             ("asset_class", "equity")],
                                      cols=["yahoo_ticker"])
        raise NotImplementedError()

    async def get(self, ticker: str = "AJBU") -> pd.DataFrame:
        url = self.URL + ticker
        results = pd.DataFrame()
        async with ClientSession() as session:
            async with session.get(url) as response:
                try:
                    response.raise_for_status()
                    print(f"Response status ({url}): {response.status}")
                    response_txt = await response.text()
                    results = self.parse(response_txt)
                    results["ticker"] = ticker
                except Exception as e:
                    print(f"An error has occured: {e}")
        return results

    def parse(self, response_txt: str) -> pd.DataFrame:
        soup = BeautifulSoup(response_txt, 'html.parser')
        table = soup.find("table")
        rows = table.find_all("tr")
        data = []
        rowspan_handler = []
        for row in rows:
            # Column Names
            if row.find_all("th"):
                headers = [x.text for x in table.find_all("th")]
                data.append(headers)
                rowspan_handler = [deque() for _ in headers]
            # Column Data
            else:
                cols = row.find_all('td')
                for i, stack in enumerate(rowspan_handler):
                    if len(stack) > 0:
                        # Handle data spanning > 1 row
                        cols.insert(
                            i,
                            BeautifulSoup(f'<td>{stack.pop()}</td>',
                                          "html.parser"))

                # Extracting data from html
                for i, col in enumerate(cols):
                    rs = col.get("rowspan")
                    if rs:
                        [
                            rowspan_handler[i].append(col.text)
                            for _ in range(int(rs) - 1)
                        ]
                data.append([ele.text.strip() for ele in cols])
        results = pd.DataFrame(data[1:], columns=data[0])

        def parseAmount(val):
            find = re.search(r'[\d.$]+', val)
            if find:
                return float(find.group())
            else:
                return None

        results["Amount"] = results["Amount"].apply(lambda x: parseAmount(x))
        return results.groupby(["Ex Date",
                                "Pay Date"])["Amount"].sum().reset_index()

    def get_sync(self, ticker: str = "AJBU"):
        """ Deprecated - For debugging only """
        import requests
        response = requests.get(self.URL + ticker).text
        return self.parse(response)


if __name__ == "__main__":
    # htmls = asyncio.gather(*tasks)
    dm = DividendManager()
    # print(dm.get_sync())

    loop = asyncio.get_event_loop()
    loop.run_until_complete(dm.get(ticker="AJBU"))
