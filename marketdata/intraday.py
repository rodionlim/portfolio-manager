""" 
IntradayPriceManager connects to TradingView and stores indicators and price series in an
in memory dictionary self._alerts. These indicators are then published to slack periodically.
"""

import datetime
import json
import pandas as pd
import random
import re
import string
import time
import threading
import websocket

from utilfns.slack import send_alert


class IntradayPriceManager():
    def __init__(self, debug=False):
        self._alerts = {
            "indicators": {},
            "price": {}
        }  # In-memory dict of alerts to be sent out to slack
        self._debug = debug
        self._histbars = 300
        self._indicators = []
        self._slackchannel = "C01UACFTMTK"  # TODO: Shift to config
        self._slackfreq = 300  # Every 5 mins
        self._state = {}
        self._syms = [
            "BINANCE:UNIUSD", "BINANCE:ETHUSD", "BINANCE:DOTUSD", "SGX:ES3",
            "SGX:CLR"
        ]
        self._t = None
        self._timeframe = 240  # Default to 4 hours chart
        self._ws_url = "wss://data.tradingview.com/socket.io/websocket"

    def get(self, type: str, **kwargs):
        """ 
        Type is either quote (live) or chart (historical + live) 
        Support kwargs: 
            syms: list of symbols, e.g. [BINANCE:ETHUSD]
            indicators: list of indicators, e.g. [rsi]
            timeframe: int of minutes of chart time frame, e.g. 240 -> 4 hours chart
            histbars: int of number of historical data points, e.g. 300
        """
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(
            self._ws_url,
            on_open=lambda ws: self.on_open(ws, type, **kwargs),
            on_close=self.on_close,
            on_message=lambda ws, message: self.on_message(ws, message),
            on_error=self.on_error)
        ws.run_forever()

    def send_slack(self):
        """
        Periodic slack alerts - Indicators
        """
        while True:
            indicators = self._alerts.get("indicators")
            if indicators:
                res = pd.DataFrame(indicators).transpose().reset_index()
                res.rename(columns={"index": "sym"}, inplace=True)
                send_alert(self._slackchannel, [("Indicators", res)])
            time.sleep(self._slackfreq)

    def on_message(self, ws, message):
        pattern = re.compile(r'~m~\d+~m~~h~\d+$')
        if pattern.match(message):
            ws.send(message)
        else:
            msg_body = re.compile(r'~m~\d+~m~')
            messages = msg_body.split(message)
            for msg in messages:
                if msg:
                    parsed_msg = json.loads(msg)
                    params = parsed_msg.get("p")
                    if parsed_msg.get("m") == "timescale_update":
                        # timescale_update -> initial historical data
                        # TODO: handling of these data for plotting on UI
                        continue
                    if parsed_msg.get("m") == "du":
                        # du -> data update
                        sym = self._state.get(params[0]).get("sym")
                        now = datetime.datetime.now().strftime(
                            '%Y-%m-%d %H:%M:%S')
                        for k, v in params[1].items():
                            if v.get("st"):
                                # study
                                indicator = k.split("_")[0]
                                vals = v.get("st")[0].get("v")
                                val = vals[1]
                                val_dict = {"dtime": now, indicator: val}
                                # print({sym: val_dict})
                                if not self._alerts["indicators"].get(sym):
                                    self._alerts["indicators"][sym] = {}
                                self._alerts["indicators"][sym][
                                    indicator] = val
                            elif v.get("s"):
                                # series
                                vals = v.get("s")[0].get("v")
                                val_dict = dict(
                                    zip([
                                        "dtime", "open", "high", "low", "last",
                                        "vol"
                                    ], vals))
                                val_dict["dtime"] = now
                                # print({sym: val_dict})
                                if not self._alerts["price"].get(sym):
                                    self._alerts["price"][sym] = {}
                                self._alerts["price"][sym]["last"] = val_dict[
                                    "last"]

    @staticmethod
    def on_error(ws, error):
        print(error)

    @staticmethod
    def on_close(ws):
        print("### closed ###")

    def on_open(self, ws, type: str, **kwargs):
        def run(*args, **kwargs):
            # ~m~52~m~{"m":"quote_create_session","p":["qs_3bDnffZvz5ur"]}
            # ~m~395~m~{"m":"quote_set_fields","p":["qs_3bDnffZvz5ur","ch","chp","lp"]}
            # ~m~89~m~{"m":"quote_add_symbols","p":["qs_3bDnffZvz5ur","SP:SPX",{"flags":["force_permission"]}]}
            # ~m~315~m~{"m":"quote_fast_symbols","p":["qs_3bDnffZvz5ur","SP:SPX","TVC:NDX","CBOE:VIX","TVC:DXY","SGX:ES3","NASDAQ:AAPL","NASDAQ:MSFT","NASDAQ:TSLA","TVC:USOIL","TVC:GOLD","TVC:SILVER","FX:AUDUSD","FX:EURUSD","FX:GBPUSD","FX:USDJPY","BITSTAMP:BTCUSD","BITSTAMP:ETHUSD","COINBASE:UNIUSD","BINANCE:DOGEUSD","BINANCE:DOTUSD"]}

            syms = kwargs.get("syms") or self._syms
            timeframe = f'{kwargs.get("timeframe") or self._timeframe}'
            indicators = kwargs.get("indicators") or self._indicators
            histbars = kwargs.get("histbars") or self._histbars
            send = self._send

            send(ws, "set_auth_token", ["unauthorized_user_token"])

            # Quote session
            if not args or (args and args[0] == "quote"):
                session = self._gen_session()  # Quote session ID
                send(ws, "quote_create_session", [session])
                send(ws, "quote_set_fields", [session, "lp", "volume"])
                [ws.send(self._add_symbol(session, s)) for s in syms]
                send(ws, "quote_fast_symbols", [session, *syms])
                send(ws, "quote_hibernate_all", [session])

            # Chart session - Prefer to use this over quote sessions since it has a historical series
            else:
                for i, sym in enumerate(syms):
                    # Each ticker warrants a separate chart session ID
                    c_session = self._gen_session(type="chart")
                    self._state[c_session] = {
                        "sym": sym,
                        "indicators": [],
                        "series": [],
                        "timeframe": timeframe
                    }

                    # Users are allowed to select specific tickers
                    send(ws, "chart_create_session", [c_session, ""])
                    send(ws, "switch_timezone", [c_session, "Asia/Singapore"])
                    send(ws, "resolve_symbol", [
                        c_session, f"symbol_{i}",
                        self._add_chart_symbol(sym)
                    ])
                    # s (in resp) -> series
                    self._state[c_session].get("series").append(f"s_{i}")
                    send(ws, "create_series", [
                        c_session, f"s_{i}", f"s_{i}", f"symbol_{i}",
                        timeframe, histbars
                    ])

                    for indicator in indicators:
                        # Users are allowed to select specific indicators
                        # st (in resp) -> study
                        self._state[c_session].get("indicators").append(
                            f"{indicator}_{i}")
                        send(ws, "create_study", [
                            c_session, f"{indicator}_{i}", f"{indicator}_{i}",
                            f"s_{i}", "Script@tv-scripting-101!",
                            self._indicator_mapper(indicator)
                        ])

        self._t = threading.Thread(target=run, args=(type, ), kwargs=kwargs)
        self._t.setDaemon(True)
        self._t.start()

    def _send(self, ws, func, params):
        """ Client sends msg to websockets server """
        ws.send(self._create_msg(func, params))

    def _indicator_mapper(self, indicator: str) -> dict:
        """ Indicator params that are accepted by the tv server """
        return {
            "rsi": {
                "text":
                "1f0fkZ72S0de2geyaUhXXw==_xwY73vljRXeew69Rl27RumLDs6aJ9NLsTYN9Xrht254BTb8uSOgccpLDt/cdRWopwJPNZx40m19yEFwJFswkSi62X4guNJYpXe4A6S9iq2n+OXM6mqWeWzDbjTl0lYmEf1ujbg7i3FvUdV/zCSrqd+iwnvvZSV+O2acpfNLpUlDdB6PZX4Y9y8tlQLWA2PiF8CVJng7DF1LPeecWC4fv+lNg+s5OXU46AjIhc+TFu8DOwiuKjNh7wWz6EZ7gpQS3",
                "pineId": "STD;RSI",
                "pineVersion": "12.0",
                "in_2": {
                    "v": "",
                    "f": True,
                    "t": "resolution"
                },
                "in_0": {
                    "v": 14,
                    "f": True,
                    "t": "integer"
                },
                "in_1": {
                    "v": "close",
                    "f": True,
                    "t": "source"
                }
            }
        }.get(indicator.lower())

    def _create_msg(self, func, params):
        """ _create_msg("set_auth_token", "unauthorized_user_token") """
        msg = self._prepend_header(json.dumps({"m": func, "p": params}))

        if self._debug:
            print("DEBUG:", msg)

        return msg

    def _gen_session(self, type="chart"):
        # ~m~52~m~{"m":"quote_create_session","p":["qs_3bDnffZvz5ur"]}
        session = ""
        if type == "quote":
            session = "qs_"
        elif type == "chart":
            session = "cs_"
        else:
            raise Exception("Invalid session type")
        return session + "".join(random.choices(string.ascii_letters, k=12))

    def _add_symbol(self, quote_session: str, sym: str):
        """ Quote symbol: _add_symbol("3bDnffZvz5ur", "BINANCE:UNIUSD") """
        return self._create_msg("quote_add_symbols", [quote_session, sym])

    def _add_chart_symbol(self, sym: str):
        """ Chart symbol - Only required for the first symbol """
        return "=" + json.dumps({"symbol": sym})

    def _prepend_header(self, msg):
        return f'~m~{len(msg)}~m~{msg}'


if __name__ == "__main__":

    ipm = IntradayPriceManager()
    alerting_thread = threading.Thread(target=ipm.send_slack)
    alerting_thread.start()

    ipm.get(type="chart",
            syms=[
                "BINANCE:BTCUSD", "BINANCE:ETHUSD", "BINANCE:DOTUSD",
                "BINANCE:UNIUSD", "BINANCE:SOLUSD"
            ],
            indicators=["rsi"],
            timeframe=240,
            histbars=300)
