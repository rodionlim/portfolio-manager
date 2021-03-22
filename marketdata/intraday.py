import json
import random
import re
import string
import threading
import websocket


class IntradayPriceManager():
    def __init__(self, debug=False):
        self.syms = [
            "BINANCE:UNIUSD", "BINANCE:ETHUSD", "BINANCE:DOTUSD", "SGX:ES3",
            "SGX:CLR"
        ]
        self.ws_url = "wss://data.tradingview.com/socket.io/websocket"
        self.t = None
        self.debug = debug

    def get(self, type: str):
        """ Type is either quote (live) or chart (historical + live) """
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(self.ws_url,
                                    on_open=lambda ws: self.on_open(ws, type),
                                    on_close=self.on_close,
                                    on_message=self.on_message,
                                    on_error=self.on_error)
        ws.run_forever()

    @staticmethod
    def on_message(ws, message):
        pattern = re.compile(r'~m~\d+~m~~h~\d+$')
        if pattern.match(message):
            ws.send(message)
        else:
            print(message)

    @staticmethod
    def on_error(ws, error):
        print(error)

    @staticmethod
    def on_close(ws):
        print("### closed ###")

    def on_open(self, ws, type: str):
        def run(*args):
            session = self._gen_session()  # Quote session ID
            c_session = self._gen_session(type="chart")  # Chart session ID

            # ~m~52~m~{"m":"quote_create_session","p":["qs_3bDnffZvz5ur"]}
            # ~m~395~m~{"m":"quote_set_fields","p":["qs_3bDnffZvz5ur","ch","chp","lp"]}
            # ~m~89~m~{"m":"quote_add_symbols","p":["qs_3bDnffZvz5ur","SP:SPX",{"flags":["force_permission"]}]}
            # ~m~315~m~{"m":"quote_fast_symbols","p":["qs_3bDnffZvz5ur","SP:SPX","TVC:NDX","CBOE:VIX","TVC:DXY","SGX:ES3","NASDAQ:AAPL","NASDAQ:MSFT","NASDAQ:TSLA","TVC:USOIL","TVC:GOLD","TVC:SILVER","FX:AUDUSD","FX:EURUSD","FX:GBPUSD","FX:USDJPY","BITSTAMP:BTCUSD","BITSTAMP:ETHUSD","COINBASE:UNIUSD","BINANCE:DOGEUSD","BINANCE:DOTUSD"]}

            syms = self.syms
            send = self._send

            send(ws, "set_auth_token", ["unauthorized_user_token"])

            # Quote session
            if not args or (args and args[0] == "quote"):
                send(ws, "quote_create_session", [session])
                send(ws, "quote_set_fields", [session, "lp", "volume"])
                [ws.send(self._add_symbol(session, s)) for s in syms]
                send(ws, "quote_fast_symbols", [session, *syms])
                send(ws, "quote_hibernate_all", [session])

            # Chart session - Always prefer to use this over quote sessions
            else:
                # TODO: allow user to select tickers
                send(ws, "chart_create_session", [c_session, ""])
                send(ws, "switch_timezone", [c_session, "Asia/Singapore"])
                send(ws, "resolve_symbol", [
                    c_session, "symbol_1",
                    self._add_chart_symbol("BINANCE:ETHUSD")
                ])
                send(ws, "create_series",
                     [c_session, "s1", "s1", "symbol_1", "15", 300])
                send(ws, "resolve_symbol",
                     [c_session, "ss_1", "BITSTAMP:BTCUSD"])
                send(ws, "create_study", [
                    c_session, "st1", "st1", "s1",
                    "Overlay@tv-basicstudies-121", {
                        "symbol": "BITSTAMP:BTCUSD"
                    }
                ])

        self.t = threading.Thread(target=run, args=(type, ))
        self.t.setDaemon(True)
        self.t.start()

    def _send(self, ws, func, params):
        """ Client sends msg to websockets server """
        ws.send(self._create_msg(func, params))

    def _create_msg(self, func, params):
        """ _create_msg("set_auth_token", "unauthorized_user_token") """
        msg = self._prepend_header(json.dumps({"m": func, "p": params}))

        if self.debug:
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
    ipm.get(type="chart")