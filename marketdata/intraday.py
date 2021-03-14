import json
import random
import re
import string
import threading
import websocket


class IntradayPriceManager():
    def __init__(self):
        self.syms = [
            "BINANCE:UNIUSD", "BINANCE:ETHUSD", "BINANCE:DOTUSD", "SGX:ES3",
            "SGX:CLR"
        ]
        self.ws_url = "wss://data.tradingview.com/socket.io/websocket"
        self.t = None

    def get(self):
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(self.ws_url,
                                    on_open=lambda ws: self.on_open(ws),
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

    def on_open(self, ws):
        def run(*args):
            session = self._gen_session()  # Quote session ID
            # c_session = self._gen_session(type="chart")  # Chart session ID

            # ~m~52~m~{"m":"quote_create_session","p":["qs_3bDnffZvz5ur"]}
            # ~m~395~m~{"m":"quote_set_fields","p":["qs_3bDnffZvz5ur","ch","chp","lp"]}
            # ~m~89~m~{"m":"quote_add_symbols","p":["qs_3bDnffZvz5ur","SP:SPX",{"flags":["force_permission"]}]}
            # ~m~315~m~{"m":"quote_fast_symbols","p":["qs_3bDnffZvz5ur","SP:SPX","TVC:NDX","CBOE:VIX","TVC:DXY","SGX:ES3","NASDAQ:AAPL","NASDAQ:MSFT","NASDAQ:TSLA","TVC:USOIL","TVC:GOLD","TVC:SILVER","FX:AUDUSD","FX:EURUSD","FX:GBPUSD","FX:USDJPY","BITSTAMP:BTCUSD","BITSTAMP:ETHUSD","COINBASE:UNIUSD","BINANCE:DOGEUSD","BINANCE:DOTUSD"]}

            syms = self.syms
            create_msg = self._create_msg

            ws.send(create_msg("set_auth_token", ["unauthorized_user_token"]))
            ws.send(create_msg("quote_create_session", [session]))
            ws.send(create_msg("quote_set_fields", [session, "lp"]))
            [ws.send(self._add_symbol(session, s)) for s in syms]
            ws.send(create_msg("quote_fast_symbols", [session, *syms]))
            # ws.send(create_msg("quote_hibernate_all", [session]))

            # ws.send(create_msg("chart_create_session", [c_session, ""]))
            # ws.send(create_msg("resolve_symbol", [c_session, "symbol_1", self._add_chart_symbol("BINANCE:ETHUSD")]))
            # ws.send(create_msg("create_series", [c_session, "s1", "s1", "symbol1", "15", 300])

        self.t = threading.Thread(target=run)
        self.t.setDaemon(True)
        self.t.start()

    def _create_msg(self, func, params):
        """ _create_msg("set_auth_token", "unauthorized_user_token") """
        return self._prepend_header(json.dumps({"m": func, "p": params}))

    def _gen_session(self, type="quote"):
        # ~m~52~m~{"m":"quote_create_session","p":["qs_3bDnffZvz5ur"]}
        session = ""
        if type == "quote":
            session = "qs"
        elif type == "chart":
            session = "cs"
        else:
            raise Exception("Invalid session type")
        return session + "".join(random.choices(string.ascii_letters, k=12))

    def _add_symbol(self, quote_session: str, sym: str):
        """ Quote symbol: _add_symbol("3bDnffZvz5ur", "BINANCE:UNIUSD") """
        return self._create_msg("quote_add_symbols", [quote_session, sym])

    def _add_chart_symbol(self, sym: str):
        """ Chart symbol """
        return "=" + json.dumps({"symbol": sym, "adjustment": "splits"})

    def _prepend_header(self, msg):
        return f'~m~{len(msg)}~m~{msg}'


if __name__ == "__main__":
    ipm = IntradayPriceManager()
    ipm.get()