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

    def run(self):
        # websocket.enableTrace(True)
        ws = websocket.WebSocketApp(self.ws_url,
                                    on_open=self.on_open,
                                    on_close=self.on_close,
                                    on_message=self.on_message,
                                    on_error=self.on_error)
        ws.run_forever()

    def on_message(self, ws, message):
        pattern = re.compile(r'~m~\d+~m~~h~\d+$')
        if pattern.match(message):
            ws.send(message)
        else:
            print(message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("### closed ###")

    def on_open(self, ws):
        def run(*args):
            session = self._gen_quote_session()

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
            ws.send(create_msg("quote_hibernate_all", [session]))

        self.t = threading.Thread(target=run)
        self.t.setDaemon(True)
        self.t.start()

    def _create_msg(self, func, params):
        """ _create_msg("set_auth_token", "unauthorized_user_token") """
        return self._prepend_header(json.dumps({"m": func, "p": params}))

    def _gen_quote_session(self):
        # ~m~52~m~{"m":"quote_create_session","p":["qs_3bDnffZvz5ur"]}
        return "qs_" + "".join(random.choices(string.ascii_letters, k=12))

    def _add_symbol(self, quote_session: str, sym: str):
        """ _add_symbol("3bDnffZvz5ur", "BINANCE:UNIUSD") """
        return self._create_msg("quote_add_symbols", [quote_session, sym])

    def _prepend_header(self, msg):
        return f'~m~{len(msg)}~m~{msg}'


if __name__ == "__main__":
    ipm = IntradayPriceManager()
    ipm.run()
