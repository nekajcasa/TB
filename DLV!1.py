# -*- coding: utf-8 -*-
"""
Created on Wed Sep  6 18:51:25 2023

@author: music

Binance data loger. Gets BTC price data aproxmetly every secodn and
 writes it to file.

"""
import websocket
import json
import pandas as pd
from sqlalchemy import create_engine


class DL():
    def __init__(self):
        self.engine = create_engine("sqlite:///E:\\projekti\\trading\\test.db")
        self.endpoint = "wss://stream.binance.com:9443/ws"
        self.ws = websocket.WebSocketApp(self.endpoint, on_message=self.on_message,
                                         on_open=self.on_open, on_close=self.on_close)
        self.ws.run_forever()

    def on_open(self, ws):
        ws.send(json.dumps({"method": "SUBSCRIBE",
                            "params": ["btcusdt@ticker"],
                            "id": 1}))

    def on_close(self, ws):
        ws.send(json.dumps({"method": "UNSUBSCRIBE",
                            "params": ["btcusdt@depth"],
                            "id": 312}))

    def on_message(self, ws, message):

        out = json.loads(message)

        time = pd.to_datetime(out["E"], unit="ms")
        price = float(out["c"])
        df = pd.DataFrame({"Time": time, "Price": price}, index=[0])
        naslov = str(time)[:11]
        df.to_sql(naslov, self.engine, if_exists="append", index=False)


if __name__ == "__main__":
    DL()
