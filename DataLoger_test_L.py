# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 22:17:56 2023

@author: music
"""
import websocket
import json
import pandas as pd


class DL():
    def __init__(self):
        self.endpoint = "wss://stream.binance.com:9443/ws"
        self.ws = websocket.WebSocketApp(self.endpoint, on_message=self.on_message,
                                         on_open=self.on_open, on_close=self.on_close)
        self.ws.run_forever()


    def on_open(self, ws):
        print("Conection opend")
        ws.send(json.dumps({"method": "SUBSCRIBE",
                            "params": ["btcusdt@ticker"],
                            "id": 1}))

    def on_close(self, ws):
        print("Conection closed")
        ws.send(json.dumps({"method": "UNSUBSCRIBE",
                            "params": ["btcusdt@depth"],
                            "id": 312}))

    def on_message(self, ws, message):

        out = json.loads(message)

        time = pd.to_datetime(out["E"], unit="ms")
        price = float(out["c"])
        
        print(time, price)


if __name__ == "__main__":
    print("Start")
    DL()
    print("Stop")
