import os
import json
import requests
import datetime
import calendar
import numpy as np
import pandas as pd

import oandapyV20
import oandapyV20.endpoints.instruments as instruments

from dateutil.relativedelta import relativedelta

import wrappers.eod_wrapper as eod_wrapper
 
class FX():
        
    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]
        self.oanda_client = data_clients["oanda_client"]

    def get_ecb_interbank_fx_rate(self, ticker, exchange="MONEY"):
        # https://eodhistoricaldata.com/financial-apis/macroeconomic-data-api/
        url = "https://eodhistoricaldata.com/api/eod/ECB{}.{}".format(ticker, exchange)
        params = {"api_token": os.getenv('EOD_KEY'), "fmt": "json"}
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json())

    def get_norge_interbank_fx_rate(self, ticker, exchange="MONEY"):
        # https://eodhistoricaldata.com/financial-apis/macroeconomic-data-api/
        url = "https://eodhistoricaldata.com/api/eod/NORGE{}.{}".format(ticker, exchange)
        params = {"api_token": os.getenv('EOD_KEY'), "fmt": "json"}
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json())

    """
    Price::OHLCV and Others
    """
    def get_ohlcv(self, ticker="", exchange="FOREX", period_end=datetime.datetime.today(), period_start="", period_days=1000, source="oanda"):
        if source == "eod":
            return eod_wrapper.get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days)
        elif source == "oanda":
            return self.get_oan_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days)
        else:
            return None

    def get_live_lagged_prices(self, ticker="", exchange="FOREX"):
        return eod_wrapper.get_live_lagged_prices(ticker=ticker, exchange=exchange)
    
    def get_intraday_data(self, ticker="", exchange="FOREX", interval="5m", to_utc=datetime.datetime.utcnow(), period_days=120):
        return eod_wrapper.get_intraday_data(ticker=ticker, exchange=exchange, interval=interval, to_utc=to_utc, period_days=period_days)

    def get_oan_ohlcv(self, ticker="", exchange="FOREX", period_end=datetime.datetime.today(), period_start="", period_days=1000): #inst, count, gran):
        params = {"count": period_days,"granularity": "D"}
        candles = instruments.InstrumentsCandles(instrument=ticker, params=params)
        self.oanda_client.request(candles)
        ohlc_dict = candles.response["candles"]
        ohlc = pd.DataFrame(ohlc_dict)
        ohlc = ohlc[ohlc["complete"]]
        ohlc_df = ohlc["mid"].dropna().apply(pd.Series)
        ohlc_df["volume"] = ohlc["volume"]
        ohlc_df.index = ohlc["time"]
        ohlc_df = ohlc_df.apply(pd.to_numeric)
        ohlc_df.reset_index(inplace=True)
        ohlc_df = ohlc_df.iloc[:,[0,1,2,3,4,5]]
        ohlc_df.columns = ["date","open","high","low","close","volume"]
        return ohlc_df.reset_index(drop=True).set_index("date")