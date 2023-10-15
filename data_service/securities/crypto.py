import os
import json
import requests
import datetime
import calendar
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta

import data_service.wrappers.eod_wrapper as eod_wrapper

class Crypto():
        
    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]

    def get_crypto_fundamentals(self, ticker, exchange="CC"):
        #refer to fields here: https://eodhistoricaldata.com/financial-apis/fundamental-data-for-cryptocurrencies/
        url = 'https://eodhistoricaldata.com/api/fundamentals/{}.{}'.format(ticker, exchange)
        params = {
            "api_token": os.getenv('EOD_KEY')
        }
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return resp.json()

    """
    Price::OHLCV and Others
    """
    def get_ohlcv(self, ticker="", exchange="CC", period_end=datetime.datetime.today(), period_start="", period_days=1000):
        return eod_wrapper.get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days)

    def get_live_lagged_prices(self, ticker="", exchange="CC"):
        return eod_wrapper.get_live_lagged_prices(ticker=ticker, exchange=exchange)
    
    def get_intraday_data(self, ticker="", exchange="CC", interval="5m", to_utc=datetime.datetime.utcnow(), period_days=120):
        return eod_wrapper.get_intraday_data(ticker=ticker, exchange=exchange, interval=interval, to_utc=to_utc, period_days=period_days)