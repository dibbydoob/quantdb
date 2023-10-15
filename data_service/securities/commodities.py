import os
import json
import requests
import datetime
import calendar
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta

import wrappers.eod_wrapper as eod_wrapper

class Commodities():
        
    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]

    """
    Price::OHLCV and Others
    """
    def get_ohlcv(self, ticker="", exchange="COMM", period_end=datetime.datetime.today(), period_start=None, period_days=1000):
        return eod_wrapper.get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days)

    def get_live_lagged_prices(self, ticker="", exchange="COMM"):
        return eod_wrapper.get_live_lagged_prices(ticker=ticker, exchange=exchange)
    
    def get_intraday_data(self, ticker="", exchange="COMM", interval="5m", to_utc=datetime.datetime.utcnow(), period_days=120):
        return eod_wrapper.get_intraday_data(ticker=ticker, exchange=exchange, interval=interval, to_utc=to_utc, period_days=period_days)