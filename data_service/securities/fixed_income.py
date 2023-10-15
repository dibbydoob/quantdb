import os
import json
import requests
import datetime
import calendar
import websockets
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta
from sockets.eod_sockclient import EodSocketClient

import wrappers.eod_wrapper as eod_wrapper

class FixedIncome():

    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]

    def get_corpbond_fundamentals(self, ticker="910047AG4"): 
        #see specs: https://eodhistoricaldata.com/financial-apis/bonds-fundamentals-and-historical-api/
        #Bonds fundamentals and historical data could be accessed either via ISIN or via CUSIP IDs
        url = 'https://eodhistoricaldata.com/api/bond-fundamentals/{}'.format(ticker)
        params = {"api_token": os.getenv('EOD_KEY')}
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return resp.json()

    def get_ohlcv(self, ticker="US910047AG49", exchange="BOND", period_end=datetime.datetime.today(), period_start="", period_days=1000):
        #https://eodhistoricaldata.com/financial-apis/bonds-fundamentals-and-historical-api/
        #for GBOND: 1 month, 3 months, 6 months, 1 year, 3 years, 5 years, 10 years
        return eod_wrapper.get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days)
