import os
import json
import requests
import datetime
import calendar
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta

class Options():
        
    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]

    def get_ticker_option_chain(self, ticker, exchange="US", add_params={}):
        #https://eodhistoricaldata.com/financial-apis/stock-options-data/
        url = 'https://eodhistoricaldata.com/api/options/{}.{}'.format(ticker, exchange)
        params = {
            "api_token": os.getenv('EOD_KEY'),     
        }
        params.update(add_params)
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            result = resp.json()
            last_traded_date = result["lastTradeDate"]
            last_underlying_close = result["lastTradePrice"]
            df = pd.DataFrame()
            for expiry in result["data"]:
                calls = pd.DataFrame(expiry["options"]["CALL"])
                puts = pd.DataFrame(expiry["options"]["PUT"])
                df = df.append(calls, ignore_index=True)
                df = df.append(puts, ignore_index=True)
            return df
    
    def get_ticker_option_terminal_historical(self, ticker, exchange="US", expiry_to=datetime.datetime.today(), period_days=365):
        return self.get_ticker_option_chain(ticker=ticker, exchange=exchange, add_params={
            "to": expiry_to,
            "from": expiry_to - datetime.timedelta(days=period_days)
        })
