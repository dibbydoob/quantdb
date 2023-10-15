import os
import json
import requests
import datetime
import calendar
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta

import data_service.wrappers.eod_wrapper as eod_wrapper

class Baskets():
    
    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]

    """
    ETF Fundamentals dump
    """
    def get_etf_fundamentals(self, ticker, exchange="US"):
        #available data:: https://eodhistoricaldata.com/financial-apis/stock-etfs-fundamental-data-feeds/
        return eod_wrapper.get_fundamental_data(eod_client=self.eod_client, ticker=ticker,exchange=exchange)
        
    """
    Mutual Fund Fundamentals dump
    """
    def get_fund_fundamentals(self, ticker, exchange="US"):
        #available data:: https://eodhistoricaldata.com/financial-apis/stock-etfs-fundamental-data-feeds/
        return eod_wrapper.get_fundamental_data(eod_client=self.eod_client, ticker=ticker,exchange=exchange)
    
    """
    Index Fundamentals
    Supported indices: https://eodhistoricaldata.com/financial-apis/list-supported-indices/
    """
    def get_index_fundamentals(self, ticker, exchange="INDX"):
        return self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange)) 

    def get_index_generals(self, ticker, exchange="INDX"):
        return self.get_index_fundamentals(ticker=ticker, exchange=exchange)["General"]

    def get_index_components(self, ticker, exchange="INDX"):
        resp = self.get_index_fundamentals(ticker=ticker, exchange=exchange)["Components"]
        return pd.DataFrame(resp).transpose()

    def get_index_historical_components(self, ticker, exchange="INDX"):
        resp = self.get_index_fundamentals(ticker=ticker, exchange=exchange)["HistoricalTickerComponents"]
        return pd.DataFrame(resp).transpose()
