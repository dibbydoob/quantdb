import os
import json
import requests
import datetime
import pandas as pd

from sec_edgar_api import EdgarClient

class Miscellaneous():

    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]
        self.edgar_client = EdgarClient("edgar_user_agent")
        
    def search_security(self, search_request):
        """
        for Stocks, ETFs, Mutual Funds and Indices
        """
        url = "https://eodhistoricaldata.com/api/search/{}".format(search_request)
        params = {"api_token": os.getenv('EOD_KEY'), "limit": 50}
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json())

    def get_eod_exchanges(self):
        resp = self.eod_client.get_exchanges()
        df = pd.DataFrame(resp)
        return df

    def get_exchange_tickers(self, exchange="US", add_params={}):
        url = 'https://eodhistoricaldata.com/api/exchange-symbol-list/{}'.format(exchange)
        params = {"api_token": os.getenv('EOD_KEY'), "fmt": "json"}
        params.update(add_params)
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json())
    
    def get_exchange_delisted(self, exchange="US"):
        return self.get_exchange_tickers(exchange=exchange, add_params={"delisted": 1})

    def get_exchange_meta(self, exchange="US", add_params={}):
        url = "https://eodhistoricaldata.com/api/exchange-details/{}".format(exchange)
        params = {"api_token": os.getenv('EOD_KEY')}
        params.update(add_params)
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return resp.json()
    
    def get_exchange_hols(self, exchange="US"):
        one_year_back = (datetime.datetime.today() - datetime.timedelta(days = 365)).strftime('%Y-%m-%d')
        one_year_forward =  (datetime.datetime.today() + datetime.timedelta(days = 365)).strftime('%Y-%m-%d')
        return pd.DataFrame(
            self.get_exchange_meta(exchange=exchange, add_params={"from": one_year_back, "to": one_year_forward})["ExchangeHolidays"]
        ).transpose()

    def get_screened_filters(self, code):
        """
        https://eodhistoricaldata.com/financial-apis/stock-market-screener-api/
        """
        pass

    def get_ticker_news_sentiment(self, ticker="AAPL", exchange="US", period_days=365, period_end=datetime.datetime.today()):
        #news sentiment on: Stocks, ETFs, Forex, and Cryptocurrencies
        url = "https://eodhistoricaldata.com/api/sentiments?s={}.{}".format(ticker, exchange)
        params = {
            "api_token": os.getenv('EOD_KEY'),
            "from": (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
            "to": period_end.strftime('%Y-%m-%d')
        }
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()["{}.{}".format(ticker, exchange)]).iloc[::-1].reset_index(drop=True).set_index("date")
    
    def get_ticker_media_sentiment(self, ticker="AAPL", exchange="US", period_days=365, period_end=datetime.datetime.today()):
        url = "https://eodhistoricaldata.com/api/tweets-sentiments?s={}.{}".format(ticker, exchange)
        params = {
            "api_token": os.getenv('EOD_KEY'),
            "from": (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
            "to": period_end.strftime('%Y-%m-%d')
        }
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()["{}.{}".format(ticker, exchange)]).iloc[::-1].reset_index(drop=True).set_index("date")

    """
    SEC Services
    1) python3 -m pip install sec-edgar-api (https://github.com/jadchaar/sec-edgar-api)
    2) python3 -m pip install sec-api (https://sec-api.io/)
    3) https://www.sec.gov/developer...
    Note: The SEC does not allow "unclassified" bots or automated tools to crawl the site.
    However, `solutions' can be found online, but we will not support them here under Fair Access protocls.
    """
    def get_edgar_submissions(self, cik):
        return self.edgar_client.get_submissions(cik=str(cik).zfill(10))

    def get_edgar_company_facts(self, cik):
        return self.edgar_client.get_company_facts(cik=cik)
    
    def get_edgar_company_concept(self, cik, taxonomy="dei", tag="EntityPublicFloat"):
        return self.edgar_client.get_company_concept(cik=cik, taxonomy=taxonomy, tag=tag)

    def get_edgar_frame(self, taxonomy="us-gaap", tag="AccountsPayableCurrent", unit="USD", year="2022", quarter=1):
        return self.edgar_client.get_frames(taxonomy=taxonomy, tag=tag, unit=unit, year=year, quarter=quarter)
