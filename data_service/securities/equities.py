import os
import json
import time
import aiohttp
import asyncio
import requests
import datetime
import urllib
import numpy as np
import pandas as pd

import data_service.db_logs as db_logs

from collections import defaultdict
from dateutil.relativedelta import relativedelta

import data_service.wrappers.eod_wrapper as eod_wrapper
import data_service.wrappers.aiohttp_wrapper as aiohttp_wrapper

class Equities():

    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]
        self.db_service = db_service
    
    """
    Master Utilities
    """
    def get_sec_tickers(self):
        url = 'https://www.sec.gov/files/company_tickers.json'
        resp = requests.get(url=url, params=None)
        data = resp.json()
        df = pd.DataFrame(data).transpose()
        return df

    def get_us_securities(self):
        resp = self.eod_client.get_exchange_symbols(exchange='US')
        df = pd.DataFrame(resp)
        return df

    """
    Fundamentals
    """
    def get_fundamentals_dump(self, ticker, exchange, read_db=True, insert_db=True, expire_db=10):
        return eod_wrapper.get_fundamental_data(eod_client=self.eod_client, ticker=ticker, exchange=exchange)
         
    """
    Fundamentals::General Information
    """
    def get_ticker_generals(self, ticker, exchange, read_db=True, insert_db=True, expire_db=24*12):
        docdata = None
        try:
            doc_identifier = {
                "type": "ticker_generals",
                "ticker": ticker,
                "exchange": exchange,
                "source": "eodhistoricaldata"
            }
            if read_db and expire_db > 0:
                exists, expired, docdata = self.db_service.read_docs(dtype="equity", dformat="fundamentals", dfreq="irregular", doc_identifier=doc_identifier, metalogs=ticker, expire_db=expire_db)
            if not read_db or expire_db <= 0 or not exists or expired:
                url = "https://eodhistoricaldata.com/api/fundamentals/{}.{}".format(ticker, exchange)
                params = {"api_token": os.getenv('EOD_KEY'), "filter":"General"}
                resp = requests.get(url, params=params)
                docdata = resp.json()
                if insert_db:
                    self.db_service.insert_docs(dtype="equity", dformat="fundamentals", dfreq="irregular", docdata=docdata, doc_identifier=doc_identifier, metalogs=ticker)
        except Exception:
            db_logs.DBLogs().critical("get_ticker_generals FAILED {}".format(ticker))
        return docdata

    async def asyn_get_ticker_generals(self, ticker, exchange, read_db=True, insert_db=True, expire_db=24*12, tries=0):
        docdata = None
        try:
            doc_identifier = {
                "type": "ticker_generals",
                "ticker": ticker,
                "exchange": exchange,
                "source": "eodhistoricaldata"
            }
            if read_db and expire_db > 0:
                exists, expired, docdata = await self.db_service.asyn_read_docs(dtype="equity", dformat="fundamentals", dfreq="irregular", doc_identifier=doc_identifier, metalogs=ticker, expire_db=expire_db)
            if not read_db or expire_db <= 0 or not exists or expired:
                url = "https://eodhistoricaldata.com/api/fundamentals/{}.{}".format(ticker, exchange)
                params = {"api_token": os.getenv('EOD_KEY'), "filter":"General"}
                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, params=params) as resp:
                        docdata = await resp.json()
                if insert_db:
                    await self.db_service.asyn_insert_docs(dtype="equity", dformat="fundamentals", dfreq="irregular", docdata=docdata, doc_identifier=doc_identifier, metalogs=ticker)
        except Exception:
            if tries < 5:
                db_logs.DBLogs().warning("asyn_get_ticker_generals RETRY {} {}".format(tries, ticker))
                return await self.asyn_get_ticker_generals(ticker=ticker, exchange=exchange, read_db=read_db, insert_db=insert_db, expire_db=expire_db, tries=tries+1)
            db_logs.DBLogs().critical("asyn_get_ticker_generals FAILED {}".format(ticker))
        return docdata
    
    async def asyn_batch_get_ticker_generals(self, tickers, exchanges, read_db=True, insert_db=True, expire_db=24*12):
        doc_identifiers = [{
            "type": "ticker_generals",
            "ticker": ticker,
            "exchange": exchange,
            "source": "eodhistoricaldata"
        } for ticker, exchange in zip(tickers, exchanges)]

        if read_db and expire_db > 0:
            existss, expireds, docdatas = await self.db_service.asyn_batch_read_docs(
                dtype="equity", dformat="fundamentals", dfreq="irregular", 
                doc_identifiers=doc_identifiers, metalogs=tickers, expire_db=expire_db
            )
        
        request_tickers, request_urls, request_identifiers = [], [], []
        for i in range(len(tickers)):
            if not read_db or expire_db <= 0 or not existss[i] or expireds[i]:
                url = "https://eodhistoricaldata.com/api/fundamentals/{}.{}?".format(tickers[i], exchanges[i])
                params = {"api_token": os.getenv('EOD_KEY'), "filter":"General"}
                request_tickers.append(tickers[i])
                request_urls.append(url + urllib.parse.urlencode(params))
                request_identifiers.append(doc_identifiers[i])
        
        request_results = await aiohttp_wrapper.async_aiohttp_get_all(request_urls)
        ticker_generals = []
        j = 0
        for i in range(len(tickers)):
            if not read_db or expire_db <= 0 or not existss[i] or expireds[i]:
                if request_results[j] is None:
                    db_logs.DBLogs().critical("asyn_batch_get_ticker_generals FAILED {}".format(request_tickers[j]))            
                ticker_generals.append(request_results[j])
                j += 1
            else:
                ticker_generals.append(docdatas[i])
        
        if insert_db:
            insert_tickers, insert_docdatas, insert_doc_identifiers = [], [], []
            for i in range(len(request_tickers)):
                if request_results[i] is not None:
                    insert_tickers.append(request_tickers[i])
                    insert_docdatas.append(request_results[i])
                    insert_doc_identifiers.append(request_identifiers[i])
            await self.db_service.asyn_batch_insert_docs(
                dtype="equity", dformat="fundamentals", dfreq="irregular", 
                docdatas=insert_docdatas, doc_identifiers=insert_doc_identifiers, metalogs=insert_tickers)
        
        return ticker_generals

    def get_ticker_classification(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return {
            "sector": ticker_fundamentals["Sector"],
            "industry": ticker_fundamentals["Industry"],
            "gicsect": ticker_fundamentals["GicSector"],
            "gicgrp": ticker_fundamentals["GicGroup"],
            "gicind": ticker_fundamentals["GicIndustry"],
            "gicsubind": ticker_fundamentals["GicSubIndustry"]
        }

    def get_is_listed(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return not ticker_fundamentals["IsDelisted"]

    def get_ticker_listings(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return ticker_fundamentals["Listings"]

    def get_ticker_personnel(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return ticker_fundamentals["Officers"]

    def get_ticker_employed(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return ticker_fundamentals["FullTimeEmployees"]

    def get_ticker_exchange(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return ticker_fundamentals["Exchange"]

    def get_ticker_countryiso(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return ticker_fundamentals["CountryISO"]

    def get_identification_codes(self, ticker="AAPL", exchange="US"):
        ticker_fundamentals = defaultdict(str)
        ticker_fundamentals.update(self.get_ticker_generals(ticker=ticker, exchange=exchange))
        return {
            "isin": ticker_fundamentals["ISIN"],
            "cusip": ticker_fundamentals["CUSIP"],
            "cik": ticker_fundamentals["CIK"]
        }

    async def asyn_get_identification_codes(self, ticker="AAPL", exchange="US"):
        ticker_fundamentals = defaultdict(str)
        ticker_fundamentals.update(await self.asyn_get_ticker_generals(ticker=ticker, exchange=exchange))
        return {
            "isin": ticker_fundamentals["ISIN"],
            "cusip": ticker_fundamentals["CUSIP"],
            "cik": ticker_fundamentals["CIK"]
        }
    
    async def asyn_batch_get_identification_codes(self, tickers=[], exchanges=[]):
        batch_generals = await self.asyn_batch_get_ticker_generals(tickers=tickers, exchanges=exchanges)
        batch_codes = []
        for general in batch_generals:
            ticker_fundamentals = defaultdict(str)
            ticker_fundamentals.update(general)
            batch_codes.append({
                "isin": ticker_fundamentals["ISIN"],
                "cusip": ticker_fundamentals["CUSIP"],
                "cik": ticker_fundamentals["CIK"]
            })
        return batch_codes

    """
    Fundamentals::Snapshots
    """
    def get_ticker_highlights(self, ticker, exchange):
        return self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='Highlights')

    def get_ticker_mcap(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker=ticker, exchange=exchange)
        return highlights["MarketCapitalization"]
    
    def get_ticker_ebitda(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker=ticker, exchange=exchange)
        return highlights["EBITDA"]   

    def get_ticker_pe(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["PERatio"]   

    def get_ticker_peg(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["PEGRatio"]   

    def get_ticker_book(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["BookValue"]   

    def get_ticker_div_ps(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["DividendShare"]   

    def get_ticker_div_yield(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["DividendYield"]   

    def get_ticker_eps(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["EarningsShare"]   

    def get_ticker_eps_est(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return {
            "current_q": highlights["EPSEstimateCurrentQuarter"],
            "next_q":  highlights["EPSEstimateNextQuarter"],
            "current_y": highlights["EPSEstimateCurrentYear"],
            "next_y":  highlights["EPSEstimateNextYear"],
        }   

    def get_ticker_last_quarter_date(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["MostRecentQuarter"]   

    def get_ticker_profit_margin(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["ProfitMargin"] 

    def get_ticker_op_margin(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["OperatingMarginTTM"] 

    def get_ticker_roa(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["ReturnOnAssetsTTM"] 

    def get_ticker_roe(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["ReturnOnEquityTTM"] 

    def get_ticker_revenue(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["RevenueTTM"] 

    def get_ticker_revenue_ps(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["RevenuePerShareTTM"] 

    def get_ticker_qoq_rev_growth(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["QuarterlyRevenueGrowthYOY"] 

    def get_ticker_qoq_earnings_growth(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["QuarterlyEarningsGrowthYOY"] 

    def get_ticker_gross_profit(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["GrossProfitTTM"] 

    def get_ticker_diluted_eps(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["DilutedEpsTTM"] 

    def get_ticker_analyst_target(self, ticker, exchange):
        highlights = self.get_ticker_highlights(ticker, exchange=exchange)
        return highlights["WallStreetTargetPrice"]

    def get_ticker_valuation(self, ticker, exchange):
        return self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='Valuation')
   
    def get_ticker_trailing_pe(self, ticker, exchange):
        return self.get_ticker_valuation(ticker=ticker, exchange=exchange)["TrailingPE"]

    def get_ticker_forward_pe(self, ticker, exchange):
        return self.get_ticker_valuation(ticker=ticker, exchange=exchange)["ForwardPE"]

    def get_ticker_price_to_sales(self, ticker, exchange):
        return self.get_ticker_valuation(ticker=ticker, exchange=exchange)["PriceSalesTTM"]
    
    def get_ticker_price_to_book(self, ticker, exchange):
        return self.get_ticker_valuation(ticker=ticker, exchange=exchange)["PriceBookMRQ"]

    def get_ticker_ev(self, ticker, exchange):
        return self.get_ticker_valuation(ticker=ticker, exchange=exchange)["EnterpriseValue"]       

    def get_ticker_ev_revenue(self, ticker, exchange):
        return self.get_ticker_valuation(ticker=ticker, exchange=exchange)["EnterpriseValueRevenue"]

    def get_ticker_ev_ebitda(self, ticker, exchange):
        return self.get_ticker_valuation(ticker=ticker, exchange=exchange)["EnterpriseValueEbitda"]

    """
    Fundamentals::Time Series Earnings and Financials
    """
    def get_ticker_earnings(self, ticker, exchange):
        return self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='Earnings')

    def get_ticker_earnings_history(self, ticker, exchange):
        resp = self.get_ticker_earnings(ticker=ticker, exchange=exchange)["History"]
        return pd.DataFrame(resp).transpose().iloc[::-1]

    def get_ticker_earnings_trend(self, ticker, exchange):
        resp = self.get_ticker_earnings(ticker=ticker, exchange=exchange)["Trend"]
        return pd.DataFrame(resp).transpose().iloc[::-1]

    def get_ticker_earnings_annual(self, ticker, exchange):
        resp = self.get_ticker_earnings(ticker=ticker, exchange=exchange)["Annual"]
        return pd.DataFrame(resp).transpose().iloc[::-1]

    def get_ticker_financials(self, ticker, exchange):
        return self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='Financials')

    def get_ticker_income_statement(self, ticker, exchange, option="q"):
        resp = self.get_ticker_financials(ticker, exchange)["Income_Statement"]
        if option == "q":
            return pd.DataFrame(resp["quarterly"]).transpose().iloc[::-1]
        elif option == "y":
            return pd.DataFrame(resp["yearly"]).transpose().iloc[::-1]
        raise Exception 

    def get_ticker_balance_sheet(self, ticker, exchange, option="q"):
        resp = self.get_ticker_financials(ticker, exchange)["Balance_Sheet"]
        if option == "q":
            return pd.DataFrame(resp["quarterly"]).transpose().iloc[::-1]
        elif option == "y":
            return pd.DataFrame(resp["yearly"]).transpose().iloc[::-1]
        raise Exception 

    def get_ticker_cash_flow(self, ticker, exchange, option="q"):
        resp = self.get_ticker_financials(ticker, exchange)["Cash_Flow"]
        if option == "q":
            return pd.DataFrame(resp["quarterly"]).transpose().iloc[::-1]
        elif option == "y":
            return pd.DataFrame(resp["yearly"]).transpose().iloc[::-1]
        raise Exception
        
    def get_ticker_historical_mcap(self, ticker, exchange):
        url = 'https://eodhistoricaldata.com/api/historical-market-cap/{}'.format("{}.{}".format(ticker, exchange))
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "from":"2000-01-01"
        }
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()).transpose().reset_index(drop=True).set_index("date")
    
    """
    Fundamentals::Share Statistics
    """
    def get_ticker_shares_stat(self, ticker, exchange):
        return self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='SharesStats')

    def get_ticker_shares_outstanding(self, ticker, exchange):
        return self.get_ticker_shares_stat(ticker=ticker)["SharesOutstanding"]

    def get_ticker_shares_float(self, ticker, exchange):
        return self.get_ticker_shares_stat(ticker=ticker)["SharesFloat"]

    def get_ticker_insider_percent(self, ticker, exchange):
        return self.get_ticker_shares_stat(ticker=ticker)["PercentInsiders"]

    def get_ticker_institutional_percent(self, ticker, exchange):
        return self.get_ticker_shares_stat(ticker=ticker)["PercentInstitutions"]

    def get_ticker_shorts(self, ticker, exchange):
        shares_stat = self.get_ticker_shares_stat(ticker=ticker, exchange=exchange)
        technicals = self.get_ticker_technicals(ticker=ticker, exchange=exchange)
        return {
            "outstanding_short": shares_stat["SharesShort"] if shares_stat["SharesShort"] is not None else technicals["SharesShort"],
            "prior_month_short": shares_stat["SharesShortPriorMonth"] if shares_stat["SharesShortPriorMonth"] is not None else technicals["SharesShortPriorMonth"],
            "short_ratio": shares_stat["ShortRatio"] if shares_stat["ShortRatio"] is not None else technicals["ShortRatio"],
            "percent_outstanding": shares_stat["ShortPercentOutstanding"] if shares_stat["ShortPercentOutstanding"] is not None else technicals["ShortPercent"],
            "percent_float": shares_stat["ShortPercentFloat"],
        }

    """
    Fundamentals::Technicals
    """
    def get_ticker_technicals(self, ticker, exchange):
        return self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='Technicals')

    def get_ticker_beta(self, ticker, exchange):
        return self.get_ticker_technicals(ticker=ticker, exchange=exchange)["Beta"]
    
    def get_ticker_52W_hilo(self, ticker, exchange):
        return {
            "high": self.get_ticker_technicals(ticker=ticker, exchange=exchange)["52WeekHigh"], 
            "low": self.get_ticker_technicals(ticker=ticker, exchange=exchange)["52WeekLow"]}

    """
    Fundamentals::Splits and Dividends
    """
    def get_ticker_splits_and_divvies(self, ticker, exchange):
        return self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='SplitsDividends')

    def get_ticker_yearly_payout_frequency(self, ticker, exchange):
        resp = self.get_ticker_splits_and_divvies(ticker=ticker, exchange=exchange)["NumberDividendsByYear"]
        return pd.DataFrame(resp).transpose()

    def get_ticker_forward_annual_divvy_rate(self, ticker, exchange):
        return self.get_ticker_splits_and_divvies(ticker=ticker, exchange=exchange)["ForwardAnnualDividendRate"]
    
    def get_ticker_forward_annual_divvy_yield(self, ticker, exchange):
        return self.get_ticker_splits_and_divvies(ticker=ticker, exchange=exchange)["ForwardAnnualDividendYield"]
   
    def get_ticker_payout_ratio(self, ticker, exchange):
        return self.get_ticker_splits_and_divvies(ticker=ticker, exchange=exchange)["PayoutRatio"]
    
    def get_ticker_divvy_dates(self, ticker, exchange):
        return {
            "date": self.get_ticker_splits_and_divvies(ticker=ticker, exchange=exchange)["DividendDate"], 
            "ex": self.get_ticker_splits_and_divvies(ticker=ticker, exchange=exchange)["ExDividendDate"]}
    
    def get_ticker_last_split(self, ticker, exchange):
        return {"factor": self.get_ticker_splits_and_divvies(ticker=ticker, exchange=exchange)["LastSplitFactor"], 
        "date": self.get_ticker_splits_and_divvies(ticker=ticker, exchange=exchange)["LastSplitDate"]}

    """
    Fundamentals::Institutional Rating and Analysts
    """
    def get_ticker_ratings(self, ticker, exchange):
        return self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='AnalystRatings')
    
    """
    Fundamentals::Institutional and Insider Holdings/Transactions
    """
    def get_ticker_institutionals(self, ticker, exchange):
        resp = self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='Holders')
        institutions = resp["Institutions"]
        funds = resp["Funds"]
        return {
            "institutions": pd.DataFrame(institutions).transpose(),
            "funds" : pd.DataFrame(funds).transpose(),
        }

    def get_insider_txns(self, ticker="", exchange=""):
        url = 'https://eodhistoricaldata.com/api/insider-transactions'
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "from":"2000-01-01", 
            "limit":1000
        }
        if ticker: params["code"] = "{}.{}".format(ticker, exchange)
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()).iloc[::-1].reset_index(drop=True)

    """
    Fundamentals::Others
    """
    def get_ticker_esg_score(self, ticker, exchange):
        resp = self.eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange), filter_='ESGScores')
        pass #beta version

    """
    Events::Recent and Upcoming
    """
    def get_ticker_nearby_earnings(self, ticker, exchange):
        url = 'https://eodhistoricaldata.com/api/calendar/earnings'
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "symbols": "{}.{}".format(ticker, exchange),
            "fmt": "json",
            "from": (datetime.datetime.today() - datetime.timedelta(days = 365)).strftime('%Y-%m-%d'),
            "to": (datetime.datetime.today() + datetime.timedelta(days = 365)).strftime('%Y-%m-%d')
        }
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()["earnings"])

    def get_ticker_earnings_trend(self, ticker, exchange):
        url = 'https://eodhistoricaldata.com/api/calendar/trends'
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "symbols": "{}.{}".format(ticker, exchange),
            "fmt": "json",
            "from": "1990-01-01",
            "to": (datetime.datetime.today() + datetime.timedelta(days = 90)).strftime('%Y-%m-%d')
        }
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()["trends"][0]).iloc[::-1].reset_index(drop=True)
        else:
            raise HttpException()

    def get_nearby_ipos(self):
        url = 'https://eodhistoricaldata.com/api/calendar/ipos'
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "fmt": "json",
            "from": (datetime.datetime.today() - datetime.timedelta(days = 365)).strftime('%Y-%m-%d'),
            "to": (datetime.datetime.today() + datetime.timedelta(days = 365)).strftime('%Y-%m-%d')
        }
        resp = requests.get(url, params=params)
        return pd.DataFrame(resp.json()["ipos"])
       
    def get_nearby_splits(self):
        url = 'https://eodhistoricaldata.com/api/calendar/splits'
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "fmt": "json",
            "from": (datetime.datetime.today() - relativedelta(months = 6)).strftime('%Y-%m-%d'),
            "to": (datetime.datetime.today() + relativedelta(months = 6)).strftime('%Y-%m-%d')
        }
        resp = requests.get(url, params=params)
        return pd.DataFrame(resp.json()["splits"])

    def get_historical_splits(self, ticker, exchange="US"):
        url = "https://eodhistoricaldata.com/api/splits/{}.{}".format(ticker, exchange)
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "fmt": "json"
        }
        resp = requests.get(url, params=params)
        return pd.DataFrame(resp.json()).reset_index(drop=True).set_index("date")

    def get_historical_dividends(self, ticker, exchange="US"):
        url = "https://eodhistoricaldata.com/api/div/{}.{}".format(ticker, exchange)
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "fmt": "json"
        }
        resp = requests.get(url, params=params)
        return pd.DataFrame(resp.json()).reset_index(drop=True).set_index("date")

    def get_bulk_fundamentals(self, tickers, exchange="US"):
        url = "http://eodhistoricaldata.com/api/bulk-fundamentals/NASDAQ"
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "fmt": "json",
            "offset": 500,
            "limit": 500
        }
        resp = requests.get(url, params=params)
        return resp.json()

    """
    Events::Sentiment and News
    """
    def get_equity_news(self, ticker="", exchange="US", tag="", period_end=datetime.datetime.today(), period_days=365, limit=1000):
        #supported tags: https://eodhistoricaldata.com/financial-apis/stock-market-financial-news-api/
        url = "https://eodhistoricaldata.com/api/news"
        params = {
            "api_token": os.getenv('EOD_KEY'), 
            "from": (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
            "to": period_end.strftime('%Y-%m-%d')
        }
        query_key = "s" if ticker else "t"
        query_value = "{}.{}".format(ticker, exchange) if query_key == "s" else tag
        params.update({query_key:query_value})
        resp = requests.get(url, params=params)
        df = pd.DataFrame(resp.json())
        df[df["sentiment"].apply(pd.Series).columns] = df["sentiment"].apply(pd.Series)
        return df.iloc[::-1].reset_index(drop=True).set_index("date")

    def _get_series_identifiers_and_metadata(self, isin, ticker, exchange, source):
        series_metadata = {
            "isin": isin,
            "ticker": ticker,
            "exchange": exchange,
            "source": source
        }
        series_identifier = {**{"type": "ticker_series"}, **series_metadata}
        return series_metadata, series_identifier

    """
    Price::OHLCV and Others
    """
    def get_ohlcv(self, ticker, exchange, period_end=datetime.datetime.today(), period_start=None, period_days=3650, read_db=True, insert_db=True):
        series_df = None
        try:
            id_codes = self.get_identification_codes(ticker=ticker, exchange=exchange)
            series_metadata, series_identifier = self._get_series_identifiers_and_metadata(
                isin=id_codes["isin"],
                ticker=ticker,
                exchange=exchange,
                source="eodhistoricaldata"
            )
            if read_db:
                period_start = period_start if period_start else period_end - datetime.timedelta(days=period_days)
                exists, series_df = self.db_service.read_timeseries(
                    dtype="equity", dformat="spot", dfreq="1d", 
                    series_metadata=series_metadata, series_identifier=series_identifier, metalogs=ticker,
                    period_start=period_start,
                    period_end=period_end
                )
            if not read_db or not exists:
                series_df = eod_wrapper.get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days)
                if not insert_db:
                    db_logs.DBLogs().info("successful get_ohlcv but not inserted {}".format(ticker))
                elif insert_db and len(series_df) > 0:
                    self.db_service.insert_timeseries_df(dtype="equity", dformat="spot", dfreq="1d", 
                                    df=series_df, series_identifier=series_identifier, series_metadata=series_metadata, metalogs=ticker)
                    db_logs.DBLogs().info("successful get_ohlcv with db write {}".format(ticker))
                elif insert_db and len(series_df) == 0:
                    db_logs.DBLogs().info("successful get_ohlcv but skipped with len-0 insert {}".format(ticker))
                else:
                    pass
        except Exception:
            db_logs.DBLogs().critical("get_ohlcv FAILED {}".format(ticker))
        return series_df

    async def asyn_get_ohlcv(self, ticker, exchange, period_end=datetime.datetime.today(), period_start=None, period_days=3650, read_db=True, insert_db=True, tries=0):
        series_df = None
        try:
            id_codes = await self.asyn_get_identification_codes(ticker=ticker, exchange=exchange)
            series_metadata, series_identifier = self._get_series_identifiers_and_metadata(
                isin=id_codes["isin"], 
                ticker=ticker, 
                exchange=exchange, 
                source="eodhistoricaldata")
            if read_db:
                period_start = period_start if period_start else period_end - datetime.timedelta(days=period_days)
                exists, series_df = await self.db_service.asyn_read_timeseries(
                    dtype="equity", dformat="spot", dfreq="1d", 
                    series_metadata=series_metadata, series_identifier=series_identifier, metalogs=ticker,
                    period_start=period_start,
                    period_end=period_end
                )
            if not read_db or not exists:
                series_df = await eod_wrapper.asyn_get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days)
                if not insert_db:
                    db_logs.DBLogs().info("successful asyn_get_ohlcv but not inserted {}".format(ticker))
                elif insert_db and len(series_df) > 0:
                    await self.db_service.asyn_insert_timeseries_df(dtype="equity", dformat="spot", dfreq="1d", 
                                    df=series_df, series_identifier=series_identifier, series_metadata=series_metadata, metalogs=ticker)
                    db_logs.DBLogs().info("successful asyn_get_ohlcv with db write {}".format(ticker))
                elif insert_db and len(series_df) == 0:
                    db_logs.DBLogs().info("successful asyn_get_ohlcv but skipped with len-0 insert {}".format(ticker))
                else:
                    pass
        except Exception:
            if tries < 3:
                db_logs.DBLogs().warning("asyn_get_ohlcv RETRY {} {}".format(tries, ticker))    
                return await self.asyn_get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days, read_db=read_db, insert_db=insert_db, tries=tries+1)
            db_logs.DBLogs().critical("asyn_get_ohlcv FAILED {}".format(ticker))
        return series_df

    async def asyn_batch_get_ohlcv(self, tickers, exchanges, 
        period_end=datetime.datetime.today(), period_start=None, period_days=3650, 
        read_db=True, insert_db=True):
        
        id_codess = await self.asyn_batch_get_identification_codes(tickers=tickers, exchanges=exchanges)
        series_metadatas, series_identifiers = [], []
   
        for i in range(len(tickers)):
            id_codes = id_codess[i]
            series_metadata, series_identifier = self._get_series_identifiers_and_metadata(
                isin=id_codes["isin"], 
                ticker=tickers[i], 
                exchange=exchanges[i], 
                source="eodhistoricaldata")
            series_metadatas.append(series_metadata)
            series_identifiers.append(series_identifier)

        if read_db:
            period_start = period_start if period_start else period_end - datetime.timedelta(days=period_days)
            existss, series_dfs = await self.db_service.asyn_batch_read_timeseries(
                dtype="equity", dformat="spot", dfreq="1d", 
                series_metadatas=series_metadatas, series_identifiers=series_identifiers, metalogs=tickers,
                period_start=period_start,
                period_end=period_end
            )
        
        request_tickers, request_exchanges, request_metadatas, request_identifiers = [], [], [], []
        for i in range(len(tickers)):
            if not read_db or not existss[i]:
                request_tickers.append(tickers[i])
                request_exchanges.append(exchanges[i])
                request_metadatas.append(series_metadatas[i])
                request_identifiers.append(series_identifiers[i])

        request_results = await eod_wrapper.asyn_batch_get_ohlcv(
            tickers=request_tickers, 
            exchanges=request_exchanges, 
            period_end=period_end, 
            period_start=period_start, 
            period_days=period_days
        )

        ohlcvs = []
        j = 0
        for i in range(len(tickers)):
            if not read_db or not existss[i]:
                if request_results[j] is None:
                    db_logs.DBLogs().critical("asyn_batch_get_ohlcv FAILED {}".format(request_tickers[j]))
                ohlcvs.append(request_results[j])
                j += 1
            else:
                ohlcvs.append(series_dfs[i])

        if insert_db:
            insert_tickers, insert_ohlcvs, insert_series_metadatas, insert_series_identifiers = [], [], [], []
            for i in range(len(request_tickers)):
                if request_results[i] is None:
                    pass
                elif request_results[i] is not None and len(request_results[i]) == 0:
                    db_logs.DBLogs().info("successful asyn_get_ohlcv but skipped with len-0 insert {}".format(request_tickers[i]))
                elif request_results[i] is not None and len(request_results[i]) > 0:
                    insert_tickers.append(request_tickers[i])
                    insert_ohlcvs.append(request_results[i])
                    insert_series_metadatas.append(request_metadatas[i])
                    insert_series_identifiers.append(request_identifiers[i])
            
            await self.db_service.asyn_batch_insert_timeseries_df(dtype="equity", dformat="spot", dfreq="1d", 
                    dfs=insert_ohlcvs, series_identifiers=insert_series_identifiers, series_metadatas=insert_series_metadatas, metalogs=insert_tickers)

        return ohlcvs

    def get_live_lagged_prices(self, ticker="", exchange="US"):
        return eod_wrapper.get_live_lagged_prices(ticker=ticker, exchange=exchange)
    
    def get_intraday_data(self, ticker="", exchange="US", interval="5m", to_utc=datetime.datetime.utcnow(), period_days=120):
        return eod_wrapper.get_intraday_data(ticker=ticker, exchange=exchange, interval=interval, to_utc=to_utc, period_days=period_days)