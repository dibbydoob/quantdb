import os
import json
import aiohttp
import requests
import datetime
import calendar
import numpy as np
import pandas as pd
import urllib
import urllib.parse

from dateutil.relativedelta import relativedelta

import data_service.wrappers.aiohttp_wrapper as aiohttp_wrapper

def get_fundamental_data(eod_client, ticker, exchange="US"):
    return eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange))
       
def get_ohlcv(ticker="", exchange="US", period_end=datetime.datetime.today(), period_start=None, period_days=3650):
    url = "https://eodhistoricaldata.com/api/eod/{}.{}".format(ticker, exchange)
    params = {
        "api_token": os.getenv('EOD_KEY'), 
        "fmt": "json",
        "from": period_start.strftime('%Y-%m-%d') if period_start else (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
        "to": period_end.strftime('%Y-%m-%d')
    }
    resp = requests.get(url, params=params)
    df = pd.DataFrame(resp.json())
    df.rename(columns={"date": "datetime", "adjusted_close": "adj_close"}, inplace=True)
    if len(df) > 0:
        df["datetime"] = pd.to_datetime(df["datetime"])
    return df

async def asyn_get_ohlcv(ticker="", exchange="US", period_end=datetime.datetime.today(), period_start=None, period_days=3650):
    url = "https://eodhistoricaldata.com/api/eod/{}.{}".format(ticker, exchange)
    params = {
        "api_token": os.getenv('EOD_KEY'), 
        "fmt": "json",
        "from": period_start.strftime('%Y-%m-%d') if period_start else (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
        "to": period_end.strftime('%Y-%m-%d')
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url=url, params=params) as resp:
            df = pd.DataFrame((await resp.json()))
            df.rename(columns={"date": "datetime", "adjusted_close": "adj_close"}, inplace=True)
            if len(df) > 0:
                df["datetime"] = pd.to_datetime(df["datetime"])
            return df

async def asyn_batch_get_ohlcv(tickers, exchanges, period_end=datetime.datetime.today(), period_start=None, period_days=3650):
    urls = []
    params = {
        "api_token": os.getenv('EOD_KEY'), 
        "fmt": "json",
        "from": period_start.strftime('%Y-%m-%d') if period_start else (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
        "to": period_end.strftime('%Y-%m-%d')
    }
    results = []
    for ticker, exchange in zip(tickers, exchanges):
        url = "https://eodhistoricaldata.com/api/eod/{}.{}?".format(ticker, exchange)
        urls.append(url + urllib.parse.urlencode(params))
    results = await aiohttp_wrapper.async_aiohttp_get_all(urls)
    dfs = []
    for result in results:
        if result is None:
            dfs.append(result)
            continue
        df = pd.DataFrame(result)
        df.rename(columns={"date": "datetime", "adjusted_close": "adj_close"}, inplace=True)
        if len(df) > 0:
            df["datetime"] = pd.to_datetime(df["datetime"])
        dfs.append(df)
    return dfs

def get_live_lagged_prices(ticker="", exchange="US"):
    #https://eodhistoricaldata.com/financial-apis/live-realtime-stocks-api/
    #supports multiple tickers pass
    url = "https://eodhistoricaldata.com/api/real-time/{}.{}".format(ticker, exchange)
    params = {
        "api_token": os.getenv('EOD_KEY'), 
        "fmt": "json"
    }
    resp = requests.get(url, params=params)
    return resp.json()

def get_intraday_data(ticker="", exchange="US", interval="5m", to_utc=datetime.datetime.utcnow(), period_days=120):
    #https://eodhistoricaldata.com/financial-apis/intraday-historical-data-api/
    url = "https://eodhistoricaldata.com/api/intraday/{}.{}".format(ticker, exchange)
    params = {
        "api_token": os.getenv('EOD_KEY'), 
        "interval": interval,
        "fmt": "json",
        "from": calendar.timegm((to_utc - datetime.timedelta(days=period_days)).utctimetuple()),
        "to": calendar.timegm(to_utc.utctimetuple())
    }
    resp = requests.get(url, params=params)
    df = pd.DataFrame(resp.json()).reset_index(drop=True).set_index("datetime")
    return df


async def new_asyn_batch_get_ohlcv(tickers, exchanges, period_end=datetime.datetime.today(), period_start=None, period_days=3650):
    urls = []
    params = {
        "api_token": os.getenv('EOD_KEY'),

        "fmt": "json",
        "from": period_start.strftime('%Y-%m-%d') if period_start else (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
        "to": period_end.strftime('%Y-%m-%d')
    }
    results = []
    for ticker, exchange in zip(tickers, exchanges):
        url = "https://eodhistoricaldata.com/api/eod/{}.{}?".format(ticker, exchange)
        urls.append(url + urllib.parse.urlencode(params))
    results = await aiohttp_wrapper.async_aiohttp_get_all(urls)

    # Initialize an empty dictionary
    dfs = {}

    for ticker, result in zip(tickers, results):  # Include tickers in your loop
        if result is None:
            dfs[ticker] = None  # Use ticker symbol as key
            continue
        df = pd.DataFrame(result)

        # Rest of your DataFrame processing code...
        df.rename(columns={"date": "datetime", "adjusted_close": "adj_close"}, inplace=True)
        if len(df) > 0:
            df["datetime"] = pd.to_datetime(df["datetime"])

        dfs[ticker] = df  # Use ticker symbol as key

    return dfs