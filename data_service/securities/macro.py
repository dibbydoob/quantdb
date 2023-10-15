import os
import re
import json
import requests
import datetime
import pandas as pd

#from cif import cif
#from dbnomics import fetch_series, fetch_series_by_api_link

class Macro():

    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]
        self.fred_client = data_clients["fred_client"]

    """
    EOD Services
    """
    def get_macro_releases(self, period_days=365, period_end=datetime.datetime.today(), limit=1000, countryiso=None):
        url = 'https://eodhistoricaldata.com/api/economic-events'
        params = {
            "api_token": os.getenv('EOD_KEY'),
            "from": (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
            "to": period_end.strftime('%Y-%m-%d'),
            "limit": limit
        }
        if countryiso:
            params.update({"country": countryiso}) 
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()).iloc[::-1].reset_index(drop=True).set_index("date")

    def get_macro_indicators(self, countryiso="USA", indicator_code="gdp_current_usd"): 
        #alpha3iso, supported indicators: https://eodhistoricaldata.com/financial-apis/macroeconomics-data-and-macro-indicators-api/
        url = 'https://eodhistoricaldata.com/api/macro-indicator/{}'.format(countryiso)
        params = {
            "api_token": os.getenv('EOD_KEY'),
            "indicator": indicator_code
        } 
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()).iloc[::-1].reset_index(drop=True).set_index("Date")

    def get_rates_universe(self):
        url = 'https://eodhistoricaldata.com/api/exchange-symbol-list/{}'.format("MONEY")
        params = {"api_token": os.getenv('EOD_KEY'), "fmt": "json"}
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()).loc[pd.DataFrame(resp.json())["Type"] == "Rate"]

    def get_borrow_rates(self, ticker="LIBORUSD", duration="1M"):
        #https://eodhistoricaldata.com/financial-apis/macroeconomic-data-api/
        url = "https://eodhistoricaldata.com/api/eod/{}{}.MONEY".format(ticker, duration)
        params = {
            "api_token": os.getenv('EOD_KEY'),
            "fmt": "json"
        } 
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()).reset_index(drop=True).set_index("date")

    def get_cds_rates(self):
        pass #create credit.py file, not available yet

    
    """
    FRED Services
    Requests can be customized according to data source, release, category, series, tags and other preferences.
    https://github.com/mortada/fredapi
    """
    def search_for_series(self, search_string="GDP"):
        return self.fred_client.search(search_string).reset_index()

    def get_data_series_list_by_category(self, category_id=101, limit=1000, order_by="popularity"):
        return self.fred_client.search_by_release(category_id, limit=limit, order_by=order_by).reset_index()

    def get_data_series_list_by_release(self, release_id=175, limit=1000, order_by="popularity"):
        return self.fred_client.search_by_release(release_id, limit=limit, order_by=order_by).iloc[::-1].reset_index(drop=True)

    def get_data_series_by_id(self, search_id="SP500", start="1950-01-01", end=datetime.datetime.today().strftime('%Y-%m-%d'), 
        frequency="d", release_type="", date=datetime.datetime.today().strftime('%Y-%m-%d')):
        if release_type == "first":
            return self.fred_client.get_series_first_release(search_id).reset_index()
        elif release_type == "latest":
            return self.fred_client.get_series_latest_release(search_id).reset_index()
        elif release_type == "date":
            return self.fred_client.get_series_as_of_date(search_id, date)
        elif release_type == "all":
            return self.fred_client.get_series_all_releases(search_id)
        return self.fred_client.get_series(search_id, observation_start=start, observation_end=end)

    def get_data_series_info(self, search_id="SP500"):
        return self.fred_client.get_series_info(search_id)

    #Note that other new API endpoints can be accessed similarly, see docs: https://fred.stlouisfed.org/docs/api/fred/
    def get_all_fred_data_tags(self):
        url = "https://api.stlouisfed.org/fred/tags?api_key={}&file_type=json".format(os.getenv("FRED_KEY"))
        resp = requests.get(url=url)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()["tags"])

    def get_all_fred_data_sources(self):
        url = "https://api.stlouisfed.org/fred/sources?api_key={}&file_type=json".format(os.getenv("FRED_KEY"))
        resp = requests.get(url=url)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()["sources"])    

    def get_all_fred_series_by_tags(self, tags=["usa"]):
        url = "https://api.stlouisfed.org/fred/tags/series?tag_names={}&api_key={}&file_type=json".format(";".join(tags), os.getenv("FRED_KEY"))
        resp = requests.get(url=url)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()["seriess"])

    def get_all_fred_releases(self, realtime_start=datetime.datetime.today() - datetime.timedelta(days=5), realtime_end=datetime.datetime.today()):
        url = "https://api.stlouisfed.org/fred/releases"
        params = {
            "api_key" : os.getenv("FRED_KEY"),
            "realtime_start" : realtime_start.strftime('%Y-%m-%d'),
            "realtime_end" : realtime_end.strftime('%Y-%m-%d'),
            "file_type" : "json"
        }
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            return pd.DataFrame(resp.json()["releases"])
  
    """
    OECD (BETA) Services
    a dataset identifier, a list of dimension item identifiers and some additional parameters must be supplied 
    in an URL in the following format:
    python3 -m pip install cif
    #Official Docs: https://data.oecd.org/api/sdmx-json-documentation/
    #Sample Request Sent: http://stats.oecd.org/SDMX-JSON/data/<dataset identifier>/<filter expression>/<agency name>[ ?<additional parameters>]
    #Github, Pipelines: https://github.com/LenkaV/CIF/blob/develop/examples/CI_minimalPipeline.ipynb
    #Sample: https://stats.oecd.org/viewhtml.aspx?datasetcode=QNA&lang=en >> customise >> use codes on all dimensions 
    #User Guide: https://stats.oecd.org/Content/themes/OECD/static/help/WBOS%20User%20Guide%20(EN).PDF
    
    Requires abit of getting to know the nomenclature of OECD - to be revisited for future use cases
    """
    def get_oecd_countries(self):
        return cif.getOECDJSONStructure(dsname="MEI", showValues=[0])

    def get_data(self, dataset="QNA", subject="P31S14_S15", measure="VPVOBARSA", frequency="Q", country="CAN"):
        data, subjects, measures = cif.createDataFrameFromOECD(countries = [country], dsname = dataset, subject = [subject], measure=[measure], frequency = frequency)
        return data


    """
    DBnomics Service
    python3 -m pip install dbnomics
    API Docs: https://api.db.nomics.world/v22/apidocs
    Search Datasets: https://db.nomics.world/?q=fund+flow
    Detailed Use Cases: https://notes.quantecon.org/submission/5bd32515f966080015bafbcd
    """
    def get_dbnomics_series(self, series_id="STATCAN/36100578/1.1.1"):
        return fetch_series(series_id)