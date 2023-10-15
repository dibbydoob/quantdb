import os
import sys
import json
import pytz
import motor
import asyncio
import pathlib
import pymongo
import pandas as pd
import motor.motor_asyncio
import data_service.db_logs as db_logs

from functools import partial
from datetime import timedelta
from datetime import datetime
from pymongo import UpdateOne
from concurrent.futures import ProcessPoolExecutor

from data_service.utils.datetime_utils import get_mongo_datetime_filter
from data_service.utils.datetime_utils import map_dfreq_to_frequency

class DbService():

    earliest_utc_cutoff = datetime(1970, 1, 31, tzinfo=pytz.utc)

    def __init__(self, db_config_path=str(pathlib.Path(__file__).parent.resolve()) + "/config.json"):
        with open(db_config_path, "r") as f: 
            config = json.load(f)
            os.environ['MONGO_CLUSTER'] = config["mongo_cluster"]
            os.environ['MONGO_DB'] = config["mongo_db"]
        self.mongo_cluster = pymongo.MongoClient(os.getenv("MONGO_CLUSTER"))        
        self.mongo_db = self.mongo_cluster[os.getenv("MONGO_DB")]
        self.asyn_mongo_cluster = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_CLUSTER"))
        self.asyn_mongo_db = self.asyn_mongo_cluster[os.getenv("MONGO_DB")]

    @staticmethod
    def unroll_df(df, metadata):
        records = df.to_dict(orient="records")
        for record in records: record.update({"metadata" : metadata})        
        return records

    @staticmethod
    async def pool_unroll_df(dfs, metadatas, use_pool=False):
        if not use_pool: 
            return [DbService.unroll_df(df, metadata) for df, metadata in zip(dfs, metadatas)]
        with ProcessPoolExecutor() as process_pool:
            loop = asyncio.get_running_loop()
            calls = [partial(DbService.unroll_df, df, metadata) for df, metadata in zip(dfs, metadatas)]
            results = await asyncio.gather(*[loop.run_in_executor(process_pool, call) for call in calls])
        return results

    @staticmethod
    def match_identifiers_to_docs(identifiers, docs):
        records = []
        for identifier in identifiers:
            matched = []
            for doc in docs:
                if identifier.items() <= doc.items():
                    matched.append(doc)
            records.append(matched)
        return records

    def _get_coll_name(self, dtype, dformat, dfreq):
        return "{}_{}_{}".format(dtype, dformat, dfreq)
    
    async def _asyn_get_collection(self, dtype, dformat, dfreq):
        return self.asyn_mongo_db[self._get_coll_name(dtype, dformat, dfreq)]

    async def _asyn_get_collection_meta(self, dtype, dformat, dfreq):
        return self.asyn_mongo_db["{}-meta".format(self._get_coll_name(dtype, dformat, dfreq))]
    
    def _ensure_coll(self, dtype, dformat, dfreq, coll_type, force_check=True):
        assert(coll_type == "timeseries" or coll_type == "regular")
        if not force_check: return True
        names = self.mongo_db.list_collection_names(filter={"name": {"$regex": r"^(?!system\.)"}})   
        exists = self._get_coll_name(dtype=dtype, dformat=dformat, dfreq=dfreq) in names
        if not exists and coll_type == "timeseries":
            frequency = map_dfreq_to_frequency(dfreq=dfreq)
            assert(frequency == "seconds" or frequency == "minutes" or frequency == "hours")
            self.mongo_db.create_collection(
                "{}_{}_{}".format(dtype, dformat, dfreq), 
                timeseries={ 'timeField': 'datetime', 'metaField': 'metadata', 'granularity': frequency },
                check_exists=True
            )
            self.mongo_db.drop_collection("{}_{}_{}-meta".format(dtype, dformat, dfreq))
            self.mongo_db.create_collection(
                "{}_{}_{}-meta".format(dtype, dformat, dfreq)
            )
        if not exists and coll_type == "regular":
            self.mongo_db.create_collection(
                "{}_{}_{}".format(dtype, dformat, dfreq), 
                check_exists=True
            )
        return True

    def _check_contiguous_series(self, record_start, record_end, new_start, new_end):
        return new_start <= record_end and record_start <= new_end
    
    """
    SECTION::TIMESERIES
    """
    async def asyn_insert_timeseries_df(self, dtype, dformat, dfreq, df, series_metadata, series_identifier, metalogs=""):
        return await self.asyn_batch_insert_timeseries_df(
            dtype=dtype, 
            dformat=dformat, 
            dfreq=dfreq, 
            dfs=[df], 
            series_metadatas=[series_metadata], 
            series_identifiers=[series_identifier], 
            metalogs=[metalogs]
        )

    async def asyn_batch_insert_timeseries_df(
        self, dtype, dformat, dfreq, dfs, series_metadatas, series_identifiers, metalogs=[]
    ):  
        print(f"START BATCH INSERTING ID {metalogs}")
        dfs = [df.loc[df["datetime"] >= DbService.earliest_utc_cutoff] for df in dfs]
        assert(all([len(df) > 0 for df in dfs]))
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="timeseries")
        series_starts = [datetime.fromtimestamp(int(df["datetime"].values[0])/1e9, tz=pytz.utc) for df in dfs]
        series_ends = [datetime.fromtimestamp(int(df["datetime"].values[-1])/1e9, tz=pytz.utc)  for df in dfs]

        docs = await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).find({"$or" : series_identifiers}).to_list(length=None)        
        meta_records = DbService.match_identifiers_to_docs(series_identifiers, docs)
        new_inserts_seriess = []
        new_inserts_series_metas = []
        new_updates_series_metas = []
        update_identifers, new_heads, new_tails, time_starts, time_ends = [], [], [], [], []
        for i in range(len(series_identifiers)):
            matched = meta_records[i]
            if len(matched) == 0:
                doc = {
                    **series_identifiers[i], 
                    **{
                        "time_start": series_starts[i], 
                        "time_end": series_ends[i], 
                        "last_updated": datetime.now(pytz.utc)
                    }
                }
                records = DbService.unroll_df(dfs[i], series_metadatas[i]) 
                new_inserts_seriess.extend(records)
                new_inserts_series_metas.append(doc)
            elif len(matched) == 1:
                meta_record = matched[0]
                meta_start = pytz.utc.localize(meta_record["time_start"])
                meta_end = pytz.utc.localize(meta_record["time_end"])
                contiguous_series = self._check_contiguous_series(
                    record_start=meta_start, 
                    record_end=meta_end, 
                    new_start=series_starts[i], 
                    new_end=series_ends[i]
                )
                if not contiguous_series:
                    db_logs.DBLogs().error(f"{sys._getframe().f_code.co_name} got discontiguous series::{metalogs[i]}")
                    continue
                new_head = dfs[i].loc[dfs[i]["datetime"] < meta_start]
                new_tail = dfs[i].loc[dfs[i]["datetime"] > meta_end]
                if len(new_head) + len(new_tail) > 0:
                    new_heads.append(new_head)
                    new_tails.append(new_tail)
                    time_starts.append(min(series_starts[i], meta_start))
                    time_ends.append(max(series_ends[i], meta_end))
                    update_identifers.append(series_identifiers[i])
            else:
                db_logs.DBLogs().error(f"{sys._getframe().f_code.co_name} got meta series corruption, series count gt 1::{metalogs[i]}")
                exit()

        unrolled_heads = await DbService.pool_unroll_df(new_heads, update_identifers) 
        unrolled_tails = await DbService.pool_unroll_df(new_tails, update_identifers)
                 
        for head_records in unrolled_heads:
            new_inserts_seriess.extend(head_records)
        for tail_records in unrolled_tails:
            new_inserts_seriess.extend(tail_records)
        for i in range(len(new_heads)):
            new_updates_series_metas.append(UpdateOne(
                update_identifers[i],
                {"$set": {
                    "time_start": time_starts[i],
                    "time_end": time_ends[i],
                    "last_updated": datetime.now(pytz.utc),
                }}
            ))
   
        if new_inserts_seriess:
            await (await self._asyn_get_collection(dtype, dformat, dfreq)).insert_many(new_inserts_seriess, ordered=False)      
        if new_inserts_series_metas:
            await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).insert_many(new_inserts_series_metas, ordered=False)
        if new_updates_series_metas:
            await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).bulk_write(new_updates_series_metas, ordered=False)

        print(f"FINISH BATCH INSERTING ID {metalogs}")
        return True

    async def asyn_read_timeseries(
        self, dtype, dformat, dfreq, period_start, period_end, series_metadata, series_identifier, metalogs=""
    ):
        result_ranges, result_dfs =  await self.asyn_batch_read_timeseries(
            dtype=dtype,
            dformat=dformat,
            dfreq=dfreq,
            period_start=period_start,
            period_end=period_end,
            series_metadatas=[series_metadata],
            series_identifiers=[series_identifier],
            metalogs=[metalogs]
        )
        return (result_ranges[0], result_dfs[0])

    async def asyn_batch_read_timeseries(
        self, dtype, dformat, dfreq, period_start, period_end, series_metadatas, series_identifiers, metalogs=[]
    ):
        assert(not period_start or period_start > DbService.earliest_utc_cutoff)
        assert(not period_end or period_end > DbService.earliest_utc_cutoff) 
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="timeseries")
        docs = await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).find({"$or" : series_identifiers}).to_list(length=None)
        series_records = DbService.match_identifiers_to_docs(series_identifiers, docs)
        series_filter = get_mongo_datetime_filter(period_start=period_start, period_end=period_end)
        async def poll_record(i):
            matched = series_records[i]
            assert(len(matched) <= 1)
            if len(matched) == 0:
                return (None, None), pd.DataFrame()
            if len(matched) == 1:   
                records = await (await self._asyn_get_collection(dtype, dformat, dfreq)).find(
                    {
                        **series_filter,
                        **{"metadata.{}".format(k) : v for k,v in series_metadatas[i].items()}
                    }
                ).to_list(length=None)
                record_df = pd.DataFrame(records).drop(columns=["metadata", "_id"]) if records else pd.DataFrame()
                record_start, record_end = None, None
                if len(record_df) > 0 : 
                    record_df["datetime"] = pd.to_datetime(record_df["datetime"]).dt.tz_localize(pytz.utc)
                    record_start, record_end = record_df["datetime"].values[0], record_df["datetime"].values[-1]
                    record_start = datetime.fromtimestamp(int(record_start)/1e9, tz=pytz.utc)
                    record_end = datetime.fromtimestamp(int(record_end)/1e9, tz=pytz.utc)
                return (record_start, record_end), record_df

        db_polls = await asyncio.gather(*[poll_record(i) for i in range(len(series_identifiers))], return_exceptions=False) 
        result_ranges = [db_poll[0] for db_poll in db_polls]
        result_dfs = [db_poll[1] for db_poll in db_polls]
        return result_ranges, result_dfs
    
    