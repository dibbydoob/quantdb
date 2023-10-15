import asyncio
import pandas as pd

from data_service.utils.datetime_utils import get_span
from collections import defaultdict

async def asyn_batch_get_timeseries(
    tickers, exchanges, read_db, insert_db, granularity, 
    db_service, datapoller, engine, dtype, dformat, 
    period_start=None, period_end=None, duration=None, 
    chunksize=100, results=[], tasks=[], batch_id=0
):
    print(f"START BATCH {batch_id}")
    assert(engine in ["eodhistoricaldata", "DWX-MT5"])
    if not tickers:
        print("gathering results")
        await asyncio.gather(*tasks)
        return results
    
    temp_tickers = list(tickers)
    temp_exchanges = list(exchanges)
    tickers = tickers[:chunksize]
    exchanges = exchanges[:chunksize]
    
    series_metadatas = None
    
    if engine == "eodhistoricaldata":
        series_metadatas = [{"ticker": tickers[i], "source": f"eodhistoricaldata-{exchanges[i]}"} for i in range(len(tickers))]
    if engine == "DWX-MT5":
        series_metadatas = [{"ticker": tickers[i], "source": engine} for i in range(len(tickers))]

    assert(series_metadatas)
    series_identifiers = [{**{"type" : "ticker_series"}, **series_metadata} for series_metadata in series_metadatas]
    period_start, period_end = get_span(
        period_start=period_start, 
        period_end=period_end, 
        duration=duration, 
        granularity=granularity
    )
    result_dfs = []
    if read_db:
        result_ranges, result_dfs = await db_service.asyn_batch_read_timeseries(
            dtype=dtype, 
            dformat=dformat, 
            dfreq=granularity, 
            period_start=period_start,
            period_end=period_end,
            series_metadatas=series_metadatas, 
            series_identifiers=series_identifiers, 
            metalogs=batch_id 
        )
    if not period_start or not period_end:
        return result_dfs if result_dfs else [pd.DataFrame() for _ in range(len(tickers))]

    requests = defaultdict(list)
    
    for i in range(len(tickers)):
        result_start, result_end = result_ranges[i]
        if not result_start and not result_end:
            requests[tickers[i]].append({"period_start" : period_start, "period_end" : period_end, "exchange": exchanges[i]})
            continue
        assert(result_start and result_end)
        
        if period_start < result_start:
            requests[tickers[i]].append({"period_start" : period_start, "period_end" : result_start, "exchange": exchanges[i]})
        if period_end > result_end:
            requests[tickers[i]].append({"period_start" : result_end, "period_end": period_end, "exchange": exchanges[i]})

    request_tickers = []
    request_exchanges = []
    request_starts, request_ends = [], []
    request_metadatas, request_identifiers = [], []
    for i in range(len(tickers)):
        ticker = tickers[i]
        v = requests[ticker]
        request_tickers.extend([ticker] * len(v))
        request_exchanges.extend([spec["exchange"] for spec in v])
        request_starts.extend([spec["period_start"] for spec in v])
        request_ends.extend([spec["period_end"] for spec in v])
        request_metadatas.extend([series_metadatas[i]] * len(v))
        request_identifiers.extend([series_identifiers[i]] * len(v))

    request_results = await datapoller.asyn_batch_get_ohlcv(
        tickers=request_tickers, 
        exchanges=request_exchanges, 
        period_starts=request_starts,
        period_ends=request_ends,
        granularity=granularity
    )
    j = 0
    ohlcvs = []
    for i in range(len(tickers)):
        result_start, result_end = result_ranges[i]
        db_df = result_dfs[i]
        if not result_start and not result_end:
            ohlcvs.append(request_results[j])
            j += 1
            continue
        head_df, tail_df = pd.DataFrame(), pd.DataFrame()
        if period_start < result_start:
            head_df = request_results[j]
            j += 1
        if period_end > result_end:
            tail_df = request_results[j]
            j += 1
        concat_dfs = [head_df, db_df, tail_df]   
        df = pd.concat(concat_dfs, axis=0).drop_duplicates("datetime").reset_index(drop=True)
        ohlcvs.append(df)
    assert(j == len(request_results))       

    if insert_db:
        insert_tickers = [tickers[i] for i in range(len(tickers)) if not ohlcvs[i].empty]
        insert_ohlcvs = [ohlcvs[i] for i in range(len(tickers)) if not ohlcvs[i].empty]
        insert_series_metadatas = [series_metadatas[i] for i in range(len(tickers)) if not ohlcvs[i].empty]
        insert_series_identifiers = [series_identifiers[i] for i in range(len(tickers)) if not ohlcvs[i].empty]
        
        task = asyncio.create_task(
                db_service.asyn_batch_insert_timeseries_df(
                dtype=dtype, 
                dformat=dformat, 
                dfreq=granularity, 
                dfs=insert_ohlcvs, 
                series_identifiers=insert_series_identifiers, 
                series_metadatas=insert_series_metadatas, 
                metalogs=batch_id
            )
        )
        tasks.append(task)
        await asyncio.sleep(0)
    results.extend(ohlcvs)
     
    return await asyn_batch_get_timeseries(   
        temp_tickers[chunksize:], temp_exchanges[chunksize:], read_db, insert_db, 
        granularity, db_service, datapoller, engine, dtype, dformat, period_start, period_end, 
        duration, chunksize, results, tasks, batch_id+1
    )