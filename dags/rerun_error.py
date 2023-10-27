import asyncio
import sqlite3
import re
import json
import pandas as pd
from dask.distributed import Client
from main_parser import get_pmid_info
from crawl_scripts.main_db import write_data

def get_errors():
    with sqlite3.connect('./results.db') as db:
        logs = pd.read_sql("select log_message from logs", db)
        results = pd.read_sql("select pmid from results", db)
    total = results.shape[0]

    # Xử lí logs
    logs.drop_duplicates(['log_message'], inplace=True)
    logs['error_id'] = logs['log_message'].apply(
        lambda input_string: re.findall(r'\d+', input_string)[0])

    merged = results.merge(logs, left_on='pmid',
                           right_on='error_id', how='outer', indicator=True)
    anti_join = merged[(merged._merge == 'right_only')].drop('_merge', axis=1)
    error_ids = anti_join['error_id'].to_list()
    return error_ids, total

async def crawl_url(pmid):
    print(f'Đang xử lí {pmid}')
    result = await get_pmid_info(pmid, False)
    json_result = json.dumps(result)
    return (pmid, json_result)

async def main():
    error_ids, total = get_errors()

    client = await Client(asynchronous=True)
    futures = []
    for i in set(error_ids):
        future = client.submit(crawl_url, i)
        futures.append(future)
    results = await client.gather(futures, errors='skip')
    await client.close()
    await write_data(results)
    remain_error = len(error_ids) - len(results)
    return (remain_error/total)*100

def rerun_error():
    asyncio.run(main())

