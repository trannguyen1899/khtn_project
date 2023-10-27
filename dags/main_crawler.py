"""
Crawl script from the beignning. 
Eutils NCBI API only return maximum 10000 entries. To overcome this, we need to divide by time intervals.
For this "biomedical data science":
    1. From 1/1/1950 to 31/12/2014
    2. From 1/1/2015 to 31/12/2016
    3. From 1/1/2017 to 31/12/2017
    4. From 1/1/2018 to 31/12/2018
    5. From 1/1/2019 to 31/12/2019
    6. From 1/1/2020 to 30/6/2020
    7. From 1/7/2020 to 31/12/2020
    8. From 1/1/2021 to 30/6/2021
    9. From 1/7/2021 to 31/12/2021
    10. From 1/1/2023 to 31/12/2023
 (YYYY/MM/DD)
"""

import json
from dask.distributed import Client
from main_parser import get_pmid_info, get_pmid_list
from main_db import create_table, write_data
from datetime import date

async def crawl_url(pmid):
    print(f'Đang xử lí {pmid}')
    result = await get_pmid_info(pmid)
    json_result = json.dumps(result)
    return (pmid, json_result)


async def main(start_date, end_date, term):
    await create_table()
    print(start_date,"-",end_date)
    LIMIT = 10000
    OFFSET = 1

    li = await get_pmid_list(LIMIT, OFFSET, start_date, end_date, term)

    client = await Client(asynchronous=True)
    futures = []
    for i in set(li):
        future = client.submit(crawl_url, i)
        futures.append(future)
    results = await client.gather(futures, errors='skip')
    await write_data(results)

    await client.close()

def main_crawler():
    import asyncio
    term = "biomedical data science"
    date_ranges = [
        ("1950/01/01", "2014/12/31"),
        ("2015/01/01", "2016/12/31"),
        ("2017/01/01", "2017/12/31"),
        ("2018/01/01", "2018/12/31"),
        ("2019/01/01", "2019/12/31"),
        ("2020/01/01", "2020/06/30"),
        ("2020/07/01", "2020/12/31"),
        ("2021/01/01", "2021/06/30"),
        ("2021/07/01", "2021/12/31"),
        ("2023/01/01", "2023/12/31")
    ]
    date_ranges.append((date_ranges[-1][1],date.today()))
    for start_date, end_date in date_ranges:
        asyncio.run(main(start_date, end_date, term))
