import aiosqlite
import sqlite3

def write_data_sync(data):
    with sqlite3.connect('./results.db') as db:
        db.executemany("INSERT INTO results (pmid, json_content) VALUES (?,?)", data)
        db.commit()
        
create_logs_table_syntax = """
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME NOT NULL DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
    log_message text not null,
    status text not null
)
"""


create_result_table_syntax = """
CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pmid text not null,
    json_content text not null
)
"""


async def write_log(log_message, status):
    async with aiosqlite.connect('./results.db') as db:
        await db.execute("INSERT INTO logs (log_message, status) VALUES (?,?)", (log_message, status))
        await db.commit()


async def write_data(data):
    async with aiosqlite.connect('./results.db') as db:
        await db.executemany("INSERT INTO results (pmid, json_content) VALUES (?,?)", data)
        await db.commit()


async def create_table():
    async with aiosqlite.connect('./results.db') as db:
        await db.execute(create_logs_table_syntax)
        await db.execute(create_result_table_syntax)


async def find_pmid(pmid_list):
    async with aiosqlite.connect('./results.db') as db:
        query = "SELECT COUNT(*) FROM results WHERE pmid IN ({})".format(
            ','.join(['?'] * len(pmid_list)))
        async with db.execute(query, pmid_list) as cursor:
            rows = await cursor.fetchone()
    return list(rows)


async def delete_by_id(pmid):
    async with aiosqlite.connect('./results.db') as db:
        if type(pmid) in (str, int) and pmid != 0 and pmid != "":
            await db.execute(f"DELETE FROM logs WHERE id = (SELECT ID FROM logs WHERE log_message LIKE '%{pmid}%')")
            await db.commit()