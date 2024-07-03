from dotenv import dotenv_values
from prefect import flow, task
from telethon.sync import TelegramClient

from backend.AsyncSQLDataService import AsyncSQLDataService
from db_utils import get_db_channels, create_db_connection
from lib.collect import authorize
from lib.collect import collect_data


@task
async def task_authorize(conn, phone):
    return await authorize(conn, phone)


@task
async def task_collect_data(sql: AsyncSQLDataService, client, channels, hours=0):
    await collect_data(sql, client, channels, hours=hours)


@flow(name="tg-collect", log_prints=True)
async def tg_collect_flow(phone_number: str, hours=0):
    sql = AsyncSQLDataService()

    try:
        telethon_session_str, session_pool_pk = await task_authorize(sql.connection, phone_number)  # сессия из БД
        if not telethon_session_str:
            raise PermissionError('NO_VALID_SESSION_IN_POOL')

        for ch in get_db_channels(sql.connection, session_pool_pk):
            await task_collect_data(sql, telethon_session_str, (ch,), hours=hours)

    finally:
        await sql.close()
