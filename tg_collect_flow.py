import asyncio

import prefect
from dotenv import dotenv_values
from prefect import flow, task
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

import config
from backend.run_collect import service_run
from db_utils import get_db_connection, get_db_channels, get_session_from_db, get_last_db_post_id
from tests.test_code import get_posts, set_user_actions, get_me, read_channels, collect_data
from tests.utils import get_tg_client


@task
def create_db_instance():
    return get_db_connection()


@task
async def task_authorize(phone):
    session_data = get_session_from_db(phone)
    if session_data:
        api_id, api_hash, session_str = session_data

        try:
            tg_client = TelegramClient(StringSession(session_str), api_id, api_hash)
        except Exception as e:
            print(f'Ошибка инициализации клиента по сессии из БД: {e}')
            print('Запуск сессии для тестового сервера!')
            tg_client = get_tg_client()

        if tg_client.is_user_authorized():
            await tg_client.connect()
            res = True
        else:
            res = False
        print(f'Клиент (тел.: {phone}){"" if res else "не"} авторизован по сохраненной сессии!')
    else:
        raise ValueError("No active session found for the given phone number.")
    return tg_client


@task
async def task_collect_data(client, channels, hours=0):
    await collect_data(client, channels, hours=0)


@flow(name="tg-collect", log_prints=True)
async def tg_collect_flow():
    conf = dotenv_values('.env')
    phone_number = conf['PHONE']

    clt = await task_authorize(phone_number)                                         # сессия из БД
    if not clt:
        clt = TelegramClient(phone_number, conf['API_ID'], conf['API_HASH'])         # новая сессия
    clt.parse_mode = 'html'
    # await clt.start()

    for ch in get_db_channels():
        await task_collect_data(clt, (ch,), hours=0)
