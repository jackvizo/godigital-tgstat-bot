from dotenv import dotenv_values
from prefect import flow, task
from telethon.sync import TelegramClient

from db_utils import get_db_connection, get_db_channels
from tests.flow_code import authorize, collect_data
from backend.run_collect import service_run


# @task
# def create_db_instance():
#     return get_db_connection()


@task
async def task_authorize(phone):
    return await authorize(phone)

@task
async def task_collect_data(client, channels, **params):
    print(f'---> parameters = {params}')
    hours = 0 if 'hours' not in params else params['hours']
    print(f'---> mode: {hours}')
    await collect_data(client, channels, hours)


@flow(name="tg-collect", log_prints=True)
async def tg_collect_flow(**params):
    conf = dotenv_values('.env')
    phone_number = conf['PHONE']

    clt = await task_authorize(phone_number)                                            # сессия из БД
    if not clt:
        clt = TelegramClient(phone_number, int(conf['API_ID']), conf['API_HASH'])       # новая сессия
    clt.parse_mode = 'html'
    # await clt.start()

    for ch in get_db_channels():
        await task_collect_data(clt, (ch,), **params)
