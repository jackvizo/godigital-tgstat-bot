import prefect
from prefect import flow, task
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

import config
from db_utils import get_db_connection, get_channels, get_session_from_db


@task
def create_db_instance():
    return get_db_connection()


@task
def task_authorize(db, phone_number):
    session_data = get_session_from_db(phone_number)
    if session_data:
        api_id, api_hash, session_bytes = session_data
        client = TelegramClient(StringSession(session_bytes), api_id, api_hash)
        return client
    else:
        raise ValueError("No active session found for the given phone number.")


@task
def task_collect_data(db, tg_client, tg_channel_name):
    # Соберите данные с использованием tg_client и сохраните в db
    channel = tg_client.get_entity(tg_channel_name)
    messages = tg_client.get_messages(channel, limit=10)
    for message in messages:
        print(message.text)
        # Сохраните данные в db


@flow(name="tg-collect", log_prints=True)
def tg_collect_flow():
    phone_number = prefect.client.secrets.Secret("phone_number")
    db = create_db_instance()
    tg_client = task_authorize(db, phone_number)

    channels = get_channels()
    for tg_channel_id, tg_channel_name in channels:
        task_collect_data(db, tg_client, tg_channel_name)
