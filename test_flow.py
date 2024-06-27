import asyncio

import prefect
# from prefect import flow, task
from telethon import TelegramClient
from telethon.sessions import StringSession

from db_utils import get_db_connection, get_channels, get_session_from_db
from my.get_test_client import get_test_client


def create_db_instance():
    return get_db_connection()


def task_authorize(db, phone_number):
    session_data = get_session_from_db(phone_number)
    if session_data:
        api_id, api_hash, session_bytes = session_data
        client = TelegramClient(StringSession(session_bytes), api_id, api_hash)
        return client
    else:
        raise ValueError("No active session found for the given phone number.")


async def task_collect_data(db, tg_client, tg_channel_name):
    # Соберите данные с использованием tg_client и сохраните в db
    channel = await tg_client.get_entity(tg_channel_name)
    messages = await tg_client.get_messages(channel, limit=10)
    for message in messages:
        # reacts_all =
        print(message.text)

        # Сохраните данные в db


def tg_collect_flow():
    phone_number = prefect.blocks.system.String.load("phone").value
    db = create_db_instance()
    tg_client = client      # task_authorize(db, phone_number)

    channels = get_channels()
    for tg_channel_id, tg_channel_name in channels:
        task_collect_data(db, tg_client, tg_channel_name)


def main():
    with client:
        tg_collect_flow()


if __name__ == '__main__':
    global client
    client = get_test_client()
    main()
