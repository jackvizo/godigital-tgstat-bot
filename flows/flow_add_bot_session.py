import os

from prefect import flow, task, pause_flow_run
from pydantic import BaseModel
from telethon import TelegramClient

from db.SyncSQLDataService import SyncSQLDataService
from lib.add_bot_session import store_bot_session, \
    request_2fa, confirm_2fa, create_tg_client


@task
async def task_add_bot_session_request_2fa(tg_client: TelegramClient, phone_number: str):
    phone_code_hash = await request_2fa(tg_client, phone_number)
    return phone_code_hash


@task
async def task_add_bot_session_confirm_2fa(tg_client: TelegramClient, phone_code_hash: str,
                                           code_2fa: str, cloud_password: str = None):
    session_str = await confirm_2fa(
        tg_client=tg_client,
        phone_code_hash=phone_code_hash,
        code_2fa=code_2fa,
        cloud_password=cloud_password
    )

    return session_str


@task
async def task_store_bot_session(sql: SyncSQLDataService, user_id: str, phone_number: str, session_str: str):
    store_bot_session(sql=sql, user_id=user_id, phone_number=phone_number, session_str=session_str)


class Input2FA(BaseModel):
    code_2fa: str
    cloud_password: str = None


@flow(log_prints=True)
async def flow_add_bot_session(api_id: str, api_hash: str, phone_number: str, user_id: str, is_test: bool = False):
    if is_test:
        os.environ['IS_TEST'] = '1'

    tg_client = create_tg_client(api_id=api_id, api_hash=api_hash)

    await tg_client.connect()
    phone_code_hash = await request_2fa(tg_client=tg_client, phone_number=phone_number)

    parameters = await pause_flow_run(wait_for_input=Input2FA)

    session_str = await confirm_2fa(
        tg_client=tg_client,
        phone_code_hash=phone_code_hash,
        code_2fa=parameters.code_2fa,
        cloud_password=parameters.cloud_password
    )

    await tg_client.disconnect()

    sql = SyncSQLDataService()
    sql.init()

    store_bot_session(sql=sql, user_id=user_id, phone_number=phone_number, session_str=session_str, api_id=api_id, api_hash=api_hash)

    sql.close()
