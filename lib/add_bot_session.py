import os

from pydantic import BaseModel
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.sync import TelegramClient

from db.SyncSQLDataService import SyncSQLDataService
from db.queries import save_session_to_db

# https://docs.telethon.dev/en/stable/developing/test-servers.html
TEST_SERVER_IP = '149.154.167.40'
TEST_DC_ID = 2
TEST_CODE_2FA = 22222


class Input2FA(BaseModel):
    code_2fa: str
    cloud_password: str


def create_tg_client(api_id: str, api_hash: str):
    tg_client = TelegramClient(StringSession(), int(api_id), api_hash) if not os.getenv('IS_TEST') else TelegramClient(
        None, int(api_id), api_hash)
    print('[create_tg_client] tg_client created')

    if os.getenv('IS_TEST'):
        tg_client.session.set_dc(TEST_DC_ID, TEST_SERVER_IP, 80)
        print('[create_tg_client] set_dc setup')

    return tg_client


async def request_2fa(tg_client: TelegramClient, phone_number: str):
    result = await tg_client.send_code_request(phone=phone_number)
    return result.phone_code_hash


async def confirm_2fa(tg_client: TelegramClient, phone_code_hash: str, code_2fa: str,
                      cloud_password: str = None):
    try:
        await tg_client.sign_in(code=code_2fa, phone_code_hash=phone_code_hash)
    except SessionPasswordNeededError:
        await tg_client.sign_in(password=cloud_password)

    print('[confirm_2fa] auth...')
    if tg_client.is_user_authorized():
        print('[confirm_2fa] auth... ok')
        session_str = tg_client.session.save()
        print(f'[confirm_2fa] session_str {session_str}')
        return session_str

    raise PermissionError('FAILED_TO_STORE_SESSION')


def store_bot_session(sql: SyncSQLDataService, user_id: str, phone_number: str,
                      session_str: str, api_id: str, api_hash: str):
    save_session_to_db(conn=sql.connection, phone_number=phone_number, session_str=session_str, status='enabled',
                       user_id=user_id, api_id=api_id, api_hash=api_hash)
