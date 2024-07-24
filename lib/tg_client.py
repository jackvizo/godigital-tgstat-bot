from telethon import TelegramClient
from telethon.sessions import StringSession

from db.SyncSQLDataService import SyncSQLDataService
from db.queries import get_session_from_db


async def get_authorized_tg_client(sql: SyncSQLDataService, phone_number):
    session_data = get_session_from_db(sql.connection, phone_number)

    if session_data:
        session_pool_pk, api_id, api_hash, session_str = session_data
        tg_client = TelegramClient(session=StringSession(session_str), api_id=int(api_id),
                                   api_hash=api_hash,
                                   flood_sleep_threshold=30)
        await tg_client.connect()
        if not await tg_client.is_user_authorized():
            raise PermissionError('NOT_AUTHORIZED')
    else:
        raise PermissionError("NO_ACTIVE_SESSION_IN_POOL")

    return tg_client, session_pool_pk
