import argparse

from telethon.sessions import StringSession
from telethon.sync import TelegramClient

from db_utils import get_session_from_db, save_channel_to_db

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Add a new Telegram channel to track.')
    parser.add_argument('--phone_number', required=True, type=str, help='Phone number associated with the bot')
    parser.add_argument('--tg_channel_name', required=True, type=str, help='Human-readable name of the channel')

    args = parser.parse_args()

    session_data = get_session_from_db(args.phone_number)
    if session_data:
        api_id, api_hash, session_str = session_data

        client = TelegramClient(StringSession(session_str), api_id, api_hash)
        client.connect()

        if not client.is_user_authorized():
            raise PermissionError('UNAUTHORIZED')

        with client:
            try:
                tg_channel = client.get_entity(args.tg_channel_name)
                save_channel_to_db(tg_channel.id, tg_channel.title)
                print(f"Channel {args.tg_channel_name} with id {tg_channel.id} saved to database.")
            except Exception as e:
                print(f'Ошибка запроса канала "{args.tg_channel_name}": {e}')
    else:
        raise PermissionError('NO_VALID_SESSIONS_IN_POOL')