import argparse
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from db_utils import get_session_from_db, save_channel_to_db

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Add a new Telegram channel to track.')
    parser.add_argument('--phone_number', required=True, type=str, help='Phone number associated with the bot')
    parser.add_argument('--tg_channel_name', required=True, type=str, help='Human-readable name of the channel')

    args = parser.parse_args()

    session_data = get_session_from_db(args.phone_number)
    if session_data:
        api_id, api_hash, session_bytes = session_data
        session_str = session_bytes.tobytes().decode('utf-8')
        client = TelegramClient(StringSession(session_str), api_id, api_hash)

        with client:
            tg_channel = client.get_entity(args.tg_channel_name)
            save_channel_to_db(tg_channel.id, tg_channel.title, session_data[0])
            print("Channel saved to database.")
    else:
        print("No active session found for the given phone number.")
