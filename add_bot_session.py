import argparse
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from db_utils import save_session_to_db

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Add a new Telegram bot session to the pool.')
    parser.add_argument('--api_id', required=True, type=int, help='API ID from telegram.org')
    parser.add_argument('--api_hash', required=True, type=str, help='API hash from telegram.org')
    parser.add_argument('--phone_number', required=True, type=str, help='Phone number associated with the bot')

    args = parser.parse_args()

    client = TelegramClient(StringSession(), args.api_id, args.api_hash)

    with client:
        session_str = client.session.save()
        save_session_to_db(args.api_id, args.api_hash, args.phone_number, session_str)
        print("Session saved to database.")
