import argparse

from telethon.errors import PhoneNumberBannedError
from telethon.sessions import StringSession
from telethon.sync import TelegramClient

from db_utils import save_session_to_db, create_db_connection

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Add a new Telegram bot session to the pool.')
    parser.add_argument('--api_id', required=True, type=int, help='API ID from telegram.org')
    parser.add_argument('--api_hash', required=True, type=str, help='API hash from telegram.org')
    parser.add_argument('--phone_number', required=True, type=str, help='Phone number associated with the bot')

    args = parser.parse_args()

    tg_client = TelegramClient(StringSession(), args.api_id, args.api_hash)
        
    with tg_client:
        if not tg_client.is_user_authorized():
            try:
                tg_client.send_code_request(args.phone_number)
            except PhoneNumberBannedError:
                print(f"Phone number {args.phone_number} is banned.")
                tg_client.disconnect()
                status = 'banned'
        else:
            status = 'enabled'

        conn = create_db_connection()

        try:
            session_str = tg_client.session.save()
            save_session_to_db(conn, args.api_id, args.api_hash, args.phone_number, session_str, status)

            print(f"Session (phone: {args.phone_number}, api_id: {args.api_id}) "
                  f"was saved to database with status '{status}'.")
        finally:
            conn.close()
