import argparse

from python_socks import ProxyType
from prefect.blocks.system import JSON
from telethon.errors import PhoneNumberBannedError
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from db_utils import save_session_to_db
from test.utils import get_test_client

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Add a new Telegram bot session to the pool.')
    parser.add_argument('--api_id', required=True, type=int, help='API ID from telegram.org')
    parser.add_argument('--api_hash', required=True, type=str, help='API hash from telegram.org')
    parser.add_argument('--phone_number', required=True, type=str, help='Phone number associated with the bot')

    args = parser.parse_args()

    # proxy = JSON.load("proxy").value
    # proxy['proxy_type'] = ProxyType.HTTP    # ProxyType.SOCKS5, ProxyType.SOCKS4
    # client = TelegramClient(StringSession(), args.api_id, args.api_hash, proxy=proxy)

    client = TelegramClient(StringSession(), args.api_id, args.api_hash)

    with client:
        if not client.is_user_authorized():
            try:
                client.send_code_request(args.phone_number)
            except PhoneNumberBannedError:
                print("Phone number is banned.")
                client.disconnect()
                status = 'banned'
        else:
            status = 'enabled'

        session_str = client.session.save()
        save_session_to_db(args.api_id, args.api_hash, args.phone_number, session_str, status)

        print(f"Session (phone: {args.phone_number}, api_id: {args.api_id}) "
              f"was saved to database with status '{status}'.")
