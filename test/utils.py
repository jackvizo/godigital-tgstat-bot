from telethon import TelegramClient
from telethon.sessions import StringSession


def get_test_client(session=None, api_id=17349, api_hash='344583e45741c457fe1862106095a5eb',
                    phone='', code='',
                    data_center=0, ip='149.154.167.40', port=80):   # test credentials by default

    client = TelegramClient(session, api_id, api_hash)
    client.session.set_dc(data_center, ip, port)
    client.start(phone=phone, code_callback=lambda: code)

    return client
