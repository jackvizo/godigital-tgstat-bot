from dotenv import dotenv_values

from telethon import TelegramClient
from telethon.sessions import StringSession


def get_tg_client(session=StringSession(), api_id='', api_hash='', phone='', code='',
                  data_center=None, ip='', port=None):
    cfg = dotenv_values('.env')

    # default credentials
    if not phone:
        phone = cfg['PHONE']            # ''
    if not code:
        code = cfg['CODE']              # ''
    if not api_id:
        api_id = cfg['API_ID']          # 12345
    if not api_hash:
        api_hash = cfg['API_HASH']      # ''
    if not data_center:
        data_center = cfg['DC']         # 0
    if not ip:
        ip = cfg['IP']                  # '149.154.167.40'
    if not port:
        port = cfg['PORT']              # 80

    client = TelegramClient(session, api_id, api_hash)
    client.session.set_dc(data_center, ip, port)
    client.start(phone=phone, code_callback=lambda: code)

    return client, phone, api_id, api_hash


def get_test_client(session=None, api_id=17349, api_hash='344583e45741c457fe1862106095a5eb',
                    phone='', code='',
                    data_center=0, ip='149.154.167.40', port=80):   # test credentials by default

    client = TelegramClient(session, api_id, api_hash)
    client.session.set_dc(data_center, ip, port)
    client.start(phone=phone, code_callback=lambda: code)

    return client
