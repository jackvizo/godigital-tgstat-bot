import psycopg
import config


def get_db_connection():
    return psycopg.connect(
        database=config.db_name,    # "TG_bot_db"
        host=config.host,           # "localhost"
        port=config.port,           # "5432"
        user=config.user,           # "postgres"
        password=config.password,   # "pass_123"
    )


def get_session_from_db(phone_number):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT api_id, api_hash, session_bytes
        FROM config__tg_bot_session_pool
        WHERE phone_number = %s AND status = 'enabled'
    """, (phone_number,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result


def save_session_to_db(api_id, api_hash, phone_number, session_str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO config__tg_bot_session_pool (api_id, api_hash, phone_number, session_bytes)
        VALUES (%s, %s, %s, %s)
    """, (api_id, api_hash, phone_number, session_str))
    conn.commit()
    cursor.close()
    conn.close()


def save_channel_to_db(tg_channel_id, tg_channel_name, bot_session_pool_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO config__tg_channel (tg_channel_id, tg_channel_name, bot_session_pool_id)
        VALUES (%s, %s, %s)
    """, (tg_channel_id, tg_channel_name, bot_session_pool_id))
    conn.commit()
    cursor.close()
    conn.close()


def get_channels():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT tg_channel_id, tg_channel_name
        FROM config__tg_channel
    """)
    channels = cursor.fetchall()
    cursor.close()
    conn.close()
    return channels
