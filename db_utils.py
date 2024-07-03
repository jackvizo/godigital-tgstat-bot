import psycopg2
import globals


def create_db_connection():
    return psycopg2.connect(
        database=globals.DB_NAME,
        host=globals.DB_HOST,
        port=globals.DB_PORT,
        user=globals.DB_USER,
        password=globals.DB_PASSWORD,
    )


def get_session_from_db(conn, phone_number):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pk, api_id, api_hash, session_bytes
        FROM config__tg_bot_session_pool
        WHERE phone_number = %s AND status = 'enabled'
    """, (phone_number,))
    result = cursor.fetchone()
    cursor.close()
    return result


def save_session_to_db(conn, api_id, api_hash, phone_number, session_str, status):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO config__tg_bot_session_pool (api_id, api_hash, phone_number, session_str, status)
        VALUES (%s, %s, %s, %s, %s)
    """, (api_id, api_hash, phone_number, session_str, status))
    conn.commit()
    cursor.close()


def save_channel_to_db(conn, tg_channel_id, tg_channel_name):  # , config__tg_bot_session_pool
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO config__tg_channel (tg_channel_id, tg_channel_name)  
        VALUES (%s, %s) 
    """, (tg_channel_id, tg_channel_name))  # , config__tg_bot_session_pool
    conn.commit()
    cursor.close()


def get_db_channels(conn, session_pool_pk):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.tg_channel_id, c.tg_channel_name
        FROM config__tg_channel c
        JOIN config__tg_bot_session_pool sp ON c.pk = sp.config__tg_channel_pk
        WHERE sp.pk = %s
    """, (session_pool_pk,))
    channels = cursor.fetchall()
    cursor.close()
    return channels


def get_last_db_post_id(conn, channel_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT tg_post_id
        FROM stat_post
        WHERE tg_channel_id = %s
        ORDER BY tg_post_id DESC
    """, (channel_id,))
    post = cursor.fetchone()
    cursor.close()
    return post[0] if post else post
