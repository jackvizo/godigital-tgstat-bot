def get_session_from_db(conn, phone_number):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pk, api_id, api_hash, session_str
        FROM config__tg_bot_session_pool
        WHERE phone_number = %s AND status = 'enabled'
    """, (phone_number,))
    result = cursor.fetchone()
    cursor.close()
    return result


def save_session_to_db(conn, phone_number, session_str, status, user_id, api_id, api_hash):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO config__tg_bot_session_pool (phone_number, session_str, status, api_id, api_hash, user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
    """, (phone_number, session_str, status, api_id, api_hash, user_id,))
    conn.commit()
    cursor.close()


def save_channel_to_db(conn, tg_channel_id, tg_channel_name):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO config__tg_channel (tg_channel_id, tg_channel_name)  
        VALUES (%s, %s) 
    """, (tg_channel_id, tg_channel_name))
    conn.commit()
    cursor.close()


def get_tracked_tg_channels(conn, session_pool_pk):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.tg_channel_id
        FROM user_tg_channel c
        JOIN tg_channel__session s ON c.pk = s.user_tg_channel_pk
        WHERE s.config__tg_bot_session_pool_pk = %s;
    """, (session_pool_pk,))
    channels = cursor.fetchall()
    cursor.close()
    return channels


def get_last_post_id_in_channel(conn, channel_id):
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
