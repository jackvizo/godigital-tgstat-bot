from db.connection import create_db_connection
from db.models import Stat_post, Stat_reaction, Stat_user, Stat_post_info, Stat_channel
from globals import TABLE_POSTS, TABLE_REACTIONS, TABLE_USERS, TABLE_TG_SESSION_POOL, TABLE_POSTS_INFO, \
    TABLE_CHANNELS


class SyncSQLDataService(object):
    connection = None
    cursor = None

    def __init__(self):
        pass

    def init(self):
        self.connection = create_db_connection()
        self.open()

    def open(self):
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.close()

    def upsert_user(self, user: Stat_user) -> Stat_user:
        values = (
            user.joined_at,
            user.left_at,
            user.tg_user_id,
            user.tg_channel_id,
            user.first_name,
            user.last_name,
            user.username,
            user.phone,
            user.scam,
            user.premium,
            user.verified,
            user.is_joined_by_link,
            user.invite_link
        )
        self.cursor.execute(Constants.SQL_UPSERT_USER_TG, values)
        user.pk = self.cursor.fetchone()[0]

        return user

    def insert_post(self, post: Stat_post) -> Stat_post:
        values = (
            post.timestamp,
            post.date_of_post,
            post.tg_post_id,
            post.tg_channel_id,
            post.views,
            post.views_1h,
            post.view_24h,
            post.total_reactions_count,
            post.reactions_1h,
            post.reaction_24h,
            post.comments_users_count,
            post.comments_channels_count,
            post.comments_messages_count,
            post.comments_messages_count_1h,
            post.comments_messages_count_24h,
            post.forwards,
        )
        self.cursor.execute(Constants.SQL_INSERT_POST, values)
        post.pk = self.cursor.fetchone()[0]

        return post

    def upsert_post_info(self, post: Stat_post_info) -> Stat_post:
        values = (
            post.timestamp,
            post.date_of_post,
            post.tg_post_id,
            post.tg_channel_id,
            post.message,
            post.link,
            post.media,

            post.views,
            post.views_1h,
            post.view_24h,
            post.total_reactions_count,
            post.reactions_1h,
            post.reaction_24h,
            post.comments_users_count,
            post.comments_channels_count,
            post.comments_messages_count,
            post.comments_messages_count_1h,
            post.comments_messages_count_24h,
            post.forwards
        )
        self.cursor.execute(Constants.SQL_UPSERT_POST_INFO, values)
        post.pk = self.cursor.fetchone()[0]

        return post

    def upsert_reaction(self, react: Stat_reaction) -> Stat_reaction:
        values = (
            react.timestamp,
            react.tg_post_id,
            react.tg_channel_id,
            react.reaction_count,
            react.reaction_emoticon,
            react.reaction_emoticon_code,
        )
        self.cursor.execute(Constants.SQL_UPSERT_REACTION, values)
        react.pk = self.cursor.fetchone()[0]

        return react

    def get_users_with_active_phone_numbers(self):
        self.cursor.execute(Constants.SQL_GET_USERS_WITH_ACTIVE_PHONES)
        result = self.cursor.fetchall()
        self.cursor.close()

        return result

    def set_phone_number_banned(self, phone_number: str):
        self.cursor.execute(Constants.SQL_SET_PHONE_BANNED, (phone_number,))
        self.cursor.close()

    def get_tg_last_event_id(self, tg_channel_id: int):
        self.cursor.execute(Constants.SQL_GET_TG_LAST_EVENT_ID, (tg_channel_id,))
        tg_last_event_id = self.cursor.fetchone()
        self.cursor.close()

        print('kek', tg_last_event_id)

        return tg_last_event_id[0] if tg_last_event_id is not None else None

    def insert_channel(self, channel: Stat_channel):
        values = (channel.timestamp, channel.tg_channel_id, channel.tg_last_admin_log_event_id, channel.total_participants,)
        self.cursor.execute(Constants.SQL_INSERT_CHANNEL, values)


class Constants:
    SQL_UPSERT_USER_TG = f'''
        INSERT INTO {TABLE_USERS} (
            joined_at, left_at, tg_user_id, tg_channel_id, first_name,
            last_name, username, phone, scam, premium, verified, is_joined_by_link, invite_link
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tg_user_id, tg_channel_id) DO UPDATE SET
            joined_at = EXCLUDED.joined_at,
            left_at = EXCLUDED.left_at,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            username = EXCLUDED.username,
            phone = EXCLUDED.phone,
            scam = EXCLUDED.scam,
            premium = EXCLUDED.premium,
            verified = EXCLUDED.verified,
            is_joined_by_link = EXCLUDED.is_joined_by_link,
            invite_link = EXCLUDED.invite_link
        RETURNING pk
    '''

    SQL_INSERT_POST = f'''
        INSERT INTO {TABLE_POSTS} (
            timestamp, date_of_post, tg_post_id, tg_channel_id, views, views_1h, view_24h,
            total_reactions_count, reactions_1h, reaction_24h, comments_users_count,
            comments_channels_count, comments_messages_count, comments_messages_count_1h,
            comments_messages_count_24h, forwards
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING pk
    '''

    SQL_UPSERT_POST_INFO = f'''
        INSERT INTO {TABLE_POSTS_INFO} (
            timestamp, date_of_post, tg_post_id, tg_channel_id, message, link, media, 
            views, views_1h, view_24h,
            total_reactions_count, reactions_1h, reaction_24h, comments_users_count,
            comments_channels_count, comments_messages_count, comments_messages_count_1h,
            comments_messages_count_24h, forwards            
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tg_post_id, tg_channel_id) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            date_of_post = EXCLUDED.date_of_post,
            tg_post_id = EXCLUDED.tg_post_id,
            tg_channel_id = EXCLUDED.tg_channel_id,
            message = EXCLUDED.message,
            link = EXCLUDED.link,
            media = EXCLUDED.media,
            
            views = EXCLUDED.views,
            views_1h = EXCLUDED.views_1h,
            view_24h = EXCLUDED.view_24h,
            total_reactions_count = EXCLUDED.total_reactions_count,
            reactions_1h = EXCLUDED.reactions_1h,
            reaction_24h = EXCLUDED.reaction_24h,
            comments_users_count = EXCLUDED.comments_users_count,
            comments_channels_count = EXCLUDED.comments_channels_count,
            comments_messages_count = EXCLUDED.comments_messages_count,
            comments_messages_count_1h = EXCLUDED.comments_messages_count_1h,
            comments_messages_count_24h = EXCLUDED.comments_messages_count_24h,
            forwards = EXCLUDED.forwards
        RETURNING pk
    '''

    SQL_UPSERT_REACTION = f'''
        INSERT INTO {TABLE_REACTIONS} (
            timestamp, tg_post_id, tg_channel_id, reaction_count, reaction_emoticon, reaction_emoticon_code
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (tg_post_id, tg_channel_id, reaction_emoticon_code) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            tg_channel_id = EXCLUDED.tg_channel_id,
            reaction_count = EXCLUDED.reaction_count,
            reaction_emoticon = EXCLUDED.reaction_emoticon,
            reaction_emoticon_code = EXCLUDED.reaction_emoticon_code
        RETURNING pk
    '''

    SQL_GET_USERS_WITH_ACTIVE_PHONES = f'''
        SELECT 
            user_id,
            phone_number
        FROM 
            {TABLE_TG_SESSION_POOL} ctbsp
        WHERE 
            status = 'enabled'
            AND pk = (
                SELECT MIN(pk)
                FROM {TABLE_TG_SESSION_POOL} sub_ctbsp
                WHERE sub_ctbsp.user_id = ctbsp.user_id
                  AND sub_ctbsp.status = 'enabled'
            );
    '''

    SQL_SET_PHONE_BANNED = f'''
        UPDATE tg_bot_session_pool
        SET status = 'banned'
        WHERE phone_number = %s;
    '''

    SQL_GET_TG_LAST_EVENT_ID = f'''
        SELECT tg_last_admin_log_event_id FROM {TABLE_CHANNELS}
        WHERE tg_channel_id = %s
        ORDER BY pk DESC
        LIMIT 1
    '''

    SQL_INSERT_CHANNEL = f'''
        INSERT INTO {TABLE_CHANNELS} (
            timestamp, tg_channel_id, tg_last_admin_log_event_id, total_participants
        ) VALUES (%s, %s, %s, %s)
    '''
