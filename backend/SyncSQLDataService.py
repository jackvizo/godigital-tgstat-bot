from db_utils import create_db_connection
from globals import TABLE_POSTS, TABLE_REACTIONS, TABLE_USERS
from models import Stat_post, Stat_reaction, Stat_user


class SyncSQLDataService(object):
    connection = None
    cursor = None

    def __init__(self):
        pass

    def init(self):
        self.connection = create_db_connection()
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
        )
        self.cursor.execute(Constants.SQL_UPSERT_USER_TG, values)
        user.pk = self.cursor.fetchone()[0]

        return user

    def upsert_post(self, post: Stat_post) -> Stat_post:
        values = (
            post.tg_post_id,
            post.tg_channel_id,
            post.message,
            post.views,
            post.views_1h,
            post.views_24h,
            post.total_reactions_count,
            post.reactions_1h,
            post.reactions_24h,
            post.comments_users_count,
            post.comments_channels_count,
            post.comments_messages_count,
            post.comments_messages_count_1h,
            post.comments_messages_count_24h,
            post.link,
            post.media,
            post.forwards,
        )
        self.cursor.execute(Constants.SQL_UPSERT_POST, values)
        post.pk = self.cursor.fetchone()[0]

        return post

    def upsert_react(self, react: Stat_reaction) -> Stat_reaction:
        values = (
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


class Constants:
    SQL_UPSERT_USER_TG = f'''
        INSERT INTO {TABLE_USERS} (
            joined_at, left_at, tg_user_id, tg_channel_id, first_name,
            last_name, username, phone, scam, premium, verified, is_joined_by_link
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            is_joined_by_link = EXCLUDED.is_joined_by_link
        RETURNING pk
    '''

    SQL_SELECT_REACTIONS_BY_ID = f'''
        SELECT 
            pk, 
            tg_post_id, 
            tg_channel_id, 
            reaction_count, 
            reaction_emoticon, 
            reaction_emoticon_code 
        FROM 
            {TABLE_REACTIONS} 
        WHERE 
            tg_channel_id=%s 
            AND tg_post_id=%s 
            AND reaction_emoticon_code=%s
    '''

    SQL_UPSERT_POST = f'''
        INSERT INTO {TABLE_POSTS} (
            tg_post_id, tg_channel_id, message, views, views_1h, views_24h,
            total_reactions_count, reactions_1h, reactions_24h, comments_users_count,
            comments_channels_count, comments_messages_count, comments_messages_count_1h,
            comments_messages_count_24h, link, media, forwards
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tg_post_id) DO UPDATE SET
            tg_channel_id = EXCLUDED.tg_channel_id,
            message = EXCLUDED.message,
            views = EXCLUDED.views,
            views_1h = EXCLUDED.views_1h,
            views_24h = EXCLUDED.views_24h,
            total_reactions_count = EXCLUDED.total_reactions_count,
            reactions_1h = EXCLUDED.reactions_1h,
            reactions_24h = EXCLUDED.reactions_24h,
            comments_users_count = EXCLUDED.comments_users_count,
            comments_channels_count = EXCLUDED.comments_channels_count,
            comments_messages_count = EXCLUDED.comments_messages_count,
            comments_messages_count_1h = EXCLUDED.comments_messages_count_1h,
            comments_messages_count_24h = EXCLUDED.comments_messages_count_24h,
            link = EXCLUDED.link,
            media = EXCLUDED.media,
            forwards = EXCLUDED.forwards
        RETURNING pk
    '''

    SQL_UPSERT_REACTION = f'''
        INSERT INTO {TABLE_REACTIONS} (
            tg_post_id, tg_channel_id, reaction_count, reaction_emoticon, reaction_emoticon_code
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (tg_post_id) DO UPDATE SET
            tg_channel_id = EXCLUDED.tg_channel_id,
            reaction_count = EXCLUDED.reaction_count,
            reaction_emoticon = EXCLUDED.reaction_emoticon,
            reaction_emoticon_code = EXCLUDED.reaction_emoticon_code
        RETURNING pk
    '''

    SQL_GET_USERS_WITH_ACTIVE_PHONES = f'''
    WITH enabled_sessions AS (
        SELECT 
            u.id AS user_id,
            upn.phone_number,
            ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY upn.pk) AS rn
        FROM 
            user u
        JOIN 
            user_phone_number upn ON u.id = upn.user_id
        JOIN 
            config__tg_bot_session_pool ctbsp ON upn.phone_number = ctbsp.phone_number
        WHERE 
            ctbsp.status = 'enabled'
    )
    SELECT 
        user_id,
        phone_number
    FROM 
        enabled_sessions
    WHERE 
        rn = 1;
    '''
