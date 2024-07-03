from db_utils import create_db_connection
from globals import TABLE_POSTS, TABLE_REACTIONS, TABLE_USERS
from models import Stat_post, Stat_reaction, Stat_user


async def _dbrecord_to_post(record) -> Stat_post:
    # fixed from new scheme
    result = Stat_post()
    result.pk = record[0]
    # result.timestamp = record[1]
    result.tg_post_id = record[1]
    result.tg_channel_id = record[2]
    result.message = record[3]
    result.views = record[4]
    result.views_1h = record[5]
    result.views_24h = record[6]
    result.total_reactions_count = record[7]
    result.reactions_1h = record[8]
    result.reactions_24h = record[9]
    result.comments_users_count = record[10]
    result.comments_channels_count = record[11]
    result.comments_messages_count = record[12]
    result.comments_messages_count_1h = record[13]
    result.comments_messages_count_24h = record[14]
    result.link = record[15]
    result.media = record[16]
    result.forwards = record[17]
    return result


class AsyncSQLDataService(object):
    connection = None
    cursor = None

    def __init__(self):
        pass

    async def init(self):
        self.connection = create_db_connection()
        self.cursor = self.connection.cursor()
        self.cursor.connection.autocommit = True

    async def close(self):
        try:
            await self.connection.close()
        except TypeError:
            self.connection.close()

    async def upsert_user(self, user: Stat_user) -> Stat_user:
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
        self.connection.commit()
        user.pk = self.cursor.fetchone()[0]

        return user

    async def get_user_id(self, user: Stat_user) -> Stat_user or None:
        self.cursor.execute(Constants.SQL_SELECT_USER_TG_BY_ID, (user.tg_user_id,))  # await
        record = self.cursor.fetchone()  # await
        if record is None:
            return None
        result = await self._dbrecord_to_user(record)
        return result

    async def upsert_post(self, post: Stat_post) -> Stat_post:
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

    async def get_post_id(self, post: Stat_post) -> Stat_post or None:
        self.cursor.execute(Constants.SQL_SELECT_POST_BY_ID, (post.tg_channel_id, post.tg_post_id))  # , obj.views,
        record = self.cursor.fetchone()
        if record is None:
            return None
        result = await _dbrecord_to_post(record)
        return result

    async def upsert_react(self, react: Stat_reaction) -> Stat_reaction:
        values = (
            react.tg_post_id,
            react.tg_channel_id,
            react.reaction_count,
            react.reaction_emoticon,
            react.reaction_emoticon_code,
        )
        self.cursor.execute(Constants.SQL_UPSERT_REACTION, values)
        self.connection.commit()
        react.pk = self.cursor.fetchone()[0]

        return react

    @staticmethod
    async def _dbrecord_to_react(record) -> Stat_reaction:
        # fixed from new scheme
        result = Stat_reaction()
        result.pk = record[0]
        # result.timestamp = record[1]
        result.tg_post_id = record[1]
        result.tg_channel_id = record[2]
        result.reaction_count = record[3]
        result.reaction_emoticon = record[4]
        result.reaction_emoticon_code = record[5]
        return result

    @staticmethod
    async def _dbrecord_to_user(record) -> Stat_user:
        # fixed from new scheme
        result = Stat_user()
        result.pk = record[0]
        # result.timestamp = record[1]
        result.joined_at = record[1]
        result.left_at = record[2]
        result.tg_user_id = record[3]
        result.tg_channel_id = record[4]
        result.first_name = record[5]
        result.last_name = record[6]
        result.username = record[7]
        result.phone = record[8]
        result.scam = record[9]
        result.premium = record[10]
        result.verified = record[11]
        result.is_joined_by_link = record[12]
        return result


# edited Constants
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

    SQL_SELECT_USER_TG_BY_ID = f'''
        SELECT 
            pk, 
            joined_at, 
            left_at, 
            tg_user_id, 
            tg_channel_id, 
            first_name, 
            last_name, 
            username, 
            phone, 
            scam, 
            premium, 
            verified, 
            is_joined_by_link 
        FROM 
            {TABLE_USERS} 
        WHERE 
            tg_user_id=%s
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

    SQL_SELECT_POST_BY_ID = f'''
        SELECT 
            pk, 
            tg_post_id, 
            tg_channel_id, 
            message, 
            views, 
            views_1h, 
            views_24h, 
            total_reactions_count, 
            reactions_1h, 
            reactions_24h, 
            comments_users_count, 
            comments_channels_count, 
            comments_messages_count, 
            comments_messages_count_1h, 
            comments_messages_count_24h, 
            link, 
            media, 
            forwards 
        FROM 
            {TABLE_POSTS} 
        WHERE 
            tg_channel_id=%s 
            AND tg_post_id=%s
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
