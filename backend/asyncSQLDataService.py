from time import sleep

from config import db_name, posts, reactions, users
from db_utils import get_db_connection
from models import Stat_post, Stat_reaction, Stat_user

from typing import Any
import sys
import aiosqlite
import asyncio
from datetime import datetime, timedelta


class asyncSQLDataService(object):

    connection = None
    cursor = None

    def __init__(self):
        pass

    async def init(self, databaseName=db_name, forceCreation=False):
        sys.stdout.reconfigure(encoding='utf-8')
        # self.connection = await aiosqlite.connect(database=databaseName, check_same_thread=False)
        self.connection = get_db_connection()       # await
        self.cursor = self.connection.cursor()      # await # psycopg2.extensions can't be used in 'await' expression
        self.cursor.connection.autocommit = True
        if forceCreation:
            await self._configure_table_posts()
            await self._configure_table_reactions()
            await self._configure_table_user()

    async def close(self):
        # print('close connection', self.connection)
        try:
            await self.connection.close()
        except TypeError:
            self.connection.close()

    async def store_user(self, user: Stat_user) -> Stat_user:
        res = await self.get_user_id(user)
        if res == None:
            return await self._insert_user(user)
        else:
            user.pk = res.pk
            return await self._update_user(user)

    async def get_user_id(self, user: Stat_user) -> Stat_user:
        self.cursor.execute(Constants.SQL_SELECT_USER_TG_BY_ID, (user.tg_user_id,))     # await
        record = self.cursor.fetchone()                                                 # await
        if record == None:
            return None
        result = await self._dbrecord_to_user(record)
        return result

    async def store_post(self, post: Stat_post) -> Stat_post:
        res = await self.get_post_id(post)
        if res == None:
            return await self._insert_post(post)
        else:
            post.pk = res.pk
            return await self._update_post(post)

    async def get_post_id(self, post: Stat_post) -> Stat_post:
        self.cursor.execute(Constants.SQL_SELECT_POST_BY_ID, (post.tg_channel_id, post.tg_post_id))     # , obj.views,
        record = self.cursor.fetchone()
        if record == None:
            return None
        result = await self._dbrecord_to_post(record)
        return result

    async def get_all_post(self) -> Stat_post:
        await self.cursor.execute(f"SELECT * FROM {posts}")
        record = await self.cursor.fetchall()
        if record == None:
            return None
        rec_list = []
        for rec in record:
            row = []
            for r in rec:
                row.append(r)
            rec_list.append(row)
        return rec_list

    async def get_all_react(self) -> Stat_reaction:
        await self.cursor.execute(f"SELECT * FROM {reactions}")
        record = await self.cursor.fetchall()
        if record == None:
            return None
        rec_list = []
        for rec in record:
            row = []
            for r in rec:
                row.append(r)
            rec_list.append(row)
        return rec_list

    async def get_react_id(self, react: Stat_reaction) -> Stat_reaction:
        self.cursor.execute(Constants.SQL_SELECT_REACTIONS_BY_ID,
                            (react.tg_channel_id, react.tg_post_id, react.reaction_emoticon_code))
        record = self.cursor.fetchone()

        if record == None:
            return None
        result = await self._dbrecord_to_react(record)
        return result

    async def _dbrecord_to_react(self, record) -> Stat_reaction:
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

    async def store_react(self, react: Stat_reaction) -> Stat_reaction:
        # fixed from new scheme
        res = await self.get_react_id(react)     #
        if not hasattr(res, 'pk'):  # and res.id == None:
            await self._insert_react(react)
        else:
            react.pk = res.pk
            await self._update_react(react)

    async def _insert_react(self, react: Stat_reaction) -> Stat_reaction:
        values = (  # fixed from new scheme
            # react.timestamp,
            react.tg_post_id,
            react.tg_channel_id,
            react.reaction_count,
            react.reaction_emoticon,
            react.reaction_emoticon_code,
        )
        self.cursor.execute(Constants.SQL_INSERT_REACTIONS, values)    # await

        # react.pk = self.cursor.lastrowid      # not for Postgres
        react.pk = self.cursor.fetchone()[0]
        # await self.cursor.connection.commit()
        return react

    async def _update_react(self, react: Stat_reaction) -> Stat_reaction:
        values = (  # fixed from new scheme
            react.reaction_count,
            react.reaction_emoticon,
            react.reaction_emoticon_code,
            # where
            react.pk,
            # react.timestamp,
            # react.tg_post_id,
            # react.tg_channel_id,
            # react.reaction_emoticon_code,
        )
        res = self.cursor.execute(Constants.SQL_UPDATE_REACTIONS, values)

        # print('rowcount= ', res.rowcount)
        # print('lastrow_id= ', res.fetchone()[0]  # lastrowid)
        # await self.cursor.connection.commit()
        return react

    async def _dbrecord_to_user(self, record) -> Stat_user:
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

    async def _dbrecord_to_post(self, record) -> Stat_post:
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

    async def _insert_user(self, user: Stat_user) -> Stat_user:
        values = (  # fixed from new scheme
            # user.timestamp,
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
            user.is_joined_by_link
        )
        self.cursor.execute(Constants.SQL_INSERT_USER_TG, values)
        # user.id = self.cursor.lastrowid       # not for Postgres
        user.id = self.cursor.fetchone()[0]
        return user

    async def _update_user(self, user: Stat_user) -> Stat_user:
        values = (  # fixed from new scheme
            # user.pk,
            # user.timestamp,
            user.joined_at,
            user.left_at,
            user.tg_channel_id,
            user.first_name,
            user.last_name,
            user.username,
            user.phone,
            user.scam,
            user.premium,
            user.verified,
            user.is_joined_by_link,
            # where
            user.tg_user_id,
        )
        self.cursor.execute(Constants.SQL_UPDATE_USER_TG, values)
        return user

    async def _insert_post(self, post: Stat_post) -> Stat_post:
        values = (  # fixed from new scheme
            # obj.timestamp,
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
        self.cursor.execute(Constants.SQL_INSERT_POSTS, values)
        # obj.pk = self.cursor.lastrowid     # not for Postgres
        post.pk = self.cursor.fetchone()[0]
        # await self.cursor.connection.commit()
        return post

    async def _update_post(self, post: Stat_post) -> Stat_post:
        values = (  # fixed from new scheme
                # obj.timestamp,
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
                # where
                # obj.tg_post_id,
                # obj.tg_channel_id,
                post.pk,
            )
        self.cursor.execute(Constants.SQL_UPDATE_POSTS, values)
        return post
    # async def comm(self):

    async def _configure_table_posts(self):
        await self.cursor.execute(Constants.SQL_CREATE_TABLE_POSTS)
        await self.cursor.execute(Constants.SQL_CREATE_INDEX_POSTS_ID)

    async def _configure_table_user(self):
        await self.cursor.execute(Constants.SQL_CREATE_TABLE_USER_TG)
        await self.cursor.execute(Constants.SQL_CREATE_INDEX_USER_TG)

    async def _configure_table_reactions(self):
        await self.cursor.execute(Constants.SQL_CREATE_TABLE_REACTIONS)
        await self.cursor.execute(Constants.SQL_CREATE_INDEX_REACT_ID)


# edited Constants
class Constants:
    SQL_CREATE_TABLE_USER_TG = f'''
        CREATE TABLE IF NOT EXISTS {users} (
        pk INTEGER PRIMARY KEY autoincrement, 
        timestamp DATETIME,
        joined_at DATETIME,
        left_at DATETIME,
        tg_user_id BIGINT,
        tg_channel_id BIGINT,       
        firstName varchar(255),
        lastName varchar(255),
        username varchar(255),
        phone varchar(255),
        scam BOOL,
        premium BOOL,
        verified BOOL,
        is_joined_by_link BOOL
        );
    '''
    SQL_CREATE_INDEX_USER_TG = 'CREATE INDEX IF NOT EXISTS idx_user_id ON stat_user (tg_user_id)'
    SQL_INSERT_USER_TG = f'INSERT INTO {users} (joined_at, left_at, tg_user_id, tg_channel_id, first_name,' \
                         'last_name, username, phone, scam, premium, verified, is_joined_by_link) ' \
                         'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING pk'
    SQL_UPDATE_USER_TG = f'UPDATE {users} SET joined_at=%s, left_at=%s, tg_channel_id=%s, first_name=%s, last_name=%s, ' \
                         f'username=%s, phone=%s, scam=%s, premium=%s, verified=%s, is_joined_by_link=%s ' \
                         'WHERE tg_user_id=%s'
    SQL_SELECT_USER_TG_BY_ID = 'SELECT pk, joined_at, left_at, tg_user_id, tg_channel_id, first_name, ' \
                               'last_name, username, phone, scam, premium, verified, is_joined_by_link ' \
                               'FROM stat_user WHERE tg_user_id=%s'

    # Consumption
    SQL_CREATE_TABLE_POSTS = f'''
        CREATE TABLE IF NOT EXISTS {posts} (
        pk INTEGER PRIMARY KEY autoincrement,
        timestamp DATETIME, 
        tg_post_id BIGINT,
        tg_channel_id BIGINT,                       
        message TEXT,     
        views INTEGER,
        views_1h INTEGER,
        views_24h INTEGER,        
        total_reactions_count INTEGER,
        reactions_1h INTEGER,
        reactions_24h INTEGER,
        comments_users_count INTEGER,
        comments_channels_count INTEGER,        
        comments_messages_count INTEGER,
        comments_messages_count_1h INTEGER,
        comments_messages_count_24h INTEGER,
        link TEXT,
        media TEXT,
        forwards INT
        );
    '''
    SQL_CREATE_INDEX_POSTS_ID = 'CREATE INDEX IF NOT EXISTS idx_post_id ON POSTS (tg_channel_id, tg_post_id)'
    SQL_INSERT_POSTS = f'INSERT INTO {posts} (tg_post_id, tg_channel_id, message, views, views_1h, views_24h,' \
                       'total_reactions_count, reactions_1h, reactions_24h, comments_users_count, comments_channels_count,' \
                       'comments_messages_count, comments_messages_count_1h, comments_messages_count_24h, link, media, forwards)' \
                       ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING pk'
    SQL_UPDATE_POSTS = f'UPDATE {posts} SET tg_post_id=%s, tg_channel_id=%s, message=%s, views=%s, views_1h=%s,' \
                       'views_24h=%s, total_reactions_count=%s, reactions_1h=%s, reactions_24h=%s, comments_users_count=%s,' \
                       'comments_channels_count=%s, comments_messages_count=%s, comments_messages_count_1h=%s,' \
                       'comments_messages_count_24h=%s, link=%s, media=%s, forwards=%s ' \
                       'WHERE pk=%s'    # tg_channel_id=%s and tg_post_id=%s and
    SQL_SELECT_POST_BY_ID = 'SELECT pk, tg_post_id, tg_channel_id, message, views, views_1h, views_24h, ' \
                            'total_reactions_count, reactions_1h, reactions_24h, comments_users_count, comments_channels_count, ' \
                            'comments_messages_count, comments_messages_count_1h, comments_messages_count_24h, link, media, forwards ' \
                            f'FROM {posts} WHERE tg_channel_id=%s and tg_post_id=%s'    # and views=%s

    SQL_CREATE_TABLE_REACTIONS = f'''
        CREATE TABLE IF NOT EXISTS {reactions} (
        pk INTEGER PRIMARY KEY autoincrement,        
        timestamp DATETIME
        tg_post_id BIGINT,
        tg_channel_id BIGINT,                       
        reaction_count INTEGER,
        reaction_emoticon varchar(5),
        reaction_emoticon_code INTEGER,
        );
    '''
    SQL_CREATE_INDEX_REACT_ID = f'CREATE INDEX IF NOT EXISTS idx_react_id ON {reactions} (tg_channel_id, tg_post_id)'
    SQL_INSERT_REACTIONS = f'INSERT INTO {reactions} (tg_post_id, tg_channel_id, reaction_count, reaction_emoticon,' \
                           'reaction_emoticon_code) VALUES (%s, %s, %s, %s, %s) RETURNING pk'
    SQL_UPDATE_REACTIONS = f'UPDATE {reactions} SET reaction_count=%s , reaction_emoticon=%s, reaction_emoticon_code=%s ' \
                           'WHERE pk=%s' # tg_channel_id=%s and tg_post_id=%s and reaction_count!=%s and reaction_emoticon_code=%s and
    SQL_SELECT_REACTIONS_BY_ID = f'SELECT pk, tg_post_id, tg_channel_id, reaction_count, reaction_emoticon, reaction_emoticon_code ' \
                                 f'FROM {reactions} WHERE tg_channel_id=%s and tg_post_id=%s and reaction_emoticon_code=%s'


# async def run_async():
#     loop = asyncio.get_event_loop()
#     res = asyncio.create_task(simple())
#     await res
#
#     # return await loop.run_in_executor(None, lambda: func())
#
#
# def simple():
#     for i in range(3):
#         print('simple')
#         sleep(1)
#

# async def main() -> None:
#     db = asyncSQLDataService()
#     await db.init("posts.db", forceCreation=True)
#     # print(await run_async())
#     # fl_DS = asyncSQLDataService()
#     # await fl_DS.init("full_links.db", forceCreation=True)
#     # print('goon')
#     # sleep(1)
#     # log_data = await fl_DS.get_all_full_link()
#
#     exit()
#     # fl = FullLinks()
#     # fl.full_link = '1324'
#     # fl.date = '12.31.00'
#     # fl.user_id = '123456789'
#     # sqlDataService.store_full_link(fl)
# if __name__ == "__main__":
#     asyncio.run(main())
