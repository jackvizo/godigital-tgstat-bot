from time import time

from colorama import Fore, Back, Style
from telethon import types
from telethon.sessions import StringSession
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetRepliesRequest

import globals
from backend.AsyncSQLDataService import AsyncSQLDataService
from backend.run_collect import service_run
from db_utils import get_session_from_db, get_last_db_post_id
from lib.utils import get_tg_client
from models import Stat_post, Stat_reaction, Stat_user


async def authorize(conn, phone):
    session_data = get_session_from_db(conn, phone)

    if session_data:
        session_pool_pk, api_id, api_hash, session_str = session_data
        tg_client = TelegramClient(session=StringSession(session_str), api_id=api_id,
                                   api_hash=api_hash,
                                   flood_sleep_threshold=30) if not globals.IS_TEST else get_tg_client()

        if tg_client.is_user_authorized():
            raise PermissionError('NOT_AUTHORIZED')
    else:
        raise PermissionError("NO_ACTIVE_SESSION_IN_POOL")

    return tg_client, session_pool_pk


def set_field_value(obj, value, field):
    if type(field) is str and hasattr(obj, field):
        setattr(obj, field, value)


async def read_channels(client, search_name):
    """
    :param client: TelegramClient
    :param search_name: точное или часть названия сущности либо её id
    :return: id сущности (чат, user, Channel) с именем search_name, на который подписан пользователь (client)
    """
    try:
        if type(int(search_name)) == int:
            search_name = str(search_name).replace('-100', '')
            id = int(search_name)
            try:
                item = await client.get_entity(id)
                return item.id
            except Exception as e:
                print(e)
                return None
    except ValueError as e:
        pass

    dialogs = await client.get_dialogs()
    for dial in dialogs:
        item = await client.get_entity(dial.id)
        try:
            username = item.username
        except Exception as e:
            print(f'Ошибка получения username для "{dial.title}": {e}')
            username = None

        suffix = f'{Back.GREEN}{Fore.BLACK} {dial.id} {dial.title} username: {username}, user: {dial.is_user}, ' \
                 f'канал: {dial.is_channel}, группа: {dial.is_group} {Style.RESET_ALL}'
        if dial.is_group:
            print('[group ]' + suffix)
        elif dial.is_channel:
            print('[chanel]' + suffix)
        else:
            print('[others]' + suffix)

        if (dial.title.lower() == search_name.lower()) or (search_name.lower() in dial.title.lower()):
            return dial.id
    return None


async def get_posts(tg_client: TelegramClient, channel_id: int, max_posts=10, hours=0):
    """
    :param sql: asyncSQLDataService
    :param tg_client: TelegramClient
    :param channel_id: id сущности (чат)
    :param max_posts: ограничение
    :param parent: предок
    :param hours: метка интервала запуска сервиса
    :return: возвращает id сущности (чат) и список постов - объектов БД сервиса
    """
    if channel_id is None:
        raise ValueError('NO_CHANNEL_ID')

    start = time()

    post_list = []
    react_list = []
    user_dict = {}

    offset_id = 1
    reverse = False
    posts_counter = 0
    maximum_messages = 0
    maximum_message_post = 0
    maximum_users = 0
    maximum_users_post = 0

    posts = tg_client.iter_messages(entity=channel_id, limit=max_posts, reverse=reverse, offset_id=offset_id - 1)

    async for p in posts:
        total_messages = posts.total  # устанавливается при получении первой итерации
        post = Stat_post()

        if posts_counter == 0:
            print(f'[{channel_id}] START reading last {max_posts} messages. Total messages: {total_messages} '
                  f'in chat: "{str(p.chat.title)[:25]}"')

        if type(p) == types.MessageService or type(p) == types.Message:
            post.message = p.message
            post.tg_post_id = p.id
            if p.forwards:
                post.forwards = p.forwards
            if p.date:
                # print((p.date + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'))
                post.date_of_post = p.date.strftime('%Y-%m-%d %H:%M:%S')  # (p.date + timedelta(hours=3))
            if p.TABLE_REACTIONS:
                # pprint(p.reactions.to_dict())
                for res in p.TABLE_REACTIONS.results:
                    react = Stat_reaction()
                    react.reaction_count = res.count
                    post.total_reactions_count = (post.total_reactions_count or 0) + res.count
                    try:
                        react.reaction_emoticon = res.reaction.emoticon
                        react.reaction_emoticon_code = ord(res.reaction.emoticon)
                    except Exception as e:
                        print(f'Ошибка извлечения реакции: {e}')
                    react.tg_channel_id = channel_id
                    react.tg_post_id = p.id

                    react_list.append(react)

                    # await sql.store_react(react)

            if p.grouped_id:
                pass
            if p.file:
                pass
            if (type(p.replies)) == types.MessageReplies:
                if p.replies.replies != 0:
                    post.comments_channels_count, post.comments_users_count, \
                    post.comments_messages_count \
                        = await get_comments(tg_client, channel_id, post.tg_post_id, user_dict)
                    if post.comments_messages_count > maximum_messages:
                        maximum_messages = post.comments_messages_count
                        maximum_message_post = post.tg_post_id
                        # print(f"[{pp}][{id}] maximum_messages: {maximum_messages} in obj: {obj.tg_post_id}")
                    if post.comments_users_count > maximum_users:
                        maximum_users = post.comments_messages_count
                        maximum_users_post = post.tg_post_id
                        # print(f"[{pp}][{id}] maximum_users: {maximum_users} in obj: {obj.tg_post_id}")

            if p.media:
                pass
                # print(type(p.media))
                if (type(p.media)) == types.MessageMediaDocument:
                    post.media = p.media.document.mime_type
                elif (type(p.media)) == types.MessageMediaPhoto:
                    post.media = 'PHOTO'

            if hasattr(p, 'views'):
                # Просмотры за всё время (hours = 0)
                if not hours:
                    set_field_value(post, p.views, field='views')

                # Просмотры с момента создания текущего поста + 1 и + 24 часа
                elif hours == 1:
                    set_field_value(post, p.views, field='views_1h')
                    set_field_value(post, p.TABLE_REACTIONS, field='reactions_1h')
                    set_field_value(post, post.comments_messages_count, field='comments_messages_count_1h')

                elif hours == 24:
                    set_field_value(post, p.views, field='views_24h')
                    set_field_value(post, p.TABLE_REACTIONS, field='reactions_24h')
                    set_field_value(post, post.comments_messages_count, field='comments_messages_count_24h')

            else:
                print(f'Объект "{type(p)}" (id={p.id}, chat: {p.chat_id}, '
                      f'user: {p.from_id.user_id}) не имеет свойства "views"')

            # pprint(p.to_dict())
            post.tg_channel_id = channel_id
            post.link = 'https://t.me/c/' + str(channel_id) + '/' + str(p.id)
            # await sql.store_post(post)
            post_list.append(post)

            # print(f'[{pp}][{id}]', p.chat.title, p.id, str(p.message).replace('\n', '')[:35], ' [users:', obj.comments_users_count, ']  [mess:', obj.comments_messages_count, ']')

        print(
            f'[{channel_id}] current post: {post.tg_post_id}, message: {posts_counter} of {total_messages}, running (sec): '
            f'{int(time() - start)}')

        posts_counter += 1
        if posts_counter >= max_posts:
            print(f'[{channel_id}] FINISH read {posts_counter} messages, TOTAL time: {int(time() - start)} sec, '
                  f'[max messages: {maximum_messages} in tg_post_id: {maximum_message_post}], '
                  f'[max users: {maximum_users} in {maximum_users_post}]')
            return channel_id, int(time() - start), post_list, react_list, user_dict

    print(f'[{channel_id}] TOTAL time: {int(time() - start)}')
    return channel_id, int(time() - start), post_list, react_list, user_dict


async def get_comments(tg_client: TelegramClient, channel_id: int, message_id: int, user_dict: dict, limit=400):
    comments_channels_count = 0
    comments_users_count = 0
    comments_messages_count = 0
    counter_comments = 0
    offset_id = 0
    infinite_loop_counter = 0

    while True:
        infinite_loop_counter += 1

        if infinite_loop_counter >= 5000:
            raise RuntimeError('GET_COMMENTS_STUCK_IN_INFINITE_LOOP')

        comments = await tg_client(
            GetRepliesRequest(peer=channel_id, msg_id=message_id, offset_id=offset_id, offset_date=0,
                              add_offset=0, limit=limit, max_id=0, min_id=0, hash=0))
        total_comments = comments.count

        if comments:
            if comments.chats:
                comments_channels_count += len(comments.chats)

            if hasattr(comments, 'users'):
                comments_users_count += len(comments.TABLE_USERS)
                pass
                for u in comments.TABLE_USERS:
                    user = Stat_user()
                    user.tg_channel_id = channel_id
                    user.tg_user_id = u.id
                    user.username = u.username
                    user.first_name = u.first_name
                    user.last_name = u.last_name
                    user.phone = u.phone
                    user.premium = u.premium
                    user.scam = u.scam
                    user.verified = u.verified

                    if user.username in user_dict:
                        user_dict[user.tg_user_id].extend(user)
                    else:
                        user_dict.update({user.tg_user_id: user})

                    # user_dict.append(user)

                    # if not db:
                    #     sql = asyncSQLDataService()
                    #     await sql.init(globals.DB_NAME)
                    # else:
                    #     sql = db
                    # await sql.store_user(user)
                    # if not db:
                    #     await sql.close()

            if hasattr(comments, 'messages'):
                comments_messages_count += len(comments.messages)
                for m in comments.messages:
                    counter_comments += 1
                    offset_id = m.id
                pass
                if counter_comments >= total_comments:
                    return comments_channels_count, comments_users_count, comments_messages_count


async def get_user_actions(sql: AsyncSQLDataService, tg_client: TelegramClient, channel_id: int):
    users = {}

    # sql = asyncSQLDataService()
    # await sql.init(globals.DB_NAME)

    res = tg_client.iter_admin_log(entity=channel_id, join=True, leave=True, invite=True, limit=1000)
    async for event in res:
        user = Stat_user()
        user.tg_user_id = event.user_id
        actions = []

        try:
            res = await sql.get_user_id(user)
        except Exception as e:
            res = False
            print(f'Ошибка получения объекта пользователя из БД: {e}')

        if res:
            user = res
        else:
            user.tg_channel_id = channel_id if type(channel_id) is int else None
            user.username = event.DB_USER.username
            user.first_name = event.DB_USER.first_name
            user.last_name = event.DB_USER.last_name
            user.phone = event.DB_USER.phone
            user.scam = event.DB_USER.scam
            user.premium = event.DB_USER.premium
            user.verified = event.DB_USER.verified

        if event.joined_by_invite:
            user.is_joined_by_link = True
            actions.append('joined_by_invite')
        if event.joined_invite:
            user.is_joined_by_link = True
            actions.append('joined_invite')
        if event.joined:
            user.joined_at = event.date
            actions.append('joined')
        if event.left:
            user.left_at = event.date
            actions.append('left')

        # await sql.store_user(user)

        if user.username in users:
            users[user.username].extend(actions)
        else:
            users.update({user.username: actions})

    # await sql.close()
    return users


async def collect_data(sql: AsyncSQLDataService, tg_client, channel, hours=0):
    """
    :param sql: asyncSQLDataService
    :param tg_client: TelegramClient
    :param channel: any
    :param hours: int --  период времени (часы) для учета статистики, 0 - первый запуск
    :return: statistic collection service for telegram-channels
    """
    async with tg_client:
        channel_id, channel_name = channel

        # Плановый запуск сервиса `tg-collect` на +1 час и +24 часа от даты создания нового поста
        # самый новый пост из БД по параметру 'tg_post_id' (не по 'timestamp')
        post_last_id = get_last_db_post_id(sql.connection, channel_id)

        posts = await get_posts(tg_client, channel_id, max_posts=5, hours=hours)

        if post_last_id != posts[2][0].tg_post_id:
            # сравнить с новым постом на сервере,

            try:
                # запуск запланированных задач
                service_run()
                print('Запуск сервиса "tg_collect": запуск...')
            except Exception as e:
                print(f'Ошибка планового запуска сервиса "tg_collect_flow": {e}')
        else:
            print('Сервис "tg_collect" не запланирован: новых постов не было.')

        # Подписки/отписки пользователей (сбор и запись в БД)
        await get_user_actions(tg_client, channel_id)

#
# async def collect_flow():
#     conf = dotenv_values('.env')
#     phone_number = conf['PHONE']
#
#     clt = await authorize(phone_number)  # сессия из БД
#     if not clt:
#         clt = TelegramClient(phone_number, conf['API_ID'], conf['API_HASH'])  # новая сессия
#     clt.parse_mode = 'html'
#     # await clt.start()
#
#     for ch in get_db_channels():
#         await collect_data(clt, (ch,), hours=0)
#
#
# if __name__ == "__main__":
#     asyncio.run(collect_flow())
