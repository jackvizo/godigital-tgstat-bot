import asyncio

from datetime import timedelta, datetime
from time import time

from telethon import functions, types, events
from telethon.sessions import StringSession
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetRepliesRequest
from pprint import pprint

import config
from backend.asyncSQLDataService import asyncSQLDataService
from models import Stat_post, Stat_reaction, Stat_user
from db_utils import get_session_from_db, get_bd_channels
from test.utils import get_test_client


async def task_authorize(phone):
    session_data = get_session_from_db(phone)
    if session_data:
        api_id, api_hash, session_str = session_data

        try:
            tg_client = TelegramClient(StringSession(session_str), api_id, api_hash)
        except Exception as e:
            print(f'Ошибка инициализации клиента: {e}')
            tg_client = get_test_client()

        if tg_client.is_user_authorized():
            await tg_client.connect()
            res = True
        else:
            res = False
        print(f'Клиент (тел.: {phone}) {"" if res else "не "}авторизован по сохраненной сессии!')
    else:
        raise ValueError("No active session found for the given phone number.")
    return tg_client


async def read_channels(client, search_name):
    try:
        if type(int(search_name)) == int:
            pass
            search_name = str(search_name).replace('-100', '')
            id = int(search_name)
            # print(id)
            try:
                item = await client.get_entity(id)
                # pprint(item)
                return item.id
            except Exception as e:
                print(e)
                return None
    except ValueError as e:
        pass
        # print(e)
    dialogs = await client.get_dialogs()
    for dial in dialogs:
        item = await client.get_entity(dial.id)
        try:
            username = item.username
        except Exception as e:
            print(f'Ошибка получения username для "{dial.title}": {e}')
            username = None

        # if dial.is_group:
        #     print('[group ]' + Back.GREEN + Fore.BLACK, dial.id,  dial.title, username, dial.is_user, dial.is_channel, dial.is_group, Style.RESET_ALL)
        # elif dial.is_channel:
        #     print('[chanel]' + Back.RED + Fore.BLACK, dial.id,  dial.title, username, dial.is_user, dial.is_channel, dial.is_group, Style.RESET_ALL)
        # else:
        #     print('[others]' + Back.YELLOW + Fore.BLACK, dial.id,  dial.title, username, dial.is_user, dial.is_channel, dial.is_group, Style.RESET_ALL)

        if (dial.title.lower() == search_name.lower()) or (search_name.lower() in dial.title.lower()):
            return dial.id
    return None


async def get_comments(client, id, msg_id, max=300, sql=None):
    comments = None
    comments_channels_count = 0
    comments_users_count = 0
    comments_messages_count = 0
    total_comments = 0
    counter_comments = 0
    offset_id = 0

    while True:
        pass
        comments = await client(GetRepliesRequest(peer=id, msg_id=msg_id, offset_id=offset_id, offset_date=0,
                                                  add_offset=0, limit=400, max_id=0, min_id=0, hash=0))
        # comments = await client(GetRepliesRequest(peer=id, msg_id=42689, offset_id=offset_id, offset_date=0,
        #                                           add_offset=0, limit=1400, max_id=0, min_id=0, hash=0))
        total_comments = comments.count

        if comments:
            if comments.chats:
                comments_channels_count = comments_channels_count + len(comments.chats)
                for c in comments.chats:
                    pass
                    # print(type(c))
                    # pprint(c.to_dict())
            if hasattr(comments, 'users'):
                comments_users_count = comments_users_count + len(comments.users)
                pass
                for u in comments.users:
                    user = Stat_post()
                    user.tg_user_id = u.id
                    user.username = u.username
                    user.first_name = u.first_name
                    user.last_name = u.last_name
                    user.phone = u.phone
                    user.premium = u.premium
                    user.scam = u.scam
                    user.verified = u.verified

                    # TODO: 1. Добавить параметры из tg-объекта
                    user.is_joined_by_link = True # if else id False
                    #joined_at = u.j
                    #left_at = u.l
                    #tg_channel_id = u.t
                    # end todo 1

                    await sql.asyncSQLDataService
                    await sql.store_user(user)
                    await sql.close()

            if hasattr(comments, 'messages'):
                comments_messages_count = comments_messages_count + len(comments.messages)
                for m in comments.messages:
                    counter_comments += 1
                    offset_id = m.id
                    # print(total_comments-counter_comments, m.id, str(m.message).replace('\n', '')[:35])
                pass
                if counter_comments >= total_comments:
                    return comments_channels_count, comments_users_count, comments_messages_count


async def get_posts(client, id, max_posts=1500, parent=None):
    if id is None:
        return False
    # posts = client.iter_messages(id, reverse=True, offset_id=0)
    pp = parent

    print(f'[{pp} {id}] start work:')
    start = time()
    post_list = []
    limit = 100
    offset_id = 1
    reverse = False
    cntr = 0
    maximum_messages = 0
    maximum_message_post = 0
    maximum_users = 0
    maximum_users_post = 0

    while offset_id:
        pass
        posts = client.iter_messages(id, limit=limit, reverse=reverse, offset_id=offset_id-1)
        sql = asyncSQLDataService()

        async for p in posts:
            post = Stat_post()
            total_messages = posts.total

            if type(p) == types.MessageService:
                pass
            elif type(p) == types.Message:

                if cntr == 0:
                    print(f'[{pp} {id}] START reading last {max_posts} messages. Total message {total_messages} '
                          f'in chat: {str(p.chat.title)[:25]} ')

                post.message = p.message
                post.tg_post_id = p.id
                if p.forwards:
                    post.forwards = p.forwards
                if p.date:
                    # print((p.date + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'))
                    post.date_of_post = (p.date + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
                if p.reactions:
                    # pprint(p.reactions.to_dict())
                    for res in p.reactions.results:
                        react = Stat_reaction()
                        react.reaction_count = res.count
                        post.total_reactions_count = post.total_reactions_count + res.count
                        try:
                            react.reaction_emoticon = res.reaction.emoticon

                            react.reaction_emoticon_code = ord(res.reaction.emoticon)
                        except Exception as e:
                            print(f'Ошибка извлечения реакции: {e}')
                        react.tg_channel_id = id
                        react.tg_post_id = p.id
                        await sql.init(config.db_name)  # , forceCreation=True
                        await sql.store_react(react)
                        await sql.close()

                if p.grouped_id:
                    pass
                if p.file:
                    pass
                if (type(p.replies)) == types.MessageReplies:
                    if p.id == 457:
                        pass
                    if p.replies.replies != 0:
                        pass
                        post.comments_channels_count, post.comments_users_count, \
                            post.comments_messages_count = await get_comments(client, id, post.tg_post_id, sql=sql)
                        if post.comments_messages_count > maximum_messages:
                            maximum_messages = post.comments_messages_count
                            maximum_message_post = post.tg_post_id
                            # print(f"[{pp}][{id}] maximum_messages: {maximum_messages} in post: {post.tg_post_id}")
                        if post.comments_users_count > maximum_users:
                            maximum_users = post.comments_messages_count
                            maximum_users_post = post.tg_post_id
                            # print(f"[{pp}][{id}] maximum_users: {maximum_users} in post: {post.tg_post_id}")

                if p.media:
                    pass
                    # print(type(p.media))
                    if (type(p.media)) == types.MessageMediaDocument:
                        post.media = p.media.document.mime_type
                    elif (type(p.media)) == types.MessageMediaPhoto:
                        post.media = 'PHOTO'

                # просмотры за всё время
                if p.views:
                    post.views = p.views
                else:
                    post.views = None

                # pprint(p.to_dict())
                post.tg_channel_id = id
                post.timestamp = datetime.today().replace(microsecond=0)
                post.link = 'https://t.me/c/' + str(id) + '/' + str(p.id)
                await sql.init(config.db_name)  # , forceCreation=True
                await sql.store_post(post)
                await sql.close()
                post_list.append(post)

                # print(f'[{pp}][{id}]', p.chat.title, p.id, str(p.message).replace('\n', '')[:35], ' [users:', post.comments_users_count, ']  [mess:', post.comments_messages_count, ']')

            if cntr % 25 == 0:
                print(f'[{pp} {id}] curent post: {post.tg_post_id}, mess: {cntr} from {total_messages}, running (sec): '
                      f'{int(time() - start)}')

            cntr += 1
            if cntr >= max_posts:
                print(f'[{pp} {id}] FINISH read {cntr} messages, TOTAL time: {int(time() - start)} sec, '
                      f'[max messages: {maximum_messages} in tg_post_id: {maximum_message_post}], '
                      f'[max users: {maximum_users} in {maximum_users_post}]')
                return id, int(time() - start), post_list
            else:
                pass

        offset_id = post.tg_post_id
        # print(f'offset: {offset_id} total: {total_messages} remain:{message_remain}')
    print(f'[{pp} {id}] TOTAL time: {int(time() - start)}')
    return id, int(time() - start), post_list


async def tg_collect_flow():
    # groups = ['test_channel_analytics']
    # api_id = clientAPI.api_id
    # api_hash = clientAPI.api_hash
    # phone = clientAPI.api_phone

    # phone_number = config.phone_number
    phone_number = config.phone_test_number

    # client = TelegramClient(phone, api_id, api_hash)
    client = await task_authorize(phone_number)

    client.parse_mode = 'html'

    # await client.start()

    async with client:
        channels = get_bd_channels()
        for channel_id, channel_name in channels:
            # id = await read_channels(client, 'https://t.me/cianoid_parser')
            # id = await read_channels(client, 'RIA/Sputnik')
            id = await read_channels(client, channel_name)


            # id, link = await read_channels(client, 'Заметки')
            res = await client.get_me(id)
            # client.send_message(entity=79258661639, message='TEST')
            # await client.send_message(entity=1392284754, message='TEST')
            # client.run_until_disconnected()
            # exit()

            # id = -1002111052057
            # start = time()  # точка отсчета времени
            posts = await get_posts(client, id, max_posts=30)
            print(type(posts))
            # pprint(posts.to_dict())
            items = client.get_dialogs(id)
            # write_comments(id, 'Тест1')

            dp = client.get_entity(id)
            # end = time() - start  # собственно время работы программы
            # print(id, end)  # вывод времени
            # if dp.title: print(dp.title)
            messages = client.iter_messages(
                id, reverse=True)

            async for item in messages:
                print(item)
                print(item.date)
                if item.media:
                    try:
                        print(item.media.document.mime_type)
                    except Exception as e:
                        print(f'Ошибка "obj.media.document.mime_type": {e}')

                if item.id == 5:
                    pass
                print(item.message)
                print(f'Просмотры (всего): {item.views}')

        client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(tg_collect_flow())
