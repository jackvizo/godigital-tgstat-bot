import asyncio
from datetime import timedelta, datetime
from prefect.client.schemas.schedules import RRuleSchedule

from time import time

from dotenv import dotenv_values
from telethon import functions, types, events
from telethon.sessions import StringSession
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetRepliesRequest

from colorama import Fore, Back, Style
from pprint import pprint

import config
from backend.asyncSQLDataService import asyncSQLDataService
from backend.run_collect import service_run
from models import Stat_post, Stat_reaction, Stat_user
from db_utils import get_session_from_db, get_db_channels, get_last_db_post_id
from tests.utils import get_tg_client
from prefect.deployments import run_deployment


async def authorize(phone):
    session_data = get_session_from_db(phone)
    if session_data:
        api_id, api_hash, session_str = session_data
        try:
            tg_client = TelegramClient(StringSession(session_str), api_id, api_hash)
        except Exception as e:
            print(f'Ошибка инициализации клиента по сессии из БД: {e}')
            print('Запуск сессии для тестового сервера!')
            tg_client = get_tg_client()

        if tg_client.is_user_authorized():
            # await tg_client.connect()
            res = True
        else:
            res = False
        print(f'Клиент (тел.: {phone}){"" if res else "не"} авторизован по сохраненной сессии!')
    else:
        raise ValueError("No active session found for the given phone number.")
    return tg_client


async def get_me(client):
    me = None
    try:
        me = await client.get_me()
    except Exception as e:
        print(f'Ошибка: {e}')
    if me:
        print('get_me(client):\n', me)
    return me


async def set_field_value(obj, value, field):
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
        # print(e)
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


async def get_posts(client, id, max_posts=1500, parent=None, hours=0):
    """
    :param client: TelegramClient
    :param id: id сущности (чат)
    :param max_posts: ограничение
    :param parent: предок
    :param hours: метка интервала запуска сервиса
    :return: возвращает id сущности (чат) и список постов - объектов БД сервиса
    """
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

    sql = asyncSQLDataService()
    await sql.init(config.db_name)  # , forceCreation=True

    posts = client.iter_messages(id, limit=limit, reverse=reverse, offset_id=offset_id - 1)

    async for p in posts:
        total_messages = posts.total  # устанавливается при получении первой итерации
        post = Stat_post()

        if cntr == 0:
            print(f'[{pp} {id}] START reading last {max_posts} messages. Total messages: {total_messages} '
                  f'in chat: "{str(p.chat.title)[:25]}"')

        if type(p) == types.MessageService or type(p) == types.Message:
            post.message = p.message
            post.tg_post_id = p.id
            if p.forwards:
                post.forwards = p.forwards
            if p.date:
                # print((p.date + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'))
                post.date_of_post = p.date.strftime(config.DATE_FORMAT)  # (p.date + timedelta(hours=3))
            if p.reactions:
                # pprint(p.reactions.to_dict())
                for res in p.reactions.results:
                    react = Stat_reaction()
                    react.reaction_count = res.count
                    post.total_reactions_count = (post.total_reactions_count or 0) + res.count
                    try:
                        react.reaction_emoticon = res.reaction.emoticon
                        react.reaction_emoticon_code = ord(res.reaction.emoticon)
                    except Exception as e:
                        print(f'Ошибка извлечения реакции: {e}')
                    react.tg_channel_id = id
                    react.tg_post_id = p.id

                    await sql.store_react(react)

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
                        post.comments_messages_count = await get_comments(client, id, post.tg_post_id, db=sql)  #
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
                    await set_field_value(post, p.views, field='views')

                # Просмотры с момента создания текущего поста + 1 и + 24 часа
                elif hours == 1:
                    await set_field_value(post, p.views, field='views_1h')
                    await set_field_value(post, p.reactions, field='reactions_1h')
                    await set_field_value(post, post.comments_messages_count, field='comments_messages_count_1h')

                elif hours == 24:
                    await set_field_value(post, p.views, field='views_24h')
                    await set_field_value(post, p.reactions, field='reactions_24h')
                    await set_field_value(post, post.comments_messages_count, field='comments_messages_count_24h')

            else:
                print(f'Объект "{type(p)}" (id={p.id}, chat: {p.chat_id}, '
                      f'user: {p.from_id.user_id}) не имеет свойства "views"')

            # pprint(p.to_dict())
            post.tg_channel_id = id
            post.link = 'https://t.me/c/' + str(id) + '/' + str(p.id)
            await sql.store_post(post)
            post_list.append(post)

            # print(f'[{pp}][{id}]', p.chat.title, p.id, str(p.message).replace('\n', '')[:35], ' [users:', obj.comments_users_count, ']  [mess:', obj.comments_messages_count, ']')

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

    await sql.close()
    print(f'[{pp} {id}] TOTAL time: {int(time() - start)}')
    return id, int(time() - start), post_list


async def get_comments(client, id, msg_id, limit=400, db=None):
    comments = None
    comments_channels_count = 0
    comments_users_count = 0
    comments_messages_count = 0
    total_comments = 0
    counter_comments = 0
    offset_id = 0

    while True:
        comments = await client(GetRepliesRequest(peer=id, msg_id=msg_id, offset_id=offset_id, offset_date=0,
                                                  add_offset=0, limit=limit, max_id=0, min_id=0, hash=0))
        total_comments = comments.count

        if comments:
            if comments.chats:
                comments_channels_count += len(comments.chats)
                for c in comments.chats:
                    pass
                    # print(type(c))
                    # pprint(c.to_dict())
            if hasattr(comments, 'users'):
                comments_users_count += len(comments.users)
                pass
                for u in comments.users:
                    user = Stat_user()
                    user.tg_channel_id = id
                    user.tg_user_id = u.id
                    user.username = u.username
                    user.first_name = u.first_name
                    user.last_name = u.last_name
                    user.phone = u.phone
                    user.premium = u.premium
                    user.scam = u.scam
                    user.verified = u.verified

                    if not db:
                        sql = asyncSQLDataService()
                        await sql.init(config.db_name)
                    else:
                        sql = db
                    await sql.store_user(user)
                    if not db:
                        await sql.close()

            if hasattr(comments, 'messages'):
                comments_messages_count += len(comments.messages)
                for m in comments.messages:
                    counter_comments += 1
                    offset_id = m.id
                    # print(total_comments-counter_comments, m.id, str(m.message).replace('\n', '')[:35])
                pass
                if counter_comments >= total_comments:
                    return comments_channels_count, comments_users_count, comments_messages_count


async def set_user_actions(client, channel):
    users = {}

    sql = asyncSQLDataService()
    await sql.init(config.db_name)

    res = client.iter_admin_log(channel, join=True, leave=True, invite=True, limit=1000)
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
            user.tg_channel_id = channel if type(channel) is int else None
            user.username = event.user.username
            user.first_name = event.user.first_name
            user.last_name = event.user.last_name
            user.phone = event.user.phone
            user.scam = event.user.scam
            user.premium = event.user.premium
            user.verified = event.user.verified

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

        await sql.store_user(user)

        if user.username in users:
            users[user.username].extend(actions)
        else:
            users.update({user.username: actions})

    await sql.close()
    return users


async def collect_data(client, channels, hours):
    """
    :param client: TelegramClient
    :param channels: list of channels
    :param hours: int --  период времени (часы) для учета статистики, 0 - первый запуск
    :return: statistic collection service for telegram-channels
    """
    print(f'\t---> Режим выполнения - hours mode: {hours}')
    async with client:
        for channel_id, channel_name in channels:
            try:
                id = await read_channels(client, channel_name)
            except Exception as e:
                id = await read_channels(client, channel_id)

            # id, link = await read_channels(client, 'Заметки')
            me = await get_me(client)
            res = await client.get_me(id)
            # print(f'About me: {me}')
            print(f'About client: {res}')
            # client.send_message(entity=79258661639, message='TEST')
            # await client.send_message(entity=1392284754, message='TEST')
            # client.run_until_disconnected()
            # exit()
            # id = -1002111052057
            # start = time()  # точка отсчета времени

            # Плановый запуск сервиса `tg-collect` на +1 час и +24 часа от даты создания нового поста
            # новый пост на сервере
            posts = await get_posts(client, id, max_posts=2500, hours=hours)
            # новый пост в БД (по параметру 'tg_post_id', не по 'timestamp')
            post_last_id = get_last_db_post_id(id)
            # Запуск задач через интервалы
            if post_last_id != posts[2][0].tg_post_id:
                await schedule_flow('tg-collect', datetime.strptime(posts[2][0].date_of_post, config.DATE_FORMAT))
            else:
                print('Сервис "tg_collect" не запланирован: новых постов не было.')

            # Подписки/отписки пользователей (сбор и запись в БД)
            users_actions = await set_user_actions(client, channel_id)
            print(f'Действия пользователей:\n{users_actions}')

            messages = client.iter_messages(
                id, reverse=True)

            async for item in messages:
                print(f'\n{item.date}: {item}')
                if item.media:
                    try:
                        print(item.media.document.mime_type)
                    except Exception as e:
                        print(f'obj.media.document.mime_type": {e}')

                if item.id == 5:
                    pass
                print(f'Message: {item.message}')
                print(f'Просмотры: {item.views}')


async def schedule_flow(service_name, date_start):
    new_start = datetime.now().replace(microsecond=0) if (datetime.now() - date_start) > timedelta(hours=1) else date_start
    # rrule_1 = new_start + timedelta(hours=1)
    # rrule_24 = new_start + timedelta(hours=24)
    # rrule_test = (new_start + timedelta(hours=1)) # .strftime(config.RRULE_FORMAT) # config.RRULE_TEST_SCHEDULE %

    print('Планирование запуска сервиса "tg_collect":')
    try:
        # await run_deployment(name="tg-collect/1_hour", scheduled_time=rrule_1, timeout=0,
        #                      parameters={'hours': 1})
        # await run_deployment(name="tg-collect/24_hour", scheduled_time=rrule_24, timeout=0,
        #                      parameters={'hours': 24})
        service_run(service_name, date_start)

        print(f'\t- запланирован запуск {service_name} на +1 час и +24 часа с даты {new_start.strftime("%")}')
    except Exception as e:
        print(f'Ошибка планировщика для "{service_name}": {e}')


async def collect_flow():
    conf = dotenv_values('.env')
    phone_number = conf['PHONE']

    clt = await authorize(phone_number)                                                 # сессия из БД
    if not clt:
        clt = TelegramClient(phone_number, int(conf['API_ID']), conf['API_HASH'])       # новая сессия
    clt.parse_mode = 'html'
    # await clt.start()

    for ch in get_db_channels():
        await collect_data(clt, (ch,), hours=0)


if __name__ == "__main__":
    asyncio.run(collect_flow())
