from datetime import datetime, timedelta
from time import time

from telethon import types
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetRepliesRequest
from telethon.tl.types import PeerChannel

from db.SyncSQLDataService import SyncSQLDataService
from db.models import Stat_post, Stat_reaction, Stat_user
from db.queries import get_last_post_id_in_channel
from lib.scheduler import schedule_tg_collect_flow_run


def set_field_value(obj, value, field):
    if type(field) is str and hasattr(obj, field):
        setattr(obj, field, value)


async def get_posts(tg_client: TelegramClient, channel_id: int, user_dict_link: dict, current_time: datetime,
                    max_posts=10):
    """
    :param tg_client: TelegramClient
    :param channel_id: id сущности (чат)
    :user_dict_link: dict
    :param current_time: datetime
    :param max_posts: ограничение
    :return: возвращает id сущности (чат) и список постов - объектов БД сервиса
    """
    if channel_id is None:
        raise ValueError('NO_CHANNEL_ID')

    start = time()

    post_list = []
    react_list = []

    offset_id = 1
    reverse = False
    posts_counter = 0
    maximum_messages = 0
    maximum_message_post = 0
    maximum_users = 0
    maximum_users_post = 0

    tg_posts = tg_client.iter_messages(entity=PeerChannel(channel_id), limit=max_posts, reverse=reverse,
                                       offset_id=offset_id - 1)

    async for tg_post in tg_posts:
        stat_post = Stat_post()

        if posts_counter == 0:
            print(
                f'start reading last {max_posts} messages. Total messages (include MessageService): {tg_posts.total} '
                f'in chat: "{str(tg_post.chat.title)[:25]}"')

        if type(tg_post) == types.Message:
            stat_post.message = tg_post.message
            stat_post.tg_post_id = tg_post.id

            if tg_post.forwards is not None:
                stat_post.forwards = tg_post.forwards

            if tg_post.date is not None:
                stat_post.date_of_post = tg_post.date.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')

            if tg_post.reactions is not None:
                for res in tg_post.reactions.results:
                    stat_reaction = Stat_reaction()
                    stat_reaction.reaction_count = res.count
                    stat_post.total_reactions_count = (stat_post.total_reactions_count or 0) + res.count
                    try:
                        stat_reaction.reaction_emoticon = res.reaction.emoticon
                        stat_reaction.reaction_emoticon_code = ord(res.reaction.emoticon)
                    except Exception as e:
                        print(f'Ошибка извлечения реакции: {e}')
                    stat_reaction.tg_channel_id = channel_id
                    stat_reaction.tg_post_id = tg_post.id
                    stat_reaction.timestamp = datetime.now()

                    react_list.append(stat_reaction)

            # if tg_post.grouped_id:
            #     pass

            # if tg_post.file:
            #     pass

            if (type(tg_post.replies)) == types.MessageReplies:
                if tg_post.replies.replies != 0:
                    stat_post.comments_channels_count, stat_post.comments_users_count, \
                    stat_post.comments_messages_count \
                        = await get_comments(tg_client=tg_client, channel_id=channel_id,
                                             message_id=stat_post.tg_post_id, user_dict_link=user_dict_link)

                    if stat_post.comments_messages_count > maximum_messages:
                        maximum_messages = stat_post.comments_messages_count
                        maximum_message_post = stat_post.tg_post_id

                    if stat_post.comments_users_count > maximum_users:
                        maximum_users = stat_post.comments_messages_count
                        maximum_users_post = stat_post.tg_post_id

            if tg_post.media is not None:
                pass
                if (type(tg_post.media)) == types.MessageMediaDocument:
                    stat_post.media = tg_post.media.document.mime_type

                elif (type(tg_post.media)) == types.MessageMediaPhoto:
                    stat_post.media = 'PHOTO'

            if hasattr(tg_post, 'views'):
                # Просмотры за всё время
                set_field_value(stat_post, tg_post.views, field='views')

                # Вычисляем время жизни поста
                post_age = current_time - tg_post.date.replace(tzinfo=None)

                # Обновляем просмотры и реакции за 1 час, если время жизни поста не более 1 часа
                # Оставляем 5 минутный запас на задержки запуска скрипта
                if post_age <= timedelta(hours=1, minutes=5) or not stat_post.views_1h:
                    set_field_value(stat_post, tg_post.views, field='views_1h')
                    set_field_value(stat_post, len(tg_post.reactions.results), field='reactions_1h')
                    set_field_value(stat_post, stat_post.comments_messages_count, field='comments_messages_count_1h')

                # Обновляем просмотры и реакции за 24 часа, если время жизни поста не более 24 часов
                # Оставляем 5 минутный запас на задержкиу запуска скрипта
                if post_age <= timedelta(hours=24, minutes=5) or not stat_post.view_24h:
                    set_field_value(stat_post, tg_post.views, field='view_24h')
                    set_field_value(stat_post, len(tg_post.reactions.results), field='reaction_24h')
                    set_field_value(stat_post, stat_post.comments_messages_count, field='comments_messages_count_24h')

            else:
                print(f'Объект "{type(tg_post)}" (id={tg_post.id}, chat: {tg_post.chat_id}, '
                      f'user: {tg_post.from_id.user_id}) не имеет свойства "views"')

            stat_post.tg_channel_id = channel_id
            stat_post.link = 'https://t.me/c/' + str(channel_id) + '/' + str(tg_post.id)
            stat_post.timestamp = datetime.utcnow()
            post_list.append(stat_post)

        posts_counter += 1
        if posts_counter >= max_posts:
            print(f'finish read {posts_counter} messages, TOTAL time: {int(time() - start)} sec, '
                  f'[max messages: {maximum_messages} in tg_post_id: {maximum_message_post}], '
                  f'[max users: {maximum_users} in {maximum_users_post}]')
            return post_list, react_list

    print(f'total time: {int(time() - start)}sec')
    return post_list, react_list


async def get_comments(tg_client: TelegramClient, channel_id: int, message_id: int, user_dict_link: dict, limit=400):
    comments_channels_count = 0
    comments_users_count = 0
    comments_messages_count = 0
    offset_id = 0
    infinite_loop_counter = 0

    while True:
        infinite_loop_counter += 1

        if infinite_loop_counter >= 5000:
            raise RuntimeError('GET_COMMENTS_STUCK_IN_INFINITE_LOOP')

        comments = await tg_client(
            GetRepliesRequest(peer=channel_id, msg_id=message_id, offset_id=offset_id, offset_date=None,
                              add_offset=0, limit=limit, max_id=0, min_id=0, hash=0))
        total_comments = comments.count

        if comments:
            if hasattr(comments, 'chats'):
                comments_channels_count += len(comments.chats)

            if hasattr(comments, 'users'):
                comments_users_count += len(comments.users)
                for u in comments.users:
                    user = user_dict_link[u.id] if u.id in user_dict_link else Stat_user()

                    user.tg_channel_id = id
                    user.tg_user_id = u.id
                    user.username = u.username
                    user.first_name = u.first_name
                    user.last_name = u.last_name
                    user.phone = u.phone
                    user.premium = u.premium
                    user.scam = u.scam
                    user.verified = u.verified

                    user_dict_link[u.id] = user

            if hasattr(comments, 'messages'):
                comments_messages_count += len(comments.messages)
                counter_comments = len(comments.messages)
                offset_id = comments.messages[-1].id if comments.messages else None

                if counter_comments >= total_comments:
                    return comments_channels_count, comments_users_count, comments_messages_count


async def get_user_actions(tg_client: TelegramClient, channel_id: int, user_dict_link: dict):
    res = tg_client.iter_admin_log(entity=PeerChannel(channel_id), join=True, leave=True, invite=True, limit=1000)
    async for event in res:
        user = user_dict_link[event.user_id] if event.user_id in user_dict_link else Stat_user()

        user.tg_user_id = event.user_id
        user.tg_channel_id = channel_id
        user.username = event.user.username
        user.first_name = event.user.first_name
        user.last_name = event.user.last_name
        user.phone = event.user.phone
        user.scam = event.user.scam
        user.premium = event.user.premium
        user.verified = event.user.verified

        if event.joined_by_invite or event.joined_invite:
            user.is_joined_by_link = True
            user.joined_at = event.date.replace(tzinfo=None)
            user.invite_link = event.action.invite.link

        if event.joined:
            user.joined_at = event.date.replace(tzinfo=None)

        if event.left:
            user.left_at = event.date.replace(tzinfo=None)

        user_dict_link.update({event.user_id: user})


async def collect_channel(tg_client, channel_id):
    """
    :param tg_client: TelegramClient
    :param channel_id: int
    :return: statistic collection service for telegram-channels
    """
    current_time = datetime.utcnow()

    async with tg_client:
        user_dict = {}

        post_list, react_list = await get_posts(tg_client=tg_client, channel_id=channel_id,
                                                current_time=current_time, user_dict_link=user_dict, max_posts=10)

        await get_user_actions(tg_client=tg_client, channel_id=channel_id, user_dict_link=user_dict)

        return user_dict, post_list, react_list


def store_channel(sql: SyncSQLDataService, user_dict, post_list, react_list):
    try:
        for user in user_dict.values():
            sql.upsert_user(user)

        for post in post_list:
            sql.insert_post(post)

        for react in react_list:
            sql.upsert_reaction(react)

        sql.connection.commit()
    except Exception as e:
        sql.connection.rollback()
        raise e
    finally:
        sql.cursor.close()


def schedule_flow_run(sql: SyncSQLDataService, post_list: list, channel_id: int, phone_number: str):
    post_last_id = get_last_post_id_in_channel(sql.connection, channel_id)
    shall_schedule_flow_run = len(post_list) > 0 and post_last_id != post_list[0].tg_post_id

    if shall_schedule_flow_run:
        schedule_tg_collect_flow_run(phone_number, channel_id)
