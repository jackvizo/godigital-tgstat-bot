import asyncio
from datetime import timedelta, datetime
from time import time

from prefect.blocks.system import String
from telethon import functions, types, events

from backend.asyncSQLDataService import asyncSQLDataService
from models import Stat_reaction, Stat_post, Stat_user


dbname = String.load("db").value
groups = [gr.strip() for gr in String.load("groups").value.split(',')]

log = None

async def run_logging(client, max_posts=15, parent=''):
    global log
    log = []
    print('log: ', log)
    log.append(parent+' start: ' + datetime.now().strftime("%H:%M:%S"))
    for channel_name in groups:
        print('run: ', channel_name)
        res = asyncio.create_task(get_posts(client, await read_channels(client, channel_name),
                                            max_posts=max_posts, parent=parent))
        # res = await get_posts(client, await read_channels(client, channel_name), max_posts=max_posts, parent=parent)
        pass
        if not res:
            log.append(['error: ', channel_name, datetime.now().strftime("%H:%M:%S")])
        else:
            pass
            # log.append([res[0], res[1], datetime.now().strftime("%H:%M:%S")])
    # log.append(' end: '+datetime.now().strftime("%H:%M:%S"))
    log.append(' start2pgsql: ' + datetime.now().strftime("%H:%M:%S"))
    # if platform == 'win32':
    #     print("win32 - skip write to mysql")
    #     await update2mysql()
    # else:
    #     await update2mysql()
    await update2pgsql()

    log.append('  end2pgsql: ' + datetime.now().strftime("%H:%M:%S"))
    print('log: ', log)


async def get_posts(client, id, max_posts=1500, parent=None):
    if id is None:
        return False
    # posts = client.iter_messages(id, reverse=True, offset_id=0)
    pp = parent

    print(f'[{pp} {id}]start work:')
    start = time()
    post_list = []
    limit = 100
    offset_id = 1
    reversed = False
    cntr = 0
    maximum_messages = 0
    maximum_message_post = 0
    maximum_users = 0
    maximum_users_post = 0

    while offset_id:
        pass
        posts = client.iter_messages(id, limit=limit, reverse=reversed, offset_id=offset_id-1)
        sql = asyncSQLDataService()

        async for p in posts:
            post = Stat_post()
            total_message = posts.total

            if type(p) == types.MessageService:
                pass
            elif type(p) == types.Message:

                if cntr == 0:
                    print(f'[{pp}{id}] START reading last {max_posts} messages. Total message {total_message} in chat: {str(p.chat.title)[:25]} ')

                post.message = p.message
                post.post_id = p.id
                if p.forwards:
                    post.forwards = p.forwards
                if p.date:
                    # print((p.date + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'))
                    post.date_of_post = (p.date + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
                if p.reactions:
                    pass
                    # pprint(p.reactions.to_dict())
                    for res in p.reactions.results:
                        react = Stat_reaction()
                        react.reaction_count = res.count
                        post.total_reactions_count = post.total_reactions_count + res.count
                        try:
                            react.reaction_emoticon = res.reaction.emoticon

                            react.reaction_emoticon_code = ord(res.reaction.emoticon)
                        except Exception as e:
                            pass
                            # print(e)
                        react.channel_id = id
                        react.post_id = p.id
                        await sql.init("posts.db", forceCreation=True)
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
                        post.comments_channels_count, post.comments_users_count, post.comments_messages_count = await get_comments(client, id, post.post_id, sql=sql)
                        if post.comments_messages_count > maximum_messages:
                            maximum_messages = post.comments_messages_count
                            maximum_message_post = post.post_id
                            # print(f"[{pp}][{id}] maximum_messages: {maximum_messages} in post: {post.post_id}")
                        if post.comments_users_count > maximum_users:
                            maximum_users = post.comments_messages_count
                            maximum_users_post = post.post_id
                            # print(f"[{pp}][{id}] maximum_users: {maximum_users} in post: {post.post_id}")

                if p.media:
                    pass
                    # print(type(p.media))
                    if (type(p.media)) == types.MessageMediaDocument:
                        post.media = p.media.document.mime_type
                    elif (type(p.media)) == types.MessageMediaPhoto:
                        post.media = 'PHOTO'
                if p.views:
                    post.views = p.views
                else:
                    post.views = None

                # pprint(p.to_dict())
                post.channel_id = id
                post.date_of_update = datetime.today().replace(microsecond=0)
                post.link = 'https://t.me/c/' + str(id)+'/'+str(p.id)
                await sql.init("posts.db", forceCreation=True)
                await sql.store_post(post)
                await sql.close()
                post_list.append(post)

                # print(f'[{pp}][{id}]', p.chat.title, p.id, str(p.message).replace('\n', '')[:35], ' [users:', post.comments_users_count, ']  [mess:', post.comments_messages_count, ']')

            if cntr % 25 == 0:
                print(f'[{pp}{id} ] curent post: {post.post_id} mess: {cntr} from {total_message}  running (sec): ', int(time() - start))

            cntr += 1
            if cntr >= max_posts:
                print(f'[{pp}{id} ] FINISH read {cntr} messages, TOTAL time:', int(time() - start),
                      f'sec  [max messages: {maximum_messages} in post_id: {maximum_message_post}]   [max users: {maximum_users} in {maximum_users_post}]')
                return id, int(time() - start), post_list
            else:
                pass

        offset_id = post.post_id
        # print(f'offset: {offset_id} total: {total_message} remain:{message_remain}')
    print(f'[{pp}{id} ] TOTAL time: ', int(time() - start))
    return id, int(time() - start), post_list


async def get_comments(client, id, msg_id, max=300, sql=None):
    comments = None
    comments_channels_count = 0
    comments_users_count = 0
    comments_messages_count = 0
    total_comments = 0
    counter_comments = 0
    offset_id = 0

    user = Stat_post()

    while True:
        pass
        comments = await client(GetRepliesRequest(peer=id, msg_id=msg_id, offset_id=offset_id, offset_date=0, add_offset=0, limit=400, max_id=0, min_id=0, hash=0))
        # comments = await client(GetRepliesRequest(peer=id, msg_id=42689, offset_id=offset_id, offset_date=0, add_offset=0, limit=1400, max_id=0, min_id=0, hash=0))
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
                    pass
                    user = Stat_post()
                    user.user_id = u.pk
                    user.username = u.username
                    user.firstName = u.first_name
                    user.lastName = u.last_name
                    user.phone = u.phone
                    user.isPremium = u.premium
                    user.isScam = u.scam
                    user.isVerified = u.verified
                    user.source = id
                    await sql.init("posts.db", forceCreation=True)
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


async def read_channels(client, search_name):
    try:
        if type(int(search_name)) == int:
            pass
            search_name = str(search_name).replace('-100', '')
            id = int(search_name)
            # print(id)
            try:
                item = await client.get_entity(id)
                # print(item)
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
            username = None

        if dial.is_group:
            print('[group ]'+Back.GREEN+Fore.BLACK, dial.id,  dial.title, username, dial.is_user, dial.is_channel, dial.is_group, Style.RESET_ALL)
        elif dial.is_channel:
            print('[chanel]'+Back.RED+Fore.BLACK, dial.id,  dial.title, username, dial.is_user, dial.is_channel, dial.is_group, Style.RESET_ALL)
        else:
            print('[others]'+Back.YELLOW+Fore.BLACK, dial.id,  dial.title, username, dial.is_user, dial.is_channel, dial.is_group, Style.RESET_ALL)

        if ((dial.title).lower() == search_name.lower()) or (search_name.lower() in (dial.title).lower()):
            return dial.id
    return None


async def write2pgsql(matrix: list, base_name: str):
    try:
        mysql = MYSQLDataService(hostname=clientAPI.mysql_host, user=clientAPI.mysql_login, password=clientAPI.mysql_pass,
                                 databaseName=''+str('posts'), forceCreation=False)

    except Exception as e:
        print(e)
        return False
    # mysql = MYSQLDataService(hostname="92.255.111.148", user='user_test3', password='QW12er34!!', databaseName=base_name_to_check, forceCreation=True)
    print('start write 2 sql ', datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
    res = mysql.store_table(base_name, matrix)
    if res is None:
        print('ERROR1 write 2 sql ', datetime.now().strftime("%d.%m.%Y %H:%M:%S"), " wait 5 sec and retry")
        await asyncio(5)
        res = mysql.store_table(base_name, matrix)
        if res is None:
            print('ERROR2 write 2 sql ', datetime.now().strftime("%d.%m.%Y %H:%M:%S"), " exiting")
            return False

    print('end write 2 sql ', datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
    return True


async def update2pgsql():
    sql = asyncSQLDataService()
    await sql.init('posts.db', forceCreation=True)
    try:
        res = await sql.get_all_post()
        res1 = await sql.get_all_react()
        await sql.close()

        if await write2pgsql(res, 'stat_post'):
            if await write2pgsql(res1, 'stat_reaction'):
                print('all OK')
            else:
                print('error update table `stat_post`')
        else:
            print('error update table `stat_reaction`')

    except Exception as e:
        print('error update PgSQL: \n', str(e))
