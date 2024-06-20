import types
from datetime import timedelta, datetime, time
from models import Stat_reaction, Stat_post


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