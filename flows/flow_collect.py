from prefect import flow

from db.SyncSQLDataService import SyncSQLDataService
from db.queries import get_tracked_tg_channels, get_last_post_id_in_channel
from lib.collect import collect_channel, store_channel
from lib.scheduler import schedule_tg_collect_flow_run
from lib.tg_client import get_authorized_tg_client


async def schedule_flow_run(sql: SyncSQLDataService, post_list: list, channel_id: int, phone_number: str):
    post_last_id = get_last_post_id_in_channel(sql.connection, channel_id)
    shall_schedule_flow_run = len(post_list) > 0 and post_last_id != post_list[0].tg_post_id

    if shall_schedule_flow_run:
        schedule_tg_collect_flow_run(phone_number, channel_id)


@flow(name="collect-tg-channels-by-phone-number", log_prints=True)
async def subflow_collect_tg_channels_by_phone_number(phone_number: str, channel_id: int = None):
    print('subflow_collect_tg_channels_by_phone_number call')
    sql = SyncSQLDataService()

    try:
        sql.init()

        tg_client, session_pool_pk = await get_authorized_tg_client(sql, phone_number)

        channels = [[channel_id]] if channel_id else get_tracked_tg_channels(sql.connection, session_pool_pk)

        for channel in channels:
            channel_id = channel[0]

            print(f'Start collecting stats from channel {channel_id}...')

            user_dict, post_list, react_list = await collect_channel(tg_client, channel_id)

            print(
                f'collected posts: {len(post_list)}; collected reacts: {len(react_list)}; collected users: {len(user_dict)}')

            store_channel(sql, user_dict, post_list, react_list)
            await schedule_flow_run(sql, post_list, channel_id, phone_number)

        await tg_client.disconnect()

    finally:
        sql.cursor.close()
        sql.close()


@flow(log_prints=True)
def flow_tg_collect_by_all_users():
    print('flow_tg_collect_by_all_users call')
    sql = SyncSQLDataService()
    sql.init()

    users = sql.get_users_with_active_phone_numbers()

    sql.close()

    for user in users:
        user_id, phone_number = user
        subflow_collect_tg_channels_by_phone_number(phone_number)

