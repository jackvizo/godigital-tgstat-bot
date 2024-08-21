from datetime import datetime, timedelta

from prefect import flow

from db.SyncSQLDataService import SyncSQLDataService
from db.queries import get_tracked_tg_channels, get_last_post_id_in_channel
from lib.collect import collect_channel, store_channel
from lib.scheduler import schedule_tg_collect_flow_run
from lib.tg_client import get_authorized_tg_client


async def schedule_flow_run(sql: SyncSQLDataService, post_list: list, channel_id: int, phone_number: str):
    record = get_last_post_id_in_channel(sql.connection, channel_id)

    if record is None:
        return

    post_last_id, date_of_post = record

    current_time = datetime.utcnow()
    is_recent_post = (current_time - date_of_post) < timedelta(hours=24, minutes=5)

    shall_schedule_flow_run = len(post_list) > 0 and post_last_id != post_list[0].tg_post_id and is_recent_post

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
            sql.open()
            channel_id = channel[0]

            print(f'Start collecting stats from channel {channel_id}...')

            tg_last_admin_event_id = sql.get_tg_last_event_id(channel_id)

            user_dict, post_list, react_list, post_info_list, stat_channel = await collect_channel(
                tg_client=tg_client,
                channel_id=channel_id,
                tg_last_admin_event_id=tg_last_admin_event_id)

            print(
                f'collected posts: {len(post_list)}; collected reacts: {len(react_list)}; collected users: {len(user_dict)}')

            store_channel(sql=sql, user_dict=user_dict, post_list=post_list,
                          react_list=react_list,
                          post_info_list=post_info_list, stat_channel=stat_channel)
            try:
                await schedule_flow_run(sql, post_list, channel_id, phone_number)
            except Exception as e:
                print(e)

            sql.cursor.close()
        await tg_client.disconnect()

    finally:
        sql.cursor.close()
        sql.close()


@flow(log_prints=True)
def flow_tg_collect_by_all_users(scheduled_phone_number: str = None, scheduled_tg_channel_id: int = None):
    if scheduled_phone_number and scheduled_tg_channel_id:
        print('flow_tg_collect_by_all_users called by schedule')

        subflow_collect_tg_channels_by_phone_number(phone_number=scheduled_phone_number,
                                                    channel_id=scheduled_tg_channel_id)
        return

    print('flow_tg_collect_by_all_users call')

    sql = SyncSQLDataService()
    sql.init()

    users = sql.get_users_with_active_phone_numbers()

    sql.close()

    for user in users:
        user_id, phone_number = user
        subflow_collect_tg_channels_by_phone_number(phone_number)
