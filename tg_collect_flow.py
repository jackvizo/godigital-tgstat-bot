from prefect import flow, task

from backend.SyncSQLDataService import SyncSQLDataService
from backend.run_collect import schedule_tg_collect_flow_run
from db_utils import get_db_channels, get_last_db_post_id
from lib.collect import collect_channel, store_channel
from lib.authorize import authorize


@task
def task_authorize(conn, phone):
    return authorize(conn, phone)


@task
async def task_collect_channel(tg_client, channel):
    return await collect_channel(tg_client, channel)


@task
def task_store_channel(sql: SyncSQLDataService, user_dict, post_list, react_list):
    return store_channel(sql, user_dict, post_list, react_list)


@task
async def task_schedule_flow_run(sql: SyncSQLDataService, post_list: list, channel_id: int, phone_number: str):
    post_last_id = get_last_db_post_id(sql.connection, channel_id)
    shall_schedule_flow_run = len(post_list) > 0 and post_last_id != post_list[0].tg_post_id

    if shall_schedule_flow_run:
        schedule_tg_collect_flow_run(phone_number, channel_id)


@flow(name="collect-tg-channels-by-phone-number", log_prints=True)
async def flow_collect_tg_channels_by_phone_number(phone_number: str, channel_id: int = None):
    sql = SyncSQLDataService()

    try:
        sql.init()

        tg_client, session_pool_pk = task_authorize(sql.connection, phone_number)

        channels = [[channel_id]] if channel_id else get_db_channels(sql.connection, session_pool_pk)

        for channel in channels:
            channel_id = channel[0]

            user_dict, post_list, react_list = await task_collect_channel(tg_client, channel)

            print(
                f'collected posts: {len(post_list)}; collected reacts: {len(react_list)}; collected users: {len(user_dict)}')

            task_store_channel(sql, user_dict, post_list, react_list)
            task_schedule_flow_run(sql, post_list, channel_id, phone_number)

    finally:
        sql.cursor.close()
        sql.close()
