from prefect import flow

from backend.SyncSQLDataService import SyncSQLDataService
from tg_collect_flow import flow_collect_tg_channels_by_phone_number


@flow
def flow_supervisor():
    sql = SyncSQLDataService()

    users = sql.get_users_with_active_phone_numbers()

    for user in users:
        flow_collect_tg_channels_by_phone_number(user.phone_number)


if __name__ == '__main__':
    flow_supervisor.serve(name="tg-collect", interval=300)
