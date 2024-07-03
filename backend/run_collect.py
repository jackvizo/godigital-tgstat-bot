import httpx
from datetime import datetime, timedelta

PREFECT_SERVER_URL = 'http://localhost:4200/api'

# http://localhost:4200/api//deployments/filter
# http://localhost:4200/api/281c6fb8-e00e-4397-a4c5-f091e621960c/create_flow_run

def get_deployment_by_flow_name(flow_name: str):
    response = httpx.post(f"{PREFECT_SERVER_URL}/deployments/filter", json={
        "flows": {
            "name": {
                "any_": [flow_name]
            }
        }
    })
    json = response.json()
    return json[0] if json else None


def create_scheduled_flow_run(deployment_id: str, scheduled_start_time: datetime, **hours):
    response = httpx.post(f"{PREFECT_SERVER_URL}/deployments/{deployment_id}/create_flow_run", json={
        "parameters": hours,
        "state": {
            "type": "SCHEDULED",
            "state_details": {
                "scheduled_time": scheduled_start_time.isoformat()
            }
        }
    })
    print(response)


def service_run(flow_name="tg_collect", date_start=None):
    deployment = get_deployment_by_flow_name(flow_name)
    try:
        deployment_id = deployment.get('id')
    except Exception as e:
        deployment_id = None
        print(f'Ошибка получения "id" в ответе сервера: {e}')
        print(f'Запуск не запланирован!')

    if deployment_id:
        scheduled_start_time_1 = date_start or datetime.utcnow() + timedelta(hours=1)
        scheduled_start_time_24 = date_start or datetime.utcnow() + timedelta(hours=24)

        create_scheduled_flow_run(deployment_id, scheduled_start_time_1, hours=1)
        create_scheduled_flow_run(deployment_id, scheduled_start_time_24, hours=24)

#
# if __name__ == '__main__':
#     service_run()
