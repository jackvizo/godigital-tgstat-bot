import httpx
from datetime import datetime, timedelta

PREFECT_SERVER_URL = 'http://localhost:4200'


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


def service_run():
    deployment = get_deployment_by_flow_name("tg_collect")
    deployment_id = deployment.get('id')

    scheduled_start_time_1 = datetime.utcnow() + timedelta(hours=1)
    scheduled_start_time_24 = datetime.utcnow() + timedelta(hours=24)

    create_scheduled_flow_run(deployment_id, scheduled_start_time_1, hours=1)
    create_scheduled_flow_run(deployment_id, scheduled_start_time_24, hours=24)
