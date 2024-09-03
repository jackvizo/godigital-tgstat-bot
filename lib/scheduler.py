from datetime import datetime, timedelta

import httpx

from globals import PREFECT_SERVER_URL


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

    
def create_scheduled_flow_run(deployment_id: str, scheduled_start_time: datetime, phone_number: str, channel_id: int):
    response = httpx.post(f"{PREFECT_SERVER_URL}/deployments/{deployment_id}/create_flow_run", json={
        "parameters": {
            "scheduled_phone_number": phone_number,
            "scheduled_tg_channel_id": channel_id
        },
        "state": {
            "type": "SCHEDULED",
            "state_details": {
                "scheduled_time": scheduled_start_time.isoformat()
            }
        }
    })
    return response


def schedule_tg_collect_flow_run(phone_number: str, channel_id: int):
    deployment = get_deployment_by_flow_name("flow-tg-collect-by-all-users")
    deployment_id = deployment.get('id')

    scheduled_start_time_1 = datetime.utcnow() + timedelta(hours=1)
    scheduled_start_time_24 = datetime.utcnow() + timedelta(hours=24)

    create_scheduled_flow_run(deployment_id, scheduled_start_time_1, phone_number, channel_id)
    create_scheduled_flow_run(deployment_id, scheduled_start_time_24, phone_number, channel_id)
