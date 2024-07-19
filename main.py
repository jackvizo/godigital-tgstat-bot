import argparse

from flows.flow_add_bot_session import flow_add_bot_session
from flows.flow_collect import flow_tg_collect_by_all_users
from globals import validate_env_variables

if __name__ == '__main__':
    validate_env_variables()

    parser = argparse.ArgumentParser()
    parser.add_argument("--flow", help="Specify the flow to run")
    args = parser.parse_args()

    switch_flow = args.flow

    if switch_flow == 'add-bot-session':
        flow_add_bot_session.serve(name="add-bot-session")
    if switch_flow == 'tg-collect-by-all-users':
        flow_tg_collect_by_all_users.serve(name="tg-collect-by-all-users", interval=3600)
    else:
        raise ValueError(f"Invalid flow: {switch_flow}. Available flows are ['add-phone-number', 'tg-collect-by-user']")
