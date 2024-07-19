import os

from dotenv import load_dotenv

load_dotenv(os.getenv('ENV_FILE', '.env'))

required_env_vars = [
    'DB_HOST',
    'DB_PORT',
    'DB_USER',
    'DB_PASSWORD',
    'DB_NAME',
    'PREFECT_SERVER_URL'
]


def validate_env_variables():
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        missing_vars_str = ", ".join(missing_vars)
        raise EnvironmentError(f"Missing environment variables: {missing_vars_str}")


DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
PREFECT_SERVER_URL = os.getenv("PREFECT_SERVER_URL")
IS_TEST = os.getenv("ENV_FILE") == '.env.test'

TEST_API_ID = os.getenv("TEST_API_ID")
TEST_API_HASH = os.getenv("TEST_API_HASH")
TEST_TG_SERVER_DC = os.getenv("TEST_TG_SERVER_DC")
TEST_TG_SERVER_IP = os.getenv("TEST_TG_SERVER_IP")
TEST_TG_SERVER_PORT = os.getenv("TEST_TG_SERVER_PORT")

TABLE_POSTS = 'stat_post'
TABLE_REACTIONS = 'stat_reaction'
TABLE_USERS = 'stat_user'
TABLE_TG_SESSION_POOL = 'config__tg_bot_session_pool'

dialect = "postgresql"
driver = "psycopg"

DB_CONNECTION_STR = f"{dialect}+{driver}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
