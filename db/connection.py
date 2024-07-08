import psycopg2
import globals


def create_db_connection():
    return psycopg2.connect(
        database=globals.DB_NAME,
        host=globals.DB_HOST,
        port=globals.DB_PORT,
        user=globals.DB_USER,
        password=globals.DB_PASSWORD,
    )
