# database
host = "localhost"
port = "5432"
user = "tgbot"
password = "pass_123"
db_name = "tgbot_db"

# db table names
posts = 'stat_post'
reactions = 'stat_reaction'
users = 'stat_user'

# SQLAlchemy
dialect = "postgresql"
driver = "psycopg"              # psycopg => 'psycopg3'

# Формат для перевода даты в строку и обратно
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
RRULE_FORMAT = '%Y%m%dT%H%M%S'

# Планирование задач на +1 и +24 часа с даты поста
# RRULE_STR_SCHEDULE_1h = 'DTSTART:%s RRULE:FREQ=HOURLY;INTERVAL=1;COUNT=1'
# RRULE_STR_SCHEDULE_24h = 'DTSTART:%s RRULE:FREQ=HOURLY;INTERVAL=24;COUNT=1'
#
# RRULE_TEST_SCHEDULE = 'DTSTART:%s RRULE:FREQ=MINUTELY;INTERVAL=1;COUNT=1'

# % (date_start + timedelta(hours=1)).strftime(RRULE_FORMAT)

# dialect+driver://username:password@host:port/database
# postgresql+psycopg://tgbot:pass_123@localhost/tgbot_db
db_engine = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{db_name}"

# telegram (takes from run file arguments)
# api_id = ""
# api_hash = ""
phone_number = ""
phone_test_number = ""
