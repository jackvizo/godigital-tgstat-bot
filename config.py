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

# dialect+driver://username:password@host:port/database
# postgresql+psycopg://tgbot:pass_123@localhost/tgbot_db
db_engine = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{db_name}"

# telegram (takes from run file arguments)
# api_id = ""
# api_hash = ""
