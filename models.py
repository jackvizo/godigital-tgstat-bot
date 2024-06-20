from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import declarative_base

from config import db_engine

engine = create_engine(db_engine)

Base = declarative_base()


class User:
    pk: int = None
    tg_user_id: int = None
    firstName = ""
    lastName = ""
    username = ""
    scam: bool = False
    premium: bool = False
    timestamp: bool = False
    date_of_update = datetime.today().replace(microsecond=0)
    phone = ""
    tg_channel_id: int = None
    is_joined_by_link: bool = False
    joined_at: datetime = None
    left_at: datetime = None


class Post:
    id: int = None
    channel_id: int
    post_id: int = None
    message: str = None
    views: int = 0
    views_1h: int = 0
    views_24h: int = 0
    total_reactions_count: int = 0
    reactions_1h: int = 0
    reactions_24h: int = 0
    comments_messages_count: int = None
    comments_messages_count_1h: int = None
    comments_messages_count_24h: int = None
    comments_users_count: int = None
    comments_channels_count: int = None
    date_of_update = datetime.today().replace(microsecond=0)
    link: str = None
    media: str = None
    date_of_post: datetime = None
    forwards: int = 0


class Reaction:
    pk: int = None
    timestamp: int
    tg_channel_id: int = None
    tg_post_id: int = None
    reaction_count: int = 0
    reaction_emoticon: str = None
    reaction_emoticon_code: int = 0

