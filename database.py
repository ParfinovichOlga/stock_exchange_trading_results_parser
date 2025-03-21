from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from sqlalchemy_utils import create_database, database_exists


class Base(DeclarativeBase):
    pass


def get_engine(user, password, host, port, db):
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    if not database_exists(url):
        create_database(url)

    eng = create_engine(url, pool_pre_ping=True)
    return eng


engine = get_engine(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)

Session = sessionmaker(bind=engine)

