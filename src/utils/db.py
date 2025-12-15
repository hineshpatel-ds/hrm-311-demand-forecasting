import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set. Copy .env.example to .env and set it")
    
    return create_engine(db_url)

def ensure_schemas():
    engine =get_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))

