import os

from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
import os
import socket

from modules import scripts
from modules import shared

load_dotenv()

database_url = os.getenv("DATABASE_URL")
database_schema = os.getenv("DATABASE_SCHEMA")
env_worker_id = os.getenv("WORKER_ID") if os.getenv("WORKER_ID") is not None else socket.gethostname()
print("workerid", env_worker_id)

Base = declarative_base()
metadata: MetaData = Base.metadata

class BaseTableManager:
    def __init__(self, engine = None):
        # Get the db connection object, making the file and tables if needed.
        try:
            self.engine = engine if engine else create_engine(
        database_url, connect_args={
            "options": "-csearch_path={},metrics".format(database_schema)})

        except Exception as e:
            print(f"Exception connecting to database: {e}")
            raise e

    def get_engine(self):
        return self.engine

    # Commit and close the database connection.
    def quit(self):
        self.engine.dispose()
