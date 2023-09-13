from contextlib import contextmanager
import traceback
from motor.motor_asyncio import AsyncIOMotorClient

from utils.exceptions.db_connection import DbConnectionException

from constants.db_settings import HOST, DATABASE_NAME

CLIENT = AsyncIOMotorClient(HOST)
DATABASE = CLIENT[DATABASE_NAME]


@contextmanager
def connect_to_collection(collection_name: str):
    """
        This method provides the collection on which we can work on.
        If any error occurs then it shows an error occurred from our side.
    """
    try:
        yield DATABASE[collection_name]
    except:
        traceback.print_exc()
        raise DbConnectionException()
