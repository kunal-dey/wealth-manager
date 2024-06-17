from dataclasses import dataclass, field
from logging import Logger
from typing import Callable

from bson import ObjectId

from models.db_models.object_models import get_save_to_db, get_update_in_db


from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def get_schema():
    return {
        "_id": "ObjectId",
        "expected_amount": "float",
        "accumulated_amount": "float",
    }


@dataclass
class Wallet:
    expected_amount: float = field(default=99999999.0)
    accumulated_amount: float = field(default=0.0)
    _id: ObjectId = field(default_factory=ObjectId)
    class_name: str = field(default="Wallet", init=False)
    COLLECTION: str = field(default="wallet", init=False)
    schema: dict = field(default_factory=get_schema, init=False)
    save_to_db: Callable = field(default=None, init=False)
    update_in_db: Callable = field(default=None, init=False)

    def __post_init__(self):
        self.save_to_db = get_save_to_db(self.COLLECTION, self)
        self.update_in_db = get_update_in_db(self.COLLECTION, self)

    async def create_wallet(self):
        """
            only to be created first time
        Returns:
        """
        try:
            await self.save_to_db()
            return "success"
        except:
            logger.exception("Failed to create wallet")
            return "failed"

    async def update_accumulated_amount(self, amount: float):
        try:
            self.accumulated_amount = amount
            await self.update_in_db()
            return "success"
        except:
            logger.exception("Failed to create wallet")
            return "failed"

    async def update_expected_amount(self, amount: float):
        try:
            self.expected_amount = amount
            await self.update_in_db()
            return "success"
        except:
            logger.exception("Failed to create wallet")
            return "failed"
