from dataclasses import dataclass, field

from typing import Callable

from bson import ObjectId

from constants.enums.position_type import PositionType
from constants.enums.product_type import ProductType
from models.stock_info import StockInfo
from models.stock_stage import Stage
from models.db_models.object_models import get_save_to_db, get_delete_from_db, get_update_in_db

from constants.settings import DELIVERY_INCREMENTAL_RETURN, DEBUG


def get_schema():
    return {
        "_id": "ObjectId",
        "position_price": "float",
        "quantity": "int",
        "product_type": ProductType,
        "position_type": PositionType,
        "stock": StockInfo,
        "trigger": "float"
    }


@dataclass
class Holding(Stage):
    _id: ObjectId = field(default_factory=ObjectId)
    class_name: str = field(default="Holding", init=False)
    COLLECTION: str = field(default="penny_holding_dbg" if DEBUG else "penny_holding", init=False)
    save_to_db: Callable = field(default=None, init=False)
    delete_from_db: Callable = field(default=None, init=False)
    update_in_db: Callable = field(default=None, init=False)
    schema: dict = field(init=False, default_factory=get_schema)

    @property
    def object_id(self):
        return self._id

    @object_id.setter
    def object_id(self, value: ObjectId):
        self._id = value

    def __post_init__(self):
        self.save_to_db = get_save_to_db(self.COLLECTION, self)
        self.delete_from_db = get_delete_from_db(self.COLLECTION, self)
        self.update_in_db = get_update_in_db(self.COLLECTION, self)

    @property
    def incremental_return(self):
        return DELIVERY_INCREMENTAL_RETURN

