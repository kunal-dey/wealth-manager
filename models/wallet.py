from dataclasses import dataclass, field
from logging import Logger
from typing import Callable
from datetime import datetime

from bson import ObjectId
from dateutil.rrule import rrule, MO, TU, WE, TH, FR, WEEKLY

from constants.settings import DEBUG, TODAY
from models.db_models.object_models import get_save_to_db, get_update_in_db
from utils.exclude_dates import load_holidays

from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def get_schema():
    return {
        "_id": "ObjectId",
        "expected_amount": "float",
        "accumulated_amount": "float",
        "starting_amount_update_time": "float",
        "starting_amount": "float"
    }


@dataclass
class Wallet:
    expected_amount: float = field(default=99999999.0)
    accumulated_amount: float = field(default=0.0)
    starting_amount: float = field(default=150000)
    starting_amount_update_time: datetime = field(default=datetime.now())
    _id: ObjectId = field(default_factory=ObjectId)
    class_name: str = field(default="Wallet", init=False)
    COLLECTION: str = field(default="wallet_dbg" if DEBUG else "wallet", init=False)
    schema: dict = field(default_factory=get_schema, init=False)
    save_to_db: Callable = field(default=None, init=False)
    update_in_db: Callable = field(default=None, init=False)

    def __post_init__(self):
        self.save_to_db = get_save_to_db(self.COLLECTION, self)
        self.update_in_db = get_update_in_db(self.COLLECTION, self)

    @property
    def number_of_days_from_last_starting_amount_update(self):
        """
            If today is a weekday and not a holiday, the number of days would be 1.
            If today is a weekday and a holiday, or if it's a weekend, the number of days would be 0.
        Returns:

        """
        dt_start, until = self.starting_amount_update_time.date(), TODAY.date()
        days = rrule(WEEKLY, byweekday=(MO, TU, WE, TH, FR), dtstart=dt_start, until=until).count()
        for day in load_holidays()['dates']:
            if dt_start < day.date() < until:
                days -= 1
        return days

    @property
    def daily_return(self):
        return ((1+(self.accumulated_amount/self.starting_amount))**(1/self.number_of_days_from_last_starting_amount_update))-1

    def metrics(self):
        return {
            "starting_amount": self.starting_amount,
            "accumulated_amount": self.accumulated_amount,
            "starting_time": self.starting_amount_update_time,
            "set_expected_amount": self.expected_amount,
            "daily_return": f"{round(self.daily_return, 5)*100} %",
            "monthly_return": f"{round((1+self.daily_return)**20 - 1, 5)*100} %"
        }

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
