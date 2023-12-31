from datetime import datetime
from logging import Logger
from time import sleep
from dataclasses import dataclass, field
from typing import Callable
import numpy as np

import requests
import pandas as pd

from constants.global_contexts import kite_context
from constants.settings import DEBUG, set_end_process, get_allocation
from models.db_models import get_save_to_db, get_delete_from_db, get_update_in_db
from utils.indicators.kaufman_indicator import kaufman_indicator
from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def get_schema():
    return {
        "stock_name": "str",
        "exchange": "str",
        "wallet": "float",
        "created_at": "datetime",
        "first_load": "bool"
    }


@dataclass
class StockInfo:
    stock_name: str
    exchange: str = 'NSE'
    wallet: float = field(default=0.0)
    class_name: str = field(default="StockInfo", init=False)
    COLLECTION: str = field(default="stock", init=False)
    latest_indicator_price: float | None = field(default=None, init=False)
    latest_price: float = field(default=None, init=False)
    low: float = field(default=None, init=False)
    high: float = field(default=None, init=False)
    created_at: datetime = field(default_factory=datetime.now)
    __result_stock_df: pd.DataFrame | None = field(default=None, init=False)
    schema: dict = field(default_factory=get_schema, init=False)
    save_to_db: Callable = field(default=None, init=False)
    delete_from_db: Callable = field(default=None, init=False)
    update_in_db: Callable = field(default=None, init=False)
    quantity: int = field(default=0, init=False)
    first_load: bool = field(default=True)  # while loading from db make it true
    in_position: bool = field(default=False)
    last_buy_price: float = field(default=None)

    def __post_init__(self):
        self.save_to_db = get_save_to_db(self.COLLECTION, self)
        self.delete_from_db = get_delete_from_db(self.COLLECTION, self)
        self.update_in_db = get_update_in_db(self.COLLECTION, self)

    @property
    def get_quote(self):
        return kite_context.quote([f"{self.exchange}:{self.stock_name}"])[f"{self.exchange}:{self.stock_name}"][
                    "depth"]

    @property
    def current_price(self):
        """
            returns the current price in the market or else None if the connection interrupts

            tries 4 times
        """
        retries = 0
        while retries < 4:
            try:
                if DEBUG:
                    response = requests.get(f"http://127.0.0.1:8082/price?symbol={self.stock_name}")
                    return response.json()['data']
                else:
                    quote: dict = self.get_quote
                    if self.in_position:
                        orders: list = quote["buy"]
                    else:
                        orders: list = quote["sell"]
                    accumulated, quantity = 0, 0
                    for item in orders:
                        for order_no in range(1, item["orders"] + 1):
                            for _ in range(1, item["quantity"] + 1):
                                if quantity + 1 > self.quantity:
                                    return accumulated / quantity
                                accumulated += item["price"]
                                quantity += 1
                    return None

            except:
                sleep(1)
            retries += 1
        return None

    def update_price(self):
        """
        This is required to update the latest price.

        It is used to update the csv containing the price for the stock.
        Using it, it updates the latest indicator price which is the last KAMA indicator price.

        The latest KAMA indicator price is used while selling

        :return: None
        """
        current_price = self.current_price
        if current_price == 'ENDED':
            set_end_process(True)
            return
        # if the current price is still none in that case, the older the latest price is used if it's not None
        if current_price is not None:
            self.latest_price = current_price
        if self.latest_price is not None:

            self.update_stock_df(self.latest_price)
            actual_price: pd.DataFrame = self.__result_stock_df.copy()
            actual_price.insert(1, "min", actual_price.price.cummin())
            if actual_price.shape[0] > 1:  # first value is the buy price only so from 2nd low value is taken for triggering in a single day
                self.low = actual_price['min'].iloc[-1]

            actual_price.loc[:, 'line'] = kaufman_indicator(actual_price['price'])
            actual_price.loc[:, 'signal'] = actual_price.line.ewm(span=5).mean()
            actual_price.dropna(inplace=True)
            actual_price.reset_index(inplace=True)
            if actual_price.shape[0] == 0:
                self.latest_indicator_price = None
            else:
                recent_price = actual_price.signal.iloc[actual_price.shape[0] - 1]
                self.latest_indicator_price = recent_price

    def buy_parameters(self):
        amount: float = get_allocation()

        def get_quantity_and_price(s_orders):
            accumulated, quantity = 0, 0
            for item in s_orders:
                for order_no in range(1, item["orders"] + 1):
                    for _ in range(1, item["quantity"] + 1):
                        if accumulated + item["price"] > amount:
                            return quantity, accumulated/quantity
                        accumulated += item["price"]
                        quantity += 1
            return quantity, accumulated / quantity
        quote: dict = self.get_quote
        sell_orders: list = quote["sell"]
        if DEBUG:
            if self.latest_price:
                self.quantity, price = int(amount/self.latest_price), self.latest_price
            else:
                self.quantity, price = 0, 0
        else:
            self.quantity, price = get_quantity_and_price(sell_orders)
        return self.quantity, price

    def update_stock_df(self, current_price: float):
        """
        This function updates the csv file which holds the price every 30 sec
        :param current_price:
        :return: None
        """
        try:
            self.__result_stock_df = pd.read_csv(f"temp/{self.stock_name}.csv")
            self.__result_stock_df.drop(self.__result_stock_df.columns[0], axis=1, inplace=True)
        except FileNotFoundError:
            self.__result_stock_df = None
        stock_df = pd.DataFrame({"price": [current_price]})
        if self.__result_stock_df is not None:
            self.__result_stock_df = pd.concat([self.__result_stock_df, stock_df], ignore_index=True)
        else:
            self.__result_stock_df = stock_df
        # logger.info(f"{self.stock_name} : {self.__result_stock_df.shape[0]} , {current_price}")
        self.__result_stock_df.to_csv(f"temp/{self.stock_name}.csv")
        self.__result_stock_df = self.__result_stock_df.bfill().ffill()
        self.__result_stock_df.dropna(axis=1, inplace=True)

    def whether_buy(self) -> bool:
        """
        Buy the stock if certain conditions are met:
        1. If total buying quantity/ total selling quantity > 0.8 then buy
        2. It has a minimum quantity which you want to buy
        3. The current price is not more than 1% of the trigger price
        :return: True, if buy else false
        """
        if self.first_load:
            # self.first_load = False  # or else it will constantly sell and buy
            return True
        else:
            if self.__result_stock_df.shape[0] > 10:
                stock_df = self.__result_stock_df.copy()
                stock_df.insert(1, "line", kaufman_indicator(stock_df['price']))
                stock_df.insert(2, "signal", stock_df.line.ewm(span=5).mean())
                stock_df.insert(3, "min", stock_df.signal.cummin())
                stock_df['positions'] = np.where((stock_df['signal'] > stock_df['min']*1.002), 1, 0)
                # logger.info(f" positions {self.stock_name}:{stock_df['positions'].iloc[-1]},{stock_df['positions'].iloc[-2]}")
                if stock_df['positions'].iloc[-1] == 1 and stock_df['positions'].iloc[-2] == 0 and self.last_buy_price > (1+0.02)*self.latest_price:
                    return True
            elif self.__result_stock_df.shape[0] > 1:
                stock_df = self.__result_stock_df.copy()
                stock_df.insert(1, "signal", stock_df.price.ewm(span=5).mean())
                stock_df.insert(2, "min", stock_df.signal.cummin())
                stock_df['positions'] = np.where((stock_df['signal'] > stock_df['min']), 1, 0)
                # logger.info(f" positions {self.stock_name}:{stock_df['positions'].iloc[-1]},{stock_df['positions'].iloc[-2]}")
                if stock_df['positions'].iloc[-1] == 1 and stock_df['positions'].iloc[-2] == 0:
                    return True
        return False
