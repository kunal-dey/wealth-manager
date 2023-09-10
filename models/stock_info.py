from logging import Logger
from time import sleep
from dataclasses import dataclass, field

import requests
import pandas as pd

from constants.global_contexts import kite_context
from constants.settings import DEBUG, set_end_process
from utils.indicators.kaufman_indicator import kaufman_indicator
from utils.logger import get_logger

logger: Logger = get_logger(__name__)


@dataclass
class StockInfo:
    stock_name: str
    exchange: str = 'NSE'
    latest_indicator_price: float | None = field(default=None, init=False)
    latest_price: float = field(default=None, init=False)
    low: float = field(default=None, init=False)
    high: float = field(default=None, init=False)
    __result_stock_df: pd.DataFrame | None = field(default=None, init=False)

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
                    current_price = kite_context.ltp([f"{self.exchange}:{self.stock_name}"])[f"{self.exchange}:{self.stock_name}"]["last_price"]
                    if current_price is not None:
                        return float(current_price)

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
        logger.info(f"{self.stock_name} : {self.latest_price}")
        if self.latest_price is not None:

            self.update_stock_df(self.latest_price)
            actual_price: pd.DataFrame = self.__result_stock_df.copy()
            kuafman_array = kaufman_indicator(actual_price['price'])
            actual_price.loc[:, 'line'] = kuafman_array
            actual_price.loc[:, 'signal'] = actual_price.line.ewm(span=10).mean()
            actual_price.dropna(inplace=True)
            actual_price.reset_index(inplace=True)
            if actual_price.shape[0] == 0:
                self.latest_indicator_price = None
            else:
                recent_price = actual_price.signal.iloc[actual_price.shape[0] - 1]
                self.latest_indicator_price = recent_price

            if self.low is None:
                self.low = self.latest_price
            else:
                if self.low > self.latest_price:
                    self.low = self.latest_price

            if self.high is None:
                self.high = self.latest_price
            else:
                if self.high < self.latest_price:
                    self.high = self.latest_price

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
        self.__result_stock_df.to_csv(f"temp/{self.stock_name}.csv")
        self.__result_stock_df = self.__result_stock_df.bfill().ffill()
        self.__result_stock_df.dropna(axis=1, inplace=True)
