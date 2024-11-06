import re
from datetime import datetime
from logging import Logger
from time import sleep
from dataclasses import dataclass, field
from typing import Callable

import requests
import pandas as pd
from bson import ObjectId
from dateutil.rrule import rrule, WEEKLY, MO, TU, WE, TH, FR

from constants.enums.shift import Shift
from utils.exclude_dates import load_holidays

from constants.global_contexts import kite_context
from constants.settings import DEBUG, set_end_process, TODAY, CURRENT_STOCK_EXCHANGE
from models.db_models.object_models import get_save_to_db, get_delete_from_db, get_update_in_db
from models.costs.delivery_trading_cost import DeliveryTransactionCost
from models.costs.intraday_trading_cost import IntradayTransactionCost
from utils.indicators.kaufman_indicator import kaufman_indicator
from utils.indicators.candlestick.patterns.bullish_engulfing import BullishEngulfing
from utils.indicators.candlestick.patterns.bullish_harami import BullishHarami
from utils.indicators.candlestick.patterns.morning_star import MorningStar
from utils.indicators.candlestick.patterns.hammer import Hammer
from utils.indicators.candlestick.patterns.inverted_hammer import InvertedHammer

from utils.indicators.candlestick.patterns.bearish_engulfing import BearishEngulfing
from utils.indicators.candlestick.patterns.bearish_harami import BearishHarami
from utils.indicators.candlestick.patterns.evening_star import EveningStar

from utils.logger import get_logger
from constants.settings import GENERATOR_URL, MAXIMUM_ALLOCATION

logger: Logger = get_logger(__name__)


def get_schema():
    return {
        "_id": "ObjectId",
        "stock_name": "str",
        "exchange": "str",
        "wallet": "float",
        "created_at": "datetime",
        "last_buy_price": "float",
        "last_quantity": "float",
        "first_load": "bool"
    }


@dataclass
class StockInfo:
    stock_name: str
    exchange: str = CURRENT_STOCK_EXCHANGE
    wallet: float = field(default=0.0)
    _id: ObjectId = field(default_factory=ObjectId)
    class_name: str = field(default="StockInfo", init=False)
    COLLECTION: str = field(default="penny_stock_dbg" if DEBUG else "penny_stock", init=False)
    latest_price: float = field(default=None, init=False)
    created_at: datetime = field(default=TODAY)
    __result_stock_df: pd.DataFrame | None = field(default=None, init=False)
    schema: dict = field(default_factory=get_schema, init=False)
    save_to_db: Callable = field(default=None, init=False)
    delete_from_db: Callable = field(default=None, init=False)
    update_in_db: Callable = field(default=None, init=False)
    quantity: int = field(default=0, init=False)
    first_load: bool = field(default=True)  # while loading from db make it true
    in_position: bool = field(default=False)
    last_buy_price: float = field(default=None)
    last_quantity: int = field(default=None)
    chosen_long_stocks: list = field(default=None)
    chosen_short_stocks: list = field(default=None)

    def __post_init__(self):
        self.save_to_db = get_save_to_db(self.COLLECTION, self)
        self.delete_from_db = get_delete_from_db(self.COLLECTION, self)
        self.update_in_db = get_update_in_db(self.COLLECTION, self)

    @property
    def get_quote(self):
        retries = 0
        while retries < 4:
            try:
                return kite_context.quote([f"{self.exchange}:{self.stock_name}"])[f"{self.exchange}:{self.stock_name}"][
                    "depth"]
            except:
                sleep(1)
                retries += 1
        return None

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
                    response = requests.get(f"http://{GENERATOR_URL}/price?symbol={self.stock_name}")
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

    @property
    def number_of_days(self):
        """
            If today is a weekday and not a holiday, the number of days would be 1.
            If today is a weekday and a holiday, or if it's a weekend, the number of days would be 0.
        Returns:

        """
        dt_start, until = self.created_at.date(), TODAY.date()
        days = rrule(WEEKLY, byweekday=(MO, TU, WE, TH, FR), dtstart=dt_start, until=until).count()
        for day in load_holidays()['dates']:
            if dt_start < day.date() < until:
                days -= 1
        return days

    def transaction_cost(self, buying_price, selling_price, short=False) -> float:
        if short:
            return IntradayTransactionCost(
                buying_price=buying_price,
                selling_price=selling_price,
                quantity=self.quantity
            ).total_tax_and_charges
        else:
            if self.number_of_days > 1:
                return DeliveryTransactionCost(
                    buying_price=buying_price,
                    selling_price=selling_price,
                    quantity=self.quantity
                ).total_tax_and_charges
            else:
                return IntradayTransactionCost(
                    buying_price=buying_price,
                    selling_price=selling_price,
                    quantity=self.quantity
                ).total_tax_and_charges

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

    def buy_parameters(self):
        amount: float = MAXIMUM_ALLOCATION

        def get_quantity_and_price(s_orders):
            accumulated, quantity = 0, 0
            for item in s_orders:
                for order_no in range(1, item["orders"] + 1):
                    for _ in range(1, item["quantity"] + 1):
                        if accumulated + item["price"] > amount:
                            return quantity, accumulated / quantity
                        accumulated += item["price"]
                        quantity += 1
            return quantity, accumulated / quantity

        if DEBUG:
            if self.latest_price:
                self.quantity, price = int(amount / self.latest_price), self.latest_price
            else:
                self.quantity, price = 0, 0
        else:
            quote: dict = self.get_quote
            sell_orders: list = quote["sell"]
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
            logger.info(f"file not found while updating stock df")
            self.__result_stock_df = None
        stock_df = pd.DataFrame({"price": [current_price]})
        if self.__result_stock_df is not None:
            self.__result_stock_df = pd.concat([self.__result_stock_df, stock_df], ignore_index=True)
        else:
            self.__result_stock_df = stock_df
        self.__result_stock_df.to_csv(f"temp/{self.stock_name}.csv")
        self.__result_stock_df = self.__result_stock_df.bfill().ffill()
        self.__result_stock_df.dropna(axis=1, inplace=True)

    def get_ohlc(self, shift: Shift):
        data = self.__result_stock_df.copy()
        # since a check is needed to verify whether the trend actually reversed or not
        window = 0
        if shift == Shift.MORNING:
            window = 5
        elif shift == Shift.EVENING:
            window = len(data)

        ohlcv_data = pd.DataFrame()

        if shift == Shift.MORNING:
            # Apply a rolling window of 15 minutes
            rolling_data = data['price'].rolling(window=window)
            # Calculate Open, Close, High, and Low prices for each window
            open_price = rolling_data.apply(lambda x: x.iloc[0] if len(x) == window else None)
            close_price = rolling_data.apply(lambda x: x.iloc[-1] if len(x) == window else None)
            high_price = rolling_data.max()
            low_price = rolling_data.min()

            # Create a new DataFrame with these values
            ohlcv_data = pd.DataFrame({
                "Open": open_price,
                "Close": close_price,
                "High": high_price,
                "Low": low_price
            })
            # logger.info(f"ohlc_data: {ohlcv_data}")

        if shift == Shift.EVENING:

            rolling_data = data['price'].rolling(window=window)

            # Calculate Open, Close, High, and Low prices for each window
            open_price = rolling_data.apply(lambda x: x.iloc[0] if len(x) == window else None)
            close_price = rolling_data.apply(lambda x: x.iloc[-1] if len(x) == window else None)
            high_price = rolling_data.max()
            low_price = rolling_data.min()

            # Create a new DataFrame with these values
            ohlcv_data = pd.DataFrame({
                "Open": open_price,
                "Close": close_price,
                "High": high_price,
                "Low": low_price
            })

        # Drop any rows with NaN values which occur at the start of the dataset
        return ohlcv_data.dropna()

    def whether_buy(self, day_based_df, shift: Shift) -> bool:
        """
        Buy the stock if certain conditions are met:
        1. If total buying quantity/ total selling quantity > 0.8 then buy
        2. It has a minimum quantity which you want to buy
        3. The current price is not more than 1% of the trigger price
        :return: True, if buy else false
        """

        logger.info(f"to check whether this function is entered or not")
        logger.info(f"stock df size {self.__result_stock_df}")

        multiindex_columns = day_based_df.columns

        if self.__result_stock_df is None or self.__result_stock_df.shape[0] < 15:
            return False

        default_ohlc = ['Open', 'High', 'Low', 'Close']

        columns_for_level2 = [col for col in multiindex_columns if
                              col[1] == f'{self.stock_name}.BO' and col[0] in default_ohlc]
        d = day_based_df[columns_for_level2]
        d.columns = [col[0] for col in columns_for_level2]

        ohlc_data = self.get_ohlc(shift)

        ohlc_data = pd.concat([d, ohlc_data.iloc[-1:]], ignore_index=True)

        ohlc_data_yes = ohlc_data.copy()
        ohlc_data_no = ohlc_data.copy()

        # if shift == Shift.EVENING:
        candl = BullishEngulfing(target='pattern0')
        ohlc_data_yes = candl.has_pattern(ohlc_data_yes, default_ohlc, False)

        candl = BullishHarami(target='pattern1')
        ohlc_data_yes = candl.has_pattern(ohlc_data_yes, default_ohlc, False)

        candl = MorningStar(target='pattern2')
        ohlc_data_yes = candl.has_pattern(ohlc_data_yes, default_ohlc, False)

        candl = Hammer(target='pattern4')
        ohlc_data_yes = candl.has_pattern(ohlc_data_yes, default_ohlc, False)

        candl = InvertedHammer(target='pattern5')
        ohlc_data_yes = candl.has_pattern(ohlc_data_yes, default_ohlc, False)
        # if shift == Shift.MORNING:
        candl = BearishEngulfing(target='pattern0')
        ohlc_data_no = candl.has_pattern(ohlc_data_no, default_ohlc, False)

        candl = BearishHarami(target='pattern1')
        ohlc_data_no = candl.has_pattern(ohlc_data_no, default_ohlc, False)

        candl = EveningStar(target='pattern2')
        ohlc_data_no = candl.has_pattern(ohlc_data_no, default_ohlc, False)

        regex = re.compile('pattern', re.IGNORECASE)

        # Filter columns where the column name matches the regex pattern
        matching_columns_yes = [col for col in ohlc_data_yes.columns if regex.search(col)]
        matching_columns_no = [col for col in ohlc_data_no.columns if regex.search(col)]

        # if True in list(ohlc_data[matching_columns].iloc[-1]):
        #     logger.info("found")

        if self.__result_stock_df.shape[0] > 15:

            line_df = self.__result_stock_df.copy()
            # line_df = line_df[['Close']]
            line_df.columns = ['price']
            line_df['line'] = line_df.apply(kaufman_indicator)
            line_df['ema'] = line_df.line.ewm(span=5, adjust=False).mean()

            if shift == Shift.EVENING:
                if True in list(ohlc_data_yes[matching_columns_yes].iloc[-1]) and True not in list(ohlc_data_no[matching_columns_no].iloc[-1]):
                    logger.info("entered on whether to buy the stock in evening")
                    if line_df['ema'].iloc[-3] < line_df['ema'].iloc[-6]:
                        return True
            if shift == Shift.MORNING:
                if True in list(ohlc_data_yes[matching_columns_yes].iloc[-1]) and True not in list(ohlc_data_no[matching_columns_no].iloc[-1]):
                    logger.info("entered on whether to buy the stock in morning")
                    if line_df['ema'].iloc[-1] > line_df['ema'].iloc[-3] > line_df['ema'].iloc[-5]:
                        return True
        return False
