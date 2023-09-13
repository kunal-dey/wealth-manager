from dataclasses import dataclass, field, asdict
from logging import Logger

from constants.enums.position_type import PositionType
from constants.enums.product_type import ProductType
from models.stages.holding import Holding
from models.stages.position import Position
from models.stock_info import StockInfo
from utils.logger import get_logger
from utils.take_position import long

logger: Logger = get_logger(__name__)


@dataclass
class Account:
    stocks_to_track: dict[str, StockInfo] = field(default_factory=dict, init=False)
    positions: dict[str, Position] = field(default_factory=dict, init=False)
    holdings: dict[str, Holding] = field(default_factory=dict, init=False)

    def buy_stocks(self):
        """
        if it satisfies all the buying criteria then it buys the stock
        :return: None
        """

        for stock_key in self.stocks_to_track:
            if stock_key not in self.positions.keys() and stock_key not in self.holdings.keys():
                buy_price = self.stocks_to_track[stock_key].latest_price
                quantity = 10000/buy_price
                if self.stocks_to_track[stock_key].whether_buy():
                    if long(
                        symbol=self.stocks_to_track[stock_key].stock_name,
                        quantity=int(quantity),
                        product_type=ProductType.DELIVERY,
                        exchange=self.stocks_to_track[stock_key].exchange
                    ):
                        logger.info(f"{self.stocks_to_track[stock_key].stock_name} has been bought @ {self.stocks_to_track[stock_key].latest_price}.")
                        self.positions[stock_key] = Position(
                            buy_price=buy_price,
                            stock=self.stocks_to_track[stock_key],
                            position_type=PositionType.LONG,
                            position_price=buy_price,
                            quantity=int(quantity),
                            product_type=ProductType.DELIVERY
                        )





