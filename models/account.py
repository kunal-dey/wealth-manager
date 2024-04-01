from dataclasses import dataclass, field
from logging import Logger

from constants.enums.position_type import PositionType
from constants.enums.product_type import ProductType
from constants.settings import DEBUG, STARTING_CASH, get_allocation
from constants.global_contexts import kite_context
from models.db_models.db_functions import retrieve_all_services, jsonify, find_by_name
from models.stages.holding import Holding
from models.stages.position import Position
from models.stock_info import StockInfo
from utils.logger import get_logger
from utils.take_position import long, short

logger: Logger = get_logger(__name__)


def get_available_cash():
    if DEBUG:
        return STARTING_CASH
    else:
        payload = kite_context.margins()
        return payload['equity']['available']['live_balance']


@dataclass
class Account:
    stocks_to_track: dict[str, StockInfo] = field(default_factory=dict, init=False)
    positions: dict[str, Position] = field(default_factory=dict, init=False)
    short_positions: dict[str, Position] = field(default_factory=dict, init=False)
    holdings: dict[str, Holding] = field(default_factory=dict, init=False)
    available_cash: float = field(default_factory=get_available_cash)
    starting_cash: float = field(default_factory=get_available_cash)
    short_stocks_to_track: dict[str, StockInfo] = field(default_factory=dict, init=False)

    async def load_holdings(self):
        """
        to load all holdings from database
        :return:
        """
        holding_list: list[Holding] = await retrieve_all_services(Holding.COLLECTION, Holding)
        logger.info(f"{[jsonify(holding) for holding in holding_list]}")

        # after the stock is put in holding the price may be lower or higher than the one bought earlier
        # this is to update the position price so that correct price is used for trading session

        holdings_from_api = {}

        for holding in kite_context.holdings():
            holdings_from_api[holding['tradingsymbol']] = holding['average_price']

        for holding_obj in holding_list:
            self.holdings[holding_obj.stock.stock_name] = holding_obj
            self.starting_cash += get_allocation()/3
            if holding_obj.stock.stock_name in list(self.stocks_to_track.keys()):
                self.holdings[holding_obj.stock.stock_name].stock = self.stocks_to_track[holding_obj.stock.stock_name]
                self.holdings[holding_obj.stock.stock_name].stock.quantity = self.holdings[holding_obj.stock.stock_name].quantity
                self.holdings[holding_obj.stock.stock_name].position_price = holdings_from_api[holding_obj.stock.stock_name]

    def buy_stocks(self):
        """
        if it satisfies all the buying criteria then it buys the stock
        :return: None
        """
        for stock_key in list(self.stocks_to_track.keys()):
            logger.info(f"{stock_key}: allocation {self.stocks_to_track[stock_key].remaining_allocation}")
            if self.stocks_to_track[stock_key].remaining_allocation > 0:
                if not DEBUG:
                    sell_orders: list = self.stocks_to_track[stock_key].get_quote["sell"]
                    logger.info(f"{stock_key}: sell_orders {sell_orders}")
                    zero_quantity = True
                    for item in sell_orders:
                        if item['quantity'] > 0:
                            zero_quantity = False
                            break
                    if zero_quantity:
                        continue
                logger.info(f"{stock_key}: get parameters")

                quantity, buy_price = self.stocks_to_track[stock_key].buy_parameters()
                logger.info(f"{stock_key}: {quantity} {buy_price}")
                if self.stocks_to_track[stock_key].whether_buy():
                    if long(
                        symbol=self.stocks_to_track[stock_key].stock_name,
                        quantity=int(quantity),
                        product_type=ProductType.DELIVERY,
                        exchange=self.stocks_to_track[stock_key].exchange
                    ):
                        logger.info(f"{self.stocks_to_track[stock_key].stock_name} has been bought @ {buy_price}.")

                        self.stocks_to_track[stock_key].in_position = True  # now it will look for buy orders

                        if self.stocks_to_track[stock_key].first_load:
                            self.positions[stock_key] = Position(
                                position_price=buy_price,
                                stock=self.stocks_to_track[stock_key],
                                position_type=PositionType.LONG,
                                quantity=int(quantity),
                                product_type=ProductType.DELIVERY
                            )
                            self.stocks_to_track[stock_key].remaining_allocation -= self.stocks_to_track[stock_key].remaining_allocation*(2/3)
                        else:
                            num = self.stocks_to_track[stock_key].last_buy_price*self.stocks_to_track[stock_key].last_quantity + buy_price * quantity
                            total_quantity = self.stocks_to_track[stock_key].last_quantity + quantity
                            avg_price = num/total_quantity
                            self.positions[stock_key] = Position(
                                position_price=avg_price,
                                stock=self.stocks_to_track[stock_key],
                                position_type=PositionType.LONG,
                                quantity=int(total_quantity),
                                product_type=ProductType.DELIVERY
                            )
                            self.stocks_to_track[stock_key].remaining_allocation = 0

                        # if this is encountered first time then it will make it false else always make it false
                        # earlier this was in stock info, but it has been moved since if there is an error in buying,
                        # then it does not buy the stock and make it false. so if the stock is increasing then false will
                        # never enter and there will be a loss
                        self.stocks_to_track[stock_key].first_load = False
                        self.stocks_to_track[stock_key].last_buy_price = buy_price
                        self.stocks_to_track[stock_key].last_quantity = quantity

    def short_stocks(self):
        for stock_key in list(self.stocks_to_track.keys()):
            logger.info(f"{stock_key}: {self.stocks_to_track[stock_key].first_load} {self.stocks_to_track[stock_key].remaining_allocation}")
            if not self.stocks_to_track[stock_key].first_load and self.stocks_to_track[stock_key].remaining_allocation > 0:
                logger.info(f"{stock_key}: get short parameters")
                quantity, sell_price = self.stocks_to_track[stock_key].short_parameters()
                logger.info(f"{stock_key}: {quantity} {sell_price}")
                logger.info(f"{stock_key}: condition {self.positions.keys()} {self.short_positions.keys()}")
                if stock_key in self.positions.keys() and stock_key not in self.short_positions.keys():
                    if self.stocks_to_track[stock_key].whether_short():
                        if short(
                            symbol=self.stocks_to_track[stock_key].stock_name,
                            quantity=int(quantity),
                            product_type=ProductType.INTRADAY,
                            exchange=self.stocks_to_track[stock_key].exchange
                        ):
                            logger.info(f"{self.stocks_to_track[stock_key].stock_name} has has been short @ {sell_price}.")
                            self.short_positions[stock_key] = Position(
                                    position_price=sell_price,
                                    stock=self.stocks_to_track[stock_key],
                                    position_type=PositionType.SHORT,
                                    quantity=int(quantity),
                                    product_type=ProductType.INTRADAY
                                )

    def convert_positions_to_holdings(self):
        """
        This method converts all the positions of the day into holdings which can be loaded next day.

        Since only holdings are stored so positions are converted into holdings
        :return: None
        """
        self.holdings = {}
        for position_key in self.positions.keys():
            position = self.positions[position_key]
            self.holdings[position_key] = Holding(
                position_price=position.position_price,
                quantity=position.quantity,
                product_type=position.product_type,
                position_type=position.position_type,
                stock=position.stock
            )

    def convert_holdings_to_positions(self):
        """
        This method converts all the positions of the day into holdings which can be loaded next day.

        Since only holdings are stored so positions are converted into holdings
        :return: None
        """
        self.positions = {}
        for holding_key in self.holdings.keys():
            holding = self.holdings[holding_key]
            self.positions[holding_key] = Position(
                position_price=holding.position_price,
                quantity=holding.quantity,
                product_type=holding.product_type,
                position_type=holding.position_type,
                stock=holding.stock
            )

    async def store_all_holdings(self):
        """
        To store all the holding information in db to be used on the next day
        :return: None
        """
        # storing all positions as holdings for the next day
        self.convert_positions_to_holdings()

        for holding_key in self.holdings.keys():
            holding_model: Holding = await find_by_name(Holding.COLLECTION, Holding, {"stock.stock_name": f"{holding_key}"})

            if holding_model is None:
                await self.holdings[holding_key].save_to_db()
            else:
                self.holdings[holding_key].object_id = holding_model.object_id  # because while updating it will take the id of the holding
                await self.holdings[holding_key].update_in_db()

    async def remove_all_sold_holdings(self, initial_list_of_holdings):
        """
        if any holding is sold, then that holding data is removed from the db
        :param initial_list_of_holdings: list[str]
        :return:
        """
        for holding_key in initial_list_of_holdings:
            if holding_key not in list(self.holdings.keys()):
                holding_model = await find_by_name(Holding.COLLECTION, Holding, {"stock.stock_name": f"{holding_key}"})
                await holding_model.delete_from_db()

    async def remove_all_sold_stocks(self, initial_list_of_stocks):
        """
        if any holding is sold, then that holding data is removed from the db
        :param initial_list_of_stocks: list[str]
        :return:
        """
        for stock_key in initial_list_of_stocks:
            if stock_key not in list(self.stocks_to_track.keys()):
                stock_model = await find_by_name(StockInfo.COLLECTION, StockInfo, {"stock_name": f"{stock_key}"})
                await stock_model.delete_from_db()


