import json
from dataclasses import dataclass, field
from logging import Logger

from constants.enums.position_type import PositionType
from constants.enums.product_type import ProductType
from constants.settings import DEBUG
from constants.global_contexts import kite_context
from models.db_models.db_functions import retrieve_all_services, jsonify, find_by_name
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

    async def load_holdings(self):
        """
        to load all holdings from database
        :return:
        """
        holding_list: list[Holding] = await retrieve_all_services(Holding.COLLECTION, Holding)
        logger.info(f"{[jsonify(holding) for holding in holding_list]}")

        for holding_obj in holding_list:
            self.holdings[holding_obj.stock.stock_name] = holding_obj
            if holding_obj.stock.stock_name in list(self.stocks_to_track.keys()):
                self.holdings[holding_obj.stock.stock_name].stock = self.stocks_to_track[holding_obj.stock.stock_name]
                self.holdings[holding_obj.stock.stock_name].stock.quantity = self.holdings[holding_obj.stock.stock_name].quantity

    def buy_stocks(self):
        """
        if it satisfies all the buying criteria then it buys the stock
        :return: None
        """
        for stock_key in list(self.stocks_to_track.keys()):
            # if stock_key not in self.positions.keys() and stock_key not in self.holdings.keys():
            if self.stocks_to_track[stock_key].remaining_allocation > 0:
                if not DEBUG:
                    sell_orders: list = self.stocks_to_track[stock_key].get_quote["sell"]
                    zero_quantity = True
                    for item in sell_orders:
                        if item['quantity'] > 0:
                            zero_quantity = False
                            break
                    if zero_quantity:
                        continue

                quantity, buy_price = self.stocks_to_track[stock_key].buy_parameters()
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
                                buy_price=buy_price,
                                stock=self.stocks_to_track[stock_key],
                                position_type=PositionType.LONG,
                                position_price=self.stocks_to_track[stock_key].latest_indicator_price if self.stocks_to_track[stock_key].latest_indicator_price else buy_price,
                                quantity=int(quantity),
                                product_type=ProductType.DELIVERY
                            )
                            self.stocks_to_track[stock_key].remaining_allocation -= self.stocks_to_track[stock_key].remaining_allocation/3
                        else:
                            num = self.stocks_to_track[stock_key].last_buy_price*self.stocks_to_track[stock_key].last_quantity + buy_price * quantity
                            total_quantity = self.stocks_to_track[stock_key].last_quantity + quantity
                            avg_price = num/total_quantity
                            self.positions[stock_key] = Position(
                                buy_price=avg_price,
                                stock=self.stocks_to_track[stock_key],
                                position_type=PositionType.LONG,
                                position_price=self.stocks_to_track[stock_key].latest_indicator_price if self.stocks_to_track[stock_key].latest_indicator_price else buy_price,
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

    def convert_positions_to_holdings(self):
        """
        This method converts all the positions of the day into holdings which can be loaded next day.

        Since only holdings are stored so positions are converted into holdings
        :return: None
        """
        for position_key in self.positions.keys():
            position = self.positions[position_key]
            self.holdings[position_key] = Holding(
                buy_price=position.buy_price,
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
        for holding_key in self.holdings.keys():
            holding = self.holdings[holding_key]
            self.positions[holding_key] = Position(
                buy_price=holding.buy_price,
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
            holding_model = await find_by_name(Holding.COLLECTION, Holding, {"stock.stock_name": f"{holding_key}"})

            if holding_model is None:
                await self.holdings[holding_key].save_to_db()
            else:
                await self.holdings[holding_key].update_in_db()

    async def remove_all_sold_holdings(self, initial_list_of_holdings):
        """
        if any holding is sold, then that holding data is removed from the db
        :param initial_list_of_holdings: list[str]
        :return:
        """
        for holding_key in initial_list_of_holdings:
            if holding_key not in list(self.holdings.keys()):
                await self.holdings[holding_key].delete_from_db(search_dict={'symbol': holding_key})

    @property
    def available_cash(self):
        payload = kite_context.margins()
        return payload['equity']['available']['live_balance']
