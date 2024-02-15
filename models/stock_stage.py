from models.stock_info import StockInfo
from dataclasses import dataclass, field

from logging import Logger

from utils.logger import get_logger

from constants.enums.position_type import PositionType
from constants.enums.product_type import ProductType
from constants.settings import DELIVERY_INITIAL_RETURN, DELIVERY_INCREMENTAL_RETURN, EXPECTED_MINIMUM_MONTHLY_RETURN, DEBUG

from utils.take_position import short, long

logger: Logger = get_logger(__name__)


@dataclass
class Stage:
    position_price: float
    quantity: int
    product_type: ProductType
    position_type: PositionType
    stock: None | StockInfo = None
    current_price: float = field(default=None, init=False)
    last_price: float = field(default=None, init=False)
    trigger: float | None = field(default=None, init=False)
    cost: float = field(default=None, init=False)

    base_return: float = field(default=DELIVERY_INITIAL_RETURN, init=False)

    @property
    def current_expected_return(self):
        if self.position_type == PositionType.LONG:
            if self.stock.number_of_days >= 2:
                # if accumulated return > 0.03 then return 0.03 else accumulated return
                returns = min(
                    ((1 + DELIVERY_INITIAL_RETURN) ** (self.stock.number_of_days+1)) - 1,
                    EXPECTED_MINIMUM_MONTHLY_RETURN
                )
                return 3/2 * returns if self.stock.remaining_allocation > 0 else returns
            else:
                return DELIVERY_INITIAL_RETURN * (3/2 if self.stock.remaining_allocation > 0 else 1)
        else:
            return 0.002

    @property
    def incremental_return(self):
        return DELIVERY_INCREMENTAL_RETURN

    def set_trigger(self, stock_price: float):
        """
            in case of cumulative position the cost is given by
            cost = sum_i(b_i*q_i)/sum_i(q_i)  + sum_i(f(b_i, q_i, s))/sum_i(q_i)
            where f is an intraday function
            i -> 1 to n.
            n being the number of positions
            b_i is the buying price of the ith position
            q_i is the quantity bought for the ith position

            this can be divided in average buying price(A) + average transaction cost (B)

            Since the cumulative only have LONG as of now so the code for short selling is unchanged
        """
        global logger
        cost: float | None = None
        selling_price: float | None = None
        buy_price: float | None = None

        if self.position_type == PositionType.LONG:
            # this handles part A
            buy_price = self.position_price
            selling_price = stock_price
        else:
            buy_price = stock_price
            selling_price = self.position_price

        # this handles the B part
        tx_cost = self.stock.transaction_cost(buying_price=buy_price, selling_price=selling_price) / self.quantity

        logger.info(f"the total transaction cost for {self.stock.stock_name} is {tx_cost * self.quantity}")

        cost = buy_price + tx_cost
        self.cost = cost

        counter = 1
        earlier_trigger = self.trigger

        logger.info(f"current : {self.current_expected_return}")

        # this section iterates and finds the current trigger achieved
        logger.info(
            f"cost tracking {cost * (1 + self.current_expected_return + counter * self.incremental_return)}")
        while cost * (1 + self.current_expected_return + counter * self.incremental_return) < selling_price:
            if self.position_type == PositionType.SHORT:

                self.trigger = selling_price / (
                        1 + self.current_expected_return + counter * self.incremental_return)
            else:
                self.trigger = cost * (1 + self.current_expected_return + counter * self.incremental_return)
            counter += 1

        if earlier_trigger is not None:
            if self.position_type == PositionType.LONG:
                if stock_price > self.trigger:
                    self.trigger = stock_price
                if earlier_trigger > self.trigger:
                    self.trigger = earlier_trigger
                if cost * (1 + self.current_expected_return) > stock_price:
                    self.trigger = None
            else:
                if stock_price < self.trigger:
                    self.trigger = stock_price
                if earlier_trigger < self.trigger:
                    self.trigger = earlier_trigger

        # if self.trigger:
        #     logger.info(
        #         f"current return for {self.stock.stock_name} is  {(self.trigger / (cost - (self.stock.wallet / self.quantity))) - 1}")

    def sell(self):
        # this has been done because if there is error while selling it still says it sold
        # suppose the stock is not even bought but still it tries to sell in that case it may fail
        if short(
                symbol=self.stock.stock_name,
                quantity=self.quantity,
                product_type=self.product_type,
                exchange=self.stock.exchange):
            logger.info(f"Selling {self.stock.stock_name} at {self.current_price} Quantity:{self.quantity}")
            buy_price = self.position_price
            selling_price = self.current_price
            tx_cost = self.stock.transaction_cost(buying_price=buy_price, selling_price=selling_price) / self.quantity
            wallet_value = selling_price - (buy_price + tx_cost)
            self.stock.wallet = wallet_value * self.quantity
            logger.info(f"Wallet: {self.stock.wallet}")
            return True
        return False

    def buy_short(self):
        if long(
                symbol=self.stock.stock_name,
                quantity=self.quantity,
                product_type=self.product_type,
                exchange=self.stock.exchange):
            logger.info(f"Bought short {self.stock.stock_name} at {self.current_price} Quantity:{self.quantity}")
            buy_price = self.current_price
            selling_price = self.position_price
            tx_cost = self.stock.transaction_cost(buying_price=buy_price, selling_price=selling_price) / self.quantity
            wallet_value = selling_price - (buy_price + tx_cost)
            self.stock.wallet = wallet_value * self.quantity
            logger.info(f"Wallet: {self.stock.wallet}")
            return True
        return False

    def breached(self):
        """
            if the current price is less than the previous trigger, then it sells else it updates the trigger
        """
        global logger

        if DEBUG:
            latest_price = self.stock.latest_price
        else:
            latest_price = self.stock.current_price  # the latest price can be None or float

        if latest_price:
            self.last_price = self.current_price
            self.current_price = latest_price

        if self.current_price is None and self.last_price is None:
            return "CONTINUE"

        # if the position was long then on achieving the trigger, it should sell otherwise it should buy
        # to clear the position
        if (self.position_type == PositionType.LONG) and (self.current_price is not None):
            buy_price = self.position_price
            selling_price = self.current_price
            tx_cost = self.stock.transaction_cost(buying_price=buy_price, selling_price=selling_price) / self.quantity
            wallet_value = selling_price - (buy_price + tx_cost)
            logger.info(f"Wallet: {wallet_value}")
            logger.info(f"{self.stock.stock_name} Earlier trigger:  {self.trigger}, latest price:{self.current_price}")
            if self.trigger is not None:
                # if it hits trigger then square off else reset a new trigger
                if self.cost * (1 + self.current_expected_return + (1/2)*self.incremental_return) < self.current_price < self.trigger/(1+(1/2)*self.incremental_return):
                    if DEBUG:
                        if self.sell():
                            if self.stock.number_of_days <= 1:
                                return "DAY1BREACHED"
                            else:
                                return "DAYNBREACHED"
                    else:
                        b_orders: list = self.stock.get_quote["buy"]
                        if sum([order['orders'] * order['quantity'] for order in b_orders]) > self.quantity:
                            if self.sell():
                                if self.stock.number_of_days <= 1:
                                    return "DAY1BREACHED"
                                else:
                                    return "DAYNBREACHED"
            self.set_trigger(self.current_price)
            return "CONTINUE"

        # if the position was short then on achieving the trigger, it should buy
        # to clear the position
        if (self.position_type == PositionType.SHORT) and (self.current_price is not None):
            buy_price = self.current_price
            selling_price = self.position_price
            tx_cost = self.stock.transaction_cost(buying_price=buy_price, selling_price=selling_price) / self.quantity
            wallet_value = selling_price - (buy_price + tx_cost)
            logger.info(f"Wallet: {wallet_value}")
            logger.info(f"{self.stock.stock_name} Earlier short trigger:  {self.trigger}, latest price:{self.current_price}")
            if self.stock.last_buy_price * 1.005 < self.current_price:
                if DEBUG:
                    if self.buy_short():
                        return "LOSS"
                else:
                    s_orders: list = self.stock.get_quote["sell"]
                    if sum([order['orders'] * order['quantity'] for order in s_orders]) > self.quantity:
                        if self.buy_short():
                            return "LOSS"

            if self.stock.latest_price * 1.08 < self.stock.last_buy_price:
                if DEBUG:
                    if self.buy_short():
                        return "BUY_ANOTHER"
                else:
                    s_orders: list = self.stock.get_quote["sell"]
                    if sum([order['orders'] * order['quantity'] for order in s_orders]) > self.quantity:
                        if self.buy_short():
                            return "BUY_ANOTHER"
            if self.trigger is not None:
                if self.current_price > self.trigger*(1+(1/2)*self.incremental_return):
                    if DEBUG:
                        if self.buy_short():
                            return "SQUARED_OFF"
                    else:
                        s_orders: list = self.stock.get_quote["sell"]
                        if sum([order['orders'] * order['quantity'] for order in s_orders]) > self.quantity:
                            if self.buy_short():
                                return "SQUARED_OFF"
            self.set_trigger(self.current_price)
            return "CONTINUE"
