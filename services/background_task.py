from datetime import datetime
from asyncio import sleep
from logging import Logger

from models.account import Account
from models.stages.holding import Holding
from models.stages.position import Position
from models.stock_info import StockInfo
from routes.stock_input import chosen_stocks
from utils.logger import get_logger

from constants.settings import END_TIME, SLEEP_INTERVAL, allocation, end_process, START_TIME, STOP_BUYING_TIME

logger: Logger = get_logger(__name__)


async def background_task():
    """
        all the tasks mentioned here will be running in the background
    """
    global logger

    logger.info("BACKGROUND TASK STARTED")

    current_time = datetime.now()

    account: Account = Account()

    # this part will loop till the trading times end
    while current_time < END_TIME:
        await sleep(SLEEP_INTERVAL)

        try:
            """
                if any new stock is added then it will be added in the stock to track
            """
            for chosen_stock in chosen_stocks():
                if chosen_stock not in list(account.stocks_to_track.keys()) and len(account.stocks_to_track) < allocation():
                    account.stocks_to_track[chosen_stock] = StockInfo(chosen_stock, 'NSE')
            """
                update price for all the stocks which are being tracked
            """
            for stock in account.stocks_to_track.keys():
                account.stocks_to_track[stock].update_price()

            if end_process():
                break

            """
                if the time is within trading interval or certain criteria is met then buy stocks
            """
            if START_TIME < current_time < STOP_BUYING_TIME:
                try:
                    # if current_time > START_BUYING_TIME:
                    account.buy_stocks()
                except:
                    pass

            """
                if the trigger for selling is breached in position then sell
            """

            positions_to_delete = []  # this is needed or else it will alter the length during loop

            for position_name in account.positions.keys():
                position: Position = account.positions[position_name]
                if position.breached():
                    logger.info(f" line 89 -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                    positions_to_delete.append(position_name)
                    del account.stocks_to_track[position_name]
                    # traced_stock_list.remove(position_name)

            for position_name in positions_to_delete:
                del account.positions[position_name]

            """
                if the trigger for selling is breached in holding then sell
            """

            holdings_to_delete = []  # this is needed or else it will alter the length during loop

            for holding_name in account.holdings.keys():
                holding: Holding = account.holdings[holding_name]

                if START_TIME < current_time:
                    if holding.breached():
                        logger.info(f" line 89 -->sell {holding.stock.stock_name} at {holding.stock.latest_price}")
                        holdings_to_delete.append(holding_name)
                        del account.stocks_to_track[holding_name]

            for holding_name in holdings_to_delete:
                account.holdings[holding_name]
                del account.holdings[holding_name]


        except:
            logger.exception("Kite error may have happened")

    logger.info("TASK ENDED")
