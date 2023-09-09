from datetime import datetime
from asyncio import sleep
from logging import Logger

from models.stock_info import StockInfo
from routes.stock_input import chosen_stocks
from utils.logger import get_logger

from constants.settings import END_TIME, SLEEP_INTERVAL, allocation

logger: Logger = get_logger(__name__)


async def background_task():
    """
        all the tasks mentioned here will be running in the background
    """
    global logger

    logger.info("BACKGROUND TASK STARTED")

    current_time = datetime.now()

    stocks_to_track: dict[str, StockInfo] = {}

    # this part will loop till the trading times end
    while current_time < END_TIME:
        await sleep(SLEEP_INTERVAL)

        try:
            """
                if any new stock is added then it will be added in the stock to track
            """
            for chosen_stock in chosen_stocks():
                if chosen_stock not in list(stocks_to_track.keys()) and len(stocks_to_track) < allocation():
                    stocks_to_track[chosen_stock] = StockInfo(chosen_stock, 'NSE')

        except:
            logger.exception("Kite error may have happened")

    logger.info("TASK ENDED")
