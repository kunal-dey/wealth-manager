from datetime import datetime
from asyncio import sleep
from logging import Logger

from utils.logger import get_logger

from constants.settings import END_TIME, SLEEP_INTERVAL

logger: Logger = get_logger(__name__)


async def background_task():
    """
        all the tasks mentioned here will be running in the background
    """
    global logger

    logger.info("BACKGROUND TASK STARTED")

    current_time = datetime.now()

    # this part will loop till the trading times end
    while current_time < END_TIME:
        await sleep(SLEEP_INTERVAL)

    logger.info("TASK ENDED")
