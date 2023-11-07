from datetime import datetime
from asyncio import sleep
from logging import Logger
import yfinance as yf
import pandas as pd

from models.account import Account
from models.db_models.db_functions import retrieve_all_services, find_by_name
from models.stages.position import Position
from models.stock_info import StockInfo
from routes.stock_input import chosen_stocks
from utils.logger import get_logger
from utils.tracking_components.fetch_prices import fetch_current_prices

from constants.settings import END_TIME, SLEEP_INTERVAL, get_allocation, end_process, START_TIME, STOP_BUYING_TIME, \
    set_allocation, get_max_stocks, set_max_stocks, DEBUG, set_end_process
from utils.tracking_components.select_stocks import select_stocks
from utils.tracking_components.verify_symbols import get_correct_symbol

logger: Logger = get_logger(__name__)


async def background_task():
    """
        all the tasks mentioned here will be running in the background
    """
    global logger

    logger.info("BACKGROUND TASK STARTED")

    current_time = datetime.now()

    account: Account = Account()

    prediction_df, obtained_stock_list = None, await get_correct_symbol()

    not_loaded = True
    filtered_stocks, selected_stocks = [], []

    logger.info(f"{account.available_cash}")

    """
    START OF DAY ACTIVITIES
    """
    stock_list: list[StockInfo] = await retrieve_all_services(StockInfo.COLLECTION, StockInfo)
    logger.info(f"{stock_list}")

    logger.info(f"{account.available_cash}")

    # fetch all the stocks already added in stock list
    for stock_obj in stock_list:
        stock_obj.first_load = False
        account.stocks_to_track[stock_obj.stock_name] = stock_obj

    # load all holdings from the database
    await account.load_holdings()

    initial_list_of_holdings = account.holdings.keys()

    account.convert_holdings_to_positions()

    # TODO: delete all flagged stock which has been sold yesterday
    #   delete the csv file for all the price data tracked

    # this part will loop till the trading times end
    while current_time < END_TIME:
        await sleep(SLEEP_INTERVAL)

        current_time = datetime.now()

        try:
            if not_loaded and current_time >= START_TIME:
                obtained_stock_list = [st for st in obtained_stock_list if '-BE' not in st]
                prediction_df = yf.download(tickers=[f"{st}.NS"for st in obtained_stock_list], period='1wk', interval='1m', progress=False)['Close']
                prediction_df.index = pd.to_datetime(prediction_df.index)
                prediction_df = prediction_df.loc["2023-10-31"]
                available_cash = 30000
                set_allocation(10000)
                set_max_stocks(3)
                prediction_df.reset_index(drop=True, inplace=True)
                prediction_df = prediction_df.ffill().bfill().dropna(axis=1)
                filtered_stocks = [i[:-3] for i in list(prediction_df.columns)]

                logger.info(f"list of stocks: {filtered_stocks}")
                logger.info(f"allocation: {get_allocation()}")
                not_loaded = False

            """
                if any new stock is added then it will be added in the stock to track
            """
            for chosen_stock in chosen_stocks():
                if chosen_stock not in list(account.stocks_to_track.keys()) and len(account.stocks_to_track) < get_allocation():
                    account.stocks_to_track[chosen_stock] = StockInfo(chosen_stock, 'NSE')

            if end_process():
                break

            """
                if the time is within trading interval or certain criteria is met then buy stocks
            """

            if START_TIME < current_time < STOP_BUYING_TIME:
                # update the prediction_df after every interval
                new_cost_df = await fetch_current_prices(filtered_stocks)
                if new_cost_df is None:
                    set_end_process(True)
                else:
                    prediction_df = pd.concat([prediction_df, new_cost_df], ignore_index=True)
                    prediction_df = prediction_df.ffill().bfill()

                selected_stocks = [st[:-3] for st in select_stocks(prediction_df)]

                # selecting stock which meets the criteria
                for stock_col in selected_stocks:
                    if len(account.stocks_to_track) < get_max_stocks() and stock_col not in account.stocks_to_track.keys():
                        account.stocks_to_track[stock_col] = StockInfo(stock_col, 'NSE')
                        account.stocks_to_track[stock_col].remaining_allocation = get_allocation()
                        if not DEBUG:

                            sell_orders: list = account.stocks_to_track[stock_col].get_quote["sell"]
                            zero_quantity = True
                            for item in sell_orders:
                                if item['quantity'] > 0:
                                    zero_quantity = False
                                break
                            if zero_quantity:
                                continue
                        _, _2 = account.stocks_to_track[stock_col].buy_parameters()
                        # stock_df = prediction_df[[f"{stock_col}.NS"]]
                        # stock_df.reset_index(inplace=True)
                        # stock_df = stock_df[[f"{stock_col}.NS"]].bfill().ffill()
                        # stock_df.columns = ['price']
                        # stock_df.to_csv(f"temp/{stock_col}.csv")

                """
                    update price for all the stocks which are being tracked
                """

                for stock in account.stocks_to_track.keys():
                    account.stocks_to_track[stock].update_price()
                    # because the instance of the stock stored in position is not the same stored in stocks_to_track
                    if stock in account.positions.keys():
                        account.positions[stock].stock = account.stocks_to_track[stock]

                try:
                    account.buy_stocks()
                except:
                    pass

            """
                if the trigger for selling is breached in position then sell
            """

            positions_to_delete = []  # this is needed or else it will alter the length during loop

            for position_name in account.positions.keys():
                position: Position = account.positions[position_name]
                status = position.breached()
                match status:
                    case "DAY1BREACHED":
                        logger.info(f" DAY1BREACHED -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                        positions_to_delete.append(position_name)
                        logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                        del account.stocks_to_track[position_name]  # delete from stocks to track
                        filtered_stocks.remove(position_name)
                    case "DAY1NOT":
                        logger.info(f" DAY1NOT -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                        account.stocks_to_track[position_name].in_position = False
                        positions_to_delete.append(position_name)

            for position_name in positions_to_delete:
                del account.positions[position_name]

            if DEBUG:
                if len(account.stocks_to_track) == 0:
                    set_end_process(True)

        except:
            logger.exception("Kite error may have happened")

    wallet_list = {st: account.stocks_to_track[st].wallet for st in account.stocks_to_track.keys()}
    logger.info(f" remaining stocks wallet : {wallet_list}")

    """
        END OF DAY ACTIVITIES
    """

    # stock information is stored in the db.
    # this is from an older code where the holdings weren't present, but the stock was tracked for more than 1 day.
    for stock_key in account.stocks_to_track.keys():
        """
            if the remaining stock to track is already available update it or else add a new record in db
        """
        stock_model = await find_by_name(StockInfo.COLLECTION, StockInfo, {"stock_name": f"{stock_key}"})
        if stock_model is None:
            await account.stocks_to_track[stock_key].save_to_db()
        else:
            await account.stocks_to_track[stock_key].update_in_db()

    # Since only positions are stored in the database, so first positions are converted to holdings.
    # After which new holdings are added and old holdings are updated.
    await account.store_all_holdings()

    # deleting all holding data from db which have been sold
    await account.remove_all_sold_holdings(initial_list_of_holdings)

    logger.info("TASK ENDED")
