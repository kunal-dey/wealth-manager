from datetime import datetime
from asyncio import sleep
from logging import Logger
import yfinance as yf
import pandas as pd

from models.account import Account
from models.db_models.db_functions import retrieve_all_services, find_by_name
from models.stages.position import Position
from models.stock_info import StockInfo
from utils.logger import get_logger
from utils.tracking_components.fetch_prices import fetch_current_prices

from constants.settings import END_TIME, SLEEP_INTERVAL, get_allocation, end_process, START_TIME, get_max_stocks, set_max_stocks, DEBUG, set_end_process, DELIVERY_INITIAL_RETURN
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
    
    # variable to check if minimum return of 0.005 is obtained in a day then free 2/3 of the portfolio for next day
    today_profit = 0

    """
    START OF DAY ACTIVITIES
    """
    stock_list: list[StockInfo] = await retrieve_all_services(StockInfo.COLLECTION, StockInfo)
    logger.info(f"{stock_list}")

    # fetch all the stocks already added in stock list
    for stock_obj in stock_list:
        stock_obj.first_load = False
        account.stocks_to_track[stock_obj.stock_name] = stock_obj

    logger.info(f"remaining { [(st.stock_name,st.remaining_allocation) for st in stock_list]}")

    # ignoring all two third stores
    if DEBUG:
        account.available_cash = account.available_cash - len(stock_list)*get_allocation()
    else:
        account.available_cash = account.available_cash - sum([st.remaining_allocation for st in stock_list])
    # load all holdings from the database
    await account.load_holdings()

    try:
        prediction_df = pd.read_csv(f"temp/prediction_df.csv")
        prediction_df.drop(prediction_df.columns[0], axis=1, inplace=True)
    except FileNotFoundError:
        prediction_df = None
    if prediction_df is None:
        logger.info("should not enter")
        prediction_df = yf.download(tickers=[f"{st}.NS"for st in obtained_stock_list], period='1wk', interval='1m', progress=False)['Close']
        prediction_df.index = pd.to_datetime(prediction_df.index)
        prediction_df = prediction_df.loc[:"2023-11-16"]
        prediction_df.reset_index(drop=True, inplace=True)
        prediction_df = prediction_df.ffill().bfill().dropna(axis=1)

    initial_list_of_holdings = list(account.holdings.keys())
    initial_list_of_stocks = list(account.stocks_to_track.keys())

    account.convert_holdings_to_positions()

    logger.info(f"starting cash : {account.available_cash}")

    # TODO: delete all flagged stock which has been sold yesterday
    #   delete the csv file for all the price data tracked

    # this part will loop till the trading times end
    while current_time < END_TIME:
        current_time = datetime.now()
        if START_TIME < current_time:
            await sleep(SLEEP_INTERVAL)
        else:
            await sleep(1)

        try:
            if not_loaded and current_time >= START_TIME:
                obtained_stock_list = [st for st in obtained_stock_list if '-BE' not in st]
                filtered_stocks = [i[:-3] for i in list(prediction_df.columns)]

                logger.info(f"list of stocks: {obtained_stock_list}")
                logger.info(f"allocation: {get_allocation()}")
                not_loaded = False

            if end_process():
                break

            """
                if the time is within trading interval or certain criteria is met then buy stocks
            """

            if START_TIME < current_time:
                # update the prediction_df after every interval
                new_cost_df = await fetch_current_prices(filtered_stocks)
                if new_cost_df is None:
                    set_end_process(True)
                else:
                    prediction_df = pd.concat([prediction_df, new_cost_df], ignore_index=True)
                    prediction_df = prediction_df.bfill().ffill()
                    prediction_df.dropna(axis=1, inplace=True)
                    prediction_df.to_csv(f"temp/prediction_df.csv")

                selected_stocks = [st[:-3] for st in select_stocks(prediction_df)]
                logger.info(f"chosen : {selected_stocks}")

                logger.info(f"starting cash : {account.starting_cash}")
                logger.info(f"available cash : {account.available_cash}")

                logger.info(f"track: {account.stocks_to_track.keys()}")

                # selecting stock which meets the criteria
                for stock_col in selected_stocks:
                    # available cash keeps on changing so max_stocks keeps on changing
                    set_max_stocks(int(account.available_cash/get_allocation()))
                    if 0 < get_max_stocks() and stock_col not in account.stocks_to_track.keys():
                        # if wealth has crossed 0.005 then keep 1/3rd of stocks
                        if today_profit > account.starting_cash*0.005:
                            if ((2/3)*account.starting_cash + get_allocation()) <= account.available_cash:
                        
                                raw_stock = StockInfo(stock_col, 'NSE')
                                if not DEBUG:

                                    sell_orders: list = raw_stock.get_quote["sell"]
                                    zero_quantity = True
                                    for item in sell_orders:
                                        if item['quantity'] > 0:
                                            zero_quantity = False
                                        break
                                    if zero_quantity:
                                        continue
                                account.stocks_to_track[stock_col] = raw_stock
                                account.stocks_to_track[stock_col].remaining_allocation = get_allocation()
                                # even if it may seem that allocation is reduced when bought, actual change is while adding the
                                # stock in stocks to track
                                account.available_cash -= get_allocation()
                                _, _2 = account.stocks_to_track[stock_col].buy_parameters()
                                stock_df = prediction_df[[f"{stock_col}.NS"]]
                                stock_df.reset_index(inplace=True, drop=True)
                                stock_df = stock_df[[f"{stock_col}.NS"]].bfill().ffill()
                                stock_df.columns = ['price']
                                stock_df.to_csv(f"temp/{stock_col}.csv")
                        else:
                            raw_stock = StockInfo(stock_col, 'NSE')
                            if not DEBUG:

                                sell_orders: list = raw_stock.get_quote["sell"]
                                zero_quantity = True
                                for item in sell_orders:
                                    if item['quantity'] > 0:
                                        zero_quantity = False
                                    break
                                if zero_quantity:
                                    continue
                            account.stocks_to_track[stock_col] = raw_stock
                            account.stocks_to_track[stock_col].remaining_allocation = get_allocation()
                            # even if it may seem that allocation is reduced when bought, actual change is while adding the
                            # stock in stocks to track
                            account.available_cash -= get_allocation()
                            _, _2 = account.stocks_to_track[stock_col].buy_parameters()
                            stock_df = prediction_df[[f"{stock_col}.NS"]]
                            stock_df.reset_index(inplace=True, drop=True)
                            stock_df = stock_df[[f"{stock_col}.NS"]].bfill().ffill()
                            stock_df.columns = ['price']
                            stock_df.to_csv(f"temp/{stock_col}.csv")

                """
                    update price for all the stocks which are being tracked
                """

                for stock in account.stocks_to_track.keys():
                    account.stocks_to_track[stock].update_price()

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
                            account.available_cash += get_allocation()
                            today_profit += float(account.stocks_to_track[position_name].wallet)
                            del account.stocks_to_track[position_name]  # delete from stocks to track
                        case "DAYNBREACHED":
                            logger.info(f" DAYNBREACHED -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                            positions_to_delete.append(position_name)
                            logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")

                            # if one third amount breaches then remaining two third cash can be used
                            if account.stocks_to_track[position_name].remaining_allocation > 0:
                                account.available_cash += account.stocks_to_track[position_name].remaining_allocation

                            today_profit += float(account.stocks_to_track[position_name].wallet)
                            del account.stocks_to_track[position_name]  # delete from stocks to track
                        case "CONTINUE":
                            continue

                for position_name in positions_to_delete:
                    del account.positions[position_name]

                # if DEBUG:
                #     if len(account.stocks_to_track) == 0:
                #         set_end_process(True)

        except:
            logger.exception("Kite error may have happened")

    # sell all the stocks which has trigger and is not None

    positions_to_delete = []  # this is needed or else it will alter the length during loop

    # to store all the stock with wallet value in ascending order
    wallet_order = {}

    for position_name in account.positions.keys():
        position: Position = account.positions[position_name]
        if position.trigger is None:
            if position.stock.latest_price > position.cost:
                if position.sell():
                    logger.info(f" crossed the cost -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                    positions_to_delete.append(position_name)
                    logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                    account.available_cash += get_allocation()
                    today_profit += float(account.stocks_to_track[position_name].wallet)
                    del account.stocks_to_track[position_name]  # delete from stocks to track
            else:
                tx_cost = position.transaction_cost(buying_price=position.buy_price, selling_price=position.current_price) / position.quantity
                wallet_value = position.current_price - (position.buy_price + tx_cost)
                wallet_order[wallet_value] = position_name
        else:
            if position.sell():
                logger.info(f" BREACHED at the end of day -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                positions_to_delete.append(position_name)
                logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                account.available_cash += get_allocation()
                today_profit += float(account.stocks_to_track[position_name].wallet)
                del account.stocks_to_track[position_name]  # delete from stocks to track

    for position_name in positions_to_delete:
        del account.positions[position_name]

    # # to store all the stock with wallet value in ascending order
    # wallet_order = {float(account.stocks_to_track[st].wallet): st for st in account.stocks_to_track.keys()}

    sorted_wallet_list = list(wallet_order.keys())

    positions_to_delete = []

    for wallet_v in sorted(sorted_wallet_list, reverse=True):
        position_name = wallet_order[wallet_v]
        position: Position = account.positions[position_name]
        if float(wallet_v) + today_profit > account.starting_cash*DELIVERY_INITIAL_RETURN:
            if position.sell():
                logger.info(f" SOLD at the end -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                positions_to_delete.append(position_name)
                logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                account.available_cash += get_allocation()
                today_profit += float(account.stocks_to_track[position_name].wallet)
                del account.stocks_to_track[position_name]  # delete from stocks to track

    for position_name in positions_to_delete:
        del account.positions[position_name]

    """
        END OF DAY ACTIVITIES
    """

    # if the stock is above trigger it should be sold or the cost will increase next day and the fund will also be trapped

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

    await account.remove_all_sold_stocks(initial_list_of_stocks)

    # Since only positions are stored in the database, so first positions are converted to holdings.
    # After which new holdings are added and old holdings are updated.
    await account.store_all_holdings()

    # deleting all holding data from db which have been sold
    await account.remove_all_sold_holdings(initial_list_of_holdings)

    logger.info("TASK ENDED")
