import os
import pickle
from datetime import datetime
from asyncio import sleep
from logging import Logger
import yfinance as yf
import pandas as pd
from keras.models import load_model

from models.account import Account
from models.db_models.db_functions import retrieve_all_services, find_by_name
from models.stages.position import Position
from models.stock_info import StockInfo
from utils.logger import get_logger
from utils.tracking_components.fetch_prices import fetch_current_prices

from constants.settings import END_TIME, SLEEP_INTERVAL, get_allocation, end_process, START_TIME, get_max_stocks, \
    set_max_stocks, DEBUG, set_end_process, DAILY_MINIMUM_RETURN, START_BUYING_TIME, STOP_BUYING_TIME, TRAINING_DATE, \
    BUY_SHORTS, EXPECTED_MINIMUM_MONTHLY_RETURN
from utils.tracking_components.select_stocks import predict_running_df
from utils.tracking_components.verify_symbols import get_correct_symbol

logger: Logger = get_logger(__name__)


async def background_task():
    """
        all the tasks mentioned here will be running in the background
    """
    global logger

    logger.info("BACKGROUND TASK STARTED")

    account: Account = Account()

    # prediction_df columns contains .NS whereas obtained_stock_list has all stocks which can be traded and are in mis
    prediction_df, obtained_stock_list = None, await get_correct_symbol()
    obtained_stock_list = [st for st in obtained_stock_list if '-BE' not in st]
    logger.info(f"non BE stock list : {obtained_stock_list}")

    not_loaded = True
    # filtered stocks contains all stocks in prediction_df with .NS removed
    filtered_stocks, selected_long_stocks, selected_short_stocks = [], [], []
    
    # variable to check if minimum return of 0.005 is obtained in a day then free 2/3 of the portfolio for next day
    today_profit = 0

    """
    START OF DAY ACTIVITIES
    """

    # fetch all the stocks already added in stock list
    stock_list: list[StockInfo] = await retrieve_all_services(StockInfo.COLLECTION, StockInfo)
    logger.info(f" list of all stocks to add: {stock_list}")
    logger.info(f" per stock allocation: {get_allocation()}")

    # load all the stock objects to stock to track
    for stock_obj in stock_list:
        account.stocks_to_track[stock_obj.stock_name] = stock_obj

    # load all holdings from the database
    await account.load_holdings()

    # loading the prediction df from the file or from yahoo finance
    try:
        prediction_df = pd.read_csv(f"temp/prediction_df.csv", index_col=0)
        stocks_present = []

        # remove .NS from the symbol which is downloaded from yahoo finance
        for a in [i[:-3] for i in list(prediction_df.columns)]:
            for b in obtained_stock_list:
                if a == b:
                    stocks_present.append(f"{a}.NS")
        prediction_df = prediction_df[stocks_present]
        prediction_df.to_csv(f"temp/prediction_df.csv")
    except FileNotFoundError:
        prediction_df = None
    if prediction_df is None:
        logger.info("should not enter")
        prediction_df = yf.download(tickers=[f"{st}.NS"for st in obtained_stock_list], period='1wk', interval='1m')['Close']
        prediction_df = prediction_df.ffill().bfill().dropna(axis=1)
        prediction_df.index = pd.to_datetime(prediction_df.index)
        prediction_df = prediction_df.loc[:str(TRAINING_DATE.date())]
        prediction_df.reset_index(drop=True, inplace=True)
        prediction_df.to_csv(f"temp/prediction_df.csv")

    # loading all holdings and stocks into a list to compare what has been sold at the end
    # these are just used for verification at the end

    initial_list_of_holdings = list(account.holdings.keys())
    initial_list_of_stocks = list(account.stocks_to_track.keys())

    # setting the starting cash and available cash
    account.starting_cash = account.starting_cash + len(initial_list_of_holdings)*get_allocation()
    account.available_cash = account.available_cash - (len(initial_list_of_stocks)-len(initial_list_of_holdings))*get_allocation()

    # this is done as mostly we are storing holding and trade occurs as positions
    account.convert_holdings_to_positions()

    logger.info(f"starting cash: {account.available_cash}")

    """
        model and parameter setup
    """

    # these are used as cache and will reduce the execution time
    day_based_data = yf.download(tickers=list(prediction_df.columns), period='6mo', interval='1d', progress=False)['Close']
    day_based_data.index = pd.to_datetime(day_based_data.index)
    day_based_data = day_based_data.loc[:TRAINING_DATE]
    day_based_data = day_based_data.ffill().bfill()

    # model to predict long stocks

    long_model = load_model(os.getcwd() + "/temp/DNN_model")

    logger.info(f"model loaded for long: {long_model}")

    long_params = pickle.load(open(os.getcwd() + "/temp/params.pkl", "rb"))

    logger.info(f"mu and sigma loaded for long: {long_params}")

    predict_long_stocks = predict_running_df(day_based_data, long_model, long_params)

    # model to predict short stocks

    short_model = load_model(os.getcwd() + "/temp/DNN_model_short")

    logger.info(f"model loaded for short: {short_model}")

    short_params = pickle.load(open(os.getcwd() + "/temp/params_short.pkl", "rb"))

    logger.info(f"mu and sigma loaded for short: {short_params}")

    predict_short_stocks = predict_running_df(day_based_data, short_model, short_params, short=True)

    # this part will loop till the trading times end
    current_time = datetime.now()
    while current_time < END_TIME:
        current_time = datetime.now()

        # if the trading has not started then iterate every 1 sec else iterate every 30 sec
        if START_TIME < current_time:
            await sleep(SLEEP_INTERVAL)
        else:
            await sleep(1)

        # even if any error occurs it will not break it
        try:
            if not_loaded and current_time >= START_TIME:
                filtered_stocks = [i[:-3] for i in list(prediction_df.columns)]

                logger.info(f"list of filtered stocks: {filtered_stocks}")
                not_loaded = False

            if end_process():
                break

            """
                if the time is within trading interval or certain criteria is met then buy stocks
            """

            if START_TIME < current_time:
                # update the prediction_df after every interval
                new_cost_df = await fetch_current_prices(filtered_stocks)
                # the if condition is for the debug process
                if new_cost_df is None:
                    set_end_process(True)
                else:
                    prediction_df = pd.concat([prediction_df, new_cost_df], ignore_index=True)
                    prediction_df = prediction_df.bfill().ffill()
                    prediction_df.dropna(axis=1, inplace=True)
                    prediction_df = prediction_df[[col for col in list(prediction_df.columns) if '-BE' not in col]]
                    prediction_df = prediction_df.iloc[-2000:]
                    prediction_df = prediction_df.reset_index(drop=True)
                    price_filter = list(prediction_df.iloc[-1][prediction_df.iloc[-1] > 30].index)
                    prediction_df = prediction_df[price_filter]
                    prediction_df.to_csv(f"temp/prediction_df.csv")

                selected_long_stocks = [st[:-3] for st in predict_long_stocks(prediction_df)]
                selected_short_stocks = [st[:-3] for st in predict_short_stocks(prediction_df)]

                logger.info(f"chosen long: {selected_long_stocks}")
                logger.info(f"chosen short: {selected_short_stocks}")

                logger.info(f"available cash : {account.available_cash}")

                logger.info(f"list of the stocks to track: {account.stocks_to_track.keys()}")
                logger.info(f"list of the short stocks to track: {account.short_stocks_to_track.keys()}")
                logger.info(f"short positions {account.short_positions}")

                if STOP_BUYING_TIME > current_time > START_BUYING_TIME:
                    # selecting stock which meets the criteria
                    for stock_col in selected_long_stocks:
                        if stock_col not in selected_short_stocks:
                            # available cash keeps on changing so max_stocks keeps on changing
                            # the stock will be added if it is added for the first time
                            set_max_stocks(int(account.available_cash/get_allocation()))
                            if 0 < get_max_stocks() and stock_col not in account.stocks_to_track.keys() and stock_col not in account.short_stocks_to_track.keys():

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
                                _, _2 = account.stocks_to_track[stock_col].buy_parameters()
                                # even if it may seem that allocation is reduced when bought, actual change is while adding the
                                # stock in stocks to track
                                account.available_cash -= get_allocation()
                                stock_df = prediction_df[[f"{stock_col}.NS"]]
                                stock_df.reset_index(inplace=True, drop=True)
                                stock_df = stock_df[[f"{stock_col}.NS"]].bfill().ffill()
                                stock_df.columns = ['price']
                                stock_df.to_csv(f"temp/{stock_col}.csv")

                                logger.info("whether actually the stock df has all the data or not")
                                logger.info(f"{stock_col}: {stock_df.shape}")

                stocks_to_shift = []

                for tracking_stock in list(account.stocks_to_track.keys()):
                    if tracking_stock not in list(account.positions.keys()) and tracking_stock in selected_short_stocks:
                        account.short_stocks_to_track[tracking_stock] = account.stocks_to_track[tracking_stock]
                        stocks_to_shift.append(tracking_stock)

                for tracking_stock in stocks_to_shift:
                    del account.stocks_to_track[tracking_stock]

                short_stocks_to_shift = []

                for tracking_stock in list(account.short_stocks_to_track.keys()):
                    if tracking_stock in selected_short_stocks:
                        continue
                    if tracking_stock not in list(account.short_positions.keys()) and tracking_stock in selected_long_stocks:
                        account.stocks_to_track[tracking_stock] = account.short_stocks_to_track[tracking_stock]
                        short_stocks_to_shift.append(tracking_stock)

                logger.info("After shifting")
                logger.info(f"list of the stocks to track: {account.stocks_to_track.keys()}")
                logger.info(f"list of the short stocks to track: {account.short_stocks_to_track.keys()}")

                for tracking_stock in short_stocks_to_shift:
                    del account.short_stocks_to_track[tracking_stock]

                """
                    update price for all the stocks which are being tracked
                """

                for stock in account.stocks_to_track.keys():
                    logger.info(f"stock to update {stock}")
                    account.stocks_to_track[stock].update_price(selected_long_stocks, selected_short_stocks)

                if STOP_BUYING_TIME > current_time > START_BUYING_TIME:
                    try:
                        account.buy_stocks()
                        account.short_stocks()
                    except:
                        pass

                logger.info(f"after bought short positions {account.short_positions}")

                """
                    if the trigger for selling is breached in position then sell
                """

                positions_to_delete = []  # this is needed or else it will alter the length during loop

                for position_name in account.positions.keys():
                    position: Position = account.positions[position_name]
                    status = position.breached()
                    match status:
                        case "SELL_PROFIT":
                            logger.info(f" profit -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                            logger.info(f"{position.stock.wallet/get_allocation()}, {(1+EXPECTED_MINIMUM_MONTHLY_RETURN)**(position.stock.number_of_days/20)}")
                            if 1+(position.stock.wallet/get_allocation()) > (1+EXPECTED_MINIMUM_MONTHLY_RETURN)**(position.stock.number_of_days/20):
                                logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                                # if its in holding then fund is added next day else for position its added same day
                                if position.stock.number_of_days == 1:
                                    account.available_cash += get_allocation()
                                os.remove(os.getcwd() + f"/temp/{position_name}.csv")
                            else:
                                account.short_stocks_to_track[position_name] = account.stocks_to_track[position_name]
                            positions_to_delete.append(position_name)

                        case "SELL_LOSS":
                            logger.info(f" loss -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                            account.short_stocks_to_track[position_name] = account.stocks_to_track[position_name]
                            positions_to_delete.append(position_name)

                        case "CONTINUE":
                            continue

                for position_name in positions_to_delete:
                    del account.positions[position_name]
                    today_profit += float(account.stocks_to_track[position_name].wallet)
                    del account.stocks_to_track[position_name]  # delete from stocks to track

                for stock in account.short_stocks_to_track.keys():
                    account.short_stocks_to_track[stock].update_price(selected_long_stocks, selected_short_stocks)

                """
                    if the trigger for buying short positions are breached in short position then buy
                """

                short_positions_to_delete = []  # this is needed or else it will alter the length during loop

                for short_position_name in account.short_positions.keys():
                    short_position: Position = account.short_positions[short_position_name]
                    status = short_position.breached()
                    match status:
                        case "BUY_PROFIT":
                            logger.info(f" buy short in profit --> buy {short_position.stock.stock_name} at {short_position.stock.latest_price}")
                            if 1+(short_position.stock.wallet/get_allocation()) > (1+EXPECTED_MINIMUM_MONTHLY_RETURN)**(short_position.stock.number_of_days/20):
                                logger.info(f"breached stock wallet {short_position_name} {account.stocks_to_track[short_position_name].wallet}")
                                if short_position.stock.number_of_days == 1:
                                    account.available_cash += get_allocation()
                                os.remove(os.getcwd() + f"/temp/{short_position_name}.csv")
                            else:
                                account.stocks_to_track[short_position_name] = account.short_stocks_to_track[short_position_name]
                            short_positions_to_delete.append(short_position_name)

                        case "BUY_LOSS":
                            logger.info(f" buy short in loss --> buy {short_position.stock.stock_name} at {short_position.stock.latest_price}")
                            account.stocks_to_track[short_position_name] = account.short_stocks_to_track[short_position_name]
                            short_positions_to_delete.append(short_position_name)
                        case "CONTINUE":
                            continue

                for short_position_name in short_positions_to_delete:
                    del account.short_positions[short_position_name]
                    today_profit += float(account.short_stocks_to_track[short_position_name].wallet)
                    del account.short_stocks_to_track[short_position_name]  # delete from stocks to track

                if current_time > BUY_SHORTS:
                    short_positions_to_delete_at_end = []
                    for short_position_name in account.short_positions.keys():
                        short_position: Position = account.short_positions[short_position_name]

                        if short_position.buy_short():
                            today_profit += float(account.short_stocks_to_track[short_position_name].wallet)
                            if 1+(short_position.stock.wallet/get_allocation()) > (1+EXPECTED_MINIMUM_MONTHLY_RETURN)**(short_position.stock.number_of_days/20):
                                logger.info(f"breached stock wallet {short_position_name} {account.short_stocks_to_track[short_position_name].wallet}")
                                os.remove(os.getcwd() + f"/temp/{short_position_name}.csv")
                                del account.short_stocks_to_track[short_position_name]
                            short_positions_to_delete_at_end.append(short_position_name)
                            # else:
                            #     account.stocks_to_track[short_position_name] = account.short_stocks_to_track[short_position_name]
                        else:
                            logger.info(f"Error occurred while deleting {short_position_name}")

                    for short_position_name in short_positions_to_delete_at_end:
                        del account.short_positions[short_position_name]

                    short_stock_to_delete = []

                    for stock_to_transfer in account.short_stocks_to_track.keys():
                        account.stocks_to_track[stock_to_transfer] = account.short_stocks_to_track[stock_to_transfer]
                        short_stock_to_delete.append(stock_to_transfer)

                    for stock_to_transfer in short_stock_to_delete:
                        del account.short_stocks_to_track[stock_to_transfer]

        except:
            logger.exception("Kite error may have happened")

    if DEBUG:
        short_positions_to_delete_at_end = []

        for short_position_name in account.short_positions.keys():
            short_position: Position = account.short_positions[short_position_name]
            if short_position.buy_short():
                today_profit += float(account.short_stocks_to_track[short_position_name].wallet)
                if 1+(short_position.stock.wallet/get_allocation()) > (1+EXPECTED_MINIMUM_MONTHLY_RETURN)**(short_position.stock.number_of_days/20):
                    logger.info(f"breached stock wallet {short_position_name} {account.stocks_to_track[short_position_name].wallet}")
                    os.remove(os.getcwd() + f"/temp/{short_position_name}.csv")
                    del account.short_stocks_to_track[short_position_name]
                short_positions_to_delete_at_end.append(short_position_name)
                # else:
                #     account.stocks_to_track[short_position_name] = account.short_stocks_to_track[short_position_name]
            else:
                logger.info(f"Error occurred while deleting {short_position_name}")
        for short_position_name in short_positions_to_delete_at_end:
            del account.short_positions[short_position_name]

        for stock_to_transfer in account.short_stocks_to_track.keys():
            account.stocks_to_track[stock_to_transfer] = account.short_stocks_to_track[stock_to_transfer]
            del account.short_stocks_to_track[stock_to_transfer]

    # sell all the stocks which has trigger and is not None

    positions_to_delete = []  # this is needed or else it will alter the length during loop

    # to store all the stock with wallet value in ascending order
    wallet_order = {}

    for position_name in account.positions.keys():
        position: Position = account.positions[position_name]
        if position.stock.number_of_days == 1:
            if position.trigger is None:
                if position.stock.latest_price > position.cost:
                    if position.sell():
                        logger.info(f" crossed the cost -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                        positions_to_delete.append(position_name)
                        account.available_cash += get_allocation()
                        today_profit += float(account.stocks_to_track[position_name].wallet)
                        del account.stocks_to_track[position_name]  # delete from stocks to track
                else:
                    tx_cost = position.stock.transaction_cost(buying_price=position.position_price, selling_price=position.current_price) / position.quantity
                    wallet_value = (position.current_price - (position.position_price + tx_cost)) * position.quantity
                    wallet_order[wallet_value] = position_name
            else:
                if position.sell():
                    logger.info(f" BREACHED at the end of day -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                    positions_to_delete.append(position_name)
                    logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                    account.available_cash += get_allocation()
                    today_profit += float(account.stocks_to_track[position_name].wallet)
                    del account.stocks_to_track[position_name]  # delete from stocks to track
        else:
            if 1+(position.stock.wallet/get_allocation()) > (1+EXPECTED_MINIMUM_MONTHLY_RETURN)**(position.stock.number_of_days/20):
                if position.sell():
                    positions_to_delete.append(position_name)
                    logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                    account.available_cash += get_allocation()
                    today_profit += float(account.stocks_to_track[position_name].wallet)
                    del account.stocks_to_track[position_name]  # delete from stocks to track

    for position_name in positions_to_delete:
        del account.positions[position_name]
        os.remove(os.getcwd() + f"/temp/{position_name}.csv")

    # # to store all the stock with wallet value in ascending order
    # wallet_order = {float(account.stocks_to_track[st].wallet): st for st in account.stocks_to_track.keys()}

    sorted_wallet_list = list(wallet_order.keys())

    positions_to_delete = []

    for wallet_v in sorted(sorted_wallet_list, reverse=True):
        position_name = wallet_order[wallet_v]
        position: Position = account.positions[position_name]
        if float(wallet_v) + today_profit > account.starting_cash*DAILY_MINIMUM_RETURN:
            if position.sell():
                logger.info(f" SOLD at the end -->sell {position.stock.stock_name} at {position.stock.latest_price}")
                positions_to_delete.append(position_name)
                logger.info(f"breached stock wallet {position_name} {account.stocks_to_track[position_name].wallet}")
                account.available_cash += get_allocation()
                today_profit += float(account.stocks_to_track[position_name].wallet)
                del account.stocks_to_track[position_name]  # delete from stocks to track

    for position_name in positions_to_delete:
        del account.positions[position_name]
        os.remove(os.getcwd() + f"/temp/{position_name}.csv")

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
