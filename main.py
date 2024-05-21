import re
from logging import Logger

from quart import Quart, request, Blueprint
from quart_cors import cors
from kiteconnect.exceptions import InputException
from datetime import datetime
from time import sleep

from constants.global_contexts import set_access_token

from services.background_task import background_task
from constants.global_contexts import kite_context
from utils.indicators.candlestick.patterns.bearish_engulfing import BearishEngulfing
from utils.logger import get_logger
from routes.stock_input import stocks_input
from utils.tracking_components.training_components.trained_model import train_model
from utils.tracking_components.verify_symbols import get_correct_symbol

import pandas as pd
from utils.indicators.candlestick.patterns.bearish_harami import BearishHarami

app = Quart(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True
app = cors(app, allow_origin="*")

logger: Logger = get_logger(__name__)


@app.get("/")
async def home():
    """
    home route
    :return:
    """
    return {"message": "Welcome to the Zerodha trading system"}


@app.route("/time")
def get_time():
    return {"current_time": datetime.now()}


@app.get("/set")
async def set_token_request():
    """
    route to set the access token which is received after login zerodha using starter app
    :return:
    """
    global logger
    try:
        set_access_token(request.args["token"])
        logger.info("TOKEN HAS BEEN SET")
        return {"message": "Token set"}
    except:
        return {"message": "there is an error"}


@app.get("/start")
async def start_process():
    """
    route checks whether login has been done and then starts the background task
    :return:
    """
    try:
        # to test whether the access toke has been set after login
        # _ = kite_context.ltp("NSE:INFY")

        # starting the background task which will run the entire process
        app.add_background_task(background_task)
        return {"message": "Background process started"}
    except InputException:
        return {"message": "Kindly login first"}


@app.route("/stop")
async def stop_background_tasks():
    """
        On being deployed if we need to manually stop the background task then
        this route is used
    """
    global logger
    for task in app.background_tasks:
        task.cancel()
    logger.info("STOPPED ALL BACKGROUND SERVICES")
    return {"message": "All task cancelled"}


@app.get("/train")
async def train():
    obtained_stock_list = await get_correct_symbol(lower_price=50, higher_price=800)
    logger.info(obtained_stock_list)
    logger.info(len([f"{st}.NS" for st in obtained_stock_list if '-BE' not in st]))

    def training():
        train_model(obtained_stock_list)

    # starting the training process
    app.add_background_task(training)
    return {"message": "Training started"}


@app.get("/test")
async def cndl():
    def get_ohlc(df, window=5):
        data = df.copy()
        data = data.iloc[-180:]

        # Apply a rolling window of 15 minutes
        rolling_data = data['price'].rolling(window=window)

        # Calculate Open, Close, High, and Low prices for each window
        open_price = rolling_data.apply(lambda x: x.iloc[0] if len(x) == window else None)
        close_price = rolling_data.apply(lambda x: x.iloc[-1] if len(x) == window else None)
        high_price = rolling_data.max()
        low_price = rolling_data.min()

        # Create a new DataFrame with these values
        ohlcv_data = pd.DataFrame({
            "Open": open_price,
            "Close": close_price,
            "High": high_price,
            "Low": low_price
        })

        # Drop any rows with NaN values which occur at the start of the dataset
        return ohlcv_data.dropna()
    stock = pd.read_csv(f"temp/BFINVEST.csv")
    # cndl = bearish_harami(get_ohlc(stock, 5))
    data = get_ohlc(stock, 5)
    cndl = BearishHarami(target='pattern0')

    data =cndl.has_pattern(data, ['Open', 'High', 'Low', 'Close'], False)

    candl = BearishEngulfing(target='pattern1')
    ohlc_data = candl.has_pattern(data, ['Open', 'High', 'Low', 'Close'], False)

    regex = re.compile('pattern', re.IGNORECASE)

    # Filter columns where the column name matches the regex pattern
    matching_columns = [col for col in ohlc_data.columns if regex.search(col)]
    print(list(ohlc_data[matching_columns].iloc[-1]))
    return {"msg": "no error"}

resource_list: list[Blueprint] = [stocks_input]

for resource in resource_list:
    app.register_blueprint(blueprint=resource)

if __name__ == "__main__":
    app.run(port=8081)
