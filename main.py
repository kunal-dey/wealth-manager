import os
import pickle
from logging import Logger

from quart import Quart, request, Blueprint
from quart_cors import cors
from kiteconnect.exceptions import InputException
from datetime import datetime

from constants.global_contexts import set_access_token
from models.db_models.db_functions import find_by_name
from models.stages.holding import Holding

from services.background_task import background_task
from constants.global_contexts import kite_context
from utils.logger import get_logger
from routes.stock_input import stocks_input
from utils.tracking_components.training_components.trained_model import train_model
from utils.tracking_components.verify_symbols import get_correct_symbol

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
        _ = kite_context.ltp("NSE:INFY")

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


@app.get("/buy")
async def buy():
    response = kite_context.place_order(
                variety=kite_context.VARIETY_REGULAR,
                order_type=kite_context.ORDER_TYPE_LIMIT,
                exchange=kite_context.EXCHANGE_NSE,
                tradingsymbol="ARTNIRMAN-BE",
                transaction_type=kite_context.TRANSACTION_TYPE_SELL,
                quantity=1,
                price=71.20,
                product=kite_context.PRODUCT_CNC,
                validity=kite_context.VALIDITY_DAY
            )
    print(response)
    return {"msg": ""}


@app.get("/predict")
async def predict():
    # obtained_stock_list = await get_correct_symbol()
    # logger.info(obtained_stock_list)
    # message = train_model(obtained_stock_list)
    params = pickle.load(open(os.getcwd() + "/temp/params.pkl", "rb"))
    mu, sigma = params

    logger.info(mu.iloc[:-1])
    return {"msg": "try"}

resource_list: list[Blueprint] = [stocks_input]

for resource in resource_list:
    app.register_blueprint(blueprint=resource)

if __name__ == "__main__":
    app.run(port=8081)
