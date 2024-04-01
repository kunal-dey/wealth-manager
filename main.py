from logging import Logger

from quart import Quart, request, Blueprint
from quart_cors import cors
from kiteconnect.exceptions import InputException
from datetime import datetime
from time import sleep

from constants.global_contexts import set_access_token

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
        sleep(10)
        train_model(obtained_stock_list, short=True)

    # starting the training process
    app.add_background_task(training)
    return {"message": "Training started"}


@app.get("/holdings")
async def holdings_test():
    holdings_from_api = {}

    for holding in kite_context.holdings():
        holdings_from_api[holding['tradingsymbol']] = holding['average_price']
    return {"msg": holdings_from_api}

resource_list: list[Blueprint] = [stocks_input]

for resource in resource_list:
    app.register_blueprint(blueprint=resource)

if __name__ == "__main__":
    app.run(port=8081)
