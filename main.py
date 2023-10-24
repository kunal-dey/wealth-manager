from logging import Logger

from quart import Quart, request, Blueprint
from quart_cors import cors
from kiteconnect.exceptions import InputException

from constants.global_contexts import set_access_token

from services.background_task import background_task
from constants.global_contexts import kite_context
from utils.logger import get_logger
from routes.stock_input import stocks_input

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

resource_list: list[Blueprint] = [stocks_input]

for resource in resource_list:
    app.register_blueprint(blueprint=resource)


@app.get("/hit")
async def save():
    # stock_info = StockInfo(
    #     stock_name="INFY",
    # )
    # holding = Holding(
    #     buy_price=100,
    #     position_price=101,
    #     quantity=4,
    #     product_type=ProductType.DELIVERY,
    #     position_type=PositionType.LONG,
    #     stock=stock_info
    # )
    # await holding.save_to_db()
    # stock_info = StockInfo(
    #     stock_name="TCS",
    # )
    # holding = Holding(
    #     buy_price=200,
    #     position_price=201,
    #     quantity=34,
    #     product_type=ProductType.DELIVERY,
    #     position_type=PositionType.LONG,
    #     stock=stock_info
    # )
    # await holding.save_to_db()
    # holding.quantity = 37
    # holding.product_type = ProductType.INTRADAY
    # await holding.update_in_db()
    # holding:Holding = await find_by_name(Holding.COLLECTION, Holding, {"stock.stock_name": "INFY"})
    # await holding.delete_from_db()
    # print(await retrieve_all_services(Holding.COLLECTION, Holding))
    get_correct_symbol()
    return {"msg":"saved"}


if __name__ == "__main__":
    app.run(port=8081)
