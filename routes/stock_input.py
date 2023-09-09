import kiteconnect
from quart import Blueprint

from constants.global_contexts import kite_context

stocks_input = Blueprint("stocks_input", __name__)

selected_stocks = []


def chosen_stocks():
    """
        function is used to get list of selected stocks
    :return:
    """
    global selected_stocks
    return selected_stocks


@stocks_input.get("/add-stock/<string:stock>")
async def add_stock(stock):
    """
        this route is used to manually add one stock at a time
        :param: stock symbol
        :return: json
    """
    global selected_stocks
    try:
        response = kite_context.ltp([f"NSE:{stock}"])
        if len(response) == 0:
            return {"message": "Incorrect stock symbol provided"}, 400
        else:
            selected_stocks.append(stock)
            return {"message": "Stock added", "data": selected_stocks}, 200

    except kiteconnect.exceptions.InputException:
        return {"message": "Kindly login first"}, 400


@stocks_input.get("/delete_stock/<string:stock>")
async def delete_stock(stock):
    """
        this route is used to manually delete one stock at a time
    :param: stock symbol
    :return: tuple
    """
    global selected_stocks
    if stock in selected_stocks:
        selected_stocks.remove(stock)
        return {
            "message": f"The stock with name {stock} has been deleted",
            "data": selected_stocks
        }, 200
    else:
        return {
            "message": f"The stock with name {stock} is not present"
        }, 400


@stocks_input.get("/all_stocks")
async def fetch_all_stocks():
    """
        returns the list of all stocks being tracked
    :return:
    """
    global selected_stocks
    return {
        "message": "List of the all the stocks being tracked",
        "data": selected_stocks
    }
