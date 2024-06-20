import kiteconnect
from quart import Blueprint

from constants.global_contexts import kite_context

stocks_input = Blueprint("stocks_input", __name__)

selected_stocks = []

stock_to_delete = None


def chosen_stocks():
    """
        function is used to get list of selected stocks
    :return:
    """
    global selected_stocks
    return selected_stocks


def delete_stock_fn():
    """
        function to get to delete stock name
    Returns:
    """
    global stock_to_delete
    return stock_to_delete


def set_delete_stock_to_none():
    """
        function to set variable to none
    Args:
        stock_name:

    Returns:

    """
    global stock_to_delete
    stock_to_delete = None


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


@stocks_input.get("/delete-stock/<string:stock>")
async def delete_stock(stock):
    """
        this route is used to manually delete one stock at a time
    :param: stock symbol
    :return: tuple
    """
    global stock_to_delete
    try:
        response = kite_context.ltp([f"NSE:{stock}"])
        if len(response) == 0:
            return {"message": "Incorrect stock symbol provided"}, 400
        else:
            stock_to_delete = stock
            return {"message": "Stock added to delete", "data": stock_to_delete}, 200

    except kiteconnect.exceptions.InputException:
        return {"message": "Kindly login first"}, 400
    # global selected_stocks
    # if stock in selected_stocks:
    #     selected_stocks.remove(stock)
    #     return {
    #         "message": f"The stock with name {stock} has been deleted",
    #         "data": selected_stocks
    #     }, 200
    # else:
    #     return {
    #         "message": f"The stock with name {stock} is not present"
    #     }, 400


@stocks_input.get("/all-stocks")
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
