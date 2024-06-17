from logging import Logger
from quart import Blueprint

from models.db_models.db_functions import jsonify, retrieve_all_services
from models.wallet import Wallet
from utils.logger import get_logger

logger: Logger = get_logger(__name__)

wallet_input = Blueprint("wallet_input", __name__)


@wallet_input.get("/expected-amount/<float:amount>")
async def update_expected_amount(amount):
    """
        this route is used to manually add one stock at a time
        :param: stock symbol
        :return: json
    """
    try:
        wallets = await retrieve_all_services(Wallet.COLLECTION, Wallet)
        wallet: Wallet = wallets[0]
        await wallet.update_expected_amount(amount)

        return {"message": "Amount updated"}, 400

    except:
        logger.exception("Failed to update the amount")
        return {"message": "Failed to update the amount"}, 500


@wallet_input.get("/accumulated-amount/<float:amount>")
async def update_accumulated_amount(amount):
    """
        this route is used to manually add one stock at a time
        :param: stock symbol
        :return: json
    """
    try:
        wallets = await retrieve_all_services(Wallet.COLLECTION, Wallet)
        wallet: Wallet = wallets[0]
        await wallet.update_accumulated_amount(amount)

        return {"message": "Amount updated"}, 400

    except:
        logger.exception("Failed to update the amount")
        return {"message": "Failed to update the amount"}, 500


@wallet_input.get("/wallet")
async def wallet_info():
    """
        returns the list of all stocks being tracked
    :return:
    """
    wallets = await retrieve_all_services(Wallet.COLLECTION, Wallet)
    wallet: Wallet = wallets[0]
    wallet_json:dict = jsonify(wallet)
    del wallet_json["_id"]
    return {
        "message": "wallet info",
        "data": wallet_json
    }
