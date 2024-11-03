from logging import Logger

from constants.enums.product_type import ProductType
from constants.global_contexts import kite_context
from constants.settings import DEBUG

from utils.logger import get_logger

logger: Logger = get_logger(__name__)


def short(symbol: str, quantity: int, product_type: ProductType, exchange: str):
    """
        takes a short position which means it will
        1. sell the position which has already been bought, or
        2. sell a negative quantity of stocks
    """
    logger.info(symbol)
    if DEBUG:
        return True
    try:
        response = kite_context.place_order(
            variety=kite_context.VARIETY_REGULAR,
            order_type=kite_context.ORDER_TYPE_MARKET,
            exchange=kite_context.EXCHANGE_NSE if exchange == 'NSE' else kite_context.EXCHANGE_BSE,
            tradingsymbol=symbol,
            transaction_type=kite_context.TRANSACTION_TYPE_SELL,
            quantity=quantity,
            product=kite_context.PRODUCT_MIS if product_type == ProductType.INTRADAY else kite_context.PRODUCT_CNC,
            validity=kite_context.VALIDITY_DAY
        )
        logger.info(f"response{response}")
        try:
            order_id = int(response)
            return True
        except:
            raise Exception(f"{response}")
    except:
        logger.exception("Error during selling")
        return False


def long(symbol: str, quantity: int, product_type: ProductType, exchange: str):
    """
        takes a long position which means it will
        1. buy the position which has already been short, or
        2. buy a positive quantity of stocks
    """
    logger.info(symbol)
    if DEBUG:
        return True
    try:
        response = kite_context.place_order(
            variety=kite_context.VARIETY_REGULAR,
            order_type=kite_context.ORDER_TYPE_MARKET,
            exchange=kite_context.EXCHANGE_NSE if exchange == 'NSE' else kite_context.EXCHANGE_BSE,
            tradingsymbol=symbol,
            transaction_type=kite_context.TRANSACTION_TYPE_BUY,
            quantity=quantity,
            product=kite_context.PRODUCT_MIS if product_type == ProductType.INTRADAY else kite_context.PRODUCT_CNC,
            validity=kite_context.VALIDITY_DAY
        )
        logger.info(f"response{response}")
        try:
            order_id = int(response)
            return True
        except:
            raise Exception(f"{response}")
    except:
        logger.exception("Error during buying")
        return False
