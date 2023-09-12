from models.stock_stage import Stage
from models.stock_info import StockInfo

from constants.enums.position_type import PositionType
from constants.enums.product_type import ProductType

from constants.settings import DELIVERY_INCREMENTAL_RETURN


class Holding(Stage):
    buy_price: float
    position_price: float
    quantity: int
    product_type: ProductType
    position_type: PositionType
    stock: None | StockInfo = None

    @property
    def incremental_return(self):
        return DELIVERY_INCREMENTAL_RETURN

