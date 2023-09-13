from  dataclasses import  dataclass

from models.stock_stage import Stage
from models.stock_info import StockInfo

from constants.enums.position_type import PositionType
from constants.enums.product_type import ProductType
from constants.settings import INTRADAY_INCREMENTAL_RETURN


@dataclass
class Position(Stage):

    @property
    def incremental_return(self):
        return INTRADAY_INCREMENTAL_RETURN
