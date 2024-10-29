from dataclasses import dataclass

from models.stock_stage import Stage

from constants.enums.product_type import ProductType
from constants.settings import INTRADAY_INCREMENTAL_RETURN


@dataclass
class Position(Stage):

    @property
    def incremental_return(self):
        if self.product_type == ProductType.DELIVERY:
            return INTRADAY_INCREMENTAL_RETURN
        else:
            return 0.03
