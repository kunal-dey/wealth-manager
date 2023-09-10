from dataclasses import dataclass, field

from models.stock_info import StockInfo


@dataclass
class Account:
    stocks_to_track: dict[str, StockInfo] = field(default_factory=dict)


