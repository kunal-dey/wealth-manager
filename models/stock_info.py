from dataclasses import dataclass


@dataclass
class StockInfo:
    symbol: str
    exchange: str = 'NSE'
