from dataclasses import dataclass, field


@dataclass
class DeliveryTransactionCost:
    """
        This class provides the details of all the transaction cost of any delivery charged by zerodha.

        It requires buying price, selling price and quantity.

        Since the calculations were not available, it has been approximated by back calculation.
        A pessimistic approximate is adopted which means the cost calculated
        here is a little more than the actual price.

        Various other costs can also be used if required.
        However, to get cost use total_tax and charges.
    """
    buying_price: float
    selling_price: float
    quantity: int
    brokerage_charges: float = field(init=False, default=0.0)

    def __post_init__(self):
        self._profit_or_loss = (self.selling_price - self.buying_price) * self.quantity
        turnover = (self.buying_price + self.selling_price) * self.quantity

        self._stt_total = (0.1 / 100) * turnover
        self._net_transaction_charges = round((0.00345 / 100) * turnover, 2)
        self._dp_charges = 15.93 if self.selling_price != 0 else 0
        self._stamp_duty = round((1500 / 10000000) * self.buying_price * self.quantity, 2)
        self._sebi_charges = round(((turnover / 10000000) * 10) * 1.18, 2)
        self._gst = 0.18 * (self.brokerage_charges + self._net_transaction_charges + self._sebi_charges / 1.18)

    @property
    def total_tax_and_charges(self):
        return self.brokerage_charges + self._stt_total + self._net_transaction_charges + self._dp_charges + self._stamp_duty + self._gst + self._sebi_charges

    @property
    def net_pl(self):
        return self._profit_or_loss - self.total_tax_and_charges
