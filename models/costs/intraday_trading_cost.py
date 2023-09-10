from dataclasses import dataclass


@dataclass
class IntradayTransactionCost:
    """
        This class provides the details of all the transaction cost  of
        intraday or MIS transaction charged by zerodha.

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

    def __post_init__(self):
        self._profit_or_loss = (self.selling_price - self.buying_price) * self.quantity
        turnover = (self.buying_price + self.selling_price) * self.quantity
        self._brokerage_charges = turnover * (0.03 / 100) if turnover * (0.03 / 100) < 40 else 40

        self._stt_total = (0.025 / 100) * self.selling_price * self.quantity
        self._net_transaction_charges = round((0.00345 / 100) * turnover, 2)
        self._clearing_charges = 0
        self._stamp_duty = round((300 / 10000000) * self.buying_price * self.quantity, 2)
        self._sebi_charges = round(((turnover / 10000000) * 10) * 1.18, 2)
        self._gst = 0.18 * (self._brokerage_charges + self._net_transaction_charges + self._sebi_charges / 1.18)

    @property
    def total_tax_and_charges(self):
        return self._brokerage_charges + self._stt_total + self._net_transaction_charges + self._clearing_charges + self._stamp_duty + self._gst + self._sebi_charges

    @property
    def net_pl(self):
        return self._profit_or_loss - self.total_tax_and_charges
