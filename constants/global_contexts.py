from kiteconnect import KiteConnect

from constants.kite_credentials import API_KEY

kite_context = KiteConnect(
    api_key=API_KEY
)


def set_access_token(access_token: str):
    global kite_context

    kite_context.set_access_token(access_token)
