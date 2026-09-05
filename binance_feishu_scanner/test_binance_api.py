import requests
import pandas as pd

url = "https://fapi.binance.com/fapi/v1/klines"

params = {
    "symbol": "BTCUSDT",
    "interval": "30m",
    "limit": 5
}

response = requests.get(url, params=params)

data = response.json()

df = pd.DataFrame(
    data,
    columns=[
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore"
    ]
)

print(
    df[
        [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    ]
)