import os

import pandas as pd

current_path = os.path.dirname(os.path.abspath(__file__))

stocks = pd.read_csv(os.path.join(current_path, "stocks.csv"))
stocks.date = pd.to_datetime(stocks.date)

__all__ = ["stocks"]
