import json

import pandas as pd

from ibsg import DEFAULTS


def read_ber_private(filepath: str, dtype=DEFAULTS["dtype"]) -> pd.DataFrame:
    return pd.read_csv(filepath, dtype=dtype)
