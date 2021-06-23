import json

import pandas as pd

from ibsg import _DIRPATH

with open(_DIRPATH / "dtypes.json", "r") as f:
    DTYPES = json.load(f)


def read_ber_private(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath, dtype=DTYPES)
