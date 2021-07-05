import csv
from io import BytesIO
from typing import Dict

import pandas as pd

from ibsg import DEFAULTS


def read(file: BytesIO, category: str, sep: str = ",") -> pd.DataFrame:
    dtype = DEFAULTS[category]["dtype"]
    mappings = DEFAULTS[category]["mappings"]
    return pd.read_csv(
        file,
        sep=sep,
        dtype=dtype,
        encoding="latin-1",
        quoting=csv.QUOTE_NONE,
        low_memory=False,
        usecols=list(dtype.keys()),
    ).rename(columns=mappings)
