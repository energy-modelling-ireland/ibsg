import csv
from io import BytesIO
from typing import Dict

import icontract
import pandas as pd


def read(
    file: BytesIO,
    dtype: Dict[str, str],
    mappings: Dict[str, str],
    sep: str = ",",
) -> pd.DataFrame:
    return pd.read_csv(
        file,
        sep=sep,
        dtype=dtype,
        encoding="latin-1",
        low_memory=False,
        usecols=list(dtype.keys()),
    ).rename(columns=mappings)
