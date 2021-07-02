from io import BytesIO

import pandas as pd

from ibsg import DEFAULTS


def read_ber_private(file: BytesIO, dtype=DEFAULTS["dtype"]) -> pd.DataFrame:
    return pd.read_csv(
        file,
        dtype=dtype,
        encoding="latin-1",
    )
