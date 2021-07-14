from io import BytesIO
from pathlib import Path
from typing import Callable
from typing import Dict

import fsspec
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


def load(read: Callable, url: str, data_dir: Path, filesystem_name: str):
    filename = url.split("/")[-1]
    filepath = data_dir / filename
    if not filepath.exists():
        fs = fsspec.filesystem(filesystem_name)
        with fs.open(url, cache_storage=filepath) as f:
            df = read(f)
    else:
        df = read(filepath)
    return df
