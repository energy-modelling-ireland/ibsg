from pathlib import Path

import fsspec
import numpy as np
import pandas as pd

from ibsg import clean


def load_census_buildings(url: str, filepath: Path) -> pd.DataFrame:
    if not filepath.exists():
        with fsspec.open(url) as f:
            pd.read_parquet(f).to_parquet(filepath)
    return pd.read_parquet(filepath)


def replace_not_stated_period_built_with_mode(stock: pd.DataFrame) -> pd.Series:
    inferred_stock = stock
    inferred_stock["is_period_built_estimated"] = inferred_stock["period_built"] == "NS"
    inferred_stock["period_built"] = inferred_stock["period_built"].replace(
        {"NS": np.nan}
    )
    modal_period_built = inferred_stock["period_built"].mode()[0]
    modal_period_built_by_small_area = inferred_stock.groupby("small_area")[
        "period_built"
    ].transform(lambda s: s.mode()[0] if not s.mode().empty else modal_period_built)
    inferred_stock["period_built"] = inferred_stock["period_built"].fillna(
        modal_period_built_by_small_area
    )
    return inferred_stock
