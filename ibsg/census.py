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
    inferred_stock = stock.copy()
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


def _add_merge_columns_to_census(
    stock: pd.DataFrame, ber_granularity: str
) -> pd.DataFrame:
    on_columns = [ber_granularity, "period_built"]
    stock["id"] = clean.get_group_id(stock, columns=on_columns)
    return stock


def _standardise_census(census: pd.DataFrame, ber_granularity: str) -> pd.DataFrame:
    census[ber_granularity] = census[ber_granularity].str.lower()
    return _add_merge_columns_to_census(census, ber_granularity)


def _add_merge_columns_to_bers(
    bers: pd.DataFrame, ber_granularity: str
) -> pd.DataFrame:
    bers["period_built"] = pd.cut(
        bers["year_of_construction"],
        bins=[
            -np.inf,
            1919,
            1945,
            1960,
            1970,
            1980,
            1990,
            2000,
            2011,
            np.inf,
        ],
        labels=[
            "PRE19",
            "19_45",
            "46_60",
            "61_70",
            "71_80",
            "81_90",
            "91_00",
            "01_10",
            "11L",
        ],
    )
    on_columns = [ber_granularity, "period_built"]
    bers["id"] = clean.get_group_id(bers, columns=on_columns)
    return bers


def _standardise_bers(bers: pd.DataFrame, ber_granularity: str) -> pd.DataFrame:
    bers[ber_granularity] = bers[ber_granularity].str.lower()
    return _add_merge_columns_to_bers(bers, ber_granularity)


def _fill_census_with_bers(
    census: pd.DataFrame,
    bers: pd.DataFrame,
    ber_granularity: str,
) -> pd.DataFrame:
    merge_columns = [ber_granularity, "period_built", "id"]

    # Small area linked postcodes are more reliable than user inputted ones
    # & if don't drop these end up with countyname_x and countyname_y columns on merge!
    if ber_granularity == "small_area":
        ber_columns_to_drop = ["countyname"]
    else:
        ber_columns_to_drop = []

    before_2016 = pd.merge(
        left=census,
        right=bers.drop(columns=ber_columns_to_drop).query(
            "year_of_construction < 2016"
        ),
        on=merge_columns,
        how="outer",
    )
    after_2016 = bers.query("year_of_construction >= 2016")
    return pd.concat([before_2016, after_2016])
