import numpy as np
import pandas as pd

from ibsg import clean


def _add_merge_columns_to_census(
    stock: pd.DataFrame, ber_granularity: str
) -> pd.DataFrame:
    on_columns = [ber_granularity, "period_built"]
    stock["id"] = clean.get_group_id(stock, columns=on_columns)
    return stock


def _setup_census_for_merging(
    census: pd.DataFrame, ber_granularity: str
) -> pd.DataFrame:
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


def _setup_bers_for_merging(bers: pd.DataFrame, ber_granularity: str) -> pd.DataFrame:
    bers[ber_granularity] = bers[ber_granularity].str.lower()
    return _add_merge_columns_to_bers(bers, ber_granularity)


def fill_census_with_bers(
    census: pd.DataFrame,
    bers: pd.DataFrame,
    ber_granularity: str,
) -> pd.DataFrame:
    mergeable_bers = _setup_bers_for_merging(bers, ber_granularity=ber_granularity)
    mergeable_census = _setup_census_for_merging(
        census, ber_granularity=ber_granularity
    )
    merge_columns = [ber_granularity, "period_built", "id"]

    # Small area linked postcodes are more reliable than user inputted ones
    # & if don't drop these end up with countyname_x and countyname_y columns on merge!
    if ber_granularity == "small_area":
        ber_columns_to_drop = ["countyname"]
    else:
        ber_columns_to_drop = []

    # dropping buildings in BERs but not in Census loses ~25k buildings
    before_2016 = pd.merge(
        left=mergeable_census,
        right=mergeable_bers.drop(columns=ber_columns_to_drop).query(
            "year_of_construction < 2016"
        ),
        on=merge_columns,
        how="left",
    )
    after_2016 = mergeable_bers.query("year_of_construction >= 2016")
    return pd.concat([before_2016, after_2016])
