from configparser import ConfigParser
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import icontract
import numpy as np
import pandas as pd
import streamlit as st

from ibsg import clean
from ibsg import CONFIG
from ibsg import io
from ibsg import _DATA_DIR


def main(
    bers: pd.DataFrame,
    selections: Dict[str, Any],
    config: ConfigParser = CONFIG,
    data_dir: Path = _DATA_DIR,
) -> Optional[pd.DataFrame]:
    raw_census = _load_census_buildings(
        url=config["urls"]["census_buildings_2016"],
        data_dir=data_dir,
        filesystem_name=config["filesystems"]["census_buildings_2016"],
    )
    filtered_census = _extract_rows_in_values(raw_census, selections["countyname"])
    if selections["replace_not_stated"]:
        with st.spinner("Replacing 'Not Stated' Period Built..."):
            census = replace_not_stated_period_built_with_mode(filtered_census)
    else:
        census = filtered_census
    with st.spinner("Filling the 2016 census building stock with BERs..."):
        census_standardised = _standardise_census(census, selections["ber_granularity"])
        bers_standardised = _standardise_bers(bers, selections["ber_granularity"])
        stock = _fill_census_with_bers(
            census=census_standardised,
            bers=bers_standardised,
            ber_granularity=selections["ber_granularity"],
        )
    return stock


@st.cache
def _load_census_buildings(
    url: str, data_dir: Path, filesystem_name: str
) -> pd.DataFrame:
    return io.load(
        read=pd.read_parquet,
        url=url,
        data_dir=data_dir,
        filesystem_name=filesystem_name,
    )


@icontract.ensure(lambda result: len(result) > 0)
def _extract_rows_in_values(df: pd.DataFrame, values: List[str]):
    where_rows_in_values = (
        df["countyname"].astype("string").str.lower().isin(map(str.lower, values))
    )
    return df[where_rows_in_values].copy()


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
